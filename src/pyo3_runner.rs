//  Async Python
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/async_py
use crate::{print_path_for_python, CmdType, PyCommand};
use pyo3::{
    exceptions::PyKeyError,
    prelude::*,
    types::{PyBool, PyCFunction, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple},
    IntoPyObjectExt,
};
use serde_json::Value;
use std::collections::HashMap;
use std::ffi::CString;
use tokio::sync::{mpsc, oneshot, Notify};

/// Holds the Python objects related to the async infrastructure.
struct AsyncPyState {
    loop_obj: Py<PyAny>,
    result_queue: Py<PyAny>,
    make_callback_fn: Py<PyAny>,
}

/// The main loop for the Python thread. This function is spawned in a new
/// thread and is responsible for all Python interaction.
pub(crate) async fn python_thread_main(mut receiver: mpsc::Receiver<PyCommand>) {
    Python::initialize();

    // State for async operations
    let mut pending: HashMap<usize, oneshot::Sender<Result<Value, String>>> = HashMap::new();
    let mut next_id: usize = 1;
    let mut async_state: Option<AsyncPyState> = None;

    // Notifier to wake up the Rust select! loop from Python.
    let notify = std::sync::Arc::new(Notify::new());

    // Create globals and inject the notifier callback.
    let globals = Python::attach(|py| -> PyResult<Py<PyDict>> {
        let globals = PyDict::new(py);
        let rust_notify_fn = {
            let notify = notify.clone();
            PyCFunction::new_closure(py, None, None, move |py, _| {
                notify.notify_one();
                Ok::<pyo3::Py<pyo3::PyAny>, PyErr>(py.py().None())
            })?
            .unbind()
        };
        globals.set_item("_rust_notify", rust_notify_fn)?;
        Ok(globals.unbind())
    })
    .expect("Failed to initialize Python globals");

    loop {
        tokio::select! {
            // Branch 1: Wait for a new command from the channel.
            Some(cmd) = receiver.recv() => {
                if let CmdType::Stop = cmd.cmd_type {
                    receiver.close();
                } else {
                    handle_command(&globals, &mut async_state, &mut pending, &mut next_id, cmd);
                }
            },
            // Branch 2: Wait for a notification from Python that a result is ready.
            _ = notify.notified(), if !pending.is_empty() && async_state.is_some() => {
                handle_notification(async_state.as_ref().unwrap(), &mut pending);
            }
            // If the receiver is closed and there are no more pending tasks, exit.
            else => {
                if receiver.is_closed() && pending.is_empty() {
                    break;
                }
            }
        }
    }
}

/// Sets up the asyncio event loop and related infrastructure in Python.
fn setup_async_infrastructure(py: Python, globals: &Bound<PyDict>) -> PyResult<AsyncPyState> {
    let code = r#"
import asyncio, threading, queue, traceback
_loop = asyncio.new_event_loop()
def _run_loop():
    asyncio.set_event_loop(_loop)
    _loop.run_forever()
_thread = threading.Thread(target=_run_loop, daemon=True)
_thread.start()
_result_queue = queue.Queue()
def _async_done_cb(fut, id):
    try:
        res = fut.result()
        _result_queue.put({'id': id, 'ok': True, 'payload': res})
    except Exception as e:
        tb = traceback.format_exception_only(type(e), e)
        _result_queue.put({'id': id, 'ok': False, 'payload': ''.join(tb)})
    finally:
        _rust_notify()
def _make_callback(id):
    def _cb(fut):
        _async_done_cb(fut, id)
    return _cb
"#;
    let c_code = CString::new(code).expect("CString::new failed");
    py.run(&c_code, Some(globals), None)?;
    Ok(AsyncPyState {
        loop_obj: globals.get_item("_loop")?.unwrap().unbind(),
        result_queue: globals.get_item("_result_queue")?.unwrap().unbind(),
        make_callback_fn: globals.get_item("_make_callback")?.unwrap().unbind(),
    })
}

/// Processes a command received from the Rust side.
fn handle_command(
    globals: &Py<PyDict>,
    async_state: &mut Option<AsyncPyState>,
    pending: &mut HashMap<usize, oneshot::Sender<Result<Value, String>>>,
    next_id: &mut usize,
    mut cmd: PyCommand,
) {
    Python::attach(|py| {
        let globals = globals.bind(py);
        let result = match std::mem::replace(&mut cmd.cmd_type, CmdType::Stop) {
            CmdType::RunCode(code) => {
                let c_code = CString::new(code).expect("CString::new failed");
                py.run(&c_code, Some(globals), None).map(|_| Value::Null)
            }
            CmdType::EvalCode(code) => {
                let c_code = CString::new(code).expect("CString::new failed");
                py.eval(&c_code, Some(globals), None)
                    .and_then(|obj| py_any_to_json(&obj))
            }
            CmdType::RunFile(file) => handle_run_file(py, globals, file),
            CmdType::ReadVariable(var_name) => {
                get_py_object(globals, &var_name).and_then(|obj| py_any_to_json(&obj))
            }
            CmdType::CallFunction { name, args } => handle_call_function(py, globals, name, args),
            CmdType::CallAsyncFunction { name, args } => {
                let id = *next_id;
                *next_id += 1;

                // Initialize async infrastructure on first use.
                if async_state.is_none() {
                    match setup_async_infrastructure(py, globals) {
                        Ok(state) => *async_state = Some(state),
                        Err(e) => {
                            let _ = cmd.responder.send(Err(e.to_string()));
                            return;
                        }
                    }
                }
                let state = async_state.as_ref().unwrap();
                pending.insert(id, cmd.responder);

                if let Err(e) = handle_call_async_function(py, globals, state, id, &name, args) {
                    if let Some(tx) = pending.remove(&id) {
                        let _ = tx.send(Err(e));
                    }
                }
                return; // Response is sent async, so we return early.
            }
            CmdType::Stop => return, // Handled in the select! loop.
        };

        let response = result.map_err(|e| e.to_string());
        let _ = cmd.responder.send(response);
    });
}

/// Resolves a potentially dot-separated Python object name from the globals dictionary.
fn get_py_object<'py>(
    globals: &pyo3::Bound<'py, PyDict>,
    name: &str,
) -> PyResult<pyo3::Bound<'py, PyAny>> {
    let mut parts = name.split('.');
    let first_part = parts.next().unwrap(); // split always yields at least one item

    let mut obj = globals
        .get_item(first_part)?
        .ok_or_else(|| PyErr::new::<PyKeyError, _>(format!("'{}' not found", first_part)))?;

    for part in parts {
        obj = obj.getattr(part)?;
    }

    Ok(obj)
}

fn check_func_callable(func: &Bound<PyAny>, name: &str) -> PyResult<()> {
    if !func.is_callable() {
        Err(PyErr::new::<PyKeyError, _>(format!(
            "'{}' is not a callable function",
            name
        )))
    } else {
        Ok(())
    }
}

fn handle_run_file(
    py: Python,
    globals: &Bound<'_, PyDict>,
    file: std::path::PathBuf,
) -> PyResult<Value> {
    let code = format!(
        r#"
import sys
sys.path.insert(0, {})
with open({}, 'r') as f:
    exec(f.read())
"#,
        print_path_for_python(&file.parent().unwrap().to_path_buf()),
        print_path_for_python(&file.to_path_buf())
    );
    let c_code = CString::new(code).expect("CString::new failed");
    py.run(&c_code, Some(globals), None).map(|_| Value::Null)
}

/// Handles the `CallFunction` command.
fn handle_call_function(
    py: Python,
    globals: &Bound<'_, PyDict>,
    name: String,
    args: Vec<Value>,
) -> PyResult<Value> {
    let func = get_py_object(globals, &name)?;
    check_func_callable(&func, &name)?;
    let t_args = vec_to_py_tuple(&py, args)?;
    let result = func.call1(t_args)?;
    py_any_to_json(&result)
}

fn vec_to_py_tuple<'py>(
    py: &Python<'py>,
    args: Vec<Value>,
) -> PyResult<Bound<'py, pyo3::types::PyTuple>> {
    let py_args = args
        .into_iter()
        .map(|v| json_value_to_pyobject(*py, v))
        .collect::<PyResult<Vec<_>>>()?;
    PyTuple::new(*py, py_args)
}

/// Handles the `CallAsyncFunction` command.
fn handle_call_async_function(
    py: Python,
    globals: &Bound<PyDict>,
    async_state: &AsyncPyState,
    id: usize,
    name: &str,
    args: Vec<Value>,
) -> Result<(), String> {
    let func = get_py_object(globals, name)
        .and_then(|f| check_func_callable(&f, name).map(|_| f))
        .map_err(|e| e.to_string())?;

    let t_args = vec_to_py_tuple(&py, args).map_err(|e| e.to_string())?;
    let coroutine = func.call1(t_args).map_err(|e| e.to_string())?;

    let asyncio = py.import("asyncio").map_err(|e| e.to_string())?;
    let run_threadsafe = asyncio
        .getattr("run_coroutine_threadsafe")
        .map_err(|e| e.to_string())?;

    let fut = run_threadsafe
        .call1((coroutine, async_state.loop_obj.bind(py)))
        .map_err(|e| e.to_string())?;

    let cb = async_state
        .make_callback_fn
        .bind(py)
        .call1((id,))
        .map_err(|e| e.to_string())?;

    fut.call_method1("add_done_callback", (cb,))
        .map_err(|e| e.to_string())?;

    Ok(())
}

/// Drains the Python-side result queue and completes any pending responders.
fn handle_notification(
    async_state: &AsyncPyState,
    pending: &mut HashMap<usize, oneshot::Sender<Result<Value, String>>>,
) {
    Python::attach(|py| {
        let get_nowait = match async_state.result_queue.bind(py).getattr("get_nowait") {
            Ok(f) => f,
            Err(_) => return, // Should not happen if setup is correct
        };
        while let Ok(item) = get_nowait.call0() {
            if let Ok(dict) = item.downcast::<PyDict>() {
                let id = dict
                    .get_item("id")
                    .unwrap()
                    .unwrap()
                    .extract::<usize>()
                    .unwrap();
                if let Some(tx) = pending.remove(&id) {
                    let ok = dict
                        .get_item("ok")
                        .unwrap()
                        .unwrap()
                        .extract::<bool>()
                        .unwrap();
                    let payload = dict.get_item("payload").unwrap().unwrap();
                    let res = if ok {
                        py_any_to_json(&payload).map_err(|e| e.to_string())
                    } else {
                        Err(payload.to_string())
                    };
                    let _ = tx.send(res);
                }
            }
        }
    });
}

/// Recursively converts a Python object to a `serde_json::Value`.
fn py_any_to_json(obj: &pyo3::Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(b) = obj.cast::<PyBool>() {
        return Ok(Value::Bool(b.is_true()));
    }
    if let Ok(i) = obj.cast::<PyInt>() {
        return Ok(Value::Number(i.extract::<i64>()?.into()));
    }
    if let Ok(f) = obj.cast::<PyFloat>() {
        // serde_json::Number does not support infinity or NaN
        let val = f.value();
        if !val.is_finite() {
            return Ok(Value::Null);
        }
        return Ok(Value::Number(
            serde_json::Number::from_f64(val).unwrap_or_else(|| serde_json::Number::from(0)),
        ));
    }
    if let Ok(s) = obj.cast::<PyString>() {
        return Ok(Value::String(s.to_string()));
    }
    if let Ok(list) = obj.cast::<PyList>() {
        let items: PyResult<Vec<Value>> = list.iter().map(|item| py_any_to_json(&item)).collect();
        return Ok(Value::Array(items?));
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (key, value) in dict.iter() {
            map.insert(key.to_string(), py_any_to_json(&value)?);
        }
        return Ok(Value::Object(map));
    }

    // Fallback for other types: convert to string representation
    Ok(Value::String(obj.to_string()))
}

/// Converts a serde_json::Value to a Python object.
fn json_value_to_pyobject(py: Python, value: Value) -> PyResult<pyo3::Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => b.into_py_any(py),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into_py_any(py)
            } else if let Some(u) = n.as_u64() {
                u.into_py_any(py)
            } else if let Some(f) = n.as_f64() {
                f.into_py_any(py)
            } else {
                Ok(py.None())
            }
        }
        Value::String(s) => s.into_py_any(py),
        Value::Array(arr) => {
            let py_list = pyo3::types::PyList::empty(py);
            for v in arr {
                py_list.append(json_value_to_pyobject(py, v)?)?;
            }
            py_list.into_py_any(py)
        }
        Value::Object(obj) => {
            let py_dict = pyo3::types::PyDict::new(py);
            for (k, v) in obj {
                py_dict.set_item(k, json_value_to_pyobject(py, v)?)?;
            }
            py_dict.into_py_any(py)
        }
    }
}
