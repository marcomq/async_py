//  Async Python
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/async_py

use crate::{print_path_for_python, CmdType, PyCommand};
use pyo3::{
    exceptions::PyKeyError,
    prelude::*,
    types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString},
    IntoPyObjectExt,
};
use serde_json::Value;
use std::{ffi::CString, future::Future};
use tokio::{
    runtime::Runtime,
    sync::{mpsc, oneshot},
};

/// The main loop for the Python thread. This function is spawned in a new
/// thread and is responsible for all Python interaction.
pub(crate) fn python_thread_main(mut receiver: mpsc::Receiver<PyCommand>) {
    Python::initialize();
    Python::attach(|py| {
        // Setup and run the asyncio event loop for the current thread.
        let asyncio = py.import("asyncio").expect("Failed to import asyncio");
        let event_loop = asyncio
            .call_method0("new_event_loop")
            .expect("Failed to create new event loop");
        asyncio
            .call_method1("set_event_loop", (event_loop.clone(),))
            .expect("Failed to set event loop");
        let locals = pyo3_async_runtimes::TaskLocals::new(event_loop.clone())
            .copy_context(py)
            .unwrap();
        let globals = PyDict::new(py);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime for async Python execution");
        let rt_future = rt.spawn(async {});
        while let Some(mut cmd) = py.detach(|| receiver.blocking_recv()) {
            let result = match std::mem::replace(&mut cmd.cmd_type, CmdType::Stop) {
                CmdType::RunCode(code) => {
                    let c_code = CString::new(code).expect("CString::new failed");
                    py.run(&c_code, Some(&globals), None).map(|_| Value::Null)
                }
                CmdType::EvalCode(code) => {
                    let c_code = CString::new(code).expect("CString::new failed");
                    py.eval(&c_code, Some(&globals), None)
                        .and_then(|obj| py_any_to_json(py, &obj))
                }
                CmdType::RunFile(file) => handle_run_file(py, &globals, file),
                CmdType::ReadVariable(var_name) => {
                    get_py_object(&globals, &var_name).and_then(|obj| py_any_to_json(py, &obj))
                }
                CmdType::CallFunction { name, args } => {
                    handle_call_function(py, &globals, name, args)
                }
                CmdType::CallAsyncFunction { name, args } => {
                    let locals = locals.clone_ref(py);
                    handle_call_async_function(
                        py,
                        &globals,
                        name,
                        args,
                        cmd.responder,
                        &rt,
                        locals,
                    );
                    continue;
                }
                CmdType::Stop => break,
            };

            // Convert PyErr to a string representation to avoid exposing it outside this module.
            let response = match result {
                Ok(value) => Ok(value),
                Err(e) => Err(e.to_string()),
            };
            let _ = cmd.responder.send(response);
        }
        rt_future.abort(); // Cleanly stop the asyncio event loop before the thread exits.
        event_loop
            .call_method0("stop")
            .expect("Failed to stop event loop");
        // After the loop, we can send a final confirmation for the Stop command if needed,
        // but the current implementation in lib.rs handles the channel closing.
    });
}

/// Resolves a potentially dot-separated Python object name from the globals dictionary.
fn get_py_object<'py>(
    globals: &pyo3::Bound<'py, PyDict>,
    name: &str,
) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
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

fn handle_run_file(
    py: Python,
    globals: &pyo3::Bound<'_, PyDict>,
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
    py.run(&c_code, Some(&globals), None).map(|_| Value::Null)
}

/// Handles the `CallFunction` command.
fn handle_call_function(
    py: Python,
    globals: &pyo3::Bound<'_, PyDict>,
    name: String,
    args: Vec<Value>,
) -> PyResult<Value> {
    let func = get_py_object(globals, &name)?;

    if !func.is_callable() {
        return Err(PyErr::new::<PyKeyError, _>(format!(
            "'{}' is not a callable function",
            name
        )));
    }

    let py_args = args
        .into_iter()
        .map(|v| json_value_to_pyobject(py, v))
        .collect::<PyResult<Vec<_>>>()?;
    let t_args = pyo3::types::PyTuple::new(py, py_args)?;
    let result = func.call1(t_args)?;
    py_any_to_json(py, &result)
}

fn handle_call_async_pre_await(
    py: &Python,
    globals: &pyo3::Bound<'_, PyDict>,
    name: String,
    args: Vec<Value>,
    locals: pyo3_async_runtimes::TaskLocals,
) -> PyResult<impl Future<Output = PyResult<Py<PyAny>>> + Send> {
    let func = get_py_object(globals, &name)?;

    if !func.is_callable() {
        return Err(PyErr::new::<PyKeyError, _>(format!(
            "'{}' is not a callable function",
            name
        )));
    }

    let py_args = args
        .into_iter()
        .map(|v| json_value_to_pyobject(*py, v))
        .collect::<PyResult<Vec<_>>>()?;
    let t_args = pyo3::types::PyTuple::new(*py, py_args)?;
    let result = func.call1(t_args)?;
    pyo3_async_runtimes::into_future_with_locals(&locals, result)
}

/// Handles the `CallAsyncFunction` command.
fn handle_call_async_function(
    py: Python,
    globals: &pyo3::Bound<PyDict>,
    name: String,
    args: Vec<Value>,
    responder: oneshot::Sender<Result<Value, String>>,
    rt: &Runtime,
    locals: pyo3_async_runtimes::TaskLocals,
) {
    let result_future = match handle_call_async_pre_await(&py, globals, name, args, locals) {
        Ok(fut) => fut,
        Err(e) => {
            let _ = responder.send(Err(e.to_string()));
            return;
        }
    };
    rt.spawn(async move {
        let result = result_future
            .await
            .map_err(|e| e.to_string())
            .and_then(|py_res| {
                Python::attach(|py| {
                    py_any_to_json(py, &py_res.into_bound(py)).map_err(|e| e.to_string())
                })
            });
        let _ = responder.send(result);
    });
}

/// Recursively converts a Python object to a `serde_json::Value`.
fn py_any_to_json(py: Python, obj: &pyo3::Bound<'_, pyo3::PyAny>) -> PyResult<Value> {
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
        let items: PyResult<Vec<Value>> =
            list.iter().map(|item| py_any_to_json(py, &item)).collect();
        return Ok(Value::Array(items?));
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (key, value) in dict.iter() {
            map.insert(key.to_string(), py_any_to_json(py, &value)?);
        }
        return Ok(Value::Object(map));
    }

    // Fallback for other types: convert to string representation
    Ok(Value::String(obj.to_string()))
}

/// Converts a serde_json::Value to a Python object.
fn json_value_to_pyobject(py: Python, value: Value) -> PyResult<pyo3::Py<pyo3::PyAny>> {
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
