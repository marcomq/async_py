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
use std::ffi::CString;
use tokio::sync::{mpsc, oneshot};

/// The main loop for the Python thread. This function is spawned in a new
/// thread and is responsible for all Python interaction.
pub(crate) async fn python_thread_main(mut receiver: mpsc::Receiver<PyCommand>) {
    Python::initialize();
    let globals = Python::attach(|py| PyDict::new(py).unbind());
    while let Some(mut cmd) = receiver.recv().await {
        Python::attach(|py| {
            let globals = globals.bind(py);
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
                    let func = get_py_object(&globals, &name).unwrap(); // TODO;
                    check_func_callable(&func, &name).unwrap(); // TODO
                    let func = func.unbind();

                    py.detach(|| {
                        tokio::spawn(handle_call_async_function(func, args, cmd.responder))
                    });
                    return; // The response is sent async, so we can return early.
                }
                CmdType::Stop => return receiver.close(),
            };

            // Convert PyErr to a string representation to avoid exposing it outside this module.
            let response = match result {
                Ok(value) => Ok(value),
                Err(e) => Err(e.to_string()),
            };
            let _ = cmd.responder.send(response);
        });
        // After the loop, we can send a final confirmation for the Stop command if needed,
        // but the current implementation in lib.rs handles the channel closing.
    }
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
    check_func_callable(&func, &name)?;
    let t_args = vec_to_py_tuple(&py, args)?;
    let result = func.call1(t_args)?;
    py_any_to_json(py, &result)
}

fn vec_to_py_tuple<'py>(
    py: &Python<'py>,
    args: Vec<Value>,
) -> PyResult<Bound<'py, pyo3::types::PyTuple>> {
    let py_args = args
        .into_iter()
        .map(|v| json_value_to_pyobject(*py, v))
        .collect::<PyResult<Vec<_>>>()?;
    pyo3::types::PyTuple::new(*py, py_args)
}

/// Handles the `CallAsyncFunction` command.
async fn handle_call_async_function(
    func: Py<PyAny>,
    args: Vec<Value>,
    responder: oneshot::Sender<Result<Value, String>>,
) {
    let result = Python::attach(|py| {
        let func = func.bind(py);
        let t_args = vec_to_py_tuple(&py, args)?;
        let coroutine = func.call1(t_args)?;

        let asyncio = py.import("asyncio")?;
        let loop_obj = asyncio.call_method0("new_event_loop")?;
        asyncio.call_method1("set_event_loop", (loop_obj.clone(),))?;
        let result = loop_obj.call_method1("run_until_complete", (coroutine,))?;
        loop_obj.call_method0("close")?;

        py_any_to_json(py, &result)
    });
    let _ = responder.send(result.map_err(|e| e.to_string()));
}

/// Recursively converts a Python object to a `serde_json::Value`.
fn py_any_to_json(py: Python, obj: &pyo3::Bound<'_, PyAny>) -> PyResult<Value> {
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
