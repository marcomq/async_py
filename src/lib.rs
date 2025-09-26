//! A library for calling Python code asynchronously from Rust.

use pyo3::{
    exceptions::PyKeyError,
    ffi::c_str,
    prelude::*,
    types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString},
    IntoPyObjectExt,
};
use serde_json::Value;
use std::{ffi::CString, thread};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

enum CmdType {
    RunCode(String),
    EvalCode(String),
    ReadVariable(String),
    CallFunction { name: String, args: Vec<Value> },
    Stop,
}
/// Represents a command to be sent to the Python execution thread. It includes the
/// command to execute and a one-shot channel sender to send the `serde_json::Value`
/// result back.
struct PyCommand {
    cmd_type: CmdType,
    responder: oneshot::Sender<PyResult<Value>>,
}

/// Custom error types for the `PyRunner`.
#[derive(Error, Debug)]
pub enum PyRunnerError {
    #[error("Failed to send command to Python thread. The thread may have panicked.")]
    SendCommandFailed,

    #[error("Failed to receive result from Python thread. The thread may have panicked.")]
    ReceiveResultFailed,

    #[error("Python execution error: {0}")]
    PyError(#[from] PyErr),
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

    let py_args = args.into_iter().map(|v| json_value_to_pyobject(py, v)).collect::<PyResult<Vec<_>>>()?;
    let t_args = pyo3::types::PyTuple::new(py, py_args)?;
    let result = func.call1(t_args)?;
    py_any_to_json(py, &result)
}
/// Manages a dedicated thread for executing Python code asynchronously.
#[derive(Clone)]
pub struct PyRunner {
    sender: mpsc::Sender<PyCommand>,
}

impl PyRunner {
    /// Creates a new `PyRunner` and spawns a dedicated thread for Python execution.
    ///
    /// This thread initializes the Python interpreter and waits for commands.
    ///
    /// # Panics
    ///
    /// This function may panic if `Python::initialize()` fails,
    /// which can happen if Python is already initialized in an incompatible way.
    pub fn new() -> Self {
        // Create a multi-producer, single-consumer channel for sending commands.
        let (sender, mut receiver) = mpsc::channel::<PyCommand>(32);

        // Spawn a new OS thread to handle all Python-related work.
        // This is crucial to avoid blocking the async runtime and to manage the GIL correctly.
        thread::spawn(move || {
            // Prepare the Python interpreter for use in a multi-threaded context.
            // This must be called before any other pyo3 functions.
            Python::initialize();
            // Acquire the GIL and enter a Python context.
            // The `py.allow_threads` call releases the GIL when we are waiting for commands,
            // allowing other Python threads (if any) to run.
            Python::attach(|py| {
                // Loop indefinitely, waiting for commands from the channel.
                let globals = PyDict::new(py);
                while let Some(cmd) = py.detach(|| receiver.blocking_recv()) {
                    match cmd.cmd_type {
                        CmdType::RunCode(code) => {
                            let c_code = CString::new(code).expect("CString::new failed");
                            let result = py.run(&c_code, Some(&globals), None);
                            let _ = cmd.responder.send(result.map(|_| Value::Null));
                        }
                        CmdType::EvalCode(code) => {
                            let c_code = CString::new(code).expect("CString::new failed");
                            let result = py.eval(&c_code, Some(&globals), None);
                            let _ = cmd
                                .responder
                                .send(result.and_then(|obj| py_any_to_json(py, &obj)));
                        }
                        CmdType::ReadVariable(var_name) => {
                            let result = get_py_object(&globals, &var_name)
                                .and_then(|obj| py_any_to_json(py, &obj));
                            let _ = cmd.responder.send(result);
                        }
                        CmdType::CallFunction { name, args } => {
                            let result = handle_call_function(py, &globals, name, args);
                            let _ = cmd.responder.send(result);
                        }
                        CmdType::Stop => {
                            let _ = cmd.responder.send(Ok(Value::Null));
                            break;
                        }
                    };
                }
            });
        });

        Self { sender }
    }

    /// Asynchronously executes a block of Python code.
    ///
    /// * `code`: A string slice containing the Python code to execute.
    /// Returns `Ok(Value::Null)` on success, or a `PyRunnerError` on failure.
    /// This is equivalent to Python's `exec()` function.
    pub async fn run(&self, code: &str) -> Result<Value, PyRunnerError> {
        // Create a one-shot channel to receive the result from the Python thread.
        let (responder, receiver) = oneshot::channel();
        let cmd_type = CmdType::RunCode(code.into());
        let cmd = PyCommand {
            cmd_type,
            responder,
        };

        // Send the command to the Python thread.
        self.sender
            .send(cmd)
            .await
            .map_err(|_| PyRunnerError::SendCommandFailed)?;

        // Await the result from the Python thread.
        let result = receiver
            .await
            .map_err(|_| PyRunnerError::ReceiveResultFailed)??;

        Ok(result)
    }

    /// Asynchronously evaluates a single Python expression.
    ///
    /// * `code`: A string slice containing the Python expression to evaluate.
    ///           Must not contain definitions or multiple lines.
    /// Returns a `Result` containing the expression's result as a `serde_json::Value` on success,
    /// or a `PyRunnerError` on failure. This is equivalent to Python's `eval()` function.
    pub async fn eval(&self, code: &str) -> Result<Value, PyRunnerError> {
        // Create a one-shot channel to receive the result from the Python thread.
        let (responder, receiver) = oneshot::channel();
        let cmd_type = CmdType::EvalCode(code.into());
        let cmd = PyCommand {
            cmd_type,
            responder,
        };

        // Send the command to the Python thread.
        self.sender
            .send(cmd)
            .await
            .map_err(|_| PyRunnerError::SendCommandFailed)?;

        // Await the result from the Python thread.
        let result = receiver
            .await
            .map_err(|_| PyRunnerError::ReceiveResultFailed)??;

        Ok(result)
    }

    /// Asynchronously reads a variable from the Python interpreter's global scope.
    ///
    /// * `var_name`: The name of the variable to read. It can be a dot-separated path
    ///   to access attributes of objects (e.g., "my_module.my_variable").
    /// Returns the variable's value as a `serde_json::Value` on success.
    pub async fn read_variable(&self, var_name: &str) -> Result<Value, PyRunnerError> {
        let (responder, receiver) = oneshot::channel();
        let cmd_type = CmdType::ReadVariable(var_name.into());
        let cmd = PyCommand {
            cmd_type,
            responder,
        };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| PyRunnerError::SendCommandFailed)?;

        let result = receiver
            .await
            .map_err(|_| PyRunnerError::ReceiveResultFailed)??;

        Ok(result)
    }

    /// Asynchronously calls a Python function in the interpreter's global scope.
    ///
    /// * `name`: The name of the function to call. It can be a dot-separated path
    ///   to access functions within modules (e.g., "my_module.my_function").
    /// * `args`: A vector of `serde_json::Value` to pass as arguments to the function.
    /// Returns the function's return value as a `serde_json::Value` on success.
    pub async fn call_function(
        &self,
        name: &str,
        args: Vec<Value>,
    ) -> Result<Value, PyRunnerError> {
        let (responder, receiver) = oneshot::channel();
        let cmd_type = CmdType::CallFunction {
            name: name.into(),
            args,
        };
        let cmd = PyCommand {
            cmd_type,
            responder,
        };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| PyRunnerError::SendCommandFailed)?;

        let result = receiver
            .await
            .map_err(|_| PyRunnerError::ReceiveResultFailed)??;

        Ok(result)
    }

    /// Stops the Python execution thread gracefully.
    pub async fn stop(&self) -> Result<(), PyRunnerError> {
        let (responder, receiver) = oneshot::channel();
        let cmd_type = CmdType::Stop;
        let cmd = PyCommand {
            cmd_type,
            responder,
        };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| PyRunnerError::SendCommandFailed)?;

        let _wait = receiver
            .await
            .map_err(|_| PyRunnerError::ReceiveResultFailed)??;
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_eval_simple_code() {
        let executor = PyRunner::new();

        // `eval` is for single expressions
        let code = "10 + 20";

        let result = executor.eval(code).await.unwrap();

        assert_eq!(result, Value::Number(30.into()));
    }

    #[tokio::test]
    async fn test_run_simple_code() {
        let executor = PyRunner::new();
        let code = r#"
x = 10
y = 20
z = x + y"#;

        let result_module = executor.run(code).await.unwrap();

        assert_eq!(result_module, Value::Null);

        let z_val = executor.read_variable("z").await.unwrap();

        assert_eq!(z_val, Value::Number(30.into()));
    }

    #[tokio::test]
    async fn test_run_with_function() {
        let executor = PyRunner::new();
        let code = r#"
def add(a, b):
    return a + b
"#;

        executor.run(code).await.unwrap();
        let result = executor
            .call_function("add", vec![5.into(), 9.into()])
            .await
            .unwrap();
        assert_eq!(result, Value::Number(14.into()));
    }

    #[tokio::test]
    async fn test_concurrent_calls() {
        let executor = PyRunner::new();

        let task1 = executor.run("import time; time.sleep(0.3); result='task1'");
        let result1 = executor.read_variable("result");
        let task2 = executor.run("result='task2'");
        let result2 = executor.read_variable("result");
        let task3 = executor.eval("'task3'");

        let (res1, result1, res2, result2, res3) =
            tokio::join!(task1, result1, task2, result2, task3);
        assert!(res1.is_ok());
        assert!(result1.is_ok());
        assert!(res2.is_ok());
        assert!(result2.is_ok());
        assert!(res3.is_ok());

        assert_eq!(res1.unwrap(), Value::Null);
        assert_eq!(result1.unwrap(), Value::String("task1".to_string()));
        assert_eq!(result2.unwrap(), Value::String("task2".to_string()));
        assert_eq!(res3.unwrap(), Value::String("task3".to_string()));
    }

    #[tokio::test]
    async fn test_python_error() {
        let executor = PyRunner::new();
        let code = "1 / 0";
        // eval will also work here
        let result = executor.eval(code).await;
        assert!(result.is_err());

        match result {
            Err(PyRunnerError::PyError(py_err)) => {
                Python::attach(|py| {
                    assert!(py_err.is_instance_of::<pyo3::exceptions::PyZeroDivisionError>(py));
                });
            }
            _ => panic!("Expected a PyError"),
        }
    }
    #[tokio::test]
    async fn test_sample_readme() {
        let runner = PyRunner::new();

        let code = r#"
counter = 0
def greet(name):
    global counter
    counter = counter + 1
    s = "" if counter < 2 else "s"
    return f"Hello {name}! Called {counter} time{s} from Python."
"#;
        // Python function "greet" and variable "counter" is added to globals
        runner.run(code).await.unwrap();

        // Calling code
        let result1 = runner
            .call_function("greet", vec!["World".into()])
            .await
            .unwrap();
        println!("{}", result1.as_str().unwrap()); // Prints: Hello World! Called 1 time from Python.
        assert_eq!(
            result1.as_str().unwrap(),
            "Hello World! Called 1 time from Python."
        );

        let result2 = runner
            .call_function("greet", vec!["World".into()])
            .await
            .unwrap();
        assert_eq!(
            result2.as_str().unwrap(),
            "Hello World! Called 2 times from Python."
        );
    }
}
