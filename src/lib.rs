//! A library for calling Python code asynchronously from Rust.

use pyo3::{exceptions::PyKeyError, ffi::c_str, prelude::*, types::PyDict};
use std::{ffi::CString, thread};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

enum CmdType {
    RunCode(String),
    EvalCode(String),
    ReadVariable(String),
    CallFunction { name: String, args: Vec<Py<PyAny>> },
    Stop,
}
/// Represents a command to be sent to the Python execution thread.
/// It includes the Python code to run and a one-shot channel sender
/// to send the result back.
struct PyCommand {
    cmd_type: CmdType,
    responder: oneshot::Sender<PyResult<Py<PyAny>>>,
}

/// Custom error types for the Python executor.
#[derive(Error, Debug)]
pub enum PyRunnerError {
    #[error("Failed to send command to Python thread. The thread may have panicked.")]
    SendCommandFailed,

    #[error("Failed to receive result from Python thread. The thread may have panicked.")]
    ReceiveResultFailed,

    #[error("Python execution error: {0}")]
    PyError(#[from] PyErr),
}

/// Manages a dedicated thread for executing Python code asynchronously.
#[derive(Clone)]
pub struct PyRunner {
    sender: mpsc::Sender<PyCommand>,
}

impl PyRunner {
    /// Creates a new `PythonExecutor` and spawns a dedicated thread for Python execution.
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
                            let _ = cmd.responder.send(result.map(|_m| py.None()));
                        }
                        CmdType::EvalCode(code) => {
                            let c_code = CString::new(code).expect("CString::new failed");
                            let result = py.eval(&c_code, Some(&globals), None);
                            let _ = cmd.responder.send(result.map(|m| m.into()));
                        }
                        CmdType::ReadVariable(var_name) => {
                            let var_dot_split: Vec<&str> = var_name.split(".").collect();
                            let var = globals.get_item(var_dot_split[0]);
                            if let Ok(Some(var)) = var {
                                let res = if var_dot_split.len() > 2 {
                                    match var.getattr(var_dot_split.get(1).unwrap()) {
                                        Ok(v) => v.getattr(var_dot_split.get(2).unwrap()),
                                        Err(e) => Err(e),
                                    }
                                } else if var_dot_split.len() > 1 {
                                    var.getattr(var_dot_split.get(1).unwrap())
                                } else {
                                    Ok(var)
                                };
                                let _ = cmd.responder.send(res.map(|m| m.into()));
                            } else {
                                let _ = cmd.responder.send(Err(PyErr::new::<PyKeyError, _>(
                                    format!("Variable '{}' not found", var_name),
                                )
                                .into()));
                            };
                        }
                        CmdType::CallFunction { name, args } => {
                            dbg!(&globals);
                            let fn_dot_split: Vec<&str> = name.split(".").collect();
                            let app_res = globals.get_item(fn_dot_split[0]);
                            let Ok(Some(app)) = app_res else {
                                let _ = cmd.responder.send(Err(PyErr::new::<PyKeyError, _>(
                                    format!("Function '{}' not found", name),
                                )
                                .into()));
                                continue;
                            };
                            let app = if fn_dot_split.len() > 2 {
                                let first = app.getattr(fn_dot_split.get(1).unwrap());
                                if let Ok(first) = first {
                                    first.getattr(fn_dot_split.get(2).unwrap())
                                } else {
                                    let _ = cmd.responder.send(Err(PyErr::new::<PyKeyError, _>(
                                        format!("Function '{}' not found", name),
                                    )
                                    .into()));
                                    continue;
                                }
                            } else if fn_dot_split.len() > 1 {
                                app.getattr(fn_dot_split.get(1).unwrap())
                            } else {
                                Ok(app)
                            };

                            if let Ok(app) = app {
                                if app.is_callable() {
                                    let t_args = pyo3::types::PyTuple::new(py, args);
                                    if let Ok(t_args) = t_args {
                                        let res = app.call1(t_args);
                                        let _ = cmd.responder.send(res.map(|m| m.into()));
                                    }
                                    else {
                                        let _ = cmd.responder.send(Err(PyErr::new::<PyKeyError, _>(
                                    format!("Function parameters cannot be applied to {name}"),
                                )
                                .into()));
                                    }
                                    
                                } else {
                                    let _ = cmd.responder.send(Err(PyErr::new::<PyKeyError, _>(
                                        format!("'{}' not a callable function", name),
                                    )
                                    .into()));
                                }
                            } else {
                                let _ = cmd.responder.send(Err(PyErr::new::<PyKeyError, _>(
                                    format!("Function '{}' not found", name),
                                )
                                .into()));
                            }
                        },
                        CmdType::Stop => {
                            let _ = cmd.responder.send(Ok(py.None()));
                            break},
                    };
                }
            });
        });

        Self { sender }
    }

    /// Asynchronously executes a block of Python code.
    ///*   `code`: A string slice containing the Python code to execute.
    /// Returns a `Result` containing the Python module object on success,
    /// or a `PythonExecutorError` on failure.
    pub async fn run(&self, code: &str) -> Result<Py<PyAny>, PyRunnerError> {
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
    pub async fn eval(&self, code: &str) -> Result<Py<PyAny>, PyRunnerError> {
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

    pub async fn read_variable(&self, var_name: &str) -> Result<Py<PyAny>, PyRunnerError> {
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

    pub async fn call_function(
        &self,
        name: &str,
        args: Vec<Py<PyAny>>,
    ) -> Result<Py<PyAny>, PyRunnerError> {
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

    pub async fn stop(
        &self,
    ) -> Result<(), PyRunnerError> {
        let (responder, receiver) = oneshot::channel();
        let cmd_type = CmdType::Stop ;
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

impl Default for PyRunner {
    /// Creates a new `PythonExecutor` using `new()`.
    fn default() -> Self {
        Self::new()
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

        Python::attach(|py| {
            let z: i32 = result.extract(py).unwrap();
            assert_eq!(z, 30);
        });
    }

    #[tokio::test]
    async fn test_run_simple_code() {
        let executor = PyRunner::new();
        let code = r#"
x = 10
y = 20
z = x + y"#;

        let result_module = executor.run(code).await.unwrap();

        Python::attach(|py| {
            assert!(result_module.is_none(py));
        });

        let z_val = executor.read_variable("z").await.unwrap();

        Python::attach(|py| {
            let z: i32 = z_val.extract(py).unwrap();
            assert_eq!(z, 30);
        });
    }

    #[tokio::test]
    async fn test_run_with_function() {
        let executor = PyRunner::new();
        let code = r#"
def add(a, b):
    return a + b
"#;

        executor.run(code).await.unwrap();

        let args = Python::attach(|py| {
            vec![
                5i32.into_pyobject(py).unwrap().into(),
                9i32.into_pyobject(py).unwrap().into(),
            ]
        });

        let result = executor.call_function("add", args).await.unwrap();
        Python::attach(|py| {
            let _result: i32 = result.extract(py).unwrap();
        });
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

        Python::attach(|py| {
            let res1 = res1.unwrap();
            assert!(res1.is_none(py));

            let result1_str: String = result1.unwrap().extract(py).unwrap();
            assert_eq!(result1_str, "task1");

            let result2_str: String = result2.unwrap().extract(py).unwrap();
            assert_eq!(result2_str, "task2");

            let result3: String = res3.unwrap().extract(py).unwrap();
            assert_eq!(result3, "task3");
        });
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
}
