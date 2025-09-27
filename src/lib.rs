//  Async Python
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/tauri-plugin-python

//! A library for calling Python code asynchronously from Rust.

use pyo3::{
    exceptions::PyKeyError,
    prelude::*,
    types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString},
    IntoPyObjectExt,
};
use serde_json::Value;
use std::{
    ffi::CString,
    path::{Path, PathBuf},
    thread,
};
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

    let py_args = args
        .into_iter()
        .map(|v| json_value_to_pyobject(py, v))
        .collect::<PyResult<Vec<_>>>()?;
    let t_args = pyo3::types::PyTuple::new(py, py_args)?;
    let result = func.call1(t_args)?;
    py_any_to_json(py, &result)
}

fn cleanup_path_for_python(path: &PathBuf) -> String {
    dunce::canonicalize(path)
        .unwrap()
        .to_string_lossy()
        .replace("\\", "/")
}

fn print_path_for_python(path: &PathBuf) -> String {
    #[cfg(not(target_os = "windows"))]
    {
        format!("\"{}\"", cleanup_path_for_python(path))
    }
    #[cfg(target_os = "windows")]
    {
        format!("r\"{}\"", cleanup_path_for_python(path))
    }
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

    /// A private helper function to encapsulate the logic of sending a command
    /// and receiving a response.
    async fn send_command(&self, cmd_type: CmdType) -> Result<Value, PyRunnerError> {
        // Create a one-shot channel to receive the result from the Python thread.
        let (responder, receiver) = oneshot::channel();
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

    /// Asynchronously executes a block of Python code.
    ///
    /// * `code`: A string slice containing the Python code to execute.
    /// This is equivalent to Python's `exec()` function.
    pub async fn run(&self, code: &str) -> Result<(), PyRunnerError> {
        self.send_command(CmdType::RunCode(code.into()))
            .await
            .map(|_| ())
    }
    /// Asynchronously runs a python file.
    /// * `file`: Absolute path to a python file to execute.
    /// Also loads the path of the file to sys.path for imports.
    pub async fn run_file(&self, file: &Path) -> Result<(), PyRunnerError> {
        let file_path = cleanup_path_for_python(&file.to_path_buf());
        let folder_path = cleanup_path_for_python(&file.parent().unwrap().to_path_buf());
        let code = format!(
            r#"
import sys
sys.path.insert(0, {})
with open({}, 'r') as f:
    exec(f.read())
"#,
            print_path_for_python(&folder_path.into()),
            print_path_for_python(&file_path.into())
        );
        self.run(&code).await
    }

    /// Asynchronously evaluates a single Python expression.
    ///
    /// * `code`: A string slice containing the Python expression to evaluate.
    ///           Must not contain definitions or multiple lines.
    /// Returns a `Result` containing the expression's result as a `serde_json::Value` on success,
    /// or a `PyRunnerError` on failure. This is equivalent to Python's `eval()` function.
    pub async fn eval(&self, code: &str) -> Result<Value, PyRunnerError> {
        self.send_command(CmdType::EvalCode(code.into())).await
    }

    /// Asynchronously reads a variable from the Python interpreter's global scope.
    ///
    /// * `var_name`: The name of the variable to read. It can be a dot-separated path
    ///   to access attributes of objects (e.g., "my_module.my_variable").
    /// Returns the variable's value as a `serde_json::Value` on success.
    pub async fn read_variable(&self, var_name: &str) -> Result<Value, PyRunnerError> {
        self.send_command(CmdType::ReadVariable(var_name.into()))
            .await
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
        self.send_command(CmdType::CallFunction {
            name: name.into(),
            args,
        })
        .await
    }

    /// Stops the Python execution thread gracefully.
    pub async fn stop(&self) -> Result<(), PyRunnerError> {
        // We can ignore the `Ok(Value::Null)` result.
        self.send_command(CmdType::Stop).await?;
        Ok(())
    }
    /// Set python venv environment folder (does not change interpreter)
    pub async fn set_venv(&self, venv_path: &Path) -> Result<(), PyRunnerError> {
        let set_venv_code = r##"
import sys
import os

def add_venv_libs_to_syspath(venv_path):
    """
    Adds the site-packages folder (and .pth entries) from a virtual environment to sys.path.
    
    Args:
        venv_path (str): Path to the root of the virtual environment.
    """
    if not os.path.isdir(venv_path):
        raise ValueError(f"{venv_path} is not a directory")

    if os.name == "nt":
        # Windows: venv\Lib\site-packages
        site_packages = os.path.join(venv_path, "Lib", "site-packages")
    else:
        # POSIX: venv/lib/pythonX.Y/site-packages
        py_version = f"python{sys.version_info.major}.{sys.version_info.minor}"
        site_packages = os.path.join(venv_path, "lib", py_version, "site-packages")

    if not os.path.isdir(site_packages):
        raise RuntimeError(f"Could not find site-packages in {venv_path}")

    # Add site-packages itself
    if site_packages not in sys.path:
        sys.path.insert(0, site_packages)

    # Process .pth files inside site-packages
    for entry in os.listdir(site_packages):
        if entry.endswith(".pth"):
            pth_file = os.path.join(site_packages, entry)
            try:
                with open(pth_file, "r") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if line.startswith("import "):
                            # Execute import statements inside .pth files
                            exec(line, globals(), locals())
                        else:
                            # Treat as a path
                            extra_path = os.path.join(site_packages, line)
                            if os.path.exists(extra_path) and extra_path not in sys.path:
                                sys.path.insert(0, extra_path)
            except Exception as e:
                print(f"Warning: Could not process {pth_file}: {e}")

    return site_packages
"##;
        self.run(&set_venv_code).await?;
        self.run(&format!(
            "add_venv_libs_to_syspath({})",
            print_path_for_python(&venv_path.to_path_buf())
        ))
        .await
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
    use std::fs::{self, File};
    use std::io::Write;

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

        let result_module = executor.run(code).await;

        assert!(result_module.is_ok());

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

        assert!(res1.is_ok());
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

    #[tokio::test]
    async fn test_run_file() {
        let runner = PyRunner::new();
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // Create a module to be imported
        let mut module_file = File::create(dir_path.join("mymodule.py")).unwrap();
        writeln!(module_file, "def my_func(): return 42").unwrap();

        // Create the main script
        let script_path = dir_path.join("main.py");
        let mut script_file = File::create(&script_path).unwrap();
        writeln!(script_file, "import mymodule\nresult = mymodule.my_func()").unwrap();

        runner.run_file(&script_path).await.unwrap();

        let result = runner.read_variable("result").await.unwrap();
        assert_eq!(result, Value::Number(42.into()));
    }

    #[tokio::test]
    async fn test_set_venv() {
        let runner = PyRunner::new();
        let venv_dir = tempfile::tempdir().unwrap();
        let venv_path = venv_dir.path();

        // Get python version to create correct site-packages path
        let version_str = runner
            .eval("f'{__import__(\"sys\").version_info.major}.{__import__(\"sys\").version_info.minor}'")
            .await
            .unwrap();
        let py_version = version_str.as_str().unwrap();

        // Create a fake site-packages directory
        let site_packages = if cfg!(target_os = "windows") {
            venv_path.join("Lib").join("site-packages")
        } else {
            venv_path
                .join("lib")
                .join(format!("python{}", py_version))
                .join("site-packages")
        };
        fs::create_dir_all(&site_packages).unwrap();

        // Create a dummy package in site-packages
        let package_dir = site_packages.join("dummy_package");
        fs::create_dir(&package_dir).unwrap();
        let mut init_file = File::create(package_dir.join("__init__.py")).unwrap();
        writeln!(init_file, "def dummy_func(): return 'hello from venv'").unwrap();

        // Set the venv
        runner.set_venv(venv_path).await.unwrap();

        // Try to import and use the dummy package
        runner.run("import dummy_package").await.unwrap();
        let result = runner.eval("dummy_package.dummy_func()").await.unwrap();

        assert_eq!(result, Value::String("hello from venv".to_string()));
    }
}
