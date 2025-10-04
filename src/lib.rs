//  Async Python
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/async_py

//! A library for calling Python code asynchronously from Rust.

#[cfg(feature = "pyo3")]
mod pyo3_runner;
#[cfg(feature = "rustpython")]
mod rustpython_runner;

use serde_json::Value;
use std::path::{Path, PathBuf};
use std::thread;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub(crate) enum CmdType {
    RunFile(PathBuf),
    RunCode(String),
    EvalCode(String),
    ReadVariable(String),
    CallFunction { name: String, args: Vec<Value> },
    CallAsyncFunction { name: String, args: Vec<Value> },
    Stop,
}
/// Represents a command to be sent to the Python execution thread. It includes the
/// command to execute and a one-shot channel sender to send the `serde_json::Value`
/// result back.
pub(crate) struct PyCommand {
    cmd_type: CmdType,
    responder: oneshot::Sender<Result<Value, String>>,
}

/// Custom error types for the `PyRunner`.
#[derive(Error, Debug)]
pub enum PyRunnerError {
    #[error("Failed to send command to Python thread. The thread may have panicked.")]
    SendCommandFailed,

    #[error("Failed to receive result from Python thread. The thread may have panicked.")]
    ReceiveResultFailed,

    #[error("Python execution error: {0:?}")]
    PyError(String),
}

fn cleanup_path_for_python(path: &PathBuf) -> String {
    dunce::canonicalize(path)
        .unwrap()
        .to_string_lossy()
        .replace("\\", "/")
}

pub fn print_path_for_python(path: &PathBuf) -> String {
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
        let (sender, receiver) = mpsc::channel::<PyCommand>(32);

        // Spawn a new OS thread to handle all Python-related work.
        // This is crucial to avoid blocking the async runtime and to manage the GIL correctly.
        thread::spawn(move || {
            #[cfg(all(feature = "pyo3", not(feature = "rustpython")))]
            {
                use tokio::runtime::Builder;
                let rt = Builder::new_multi_thread().enable_all().build().unwrap();
                rt.block_on(pyo3_runner::python_thread_main(receiver));
            }

            #[cfg(feature = "rustpython")]
            rustpython_runner::python_thread_main(receiver);
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
        receiver
            .await
            .map_err(|_| PyRunnerError::ReceiveResultFailed)?
            .map_err(PyRunnerError::PyError)
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
        self.send_command(CmdType::RunFile(file.to_path_buf()))
            .await
            .map(|_| ())
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
    /// Does not release GIL during await.
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

    /// Asynchronously calls an async Python function in the interpreter's global scope.
    ///
    /// * `name`: The name of the function to call. It can be a dot-separated path
    ///   to access functions within modules (e.g., "my_module.my_function").
    /// * `args`: A vector of `serde_json::Value` to pass as arguments to the function.
    /// Returns the function's return value as a `serde_json::Value` on success.
    /// Will release GIL during await.
    pub async fn call_async_function(
        &self,
        name: &str,
        args: Vec<Value>,
    ) -> Result<Value, PyRunnerError> {
        self.send_command(CmdType::CallAsyncFunction {
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
        if !venv_path.is_dir() {
            return Err(PyRunnerError::PyError(format!(
                "Could not find venv directory {}",
                venv_path.display()
            )));
        }
        let set_venv_code = include_str!("set_venv.py");
        self.run(&set_venv_code).await?;

        let site_packages = if cfg!(target_os = "windows") {
            venv_path.join("Lib").join("site-packages")
        } else {
            let version_code = "f\"python{sys.version_info.major}.{sys.version_info.minor}\"";
            let py_version = self.eval(version_code).await?;
            venv_path
                .join("lib")
                .join(py_version.as_str().unwrap())
                .join("site-packages")
        };
        #[cfg(all(feature = "pyo3", not(feature = "rustpython")))]
        let with_pth = "True";
        #[cfg(feature = "rustpython")]
        let with_pth = "False";

        self.run(&format!(
            "add_venv_libs_to_syspath({}, {})",
            print_path_for_python(&site_packages),
            with_pth
        ))
        .await
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

    #[cfg(feature = "pyo3")]
    #[tokio::test]
    async fn test_run_with_async_function() {
        let executor = PyRunner::new();
        let code = r#"
import asyncio
counter = 0

async def add_and_sleep(a, b, sleep_time):
    global counter
    await asyncio.sleep(sleep_time)
    counter += 1
    return a + b + counter
"#;

        executor.run(code).await.unwrap();
        let result1 = executor.call_async_function("add_and_sleep", vec![5.into(), 10.into(), 1.into()]);
        let result2 = executor.call_async_function("add_and_sleep", vec![5.into(), 10.into(), 0.1.into()]);
        let (result1, result2) = tokio::join!(result1, result2);
        assert_eq!(result1.unwrap(), Value::Number(17.into()));
        assert_eq!(result2.unwrap(), Value::Number(16.into()));
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
        writeln!(
            module_file,
            r#"
def my_func(): 
    return 42
"#
        )
        .unwrap();

        // Create the main script
        let script_path = dir_path.join("main.py");
        let mut script_file = File::create(&script_path).unwrap();
        writeln!(
            script_file,
            r#"
import mymodule
result = mymodule.my_func()
"#
        )
        .unwrap();

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
