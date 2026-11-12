#![allow(dead_code)]

use async_py::{print_path_for_python, PyRunner, PyRunnerError};
use std::fs;
use std::path::{Path, PathBuf};

pub const PYO3: bool = cfg!(all(feature = "pyo3", not(feature = "rustpython")));
pub const RUSTPYTHON: bool = cfg!(feature = "rustpython");

/// Writes `source` to `dir/relative_path`, creating parent directories.
pub fn write_file(dir: &Path, relative_path: &str, source: &str) -> PathBuf {
    let path = dir.join(relative_path);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, source).unwrap();
    path
}

pub async fn add_to_sys_path(runner: &PyRunner, dir: &Path) {
    runner
        .run(&format!(
            "import sys\nsys.path.insert(0, {})",
            print_path_for_python(&dir.to_path_buf())
        ))
        .await
        .unwrap();
}

/// Returns the message of a `PyRunnerError::PyError`, panicking on anything else.
pub fn py_error_message<T: std::fmt::Debug>(result: Result<T, PyRunnerError>) -> String {
    match result {
        Err(PyRunnerError::PyError(message)) => message,
        other => panic!("expected PyRunnerError::PyError, got {other:?}"),
    }
}

/// Asserts that the runner still answers after a failed command.
pub async fn assert_alive(runner: &PyRunner) {
    assert_eq!(
        runner.eval("40 + 2").await.unwrap(),
        serde_json::Value::Number(42.into())
    );
}
