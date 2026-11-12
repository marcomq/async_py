//! Starting CPython with an explicit home (bundled / relocated Python).
#![cfg(all(feature = "pyo3", not(feature = "rustpython")))]

use async_py::{init_python_home, is_python_home, PyRunner};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const HOME_ENV: &str = "ASYNC_PY_TEST_HOME";

/// `init_python_home` must run before Python starts, so it is tested in a
/// fresh child process running one of the ignored tests below.
fn run_child(test: &str, home: &Path) -> Output {
    Command::new(std::env::current_exe().unwrap())
        .args([test, "--exact", "--ignored", "--nocapture"])
        .env(HOME_ENV, home)
        .output()
        .unwrap()
}

fn assert_success(out: &Output) {
    assert!(
        out.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

async fn linked_python_home() -> PathBuf {
    let runner = PyRunner::new();
    let prefix = runner.eval("__import__('sys').base_prefix").await.unwrap();
    PathBuf::from(prefix.as_str().unwrap())
}

#[tokio::test]
async fn linked_python_prefix_is_a_python_home() {
    let home = linked_python_home().await;
    assert!(is_python_home(&home), "{}", home.display());
}

#[test]
fn dir_without_stdlib_is_not_a_python_home() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("lib").join("python3.13")).unwrap();
    assert!(!is_python_home(dir.path()));
    assert!(!is_python_home(&dir.path().join("missing")));
}

#[test]
fn windows_layout_is_a_python_home() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("Lib")).unwrap();
    std::fs::write(dir.path().join("Lib").join("os.py"), "").unwrap();
    assert!(is_python_home(dir.path()));
}

#[tokio::test]
async fn init_uses_given_home() {
    let home = linked_python_home().await;
    assert_success(&run_child("child_init_uses_given_home", &home));
}

#[test]
fn init_with_bad_home_fails() {
    let dir = tempfile::tempdir().unwrap();
    assert_success(&run_child("child_init_with_bad_home_fails", dir.path()));
}

#[test]
#[ignore = "runs in a child process"]
fn child_init_uses_given_home() {
    let home = PathBuf::from(std::env::var_os(HOME_ENV).unwrap());
    assert!(init_python_home(&home).unwrap());
    assert!(
        !init_python_home(&home).unwrap(),
        "second call must be a no-op"
    );
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let runner = PyRunner::new();
        let prefix = runner.eval("__import__('sys').prefix").await.unwrap();
        assert_eq!(PathBuf::from(prefix.as_str().unwrap()), home);
        runner.run("import json, asyncio").await.unwrap();
    });
}

#[test]
#[ignore = "runs in a child process"]
fn child_init_with_bad_home_fails() {
    let home = PathBuf::from(std::env::var_os(HOME_ENV).unwrap());
    let err = init_python_home(&home).unwrap_err().to_string();
    assert!(err.contains("Cannot initialize Python with home"), "{err}");
    let retry = init_python_home(&home).unwrap_err().to_string();
    assert_eq!(retry, err, "a failed init must not be retried");
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let runner = PyRunner::new();
        let err = runner.eval("1").await.unwrap_err().to_string();
        assert!(err.contains("Cannot initialize Python with home"), "{err}");
        runner.stop().await.unwrap();
    });
}
