//! Python errors must surface as `PyRunnerError::PyError` and never break the runner.
mod common;

use async_py::{PyRunner, PyRunnerError};
use common::*;
use serde_json::json;
use std::path::Path;

#[tokio::test]
async fn error_message_names_the_exception_type() {
    let runner = PyRunner::new();
    runner
        .run("class AppError(Exception):\n    pass\ndef fail():\n    raise AppError('custom failure')\n")
        .await
        .unwrap();

    let cases = [
        (runner.eval("1 / 0").await, "ZeroDivisionError"),
        (runner.eval("undefined_name").await, "NameError"),
        (
            runner.run("def (").await.map(|_| Default::default()),
            "SyntaxError",
        ),
        (runner.eval("x = 1").await, "SyntaxError"),
        (runner.eval("int('abc')").await, "ValueError"),
        (runner.call_function("fail", vec![]).await, "custom failure"),
    ];
    for (result, expected) in cases {
        let message = py_error_message(result);
        assert!(
            message.contains(expected),
            "expected {expected:?} in {message:?}"
        );
    }
    assert_alive(&runner).await;
}

#[tokio::test]
async fn system_exit_and_keyboard_interrupt_do_not_stop_the_runner() {
    let runner = PyRunner::new();

    let message = py_error_message(runner.run("raise SystemExit(3)").await);
    assert!(message.contains("SystemExit"), "{message}");
    assert_alive(&runner).await;

    let message = py_error_message(runner.run("raise KeyboardInterrupt").await);
    assert!(message.contains("KeyboardInterrupt"), "{message}");
    assert_alive(&runner).await;

    py_error_message(runner.run("import sys\nsys.exit(0)").await);
    assert_alive(&runner).await;
}

#[tokio::test]
async fn failed_run_keeps_effects_before_the_error() {
    let runner = PyRunner::new();

    py_error_message(
        runner
            .run("before = 1\nraise ValueError('x')\nafter = 2")
            .await,
    );

    assert_eq!(runner.read_variable("before").await.unwrap(), json!(1));
    py_error_message(runner.read_variable("after").await);
}

#[tokio::test]
async fn missing_or_invalid_call_targets_report_errors() {
    let runner = PyRunner::new();
    runner.run("import sys\nnot_callable = 5").await.unwrap();

    for name in [
        "missing_function",
        "missing.attr",
        "sys.no_such_attribute",
        "not_callable",
        "",
        ".",
        "sys.",
    ] {
        py_error_message(runner.call_function(name, vec![]).await);
        py_error_message(runner.call_async_function(name, vec![]).await);
    }
    for name in ["missing_variable", "sys.no_such_attribute", "", ".", "sys."] {
        py_error_message(runner.read_variable(name).await);
    }
    assert_alive(&runner).await;
}

#[tokio::test]
async fn call_function_rejects_coroutine_result() {
    let runner = PyRunner::new();
    runner
        .run("async def coro():\n    return 1\n")
        .await
        .unwrap();

    let message = py_error_message(runner.call_function("coro", vec![]).await);

    assert!(message.contains("call_async_function"), "{message}");
    assert_alive(&runner).await;
}

#[tokio::test]
async fn wrong_argument_count_reports_type_error() {
    let runner = PyRunner::new();
    runner.run("def one(a):\n    return a").await.unwrap();

    let message = py_error_message(runner.call_function("one", vec![1.into(), 2.into()]).await);

    assert!(message.contains("TypeError"), "{message}");
    assert_alive(&runner).await;
}

#[tokio::test]
async fn run_file_with_missing_file_reports_error() {
    let runner = PyRunner::new();
    let dir = tempfile::tempdir().unwrap();

    py_error_message(runner.run_file(&dir.path().join("missing.py")).await);

    assert_alive(&runner).await;
}

#[tokio::test]
async fn run_file_with_failing_script_reports_error() {
    let runner = PyRunner::new();
    let dir = tempfile::tempdir().unwrap();
    let script = write_file(
        dir.path(),
        "failing.py",
        "raise RuntimeError('script failed')\n",
    );

    let message = py_error_message(runner.run_file(&script).await);

    assert!(message.contains("script failed"), "{message}");
    assert_alive(&runner).await;
}

#[tokio::test]
async fn error_message_has_no_traceback_by_default() {
    let runner = PyRunner::new();
    runner
        .run("def fail():\n    raise ValueError('bad input')\n")
        .await
        .unwrap();

    let message = py_error_message(runner.call_function("fail", vec![]).await);

    assert_eq!(message, "ValueError: bad input");
}

#[tokio::test]
async fn error_message_includes_traceback_when_enabled() {
    let runner = PyRunner::new().with_tracebacks(true);
    let dir = tempfile::tempdir().unwrap();
    let script = write_file(
        dir.path(),
        "app.py",
        "def fail():\n    raise ValueError('bad input')\n",
    );
    runner.run_file(&script).await.unwrap();

    let message = py_error_message(runner.call_function("fail", vec![]).await);

    assert!(message.starts_with("Traceback"), "{message}");
    assert!(message.contains("app.py"), "{message}");
    assert!(message.contains("line 2"), "{message}");
    assert!(message.ends_with("ValueError: bad input"), "{message}");
}

#[tokio::test]
async fn traceback_includes_chained_exceptions() {
    let runner = PyRunner::new().with_tracebacks(true);
    runner
        .run("def fail():\n    try:\n        1 / 0\n    except ZeroDivisionError as e:\n        raise ValueError('wrapped') from e\n")
        .await
        .unwrap();

    let message = py_error_message(runner.call_function("fail", vec![]).await);

    assert!(message.contains("ZeroDivisionError"), "{message}");
    assert!(message.contains("direct cause"), "{message}");
    assert!(message.ends_with("ValueError: wrapped"), "{message}");
}

#[tokio::test]
async fn run_file_traceback_points_into_the_script() {
    let runner = PyRunner::new().with_tracebacks(true);
    let dir = tempfile::tempdir().unwrap();
    let script = write_file(
        dir.path(),
        "failing.py",
        "x = 1\nraise RuntimeError('script failed')\n",
    );

    let message = py_error_message(runner.run_file(&script).await);

    assert!(message.contains("failing.py\", line 2"), "{message}");
    assert!(
        message.ends_with("RuntimeError: script failed"),
        "{message}"
    );
}

#[tokio::test]
async fn run_file_handles_paths_with_spaces_and_quotes() {
    let runner = PyRunner::new();
    let dir = tempfile::tempdir().unwrap();
    let script = write_file(
        dir.path(),
        "dir with space/it's ü/main.py",
        "result = 'loaded'\n",
    );

    runner.run_file(&script).await.unwrap();

    assert_eq!(
        runner.read_variable("result").await.unwrap(),
        json!("loaded")
    );
}

#[tokio::test]
async fn set_venv_with_missing_directory_reports_error() {
    let runner = PyRunner::new();

    let result = runner.set_venv(Path::new("/definitely/not/a/venv")).await;

    assert!(matches!(result, Err(PyRunnerError::PyError(_))));
    let result = runner.set_venv_sync(Path::new("/definitely/not/a/venv"));
    assert!(matches!(result, Err(PyRunnerError::PyError(_))));
    assert_alive(&runner).await;
}

#[test]
fn sync_api_reports_python_errors() {
    let runner = PyRunner::new();

    assert!(matches!(
        runner.eval_sync("1 / 0"),
        Err(PyRunnerError::PyError(_))
    ));
    assert!(matches!(
        runner.run_sync("raise ValueError"),
        Err(PyRunnerError::PyError(_))
    ));
    assert!(matches!(
        runner.call_function_sync("missing", vec![]),
        Err(PyRunnerError::PyError(_))
    ));
    assert!(matches!(
        runner.read_variable_sync("missing"),
        Err(PyRunnerError::PyError(_))
    ));
    assert_eq!(runner.eval_sync("1 + 1").unwrap(), json!(2));
}
