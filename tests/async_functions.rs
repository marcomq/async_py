//! `call_async_function` is only supported by the PyO3 backend.
#![cfg(all(feature = "pyo3", not(feature = "rustpython")))]
mod common;

use async_py::{PyRunner, PyRunnerError};
use common::*;
use serde_json::json;
use std::time::{Duration, Instant};

const ASYNC_CODE: &str = r#"
import asyncio

async def sleep_and_return(value, seconds):
    await asyncio.sleep(seconds)
    return value

async def fail_after(seconds):
    await asyncio.sleep(seconds)
    raise RuntimeError("async failure")

async def nested_result():
    await asyncio.sleep(0)
    return {"items": [1, 2, {"ok": True}], "none": None}

async def gather_three():
    return await asyncio.gather(*(sleep_and_return(i, 0.05) for i in range(3)))

async def circular():
    loop = []
    loop.append(loop)
    return loop

def plain_function():
    return "not a coroutine"

def returns_asyncio_future():
    future = asyncio.get_event_loop().create_future()
    future.get_loop().call_later(0.01, future.set_result, "future")
    return future

def returns_task():
    return asyncio.ensure_future(sleep_and_return("task", 0.01))

def returns_concurrent_future():
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: {"from": "thread"})
"#;

async fn async_runner() -> PyRunner {
    let runner = PyRunner::new();
    runner.run(ASYNC_CODE).await.unwrap();
    runner
}

#[tokio::test]
async fn coroutine_results_are_converted() {
    let runner = async_runner().await;

    let result = runner
        .call_async_function("nested_result", vec![])
        .await
        .unwrap();

    assert_eq!(result, json!({"items": [1, 2, {"ok": true}], "none": null}));
}

#[tokio::test]
async fn concurrent_coroutines_overlap() {
    let runner = async_runner().await;
    let start = Instant::now();

    let handles: Vec<_> = (0..5)
        .map(|i| {
            let runner = runner.clone();
            tokio::spawn(async move {
                runner
                    .call_async_function("sleep_and_return", vec![i.into(), 0.3.into()])
                    .await
            })
        })
        .collect();

    for (i, handle) in handles.into_iter().enumerate() {
        assert_eq!(handle.await.unwrap().unwrap(), json!(i));
    }
    assert!(
        start.elapsed() < Duration::from_millis(1200),
        "5 x 0.3s coroutines took {:?}, expected them to overlap",
        start.elapsed()
    );
}

#[tokio::test]
async fn sync_commands_are_not_blocked_by_awaiting_coroutine() {
    let runner = async_runner().await;
    let slow = runner.call_async_function("sleep_and_return", vec![1.into(), 1.into()]);
    let quick = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let start = Instant::now();
        let value = runner.eval("2 + 2").await.unwrap();
        (value, start.elapsed())
    };

    let (slow, (quick_value, quick_elapsed)) = tokio::join!(slow, quick);

    assert_eq!(slow.unwrap(), json!(1));
    assert_eq!(quick_value, json!(4));
    assert!(
        quick_elapsed < Duration::from_millis(500),
        "{quick_elapsed:?}"
    );
}

#[tokio::test]
async fn coroutine_can_use_asyncio_gather() {
    let runner = async_runner().await;

    let result = runner
        .call_async_function("gather_three", vec![])
        .await
        .unwrap();

    assert_eq!(result, json!([0, 1, 2]));
}

#[tokio::test]
async fn exception_in_coroutine_reports_error() {
    let runner = async_runner().await;

    let message = py_error_message(
        runner
            .call_async_function("fail_after", vec![0.01.into()])
            .await,
    );

    assert!(message.contains("async failure"), "{message}");
    assert_alive(&runner).await;
}

#[tokio::test]
async fn non_coroutine_function_reports_error() {
    let runner = async_runner().await;

    py_error_message(runner.call_async_function("plain_function", vec![]).await);

    assert_alive(&runner).await;
}

#[tokio::test]
async fn awaitables_returned_by_plain_functions_are_awaited() {
    let runner = async_runner().await;

    for (name, expected) in [
        ("returns_asyncio_future", json!("future")),
        ("returns_task", json!("task")),
        ("returns_concurrent_future", json!({"from": "thread"})),
    ] {
        let result = runner.call_async_function(name, vec![]).await;
        assert_eq!(result.unwrap(), expected, "{name}");
    }
}

#[tokio::test]
async fn circular_coroutine_result_reports_error_instead_of_aborting() {
    let runner = async_runner().await;

    let message = py_error_message(runner.call_async_function("circular", vec![]).await);

    assert!(message.contains("circular reference"), "{message}");
    assert_alive(&runner).await;
}

#[tokio::test]
async fn deeply_nested_coroutine_result_converts_on_worker_stack() {
    let runner = PyRunner::new();
    runner
        .run("async def deep():\n    value = 1\n    for _ in range(500):\n        value = [value]\n    return value")
        .await
        .unwrap();

    let result = runner.call_async_function("deep", vec![]).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn timeout_frees_caller_of_slow_coroutine() {
    let runner = async_runner()
        .await
        .with_timeout(Duration::from_millis(100));

    let result = runner
        .call_async_function("sleep_and_return", vec![1.into(), 2.into()])
        .await;

    assert!(matches!(result, Err(PyRunnerError::Timeout)));
    assert_alive(&runner).await;
}

#[tokio::test]
async fn asyncio_run_inside_run_works() {
    let runner = async_runner().await;

    runner
        .run("result = asyncio.run(sleep_and_return('done', 0.01))")
        .await
        .unwrap();

    assert_eq!(runner.read_variable("result").await.unwrap(), json!("done"));
}

#[test]
fn call_async_function_sync_without_runtime() {
    let runner = PyRunner::new();
    runner.run_sync(ASYNC_CODE).unwrap();

    let result = runner
        .call_async_function_sync("sleep_and_return", vec!["sync".into(), 0.01.into()])
        .unwrap();

    assert_eq!(result, json!("sync"));
}

#[tokio::test]
async fn call_async_function_on_module_entry_point() {
    let dir = tempfile::tempdir().unwrap();
    write_file(dir.path(), "async_entry.py", ASYNC_CODE);
    let runner = PyRunner::new();
    add_to_sys_path(&runner, dir.path()).await;
    runner.run_module("async_entry").await.unwrap();

    let result = runner
        .call_async_function("sleep_and_return", vec!["from module".into(), 0.into()])
        .await
        .unwrap();

    assert_eq!(result, json!("from module"));
}
