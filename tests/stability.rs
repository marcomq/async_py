//! Regression guards for failures that would otherwise crash or wedge the host process.
mod common;

use async_py::{PyRunner, PyRunnerError};
use common::*;
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn circular_list_reports_error_instead_of_aborting() {
    let runner = PyRunner::new();
    runner.run("loop = []\nloop.append(loop)").await.unwrap();

    let message = py_error_message(runner.read_variable("loop").await);

    assert!(message.contains("circular reference"), "{message}");
    assert_alive(&runner).await;
}

#[tokio::test]
async fn circular_dict_reports_error_instead_of_aborting() {
    let runner = PyRunner::new();
    runner
        .run("cycle = {}\ncycle['self'] = cycle\ndef get_cycle():\n    return cycle")
        .await
        .unwrap();

    py_error_message(runner.eval("cycle").await);
    py_error_message(runner.call_function("get_cycle", vec![]).await);
    assert_alive(&runner).await;
}

#[tokio::test]
async fn circular_tuple_reports_error_instead_of_aborting() {
    let runner = PyRunner::new();
    runner.run("cyc = ([],)\ncyc[0].append(cyc)").await.unwrap();

    let message = py_error_message(runner.read_variable("cyc").await);

    assert!(message.contains("circular reference"), "{message}");
    assert_alive(&runner).await;
}

const MUTATING_STR: &str = r#"
class Mutator:
    def __init__(self, mutate):
        self.mutate = mutate
    def __str__(self):
        self.mutate()
        return "mutated"

grow_set = set()
grow_set.add(Mutator(lambda: grow_set.add(object())))
grow_dict = {}
grow_dict["a"] = Mutator(lambda: grow_dict.__setitem__("new", 1))
"#;

#[tokio::test]
async fn containers_mutated_during_conversion_do_not_kill_runner() {
    let runner = PyRunner::new();
    runner.run(MUTATING_STR).await.unwrap();

    assert_eq!(
        runner.read_variable("grow_set").await.unwrap(),
        json!(["mutated"])
    );
    let dict = runner.read_variable("grow_dict").await.unwrap();
    assert_eq!(dict["a"], json!("mutated"));
    assert_alive(&runner).await;
}

#[tokio::test]
async fn deeply_nested_but_finite_values_still_convert() {
    let runner = PyRunner::new();
    runner
        .run("deep = 1\nfor _ in range(500):\n    deep = [deep]")
        .await
        .unwrap();

    let mut value = runner.read_variable("deep").await.unwrap();
    let mut depth = 0;
    while let Some(inner) = value.as_array().and_then(|a| a.first()).cloned() {
        value = inner;
        depth += 1;
    }
    assert_eq!(depth, 500);
    assert_eq!(value, json!(1));
}

#[tokio::test]
async fn values_beyond_depth_limit_report_error() {
    let runner = PyRunner::new();
    runner
        .run("too_deep = 1\nfor _ in range(10_000):\n    too_deep = [too_deep]")
        .await
        .unwrap();

    py_error_message(runner.read_variable("too_deep").await);
    assert_alive(&runner).await;
}

#[tokio::test]
async fn object_with_failing_str_does_not_crash() {
    let runner = PyRunner::new();
    runner
        .run("class Bad:\n    def __str__(self):\n        raise ValueError('no str')\n    __repr__ = __str__\n")
        .await
        .unwrap();

    // pyo3 renders an "<unprintable ...>" placeholder, RustPython reports the error.
    let _ = runner.eval("Bad()").await;

    assert_alive(&runner).await;
}

#[tokio::test]
async fn stop_is_final_for_all_clones() {
    let runner = PyRunner::new();
    let clone = runner.clone();

    runner.stop().await.unwrap();

    assert!(matches!(
        clone.eval("1").await,
        Err(PyRunnerError::SendCommandFailed)
    ));
    assert!(matches!(
        runner.stop().await,
        Err(PyRunnerError::SendCommandFailed)
    ));
    assert!(matches!(
        clone.run_module("math").await,
        Err(PyRunnerError::SendCommandFailed)
    ));
}

#[tokio::test]
async fn runners_have_isolated_globals() {
    let first = PyRunner::new();
    let second = PyRunner::new();

    first.run("owner = 'first'").await.unwrap();
    second.run("owner = 'second'").await.unwrap();

    assert_eq!(first.read_variable("owner").await.unwrap(), json!("first"));
    assert_eq!(
        second.read_variable("owner").await.unwrap(),
        json!("second")
    );
}

#[tokio::test]
async fn clones_share_globals() {
    let runner = PyRunner::new();
    let clone = runner.clone();

    runner.run("shared = 1").await.unwrap();
    clone.run("shared += 1").await.unwrap();

    assert_eq!(runner.read_variable("shared").await.unwrap(), json!(2));
}

#[tokio::test]
async fn many_runners_can_be_created_and_dropped() {
    for i in 0..20 {
        let runner = PyRunner::new();
        assert_eq!(
            runner.eval(&format!("{i} * 2")).await.unwrap(),
            json!(i * 2)
        );
    }
}

#[tokio::test]
async fn commands_execute_in_submission_order() {
    let runner = PyRunner::new();
    runner.run("log = []").await.unwrap();

    let code: Vec<String> = (0..20).map(|i| format!("log.append({i})")).collect();
    let calls: Vec<_> = code.iter().map(|c| runner.run(c)).collect();
    for call in calls {
        call.await.unwrap();
    }

    assert_eq!(
        runner.read_variable("log").await.unwrap(),
        json!((0..20).collect::<Vec<_>>())
    );
}

#[tokio::test]
async fn background_python_threads_keep_running() {
    let runner = PyRunner::new();
    runner
        .run(
            "import _thread, time\nticks = []\ndef work():\n    for i in range(3):\n        ticks.append(i)\n        time.sleep(0.01)\n_thread.start_new_thread(work, ())",
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(
        runner.read_variable("ticks").await.unwrap(),
        json!([0, 1, 2])
    );
}

#[tokio::test]
async fn timeout_applies_per_clone() {
    let base = PyRunner::new();
    let impatient = base.clone().with_timeout(Duration::from_millis(50));

    let slow = base.run("import time\ntime.sleep(0.3)\nslow_done = True");
    let quick = impatient.eval("1");
    let (slow, quick) = tokio::join!(slow, quick);

    slow.unwrap();
    assert!(matches!(quick, Err(PyRunnerError::Timeout)));
    assert_eq!(base.read_variable("slow_done").await.unwrap(), json!(true));
}
