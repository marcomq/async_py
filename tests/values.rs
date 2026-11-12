//! Conversion between `serde_json::Value` and Python objects.
mod common;

use async_py::PyRunner;
use common::*;
use serde_json::{json, Value};

async fn echo_runner() -> PyRunner {
    let runner = PyRunner::new();
    runner
        .run("def echo(value):\n    return value\n")
        .await
        .unwrap();
    runner
}

#[tokio::test]
async fn json_values_round_trip_through_python() {
    let runner = echo_runner().await;
    let cases = [
        Value::Null,
        json!(true),
        json!(false),
        json!(0),
        json!(-17),
        json!(i64::MAX),
        json!(i64::MIN),
        json!(1.5),
        json!(-0.25),
        json!(1e300),
        json!(""),
        json!("plain ascii"),
        json!("ünïcödé € 😀 \n\t \"quoted\""),
        json!([]),
        json!({}),
        json!([1, "two", 3.0, null, true]),
        json!({"a": 1, "nested": {"list": [1, {"deep": [null]}]}}),
    ];
    for case in cases {
        let result = runner.call_function("echo", vec![case.clone()]).await;
        assert_eq!(result.unwrap(), case, "round trip of {case}");
    }
}

#[tokio::test]
async fn multiple_arguments_keep_order() {
    let runner = PyRunner::new();
    runner
        .run("def collect(*args):\n    return list(args)\n")
        .await
        .unwrap();

    let args = vec![json!(1), json!("b"), json!([3]), json!({"d": 4})];
    let result = runner.call_function("collect", args.clone()).await.unwrap();

    assert_eq!(result, Value::Array(args));
}

#[tokio::test]
async fn arguments_arrive_as_native_python_types() {
    let runner = PyRunner::new();
    runner
        .run("def kinds(*args):\n    return [type(a).__name__ for a in args]\n")
        .await
        .unwrap();

    let result = runner
        .call_function(
            "kinds",
            vec![
                json!(null),
                json!(true),
                json!(1),
                json!(1.5),
                json!("s"),
                json!([1]),
                json!({"k": 1}),
            ],
        )
        .await
        .unwrap();

    assert_eq!(
        result,
        json!(["NoneType", "bool", "int", "float", "str", "list", "dict"])
    );
}

#[tokio::test]
async fn large_payloads_round_trip() {
    let runner = echo_runner().await;
    let big_string = "x".repeat(1_000_000);
    let big_list: Vec<Value> = (0..100_000).map(Value::from).collect();

    let result = runner
        .call_function("echo", vec![json!(big_string)])
        .await
        .unwrap();
    assert_eq!(result.as_str().unwrap().len(), 1_000_000);

    let result = runner
        .call_function("echo", vec![Value::Array(big_list.clone())])
        .await
        .unwrap();
    assert_eq!(result, Value::Array(big_list));
}

#[tokio::test]
async fn subclasses_of_builtin_types_convert_like_their_base() {
    let runner = PyRunner::new();
    runner
        .run(
            r#"
class Number(int):
    pass
class Name(str):
    pass
class Mapping(dict):
    pass
class Items(list):
    pass
"#,
        )
        .await
        .unwrap();

    assert_eq!(runner.eval("Number(7)").await.unwrap(), json!(7));
    assert_eq!(runner.eval("Name('ann')").await.unwrap(), json!("ann"));
    assert_eq!(
        runner.eval("Mapping(a=1, b=2)").await.unwrap(),
        json!({"a": 1, "b": 2})
    );
    assert_eq!(runner.eval("Items([1, 2])").await.unwrap(), json!([1, 2]));
}

#[tokio::test]
async fn unsupported_objects_fall_back_to_str() {
    let runner = PyRunner::new();
    runner
        .run("class Thing:\n    def __str__(self):\n        return 'a thing'\n")
        .await
        .unwrap();

    assert_eq!(runner.eval("Thing()").await.unwrap(), json!("a thing"));
    assert_eq!(runner.eval("1+2j").await.unwrap(), json!("(1+2j)"));
    assert_eq!(runner.eval("range(3)").await.unwrap(), json!("range(0, 3)"));
}

#[tokio::test]
async fn tuples_and_sets_become_arrays() {
    let runner = PyRunner::new();

    assert_eq!(runner.eval("(1, 2)").await.unwrap(), json!([1, 2]));
    assert_eq!(runner.eval("()").await.unwrap(), json!([]));
    assert_eq!(runner.eval("{1}").await.unwrap(), json!([1]));
    assert_eq!(runner.eval("frozenset([2])").await.unwrap(), json!([2]));
    assert_eq!(
        runner.eval("[1, (2, ({'k': (3,)},))]").await.unwrap(),
        json!([1, [2, [{"k": [3]}]]])
    );
}

/// Bytes are still returned as their `str()`. Pinned so that a change is deliberate.
#[tokio::test]
async fn bytes_become_strings() {
    let runner = PyRunner::new();

    assert_eq!(runner.eval("b'ab'").await.unwrap(), json!("b'ab'"));
}

/// The backends currently disagree on these values. Pinned per backend so that
/// unifying them is a visible, deliberate change.
#[tokio::test]
async fn backend_specific_conversions_are_pinned() {
    let runner = PyRunner::new();

    let nan = runner.eval("float('nan')").await.unwrap();
    let big_int = runner.eval("2**64").await;
    let int_keys = runner.eval("{1: 'a'}").await;

    if PYO3 {
        assert_eq!(nan, Value::Null);
        assert!(py_error_message(big_int).contains("OverflowError"));
        assert_eq!(int_keys.unwrap(), json!({"1": "a"}));
    } else if RUSTPYTHON {
        assert_eq!(nan, json!(0));
        assert_eq!(big_int.unwrap(), json!(18446744073709551616.0));
        assert!(py_error_message(int_keys).contains("dict keys must be strings"));
    }
}

#[tokio::test]
async fn read_variable_follows_attribute_paths() {
    let runner = PyRunner::new();
    runner
        .run("import sys\nclass Box:\n    pass\nbox = Box()\nbox.inner = Box()\nbox.inner.value = [1, 2]\n")
        .await
        .unwrap();

    assert_eq!(
        runner.read_variable("box.inner.value").await.unwrap(),
        json!([1, 2])
    );
    assert_eq!(
        runner.read_variable("sys.maxsize").await.unwrap(),
        json!(i64::MAX)
    );
}

#[tokio::test]
async fn call_function_follows_attribute_paths() {
    let runner = PyRunner::new();
    runner
        .run("class Api:\n    @staticmethod\n    def double(x):\n        return x * 2\napi = Api()\n")
        .await
        .unwrap();

    assert_eq!(
        runner
            .call_function("api.double", vec![21.into()])
            .await
            .unwrap(),
        json!(42)
    );
}
