mod common;

use async_py::PyRunner;
use common::*;
use serde_json::{json, Value};

const GREETER: &str = r#"
counter = 0
GREETING = "Hello"

def greet(name):
    global counter
    counter += 1
    return f"{GREETING} {name} #{counter}"

def uses_late_global():
    return late_value * 2
"#;

#[tokio::test]
async fn module_namespace_becomes_global_scope() {
    let dir = tempfile::tempdir().unwrap();
    write_file(dir.path(), "greeter.py", GREETER);
    let runner = PyRunner::new();
    add_to_sys_path(&runner, dir.path()).await;

    runner.run_module("greeter").await.unwrap();

    assert_eq!(
        runner.read_variable("GREETING").await.unwrap(),
        json!("Hello")
    );
    assert_eq!(
        runner
            .call_function("greet", vec!["Ann".into()])
            .await
            .unwrap(),
        json!("Hello Ann #1")
    );
    assert_eq!(runner.eval("__name__").await.unwrap(), json!("greeter"));
}

#[tokio::test]
async fn module_globals_are_live_not_copied() {
    let dir = tempfile::tempdir().unwrap();
    write_file(dir.path(), "greeter_live.py", GREETER);
    let runner = PyRunner::new();
    add_to_sys_path(&runner, dir.path()).await;
    runner.run_module("greeter_live").await.unwrap();

    runner
        .call_function("greet", vec!["A".into()])
        .await
        .unwrap();
    runner
        .call_function("greet", vec!["B".into()])
        .await
        .unwrap();

    assert_eq!(runner.read_variable("counter").await.unwrap(), json!(2));
}

#[tokio::test]
async fn run_after_run_module_executes_in_module_namespace() {
    let dir = tempfile::tempdir().unwrap();
    write_file(dir.path(), "greeter_late.py", GREETER);
    let runner = PyRunner::new();
    add_to_sys_path(&runner, dir.path()).await;
    runner.run_module("greeter_late").await.unwrap();

    runner.run("late_value = 21").await.unwrap();

    assert_eq!(
        runner
            .call_function("uses_late_global", vec![])
            .await
            .unwrap(),
        json!(42)
    );
}

#[tokio::test]
async fn run_module_replaces_previous_globals() {
    let dir = tempfile::tempdir().unwrap();
    write_file(dir.path(), "greeter_replace.py", GREETER);
    let runner = PyRunner::new();
    add_to_sys_path(&runner, dir.path()).await;
    runner.run("defined_before = 1").await.unwrap();

    runner.run_module("greeter_replace").await.unwrap();

    py_error_message(runner.read_variable("defined_before").await);
}

#[tokio::test]
async fn dotted_name_loads_submodule_namespace() {
    let dir = tempfile::tempdir().unwrap();
    write_file(dir.path(), "mypkg/__init__.py", "LEVEL = 'package'\n");
    write_file(
        dir.path(),
        "mypkg/backend.py",
        "LEVEL = 'submodule'\ndef add(a, b):\n    return a + b\n",
    );
    let runner = PyRunner::new();
    add_to_sys_path(&runner, dir.path()).await;

    runner.run_module("mypkg.backend").await.unwrap();

    assert_eq!(
        runner.read_variable("LEVEL").await.unwrap(),
        json!("submodule")
    );
    assert_eq!(
        runner
            .call_function("add", vec![2.into(), 3.into()])
            .await
            .unwrap(),
        json!(5)
    );
}

#[tokio::test]
async fn module_can_import_its_siblings() {
    let dir = tempfile::tempdir().unwrap();
    write_file(
        dir.path(),
        "sibling_helper.py",
        "def value():\n    return 7\n",
    );
    write_file(
        dir.path(),
        "sibling_main.py",
        "from sibling_helper import value\nRESULT = value() * 6\n",
    );
    let runner = PyRunner::new();
    add_to_sys_path(&runner, dir.path()).await;

    runner.run_module("sibling_main").await.unwrap();

    assert_eq!(runner.read_variable("RESULT").await.unwrap(), json!(42));
}

#[tokio::test]
async fn missing_module_reports_error_and_keeps_previous_scope() {
    let runner = PyRunner::new();
    runner.run("kept = 'still here'").await.unwrap();

    let message = py_error_message(runner.run_module("does_not_exist_xyz").await);

    assert!(message.contains("does_not_exist_xyz"), "{message}");
    assert_eq!(
        runner.read_variable("kept").await.unwrap(),
        json!("still here")
    );
}

#[tokio::test]
async fn module_raising_on_import_reports_error_and_keeps_scope() {
    let dir = tempfile::tempdir().unwrap();
    write_file(
        dir.path(),
        "broken_mod.py",
        "raise RuntimeError('boom on import')\n",
    );
    let runner = PyRunner::new();
    add_to_sys_path(&runner, dir.path()).await;
    runner.run("kept = 1").await.unwrap();

    let message = py_error_message(runner.run_module("broken_mod").await);

    assert!(message.contains("boom on import"), "{message}");
    assert_eq!(runner.read_variable("kept").await.unwrap(), json!(1));
}

#[tokio::test]
async fn native_stdlib_module_can_be_entry_point() {
    // `math` is a compiled extension in CPython and a native module in RustPython.
    let runner = PyRunner::new();

    runner.run_module("math").await.unwrap();

    assert_eq!(
        runner.call_function("sqrt", vec![16.into()]).await.unwrap(),
        json!(4.0)
    );
    let pi = runner.read_variable("pi").await.unwrap();
    assert!((pi.as_f64().unwrap() - std::f64::consts::PI).abs() < 1e-12);
}

#[tokio::test]
async fn module_is_imported_once_and_cached() {
    let dir = tempfile::tempdir().unwrap();
    write_file(
        dir.path(),
        "import_counter.py",
        "import builtins\nbuiltins.__dict__.setdefault('_async_py_imports', []).append(1)\n",
    );
    let runner = PyRunner::new();
    add_to_sys_path(&runner, dir.path()).await;

    runner.run_module("import_counter").await.unwrap();
    runner.run_module("import_counter").await.unwrap();

    assert_eq!(
        runner.eval("len(_async_py_imports)").await.unwrap(),
        json!(1)
    );
}

#[cfg(all(feature = "pyo3", not(feature = "rustpython")))]
#[tokio::test]
async fn pyc_only_module_can_be_loaded() {
    let dir = tempfile::tempdir().unwrap();
    let source = write_file(
        dir.path(),
        "src/compiled_mod.py",
        "def answer():\n    return 42\n",
    );
    let compiled = dir.path().join("compiled_mod.pyc");
    let runner = PyRunner::new();
    runner
        .run(&format!(
            "import py_compile\npy_compile.compile({}, cfile={}, doraise=True)",
            async_py::print_path_for_python(&source),
            async_py::print_path_for_python(&compiled)
        ))
        .await
        .unwrap();
    std::fs::remove_dir_all(dir.path().join("src")).unwrap();
    add_to_sys_path(&runner, dir.path()).await;

    runner.run_module("compiled_mod").await.unwrap();

    assert_eq!(
        runner.call_function("answer", vec![]).await.unwrap(),
        json!(42)
    );
    let origin = runner.eval("__file__").await.unwrap();
    assert!(origin.as_str().unwrap().ends_with(".pyc"), "{origin}");
}

#[test]
fn run_module_sync_works_without_runtime() {
    let dir = tempfile::tempdir().unwrap();
    write_file(dir.path(), "sync_mod.py", "VALUE = [1, 2, 3]\n");
    let runner = PyRunner::new();
    runner
        .run_sync(&format!(
            "import sys\nsys.path.insert(0, {})",
            async_py::print_path_for_python(&dir.path().to_path_buf())
        ))
        .unwrap();

    runner.run_module_sync("sync_mod").unwrap();

    assert_eq!(
        runner.read_variable_sync("VALUE").unwrap(),
        json!([1, 2, 3])
    );
}

#[tokio::test]
async fn run_module_sync_works_inside_runtime() {
    let dir = tempfile::tempdir().unwrap();
    write_file(dir.path(), "sync_mod_rt.py", "VALUE = 'ok'\n");
    let runner = PyRunner::new();
    add_to_sys_path(&runner, dir.path()).await;

    runner.run_module_sync("sync_mod_rt").unwrap();

    assert_eq!(
        runner.read_variable("VALUE").await.unwrap(),
        Value::from("ok")
    );
}
