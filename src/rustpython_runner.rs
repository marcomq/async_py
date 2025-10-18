//  Async Python
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/async_py

use crate::{CmdType, PyCommand};
use rustpython::InterpreterConfig;
use rustpython_vm::{
    builtins::{PyBaseException, PyBool, PyDict, PyDictRef, PyFloat, PyInt, PyList, PyStr}, convert::ToPyObject, eval, function::ArgMapping, scope::Scope, AsObject, PyObjectRef, PyRef, PyResult, Settings, VirtualMachine
};
use serde_json::{json, Map, Number, Value};
use tokio::sync::{mpsc, oneshot};

/// The main loop for the RustPython thread.
pub(crate) async fn python_thread_main(mut receiver: mpsc::Receiver<PyCommand>) {
    // Create a single, shared sys.modules dictionary that is thread-safe.
    let (shared_sys_modules, shared_sys_path) = InterpreterConfig::new()
        .init_stdlib()
        .interpreter()
        .enter(|vm| {
            let modules = vm.sys_module.get_attr("modules", vm).unwrap().downcast::<PyDict>().unwrap();
            let path = vm.sys_module.get_attr("path", vm).unwrap().downcast::<PyList>().unwrap();
            (modules, path)
        });

    let interp = InterpreterConfig::new().init_stdlib().interpreter();
    let (scope, main_globals) = interp.enter(|vm| {
        // Replace the default sys.modules with our shared one.
        vm.sys_module.set_attr("modules", shared_sys_modules.clone().to_pyobject(vm), vm).unwrap();
        // Replace the default sys.path with our shared one.
        vm.sys_module.set_attr("path", shared_sys_path.clone().to_pyobject(vm), vm).unwrap();

        let scope = vm.new_scope_with_builtins();
        // Add current directory to path for local module imports in tests.
        vm.run_code_string(
            scope.clone(),
            "import sys; sys.path.append('./')",
            "<init>".into(),
        )
        .unwrap();
        (scope.clone(), scope.globals.clone())
    });
    while let Some(cmd) = receiver.recv().await {
        interp.enter(|vm| {
            let result = match &cmd.cmd_type {
                CmdType::RunCode(code) => vm
                    .run_code_string(scope.clone(), code, "<string, run>".to_owned())
                    .map(|_| Value::Null),
                CmdType::EvalCode(code) => eval::eval(vm, code, scope.clone(), "<string, eval>")
                    .and_then(|obj| py_to_json(vm, &obj)),
                CmdType::RunFile(file) => vm
                    .run_script(scope.clone(), file.to_str().unwrap())
                    .map(|_| Value::Null),
                CmdType::ReadVariable(var_name) => {
                    read_variable(vm, scope.clone(), var_name).and_then(|obj| py_to_json(vm, &obj))
                }
                CmdType::CallFunction { name, args } => {
                    call_function(vm, scope.clone(), name, args.clone())
                        .and_then(|obj| py_to_json(vm, &obj))
                }
                CmdType::CallAsyncFunction { name, args } => {
                    // We need to clone the globals from the current scope to pass to the async task.
                    tokio::spawn(handle_call_async_function(
                        main_globals.clone(),
                        name.clone(),
                        args.clone(),
                        shared_sys_modules.clone(),
                        shared_sys_path.clone(),
                        cmd.responder,
                    ));
                    // The response is sent async, so we can return early.
                    return;
                }
                CmdType::Stop => {
                    receiver.close();
                    return;
                }
            };
            let response = result.map_err(|err| {
                format!(
                    "Cannot apply cmd {:?}: {}",
                    cmd.cmd_type,
                    print_err_msg(err)
                )
            });
            let _ = cmd.responder.send(response);
        });
    }
}

fn read_variable(vm: &VirtualMachine, scope: Scope, var_name: &str) -> PyResult<PyObjectRef> {
    let parts: Vec<String> = var_name.split('.').map(|s| s.to_owned()).collect();
    let mut obj = scope.globals.get_item(&parts[0], vm)?;
    for part in parts.iter().skip(1) {
        obj = obj.get_attr(&vm.ctx.new_str(part.as_str()), vm)?;
    }
    Ok(obj)
}

fn call_function(vm: &VirtualMachine, scope: Scope, name: &str, args: Vec<Value>) -> PyResult {
    let func = read_variable(vm, scope, name)?;
    let py_args = args
        .into_iter()
        .map(|v| json_to_py(vm, v))
        .collect::<Vec<_>>();
    func.call(py_args, vm)
}

async fn handle_call_async_function(
    globals: PyDictRef,
    name: String,
    args: Vec<Value>,
    shared_sys_modules: PyDictRef,
    shared_sys_path: PyRef<PyList>,
    responder: oneshot::Sender<Result<Value, String>>,
) {
    // Each async task runs in its own interpreter to allow for concurrency.
    // They share the globals from the main interpreter's scope.
    let name_clone = name.clone();
    let result = tokio::task::spawn_blocking(move || {
        let mut settings = Settings::default();
        settings.path_list.push("Lib".to_owned());
        let interp = InterpreterConfig::new()
            .settings(settings)
            .init_stdlib()
            .interpreter();

        interp.enter(|vm| {
            // Use the globals from the main thread and the shared sys.modules.
            vm.sys_module.set_attr("modules", shared_sys_modules.to_pyobject(vm), vm).unwrap();
            // Use the shared sys.path.
            vm.sys_module.set_attr("path", shared_sys_path.to_pyobject(vm), vm).unwrap();
            // We create a new scope but use the globals from the main interpreter.
            // This gives us access to functions defined there.
            let scope = Scope::with_builtins(None, globals, vm);
            let result: PyResult<Value> = (|| {
                let asyncio_run = vm.import("asyncio", 0)?.get_attr("run", vm)?;

                let py_args: Vec<PyObjectRef> =
                    args.into_iter().map(|v| json_to_py(vm, v)).collect();

                let coroutine = read_variable(vm, scope.clone(), &name)?.call(py_args, vm)?;

                let result_obj = asyncio_run.call(vec![coroutine], vm)?;
                py_to_json(vm, &result_obj)
            })();
            result
        })
    })
    .await
    .unwrap(); // unwrap the JoinError

    let response = result.map_err(|err| {
        format!(
            "Cannot apply async function call '{}': {}",
            name_clone,
            print_err_msg(err)
        )
    });
    let _ = responder.send(response);
}

/// Converts a `serde_json::Value` to a `PyObjectRef`.
fn json_to_py(vm: &VirtualMachine, val: Value) -> PyObjectRef {
    match val {
        Value::Null => vm.ctx.none(),
        Value::Bool(b) => vm.ctx.new_bool(b).to_pyobject(vm),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                vm.ctx.new_int(i).to_pyobject(vm)
            } else if let Some(f) = n.as_f64() {
                vm.ctx.new_float(f).to_pyobject(vm)
            } else {
                vm.ctx.none()
            }
        }
        Value::String(s) => vm.ctx.new_str(s).to_pyobject(vm),
        Value::Array(a) => {
            let elements = a.into_iter().map(|v| json_to_py(vm, v)).collect();
            vm.ctx.new_list(elements).to_pyobject(vm)
        }
        Value::Object(o) => {
            let dict = vm.ctx.new_dict();
            for (k, v) in o {
                dict.set_item(&k, json_to_py(vm, v), vm).unwrap();
            }
            dict.to_pyobject(vm)
        }
    }
}

/// Converts a `PyObjectRef` to a `serde_json::Value`.
fn py_to_json(vm: &VirtualMachine, obj: &PyObjectRef) -> PyResult<Value> {
    if obj.is(&vm.ctx.none()) {
        return Ok(Value::Null);
    }
    if let Some(b) = obj.downcast_ref::<PyBool>() {
        return Ok(Value::Bool(b.is(&vm.ctx.new_bool(true))));
    }
    if let Some(i) = obj.downcast_ref::<PyInt>() {
        return match i.try_to_primitive::<i64>(vm) {
            Ok(n) => Ok(Value::Number(n.into())),
            Err(_) => {
                let f = i
                    .to_string()
                    .parse::<f32>()
                    .map_err(|_| vm.new_type_error("number cannot be converted".to_owned()))?;
                Ok(json!(f))
            }
        };
    }
    if let Some(f) = obj.downcast_ref::<PyFloat>() {
        return Ok(Value::Number(
            Number::from_f64(f.to_f64()).unwrap_or_else(|| Number::from(0)),
        ));
    }
    if let Some(s) = obj.downcast_ref::<PyStr>() {
        return Ok(Value::String(s.as_str().to_owned()));
    }
    if let Some(list) = obj.downcast_ref::<PyList>() {
        let vec = list
            .borrow_vec()
            .iter()
            .map(|item| py_to_json(vm, item))
            .collect::<PyResult<Vec<Value>>>()?;
        return Ok(Value::Array(vec));
    }
    if let Some(dict) = obj.downcast_ref::<PyDict>() {
        let mut map = Map::new();
        for (key, value) in dict {
            let key_str = key
                .downcast_ref::<PyStr>()
                .ok_or_else(|| vm.new_type_error("dict keys must be strings".to_owned()))?
                .as_str()
                .to_owned();
            map.insert(key_str, py_to_json(vm, &value)?);
        }
        return Ok(Value::Object(map));
    }
    // fallback for types that can convert to primitives
    if let Some(b) = obj.clone().try_into_value::<bool>(vm).ok() {
        return Ok(Value::Bool(b));
    }
    if let Some(i) = obj.clone().try_into_value::<i64>(vm).ok() {
        return Ok(Value::Number(i.into()));
    }
    if let Some(f) = obj.clone().try_into_value::<f64>(vm).ok() {
        return Ok(Value::Number(
            Number::from_f64(f).unwrap_or_else(|| Number::from(0)),
        ));
    }

    // Fallback for other types

    let fallback = obj.str(vm)?;
    Ok(Value::String(fallback.to_string()))
}

fn print_err_msg(error: PyRef<PyBaseException>) -> String {
    let msg = format!("{:?}", &error);
    println!("error: {}", &msg);
    if let Some(tb) = error.traceback() {
        println!("Traceback (most recent call last):");
        for trace in tb.iter() {
            let file = trace.frame.code.source_path.as_str();
            let original_line = trace.lineno.to_usize();
            let line = if file == "main.py" {
                original_line - 2 // sys.path import has 2 additional lines
            } else {
                original_line
            };
            println!(
                "  File \"{file}\", line {line}, in {}",
                trace.frame.code.obj_name
            );
        }
    }
    msg
}
