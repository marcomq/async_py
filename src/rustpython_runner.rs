//  Async Python
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/async_py

use crate::{CmdType, PyCommand};
use rustpython_vm::{
    builtins::{PyBaseException, PyBool, PyDict, PyFloat, PyInt, PyList, PyStr},
    convert::ToPyObject,
    eval,
    scope::Scope,
    AsObject, Interpreter, PyObjectRef, PyRef, PyResult, Settings, VirtualMachine,
};
use serde_json::{json, Map, Number, Value};
use tokio::sync::mpsc;

/// The main loop for the RustPython thread.
pub(crate) fn python_thread_main(mut receiver: mpsc::Receiver<PyCommand>) {
    let mut settings = Settings::default();
    settings.path_list.push("Lib".to_owned());
    let interp = Interpreter::with_init(settings, |vm| {
        vm.add_native_modules(rustpython_stdlib::get_module_inits());
    });

    interp.enter(|vm| {
        let scope = vm.new_scope_with_builtins();
        vm.run_code_string(
            scope.clone(),
            "import sys; sys.path.append('./')",
            "<init>".into(),
        )
        .unwrap();
        while let Some(cmd) = receiver.blocking_recv() {
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
                CmdType::Stop => break,
            };
            let response = result.map_err(|err| {
                format!(
                    "Cannot apply cmd {:?}: {}",
                    cmd.cmd_type,
                    print_err_msg(err)
                )
            });
            let _ = cmd.responder.send(response);
        }
    });
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
