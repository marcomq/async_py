//  Async Python
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/async_py

use crate::{CmdType, PyCommand};
use rustpython::InterpreterConfig;
use rustpython_vm::{
    builtins::{PyBaseException, PyBool, PyDict, PyDictRef, PyFloat, PyInt, PyList, PyStr},
    convert::ToPyObject,
    eval,
    scope::Scope,
    AsObject, PyObjectRef, PyRef, PyResult, VirtualMachine,
};
use serde_json::{json, Map, Number, Value};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot, Notify};

/// Holds the Python objects related to the async infrastructure.
struct AsyncPyState {
    loop_obj: PyObjectRef,
    make_callback_fn: PyObjectRef,
}

/// The main loop for the RustPython thread.
///
/// This function implements a sophisticated single-interpreter model for concurrency.
/// It spawns a dedicated Python thread that runs an `asyncio` event loop.
/// Asynchronous Python function calls from Rust are scheduled onto this single,
/// persistent event loop using `run_coroutine_threadsafe`.
///
/// A `tokio::sync::Notify` channel is used as a signaling mechanism to efficiently
/// wake up the Rust `select!` loop only when results are available in the Python-side queue,
/// avoiding the need for polling.
pub(crate) async fn python_thread_main(mut receiver: mpsc::Receiver<PyCommand>) {
    let interp = InterpreterConfig::new().init_stdlib().interpreter();
    // pending responders (id -> oneshot sender)
    let mut pending: HashMap<usize, oneshot::Sender<Result<Value, String>>> = HashMap::new();
    let mut next_id: usize = 1;
    let mut async_state: Option<AsyncPyState> = None;

    // Create a notifier that the Python thread can use to wake up the Rust select! loop.
    let notify = std::sync::Arc::new(Notify::new());
    let rust_notify_fn = {
        let notify = notify.clone();
        move || {
            notify.notify_one();
        }
    };

    let (scope, main_globals) = interp.enter(|vm| {
        let scope = vm.new_scope_with_builtins();
        // Add current directory to path for local module imports in tests.
        vm.run_code_string(
            scope.clone(),
            "import sys; sys.path.append('./')",
            "<init>".into(),
        )
        .unwrap();

        // Add the native rust_notify function to Python globals.
        scope
            .globals
            .set_item(
                "_rust_notify",
                vm.new_function("_rust_notify", rust_notify_fn)
                    .to_pyobject(vm),
                vm,
            )
            .unwrap();

        (scope.clone(), scope.globals.clone())
    });

    loop {
        tokio::select! {
            // Branch 1: Wait for a new command from the channel.
            Some(cmd) = receiver.recv() => {
                if let CmdType::Stop = cmd.cmd_type {
                    receiver.close();
                } else {
                    handle_command(&interp, &scope, &mut async_state, &mut pending, &mut next_id, cmd);
                }
            },
            // Branch 2: Wait for a notification from Python that a result is ready.
            // The `if !pending.is_empty()` guard ensures this branch is only
            // active when there are outstanding async tasks.
            _ = notify.notified(), if !pending.is_empty() => {
                handle_notification(&interp, &main_globals, &mut pending);
            }
            // If the receiver is closed and there are no more pending tasks, exit.
            else => {
                if receiver.is_closed() && pending.is_empty() {
                    break;
                }
            }
        }
    }
}

/// Sets up the asyncio event loop and related infrastructure in the Python interpreter.
/// This is called on-demand when the first async function is executed.
fn setup_async_infrastructure(
    vm: &VirtualMachine,
    scope: &Scope,
) -> PyResult<AsyncPyState> {
    // Initialize a dedicated asyncio loop, a result queue and helper callbacks
    // in the Python interpreter. The loop will be run in a separate Python
    // thread so that run_coroutine_threadsafe can schedule coroutines onto it.
    vm.run_code_string(
        scope.clone(),
        r#"
import asyncio, threading, queue, traceback
_loop = asyncio.new_event_loop()
def _run_loop():
    asyncio.set_event_loop(_loop)
    _loop.run_forever()
_thread = threading.Thread(target=_run_loop, daemon=True)
_thread.start()
_result_queue = queue.Queue()
def _async_done_cb(fut, id):
    try:
        res = fut.result()
        _result_queue.put({'id': id, 'ok': True, 'payload': res})
        _rust_notify()
    except Exception as e:
        tb = traceback.format_exception_only(type(e), e)
        _result_queue.put({'id': id, 'ok': False, 'payload': ''.join(tb)})
def _make_callback(id):
    def _cb(fut):
        _async_done_cb(fut, id)
    return _cb
"#,
        "<init_loop>".into(),
    )?;

    let globals = &scope.globals;
    let loop_obj = globals.get_item("_loop", vm)?;
    let make_callback_fn = globals.get_item("_make_callback", vm)?;
    Ok(AsyncPyState { loop_obj, make_callback_fn })
}

/// Processes a command received from the Rust side.
fn handle_command(
    interp: &rustpython_vm::Interpreter,
    scope: &Scope,
    async_state: &mut Option<AsyncPyState>,
    pending: &mut HashMap<usize, oneshot::Sender<Result<Value, String>>>,
    next_id: &mut usize,
    cmd: PyCommand,
) {
    interp.enter(|vm| {
        match cmd.cmd_type {
            CmdType::RunCode(code) => {
                let result = vm
                    .run_code_string(scope.clone(), &code, "<string, run>".to_owned())
                    .map(|_| Value::Null)
                    .map_err(print_err_msg);
                let _ = cmd.responder.send(result);
            }
            CmdType::EvalCode(code) => {
                let result = eval::eval(vm, &code, scope.clone(), "<string, eval>")
                    .and_then(|obj| py_to_json(vm, &obj))
                    .map_err(print_err_msg);
                let _ = cmd.responder.send(result);
            }
            CmdType::RunFile(file) => {
                let result = vm
                    .run_script(scope.clone(), file.to_str().unwrap())
                    .map(|_| Value::Null)
                    .map_err(print_err_msg);
                let _ = cmd.responder.send(result);
            }
            CmdType::ReadVariable(var_name) => {
                let result = read_variable(vm, scope.clone(), &var_name)
                    .and_then(|obj| py_to_json(vm, &obj))
                    .map_err(print_err_msg);
                let _ = cmd.responder.send(result);
            }
            CmdType::CallFunction { name, args } => {
                let result = call_function(vm, scope.clone(), &name, args)
                    .and_then(|obj| py_to_json(vm, &obj))
                    .map_err(print_err_msg);
                let _ = cmd.responder.send(result);
            }
            CmdType::CallAsyncFunction { name, args } => {
                let id = *next_id;
                *next_id += 1;

                // Initialize async infrastructure on first use.
                if async_state.is_none() {
                    match setup_async_infrastructure(vm, scope) {
                        Ok(state) => *async_state = Some(state),
                        Err(e) => {
                            let _ = cmd.responder.send(Err(print_err_msg(e)));
                            return;
                        }
                    }
                }

                let state = async_state.as_ref().unwrap();
                pending.insert(id, cmd.responder); // Must insert before calling async.
                if let Err(e) = handle_call_async_function(
                    vm,
                    scope.clone(),
                    state,
                    id,
                    &name,
                    args,
                ) {
                    if let Some(tx) = pending.remove(&id) {
                        let _ = tx.send(Err(e));
                    }
                }
                // Response will be sent asynchronously via the queue; return.
            }
            CmdType::Stop => {
                // This case is handled in the select! loop to close the receiver.
            }
        }
    });
}

/// Drains the Python-side result queue and completes any pending responders.
fn handle_notification(
    interp: &rustpython_vm::Interpreter,
    main_globals: &PyDictRef,
    pending: &mut HashMap<usize, oneshot::Sender<Result<Value, String>>>,
) {
    interp.enter(|vm| {
        if let Ok(rq) = main_globals.get_item("_result_queue", vm) {
            if let Ok(get_nowait) = rq.get_attr("get_nowait", vm) {
                while let Ok(item) = get_nowait.call(vec![], vm) {
                    if let Some(dict) = item.downcast_ref::<PyDict>() {
                        let id_obj = dict.get_item("id", vm).unwrap();
                        let ok_obj = dict.get_item("ok", vm).unwrap();
                        let payload_obj = dict.get_item("payload", vm).unwrap();
                        let id = id_obj.clone().try_into_value::<i64>(vm).ok().unwrap_or(0) as usize;
                        if let Some(tx) = pending.remove(&id) {
                            let ok = ok_obj.clone().try_into_value::<bool>(vm).ok().unwrap_or(false);
                            let res = if ok { py_to_json(vm, &payload_obj).map_err(print_err_msg) } else { Err(payload_obj.str(vm).map(|s| s.to_string()).unwrap_or_else(|_| "<error>".to_string())) };
                            let _ = tx.send(res);
                        }
                    }
                }
            }
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

fn handle_call_async_function(
    vm: &VirtualMachine,
    scope: Scope,
    async_state: &AsyncPyState,
    id: usize,
    name: &str,
    args: Vec<Value>,
) -> Result<(), String> {
    // Build the coroutine and schedule it on the dedicated loop via
    // asyncio.run_coroutine_threadsafe. Then attach a done-callback
    // that will put the result into `_result_queue` with the id.
    let func = read_variable(vm, scope.clone(), name)
        .map_err(|e| format!("Cannot find function {}: {}", name, print_err_msg(e)))?;

    let py_args = args
        .iter()
        .map(|v| json_to_py(vm, v.clone()))
        .collect::<Vec<_>>();
    let coroutine = func
        .call(py_args, vm)
        .map_err(|err| format!("Cannot call function {}: {}", name, print_err_msg(err)))?;

    // call asyncio.run_coroutine_threadsafe(coroutine, loop)
    let asyncio = vm
        .import("asyncio", 0)
        .map_err(|err| format!("Cannot import asyncio: {}", print_err_msg(err)))?;
    let run_threadsafe = asyncio
        .get_attr("run_coroutine_threadsafe", vm)
        .map_err(|err| {
            format!(
                "asyncio.run_coroutine_threadsafe not available: {}",
                print_err_msg(err)
            )
        })?;

    // Call run_coroutine_threadsafe(coroutine, loop_obj)
    let loop_obj = async_state.loop_obj.clone();
    let fut = run_threadsafe
        .call(vec![coroutine, loop_obj.clone().to_pyobject(vm)], vm)
        .map_err(|err| format!("Failed to schedule coroutine: {}", print_err_msg(err)))?;

    // Create callback using _make_callback(id)
    let make_cb = async_state.make_callback_fn.clone();
    let cb = make_cb
        .call(vec![vm.ctx.new_int(id as i64).to_pyobject(vm)], vm)
        .map_err(|err| format!("Failed to create callback: {}", print_err_msg(err)))?;

    // Attach the callback: fut.add_done_callback(cb)
    if let Ok(add_done) = fut.get_attr("add_done_callback", vm) {
        if let Err(err) = add_done.call(vec![cb], vm) {
            return Err(format!(
                "Failed to add done callback: {}",
                print_err_msg(err)
            ));
        }
    } else {
        return Err("fut.add_done_callback not found".to_string());
    }

    Ok(())
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
