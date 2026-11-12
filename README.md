# async_py

A Rust library for calling Python code asynchronously using [pyo3](https://github.com/PyO3/pyo3) and [tokio](https://tokio.rs/).

This library provides a `PyRunner` that runs a Python interpreter in a dedicated 
background thread. Global variables are preserved within the runner's scope. 
You can send Python code to this executor from any async Rust task, 
and it will be executed without blocking your application.

Due to the Python Global Interpreter Lock (GIL), you cannot run Python code simultaneously. If one Python
task is performing a computation or a sleep, no other task can access the interpreter.

Using multiple `PyRunner` instances does not enable simultaneous Python code execution due to the GIL. 
However, it does allow you to isolate global variables between different runners.

## Usage

Add `async_py` to your `Cargo.toml`:

```toml
[dependencies]
async_py = "0.4"
serde_json = "1"
tokio = { version = "1", features = ["full"] }
```

### Example

```rust
use async_py::PyRunner;
use serde_json::Value;

#[tokio::main]
async fn main() {
    let runner = PyRunner::new();

    let code = r#"
counter = 0
def greet(name):
    global counter
    counter = counter + 1
    s = "" if counter < 2 else "s"
    return f"Hello {name}! Called {counter} time{s} from Python."
"#;
    // Python function "greet" and variable "counter" is added to globals
    runner.run(code).await.unwrap();

    // Calling code
    let result1 = runner.call_function("greet", vec!["World".into()]).await.unwrap();
    println!("{}", result1.as_str().unwrap()); // Prints: Hello World! Called 1 time from Python.
    let result2 = runner.call_function("greet", vec!["World".into()]).await.unwrap();
    println!("{}", result2.as_str().unwrap()); // Prints: Hello World! Called 2 times from Python.
}
```

### Async Python Example

`PyRunner` can call regular Python functions with `call_function` and Python
coroutines with `call_async_function`. Use the async call for functions declared
with `async def`; calling them through `call_function` returns a `PyError`
instead of the un-awaited coroutine.

`call_async_function` awaits whatever the function returns: a coroutine, an
`asyncio.Future`/`Task` (e.g. from `asyncio.ensure_future`), or a
`concurrent.futures.Future` (e.g. from `ThreadPoolExecutor.submit`). The event
loop is set before the function is called, so futures it creates via
`asyncio.get_event_loop()` belong to the loop that awaits them.

The Python interpreter still runs on one dedicated worker thread. Normal CPU-bound
Python code is serialized by that worker and by the Python GIL. Python coroutines
can make progress while they are awaiting, so multiple calls to async Python
functions can overlap during `await` points such as `asyncio.sleep`, network I/O,
or other cooperative async operations.

```rust
use async_py::PyRunner;
use serde_json::Value;

#[tokio::main]
async fn main() {
    let runner = PyRunner::new();
    let code = r#"
import asyncio
counter = 0

async def add_and_sleep(a, b, sleep_time):
    global counter
    await asyncio.sleep(sleep_time)
    counter += 1
    return a + b + counter
"#;

    runner.run(code).await.unwrap();
    let result1 = runner.call_async_function("add_and_sleep", vec![5.into(), 10.into(), 1.into()]);
    let result2 = runner.call_async_function("add_and_sleep", vec![5.into(), 10.into(), 0.1.into()]);
    let (result1, result2) = tokio::join!(result1, result2);
    assert_eq!(result1.unwrap(), Value::Number(17.into()));
    assert_eq!(result2.unwrap(), Value::Number(16.into()));
}
```
Both function calls are triggered to run async code at the same time. While the first call waits for the sleep,
the second can already start and also increment the counter first. Therefore,
result1 will wait longer and compute 5 + 10 + 2, while the result2 can compute 5 + 10 + 1.

Each `call_async_function` call creates and drives its own Python event loop.
That keeps calls independent and easy to use from Rust async code, but it is not
the same as a shared long-running Python event loop. If you need shared Python
async state, keep it in Python globals or wrap the behavior in a Python function.

`call_async_function` is part of the public API for both backends, but only the
default PyO3 backend provides runtime support for awaiting Python coroutines.
With the RustPython backend, calling `call_async_function` returns an unsupported
error at runtime.

### Thread Safety

The `PyRunner` is designed to be safely shared across multiple threads.

You can clone a `PyRunner` instance and move it into different threads. All commands sent from any clone are funneled through a channel to a single, dedicated OS thread that houses the Python interpreter. This design correctly handles Python's Global Interpreter Lock (GIL) by ensuring that only one Python operation is attempted at any given time within that interpreter instance.

Here is an example of using `PyRunner` from multiple `std::thread`s:

```rust
use async_py::{PyRunner, PyRunnerError};
use std::thread;

fn main() -> Result<(), PyRunnerError> {
    let runner = PyRunner::new();
    runner.run_sync("x = 0")?;

    let mut handles = vec![];

    for i in 0..5 {
        let runner_clone = runner.clone();
        let handle = thread::spawn(move || {
            // Use the _sync versions for non-async contexts
            runner_clone.run_sync(&format!("x += {}", i)).unwrap();
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let final_x = runner.read_variable_sync("x")?;
    // Expected: 0 + 1 + 2 + 3 + 4 = 10
    assert_eq!(final_x, Value::Number(10.into()));

    runner.stop_sync()?;
    Ok(())
}
```

### Using a venv
It is generally recommended to use a venv to install pip packages.
While you cannot switch the interpreter version with this crate, you can use an
existing venv to load installed packages.

```rust
let runner = PyRunner::new();
runner.set_venv(&Path::new("/path/to/venv")).await?;
```

`set_venv` does not activate a shell environment or replace the Python executable.
It adds the venv's `site-packages` directory to the runner's `sys.path`, which is
enough for imports from that environment.

This works well with [`uv`](https://docs.astral.sh/uv/). `uv venv` creates a
`.venv` directory by default, and `uv sync` keeps that environment populated from
your Python project metadata. From Rust, point `async_py` at the same directory:

```rust
use async_py::PyRunner;
use std::path::Path;

# async fn example() -> Result<(), async_py::PyRunnerError> {
let runner = PyRunner::new();
runner.set_venv(Path::new(".venv")).await?;
runner.run("import your_python_dependency").await?;
# Ok(())
# }
```

For a Python project managed by `uv`, a typical setup is:

```sh
uv init
uv add numpy
uv sync
```

Then call `set_venv(".venv")` before importing those packages. If you configure
`uv` to use a different project environment path, pass that path instead.

### Using a module as entry point

Instead of executing a `.py` file with `run_file`, you can import a module and use
its namespace as the runner's global scope with `run_module`:

```rust
use async_py::PyRunner;
use std::path::Path;

# async fn example() -> Result<(), async_py::PyRunnerError> {
let runner = PyRunner::new();
runner.set_venv(Path::new(".venv")).await?;
runner.run_module("my_app.backend").await?;
let reply = runner.call_function("handle", vec!["ping".into()]).await?;
# Ok(())
# }
```

The module is loaded through Python's normal import system, so it must be on
`sys.path` (for example installed into the venv, or added with
`runner.run("import sys; sys.path.insert(0, '...')")`). This makes it possible to
use compiled entry points directly, such as Cython or `nuitka --module` extensions
(`.so`/`.pyd`) or `.pyc`-only packages, without a `.py` stub.

- `call_function`, `call_async_function`, `read_variable`, `eval` and `run` then
  operate on the module's namespace. Changes made by the module's own functions
  (e.g. via `global`) are visible to `read_variable`.
- The previous global scope is replaced; names defined earlier with `run` are no
  longer visible. If the import fails, the previous scope is kept.
- A module that was already imported is reused from `sys.modules` and not
  executed again.
- With the RustPython backend only pure Python modules can be loaded; CPython
  extension modules (`.so`/`.pyd`) are not supported.

### Value conversion

Arguments and results are exchanged as `serde_json::Value`:

| Python                         | `serde_json::Value`           |
|--------------------------------|-------------------------------|
| `None`                         | `Null`                        |
| `bool`                         | `Bool`                        |
| `int` (fits in `i64`), `float` | `Number`                      |
| `str`                          | `String`                      |
| `list`, `tuple`, `set`, `frozenset` | `Array`                  |
| `dict`                         | `Object`                      |
| anything else                  | `String` with the object's `str()` |

Subclasses (e.g. `IntEnum`, `OrderedDict`, named tuples) convert like their base
type. Sets have no defined order. Bytes are returned as their `str()`. Values nested deeper than 512 levels, including circular
references such as a list that contains itself, are reported as a `PyError`
instead of being converted.

### Shipping a bundled Python (PyO3)

With PyO3 the binary links the `libpython` found at build time. To ship an
application that doesn't depend on a Python installed on the target machine,
bundle a relocatable interpreter such as
[python-build-standalone](https://github.com/astral-sh/python-build-standalone)
and tell CPython where it is before the first `PyRunner` is created:

```rust
use async_py::{init_python_home, is_python_home, PyRunner, PyRunnerError};
use std::path::Path;

async fn start_runner() -> Result<PyRunner, PyRunnerError> {
    let home = Path::new("resources/python");
    if is_python_home(home) {
        init_python_home(home)?;
    }
    Ok(PyRunner::new())
}
```

Without this, a relocated CPython can't find its stdlib, because it derives
`sys.prefix` from its executable or its build prefix. `init_python_home` returns
`Ok(false)` if Python was already initialized. The binary must also find
`libpython` at runtime, e.g. via an rpath (`@executable_path/...` on macOS,
`$ORIGIN/...` on Linux) or by placing `python3XX.dll` next to the executable on
Windows.

### rustpython
PyO3 has usually the best compatibility for python packages.
But if you want to use python on android, wasm, or if you don't want to use any
external library, you can use [rustpython](https://github.com/RustPython/RustPython).

The pure-Python part of the stdlib (`json`, `typing`, `asyncio`, `dataclasses`,
`pathlib`, `logging`, ...) is compiled into the binary, so no Python
installation is needed or used. Modules that need CPython C extensions, such as
`sqlite3` or `ssl`, as well as third-party C extension packages, are not
available. `call_async_function` is not supported yet.

Cargo.toml
```toml
[dependencies]
async_py = { version = "0.4", default-features = false, features = ["rustpython"] }
```

`rustpython-stdlib` 0.5 pulls in `malachite-bigint` twice (0.9 directly and 0.14
via `pymath`) on a fresh lockfile, which fails to compile with type mismatches.
Pin the second copy to 0.9.2 once after adding the dependency:

```sh
cargo update -p malachite-bigint@0.14.0 --precise 0.9.2
```

### Comparison with inline-python

[`inline-python`](https://docs.rs/inline-python/) is a good fit when you want to
write small Python snippets directly inside Rust source with the `python!` macro.
It also has a reusable `Context` for sharing globals between macro invocations.

`async_py` is aimed at a different workflow:

- Python code is provided as strings or files at runtime, not as Rust-tokenized
  macro input.
- Calls are async-friendly from Rust: commands are sent to a dedicated worker
  thread and awaited through Tokio.
- A `PyRunner` can be cloned and shared across Rust tasks or threads while still
  serializing interpreter access safely.
- It has explicit helpers for calling named Python functions, reading globals,
  running files, and adding venv/`uv`-managed dependencies to `sys.path`.
- It offers an optional RustPython backend for targets where embedding CPython
  through PyO3 is not desirable.

Use `inline-python` when compile-time inline snippets and macro ergonomics are
the main goal. Use `async_py` when Python execution should be driven from async
Rust code, loaded from runtime strings/files, shared through a runner, or wired
to a project venv.
