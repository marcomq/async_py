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
async_py = "0.2.0"
tokio = { version = "1", features = ["full"] }
```

### Example

```rust
use async_py::PyRunner;

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
### Using a venv
It is generally recommended to use a venv to install pip packages.
While you cannot switch the interpreter version with this crate, you can use an
existing venv to load installed packages.

```rust
let runner = PyRunner::new();
runner.set_venv(&Path::new("/path/to/venv")).await?;
```

### rustpython
PyO3 has usually the best compatibility for python packages.
But if you want to use python on android, wasm, or if you don't want to use any
external library, you can use [rustpython](https://github.com/RustPython/RustPython).
Keep in mind that some essential packages like `os` are missing on rustpython.

Cargo.toml
```toml
[dependencies]
async_py = { features = ["rustpython"] } 
```