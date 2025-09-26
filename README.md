# async_py

A Rust library for calling Python code asynchronously using `pyo3` and `tokio`.

This library provides a `PyRunner` that runs a Python interpreter in a dedicated background thread. Global variables will be kept and stored in this runner. 
You can send Python code to this executor from any async Rust task, and it will be executed without blocking your application.

Due to the Python global interpreter lock (GIL), there will not be any multithreading. If one python
task is performing a computation or a sleep, no other task can access the interpreter.

Using multiple `PyRunner` does not allow multithreading, due to the architecture 
of python. But you can isolate your global variables by using multiple runners.

## Usage

Add `async_py` to your `Cargo.toml`:

```toml
[dependencies]
async_py = { git = "..." } # Or path, or crates.io version
tokio = { version = "1", features = ["full"] }
```

### Example

This is how it is supposed to work.
The library is still work in progress and might not work yet.

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
