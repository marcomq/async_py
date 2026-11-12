//! Which Python libraries are usable, and how external packages are wired in.
mod common;

use async_py::PyRunner;
use common::*;
use serde_json::json;

/// Native modules that RustPython ships without needing a Python stdlib directory.
/// `array` is excluded: RustPython's native `array` imports `collections.abc` on load.
const NATIVE_MODULES: &[&str] = &[
    "sys",
    "math",
    "time",
    "itertools",
    "binascii",
    "zlib",
    "cmath",
    "unicodedata",
    "_thread",
    "_json",
    "_random",
    "_struct",
    "_sre",
];

/// Pure-Python and extension stdlib modules commonly needed by applications.
#[cfg(all(feature = "pyo3", not(feature = "rustpython")))]
const CPYTHON_STDLIB_MODULES: &[&str] = &[
    "json",
    "re",
    "collections",
    "dataclasses",
    "enum",
    "typing",
    "datetime",
    "decimal",
    "fractions",
    "statistics",
    "random",
    "hashlib",
    "hmac",
    "secrets",
    "base64",
    "struct",
    "os",
    "pathlib",
    "io",
    "csv",
    "sqlite3",
    "zipfile",
    "gzip",
    "tempfile",
    "shutil",
    "pickle",
    "copy",
    "logging",
    "threading",
    "queue",
    "asyncio",
    "concurrent.futures",
    "socket",
    "ssl",
    "urllib.request",
    "http.client",
    "email",
    "xml.etree.ElementTree",
    "uuid",
    "subprocess",
    "ctypes",
    "multiprocessing",
    "unittest",
];

#[tokio::test]
async fn native_modules_import() {
    assert_all_import(NATIVE_MODULES).await;
}

async fn assert_all_import(modules: &[&str]) {
    let runner = PyRunner::new();
    let mut failures = Vec::new();
    for module in modules {
        if let Err(err) = runner.run(&format!("import {module}")).await {
            failures.push(format!("{module}: {err}"));
        }
    }
    assert!(
        failures.is_empty(),
        "failed imports:\n{}",
        failures.join("\n")
    );
}

#[cfg(all(feature = "pyo3", not(feature = "rustpython")))]
#[tokio::test]
async fn cpython_stdlib_modules_import() {
    assert_all_import(CPYTHON_STDLIB_MODULES).await;
}

#[cfg(all(feature = "pyo3", not(feature = "rustpython")))]
#[tokio::test]
async fn cpython_stdlib_smoke_usage() {
    let runner = PyRunner::new();
    runner
        .run(
            r#"
# dataclasses reads sys.modules["typing"]; importing typing first waits for concurrent imports.
import typing
import json, sqlite3, hashlib, dataclasses, datetime, decimal, re

@dataclasses.dataclass
class Point:
    x: int
    y: int

db = sqlite3.connect(":memory:")
db.execute("create table t (v integer)")
db.executemany("insert into t values (?)", [(1,), (2,), (3,)])

results = {
    "json": json.loads(json.dumps({"a": [1, 2]})),
    "sqlite": db.execute("select sum(v) from t").fetchone()[0],
    "sha256": hashlib.sha256(b"abc").hexdigest()[:8],
    "dataclass": dataclasses.asdict(Point(1, 2)),
    "date": datetime.date(2024, 2, 29).isoformat(),
    "decimal": str(decimal.Decimal("0.1") + decimal.Decimal("0.2")),
    "regex": re.findall(r"\d+", "a1b22c333"),
}
"#,
        )
        .await
        .unwrap();

    assert_eq!(
        runner.read_variable("results").await.unwrap(),
        json!({
            "json": {"a": [1, 2]},
            "sqlite": 6,
            "sha256": "ba7816bf",
            "dataclass": {"x": 1, "y": 2},
            "date": "2024-02-29",
            "decimal": "0.3",
            "regex": ["1", "22", "333"],
        })
    );
}

/// Pure-Python stdlib modules embedded into the binary for RustPython.
#[cfg(feature = "rustpython")]
const RUSTPYTHON_STDLIB_MODULES: &[&str] = &[
    "json",
    "re",
    "collections",
    "dataclasses",
    "enum",
    "typing",
    "inspect",
    "datetime",
    "decimal",
    "fractions",
    "random",
    "base64",
    "os",
    "pathlib",
    "io",
    "csv",
    "copy",
    "logging",
    "threading",
    "asyncio",
    "urllib.parse",
    "email",
];

#[cfg(feature = "rustpython")]
#[tokio::test]
async fn rustpython_stdlib_modules_import() {
    assert_all_import(RUSTPYTHON_STDLIB_MODULES).await;
}

/// The stdlib is frozen into the binary, so it neither depends on nor picks up
/// a (version-incompatible) CPython install of the host.
#[cfg(feature = "rustpython")]
#[tokio::test]
async fn rustpython_stdlib_is_embedded() {
    let runner = PyRunner::new();
    runner
        .run(
            r#"
import json, typing, codecs
results = {
    "origin": json.__spec__.origin,
    "typing_origin": typing.__spec__.origin,
    "latin1": "\u00e4".encode("latin-1").hex(),
    "cp1252": codecs.lookup("cp1252").name,
}
"#,
        )
        .await
        .unwrap();
    assert_eq!(
        runner.read_variable("results").await.unwrap(),
        json!({
            "origin": "frozen",
            "typing_origin": "frozen",
            "latin1": "e4",
            "cp1252": "cp1252",
        })
    );
}

async fn site_packages_for(runner: &PyRunner, venv: &std::path::Path) -> std::path::PathBuf {
    if cfg!(target_os = "windows") {
        return venv.join("Lib").join("site-packages");
    }
    let version = runner
        .eval("f'python{__import__(\"sys\").version_info.major}.{__import__(\"sys\").version_info.minor}'")
        .await
        .unwrap();
    venv.join("lib")
        .join(version.as_str().unwrap())
        .join("site-packages")
}

#[tokio::test]
async fn set_venv_makes_packages_importable() {
    let runner = PyRunner::new();
    let venv = tempfile::tempdir().unwrap();
    let site_packages = site_packages_for(&runner, venv.path()).await;
    std::fs::create_dir_all(&site_packages).unwrap();
    write_file(
        &site_packages,
        "venv_pkg/__init__.py",
        "from .core import VALUE\n",
    );
    write_file(&site_packages, "venv_pkg/core.py", "VALUE = 'from venv'\n");

    runner.set_venv(venv.path()).await.unwrap();
    runner.run("import venv_pkg").await.unwrap();

    assert_eq!(
        runner.eval("venv_pkg.VALUE").await.unwrap(),
        json!("from venv")
    );
}

#[tokio::test]
async fn set_venv_then_run_module_from_venv() {
    let runner = PyRunner::new();
    let venv = tempfile::tempdir().unwrap();
    let site_packages = site_packages_for(&runner, venv.path()).await;
    std::fs::create_dir_all(&site_packages).unwrap();
    write_file(&site_packages, "venv_app/__init__.py", "");
    write_file(
        &site_packages,
        "venv_app/main.py",
        "def handler(x):\n    return {'handled': x}\n",
    );

    runner.set_venv(venv.path()).await.unwrap();
    runner.run_module("venv_app.main").await.unwrap();

    assert_eq!(
        runner
            .call_function("handler", vec![1.into()])
            .await
            .unwrap(),
        json!({"handled": 1})
    );
}

/// Editable installs (`pip install -e`, `uv sync` for workspace members) are wired in
/// through `.pth` files, which only the PyO3 backend processes.
#[cfg(all(feature = "pyo3", not(feature = "rustpython")))]
#[tokio::test]
async fn set_venv_processes_pth_files() {
    let runner = PyRunner::new();
    let venv = tempfile::tempdir().unwrap();
    let editable_src = tempfile::tempdir().unwrap();
    let site_packages = site_packages_for(&runner, venv.path()).await;
    std::fs::create_dir_all(&site_packages).unwrap();
    write_file(
        editable_src.path(),
        "editable_pkg.py",
        "NAME = 'editable'\n",
    );
    write_file(
        &site_packages,
        "_editable.pth",
        &format!("# comment\n\n{}\n", editable_src.path().display()),
    );

    runner.set_venv(venv.path()).await.unwrap();
    runner.run("import editable_pkg").await.unwrap();

    assert_eq!(
        runner.eval("editable_pkg.NAME").await.unwrap(),
        json!("editable")
    );
}

#[test]
fn set_venv_sync_makes_packages_importable() {
    let runner = PyRunner::new();
    let venv = tempfile::tempdir().unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let site_packages = rt.block_on(site_packages_for(&runner, venv.path()));
    std::fs::create_dir_all(&site_packages).unwrap();
    write_file(&site_packages, "venv_sync_pkg.py", "VALUE = 3\n");

    runner.set_venv_sync(venv.path()).unwrap();
    runner.run_sync("import venv_sync_pkg").unwrap();

    assert_eq!(runner.eval_sync("venv_sync_pkg.VALUE").unwrap(), json!(3));
}
