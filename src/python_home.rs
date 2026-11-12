//  Async Python
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/async_py

//! Running a bundled, relocatable CPython (e.g. python-build-standalone).
//!
//! CPython derives `sys.prefix`, and with it the stdlib location, from the
//! executable path or its build prefix. An interpreter shipped inside an
//! application bundle therefore can't find its stdlib on its own. Call
//! [`init_python_home`] before the first [`PyRunner`](crate::PyRunner) is
//! created to point it to the bundled installation.

use crate::PyRunnerError;
use pyo3::ffi;
use std::{ffi::CStr, path::Path};

/// Returns whether `dir` looks like a Python installation root, i.e. contains
/// the stdlib (`lib/python3.X/os.py` on Unix, `Lib/os.py` on Windows).
pub fn is_python_home(dir: &Path) -> bool {
    let unix_stdlib = dir.join("lib").read_dir().ok().is_some_and(|entries| {
        entries.flatten().any(|e| {
            e.file_name().to_string_lossy().starts_with("python3")
                && e.path().join("os.py").is_file()
        })
    });
    unix_stdlib || dir.join("Lib").join("os.py").is_file()
}

/// Initializes the embedded CPython with `home` as `PYTHONHOME`.
///
/// Must run before the first `PyRunner` is created or Python is otherwise
/// used in this process. Returns `Ok(false)` without changes if Python is
/// already initialized, as the home can't be changed anymore. Signal handlers
/// are not installed, as the host application owns them.
///
/// # Errors
///
/// Returns [`PyRunnerError::PyError`] if CPython can't start with that home,
/// e.g. because the stdlib is missing.
pub fn init_python_home(home: &Path) -> Result<bool, PyRunnerError> {
    let wide_home = to_wide(home);
    // SAFETY: plain CPython initialization sequence on a zeroed config that is
    // initialized by `PyConfig_InitPythonConfig` and cleared afterwards; the GIL
    // is released at the end so the runner's worker thread can attach.
    unsafe {
        if ffi::Py_IsInitialized() != 0 {
            return Ok(false);
        }
        let mut config: ffi::PyConfig = std::mem::zeroed();
        ffi::PyConfig_InitPythonConfig(&mut config);
        config.install_signal_handlers = 0;
        let status =
            ffi::PyConfig_SetString(&mut config, &mut config.home, wide_home.as_ptr().cast());
        if ffi::PyStatus_Exception(status) != 0 {
            ffi::PyConfig_Clear(&mut config);
            return Err(status_error(status));
        }
        let status = ffi::Py_InitializeFromConfig(&config);
        ffi::PyConfig_Clear(&mut config);
        if ffi::PyStatus_Exception(status) != 0 {
            return Err(status_error(status));
        }
        ffi::PyEval_SaveThread();
    }
    Ok(true)
}

fn status_error(status: ffi::PyStatus) -> PyRunnerError {
    let msg = if status.err_msg.is_null() {
        "unknown error".into()
    } else {
        // SAFETY: CPython sets `err_msg` to a static NUL-terminated string.
        unsafe { CStr::from_ptr(status.err_msg) }
            .to_string_lossy()
            .into_owned()
    };
    PyRunnerError::PyError(format!("Cannot initialize Python with home: {msg}"))
}

#[cfg(windows)]
fn to_wide(path: &Path) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;
    path.as_os_str().encode_wide().chain(Some(0)).collect()
}

#[cfg(not(windows))]
fn to_wide(path: &Path) -> Vec<u32> {
    path.to_string_lossy()
        .chars()
        .map(u32::from)
        .chain(Some(0))
        .collect()
}
