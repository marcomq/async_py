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
use std::{
    ffi::CStr,
    path::Path,
    sync::{Mutex, PoisonError},
};

/// Error of a failed `Py_InitializeFromConfig`. CPython can't be cleaned up or
/// initialized again after that, so it is terminal for the process.
static INIT_FAILURE: Mutex<Option<String>> = Mutex::new(None);

pub(crate) fn init_failure() -> Option<String> {
    INIT_FAILURE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
}

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
/// e.g. because the stdlib is missing. Such a failure is permanent: later calls
/// and every `PyRunner` command return the same error.
pub fn init_python_home(home: &Path) -> Result<bool, PyRunnerError> {
    let mut failure = INIT_FAILURE.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(msg) = failure.as_ref() {
        return Err(PyRunnerError::PyError(msg.clone()));
    }
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
        let status = match set_home(&mut config, home) {
            Ok(status) => status,
            Err(err) => {
                ffi::PyConfig_Clear(&mut config);
                return Err(err);
            }
        };
        if ffi::PyStatus_Exception(status) != 0 {
            ffi::PyConfig_Clear(&mut config);
            return Err(PyRunnerError::PyError(status_message(status)));
        }
        let status = ffi::Py_InitializeFromConfig(&config);
        ffi::PyConfig_Clear(&mut config);
        if ffi::PyStatus_Exception(status) != 0 {
            let msg = status_message(status);
            *failure = Some(msg.clone());
            return Err(PyRunnerError::PyError(msg));
        }
        ffi::PyEval_SaveThread();
    }
    Ok(true)
}

fn status_message(status: ffi::PyStatus) -> String {
    let msg = if status.err_msg.is_null() {
        "unknown error".into()
    } else {
        // SAFETY: CPython sets `err_msg` to a static NUL-terminated string.
        unsafe { CStr::from_ptr(status.err_msg) }
            .to_string_lossy()
            .into_owned()
    };
    format!("Cannot initialize Python with home: {msg}")
}

#[cfg(windows)]
unsafe fn set_home(
    config: &mut ffi::PyConfig,
    home: &Path,
) -> Result<ffi::PyStatus, PyRunnerError> {
    use std::os::windows::ffi::OsStrExt;
    let wide: Vec<u16> = home.as_os_str().encode_wide().chain(Some(0)).collect();
    Ok(ffi::PyConfig_SetString(
        config,
        &mut config.home,
        wide.as_ptr().cast(),
    ))
}

/// Passes the raw path bytes, which CPython decodes like `os.fsdecode`, so
/// non-UTF-8 paths survive unchanged.
#[cfg(not(windows))]
unsafe fn set_home(
    config: &mut ffi::PyConfig,
    home: &Path,
) -> Result<ffi::PyStatus, PyRunnerError> {
    use std::os::unix::ffi::OsStrExt;
    let bytes = std::ffi::CString::new(home.as_os_str().as_bytes())
        .map_err(|_| PyRunnerError::PyError("Python home path contains a NUL byte".into()))?;
    Ok(ffi::PyConfig_SetBytesString(
        config,
        &mut config.home,
        bytes.as_ptr(),
    ))
}
