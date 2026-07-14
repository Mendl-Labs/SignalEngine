//! Live/paper strategy execution via an embedded Python interpreter (PyO3).
//!
//! Hand-ported from `BacktestingEngine/strategy/src/strategies/python_strategy.rs`,
//! not shared as a common crate: that crate's transitive dependencies
//! (`signal`, `config`, `portfoliomanager`, `riskmanager`, `derivatives`, `greeks`)
//! collide by name/shape with SignalEngine's own differently-implemented
//! crates of the same names, so a path dependency would need a permanent
//! adapter layer -- effectively "extract a new crate" wearing a "shared
//! dependency" costume. This crate hand-ports the parts that are actually
//! needed for live/paper execution instead:
//!
//! - The sandbox (import hook + AST scan + timeout watchdog) is ported
//!   closely, matching the source almost line-for-line, since security fixes
//!   there should be checked against both copies by hand.
//! - The calling convention is narrower than the source on purpose: only the
//!   `compute_signals(prices, volumes, timestamps) -> int8[]` vectorized path
//!   is supported (not the object-based `generate_signals(data, context)`
//!   path, which requires a Python-side method most strategies -- including
//!   every AI-generated one produced by this platform's prompt template --
//!   never define). Live/paper calls this with a rolling window of recent
//!   bars (sized to the strategy's own declared lookback) and takes the last
//!   element of the returned array as the current signal, mirroring the
//!   `HybridExecutor::on_tick` calling convention already used for
//!   incremental calls in the source file.
//!
//! # Security
//!
//! Same caveat as the source: this in-process sandbox is NOT the security
//! boundary. It's a defense-in-depth speed bump. The real isolation boundary
//! is this crate running in its own dedicated OS process (see Phase 4's
//! supervisor), with no filesystem/network access enforced at the k8s layer.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyModule};
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;
use thiserror::Error;

/// Pre-import allowed scientific libraries before the sandbox blocks `os` etc.
/// numpy/pandas internally import `os`, `subprocess`, etc. during init -- we
/// must let that happen first, then lock down the sandbox.
///
/// Ported verbatim from `python_strategy.rs::PRE_IMPORT_ALLOWED_LIBS`.
const PRE_IMPORT_ALLOWED_LIBS: &str = r#"
import sys
for _lib in ["numpy", "pandas"]:
    try:
        __import__(_lib)
    except ImportError:
        pass  # not installed — skip
"#;

/// Sandbox import hook that blocks dangerous Python modules.
/// Guarded so it only installs once per interpreter (avoids recursive wrapping).
///
/// Ported verbatim from `python_strategy.rs::SANDBOX_IMPORT_HOOK`.
const SANDBOX_IMPORT_HOOK: &str = r#"
import builtins

if not getattr(builtins, '_sandbox_installed', False):
    _original_import = builtins.__import__

    _BLOCKED_MODULES = frozenset({
        'os', 'subprocess', 'socket', 'ctypes', 'shutil',
        'multiprocessing', 'threading', 'signal', 'resource',
        'pty', 'fcntl', 'termios', 'mmap', 'tempfile',
        'webbrowser', 'http', 'urllib', 'ftplib', 'smtplib',
        'xmlrpc', 'asyncio.subprocess',
    })

    def _restricted_import(name, *args, **kwargs):
        if name in _BLOCKED_MODULES or any(name.startswith(b + '.') for b in _BLOCKED_MODULES):
            raise ImportError(f"Module '{name}' is not allowed in sandboxed strategies")
        return _original_import(name, *args, **kwargs)

    builtins.__import__ = _restricted_import
    builtins._sandbox_installed = True
"#;

/// Static AST security scan source, ported verbatim from
/// `python_strategy.rs::ast_security_scan`'s embedded `scan_code`.
const AST_SCAN_CODE: &str = r#"
import ast

_BANNED_CALLS = frozenset({
    'exec', 'eval', 'compile', 'open', 'breakpoint',
    '__import__', 'getattr', 'setattr', 'delattr',
    'globals', 'locals', 'vars', 'dir',
    'memoryview', 'type',
})

_BANNED_ATTRS = frozenset({
    '__builtins__', '__subclasses__', '__bases__', '__mro__',
    '__globals__', '__code__', '__closure__', '__func__',
    '__self__', '__dict__', '__class__',
})

def scan(source):
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        return f"Syntax error: {e}"

    for node in ast.walk(tree):
        # Reject banned function calls: exec(...), eval(...), etc.
        if isinstance(node, ast.Call):
            fn = node.func
            name = None
            if isinstance(fn, ast.Name):
                name = fn.id
            elif isinstance(fn, ast.Attribute):
                name = fn.attr
            if name and name in _BANNED_CALLS:
                return f"Forbidden call: {name}() is not allowed in strategies (line {node.lineno})"

        # Reject access to dangerous dunder attributes
        if isinstance(node, ast.Attribute):
            if node.attr in _BANNED_ATTRS:
                return f"Forbidden attribute access: .{node.attr} is not allowed (line {node.lineno})"

        # Reject string-based attribute access via subscript on __dict__
        if isinstance(node, ast.Subscript):
            if isinstance(node.value, ast.Attribute) and node.value.attr == '__dict__':
                return f"Forbidden: __dict__ subscript access is not allowed (line {node.lineno})"

    return None  # all clear

result = scan(_source_code)
"#;

/// Python SDK source -- embedded from `python_api/` at compile time (copied
/// verbatim from `BacktestingEngine/strategy/python_api/`, not shared via a
/// cross-workspace path -- see module doc).
const PYTHON_SDK_TYPES: &str = include_str!("../python_api/types.py");
const PYTHON_SDK_STRATEGY: &str = include_str!("../python_api/strategy.py");
const PYTHON_SDK_INDICATORS: &str = include_str!("../python_api/indicators.py");
const PYTHON_SDK_INIT: &str = include_str!("../python_api/__init__.py");

/// Code that registers the `trading_platform` package in `sys.modules`.
/// Ported verbatim from `python_strategy.rs::sdk_injection_code`.
fn sdk_injection_code() -> String {
    r#"
import sys, types

_tp = types.ModuleType("trading_platform")
_tp.__package__ = "trading_platform"
_tp.__path__ = []  # make it a package

for _name, _src in [
    ("types", _sdk_types_src),
    ("strategy", _sdk_strategy_src),
    ("indicators", _sdk_indicators_src),
]:
    _sub = types.ModuleType(f"trading_platform.{_name}")
    _sub.__package__ = "trading_platform"
    sys.modules[f"trading_platform.{_name}"] = _sub
    exec(compile(_src, f"trading_platform/{_name}.py", "exec"), _sub.__dict__)
    for _k in dir(_sub):
        if not _k.startswith("_"):
            setattr(_tp, _k, getattr(_sub, _k))

sys.modules["trading_platform"] = _tp
exec(compile(_sdk_init_src, "trading_platform/__init__.py", "exec"), _tp.__dict__)

del _name, _src, _sub, _k
"#.to_string()
}

/// Inject the SDK into a Python interpreter. Must be called after the
/// sandbox is installed but before user code. Ported from
/// `python_strategy.rs::inject_sdk`.
fn inject_sdk(py: Python<'_>) -> Result<(), PythonBridgeError> {
    let locals = PyDict::new_bound(py);
    locals.set_item("_sdk_types_src", PYTHON_SDK_TYPES).ok();
    locals.set_item("_sdk_strategy_src", PYTHON_SDK_STRATEGY).ok();
    locals.set_item("_sdk_indicators_src", PYTHON_SDK_INDICATORS).ok();
    locals.set_item("_sdk_init_src", PYTHON_SDK_INIT).ok();

    py.run_bound(&sdk_injection_code(), None, Some(&locals))
        .map_err(|e| PythonBridgeError::Setup(format!("SDK injection error: {}", e)))?;
    Ok(())
}

/// Static AST security scan -- rejects dangerous Python patterns before
/// execution. Ported verbatim from `python_strategy.rs::ast_security_scan`.
fn ast_security_scan(py: Python<'_>, source_code: &str) -> Result<(), String> {
    let globals = PyDict::new_bound(py);
    globals
        .set_item(
            "__builtins__",
            py.import_bound("builtins")
                .map_err(|e| format!("AST scan setup error: {}", e))?,
        )
        .map_err(|e| format!("AST scan setup error: {}", e))?;
    globals
        .set_item("_source_code", source_code)
        .map_err(|e| format!("AST scan setup error: {}", e))?;

    py.run_bound(AST_SCAN_CODE, Some(&globals), None)
        .map_err(|e| format!("AST scan internal error: {}", e))?;

    let result = globals
        .get_item("result")
        .map_err(|e| format!("AST scan result error: {}", e))?;

    if let Some(val) = result {
        if !val.is_none() {
            let msg: String = val
                .extract()
                .map_err(|e| format!("AST scan extract error: {}", e))?;
            return Err(msg);
        }
    }

    Ok(())
}

#[derive(Debug, Error)]
pub enum PythonBridgeError {
    #[error("strategy setup error: {0}")]
    Setup(String),
    #[error("strategy not initialized")]
    NotInitialized,
    #[error("strategy execution error: {0}")]
    Execution(String),
    #[error("strategy execution timed out after {0}s")]
    Timeout(u64),
}

/// Validate Python source code without running a full initialization:
/// static AST scan, syntax check, and confirms a `Strategy` class exists
/// with the required `compute_signals` method. Ported from
/// `python_strategy.rs::validate_source`.
pub fn validate_source(source_code: &str) -> Result<(), String> {
    Python::with_gil(|py| {
        ast_security_scan(py, source_code)?;

        inject_sdk(py).map_err(|e| e.to_string())?;

        py.run_bound(PRE_IMPORT_ALLOWED_LIBS, None, None)
            .map_err(|e| format!("Pre-import error: {}", e))?;

        py.run_bound(SANDBOX_IMPORT_HOOK, None, None)
            .map_err(|e| format!("Sandbox setup error: {}", e))?;

        let module = PyModule::from_code_bound(py, source_code, "strategy.py", "user_strategy")
            .map_err(|e| format!("Syntax error: {}", e))?;

        let strategy_class = module.getattr("Strategy").map_err(|_| {
            "Missing 'Strategy' class. Your code must define a class named 'Strategy'.".to_string()
        })?;

        let instance = strategy_class
            .call0()
            .map_err(|e| format!("Failed to instantiate Strategy: {}", e))?;

        if !instance.hasattr("compute_signals").unwrap_or(false) {
            return Err(
                "Strategy class must implement 'compute_signals(self, prices, volumes, timestamps)'"
                    .to_string(),
            );
        }

        Ok(())
    })
}

/// A live/paper strategy backed by an embedded, persistent Python
/// interpreter instance. One `PythonStrategyRunner` per deployment, running
/// inside its own dedicated OS process (see Phase 4's supervisor) -- never
/// shared across tenants and never invoked from inside a shared async
/// runtime.
///
/// # Thread affinity is load-bearing
///
/// Every call into this type (`initialize`, `compute_signal`) MUST happen
/// from the same OS thread for the lifetime of the process. The timeout
/// watchdog uses `_thread.interrupt_main()`, which targets CPython's
/// *registered* main thread -- whichever OS thread first called
/// `Python::with_gil` in this process (via PyO3's auto-initialize). If a
/// later call arrives on a different thread, the watchdog silently signals
/// the wrong thread and a hung strategy call never gets interrupted. Phase
/// 4's supervisor satisfies this by construction (one worker process, one
/// simple request loop, no thread pool); this is exactly why the
/// architecture decision requires a dedicated process per worker rather
/// than a thread pool inside a shared runtime. The unit tests below
/// reproduce this constraint deliberately (`on_python_thread`) rather than
/// relying on Rust's default per-test-function threading, since that's what
/// caught this in the first place.

pub struct PythonStrategyRunner {
    source_code: String,
    py_strategy: Mutex<Option<PyObject>>,
    cached_name: String,
    initialized: bool,
    timeout_secs: u64,
    /// Rolling window of recent bars, bounded to `window_size`. Maintained
    /// here (not in Python) so `compute_signals` always sees enough history
    /// to produce a meaningful last-element signal, matching the source's
    /// `HybridExecutor::on_tick` incremental calling convention.
    prices: VecDeque<f64>,
    volumes: VecDeque<f64>,
    timestamps: VecDeque<i64>,
    window_size: usize,
}

impl std::fmt::Debug for PythonStrategyRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonStrategyRunner")
            .field("name", &self.cached_name)
            .field("initialized", &self.initialized)
            .field("window_len", &self.prices.len())
            .finish()
    }
}

impl PythonStrategyRunner {
    /// Create a new runner. `window_size` should be at least the strategy's
    /// declared lookback + 1 (e.g. `AggressiveMeanReversion`'s `lookback: 25`
    /// needs at least 26 bars before `compute_signals` produces anything but
    /// zeros) -- callers without a better source should use a generous
    /// default (e.g. 200) rather than guess low.
    pub fn new(source_code: String, window_size: usize) -> Self {
        Self {
            source_code,
            py_strategy: Mutex::new(None),
            cached_name: "PythonStrategy".to_string(),
            initialized: false,
            timeout_secs: 30,
            prices: VecDeque::with_capacity(window_size),
            volumes: VecDeque::with_capacity(window_size),
            timestamps: VecDeque::with_capacity(window_size),
            window_size,
        }
    }

    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.timeout_secs = timeout_secs;
        self
    }

    /// Initialize the interpreter, load the user's strategy, and apply
    /// parameter overrides (e.g. GA-tuned or deployment-configured values).
    /// Ported/simplified from `python_strategy.rs::init_python` +
    /// `initialize` -- state_schema instance-attribute seeding is omitted
    /// (out of scope: `compute_signals`-only strategies recompute their
    /// entry/exit state as local variables inside each call over the given
    /// window, not via persisted instance attributes).
    pub fn initialize(&mut self, parameters: &std::collections::HashMap<String, f64>) -> Result<(), PythonBridgeError> {
        let py_obj = Python::with_gil(|py| -> Result<PyObject, PythonBridgeError> {
            ast_security_scan(py, &self.source_code).map_err(PythonBridgeError::Setup)?;

            inject_sdk(py)?;

            py.run_bound(PRE_IMPORT_ALLOWED_LIBS, None, None)
                .map_err(|e| PythonBridgeError::Setup(format!("Pre-import error: {}", e)))?;

            // NOTE: RLIMIT_AS memory sandboxing is intentionally NOT applied
            // here either -- same rationale as the source: it restricts the
            // whole process's virtual address space (breaking thread
            // creation), not just Python, and the container cgroup memory
            // limit is the real boundary.
            py.run_bound(SANDBOX_IMPORT_HOOK, None, None)
                .map_err(|e| PythonBridgeError::Setup(format!("Sandbox setup error: {}", e)))?;

            let module = PyModule::from_code_bound(py, &self.source_code, "strategy.py", "user_strategy")
                .map_err(|e| PythonBridgeError::Setup(format!("Python compilation error: {}", e)))?;

            let strategy_class = module.getattr("Strategy").map_err(|_| {
                PythonBridgeError::Setup("No 'Strategy' class found in Python code".to_string())
            })?;

            let instance = strategy_class.call0().map_err(|e| {
                PythonBridgeError::Setup(format!("Failed to instantiate Strategy: {}", e))
            })?;

            if let Ok(name) = instance.call_method0("name") {
                if let Ok(s) = name.extract::<String>() {
                    self.cached_name = s;
                }
            }

            let params_dict = PyDict::new_bound(py);
            for (k, v) in parameters {
                params_dict.set_item(k, v).ok();
            }
            instance
                .call_method1("initialize", (params_dict,))
                .map_err(|e| PythonBridgeError::Setup(format!("Python initialize() error: {}", e)))?;

            Ok(instance.into())
        })?;

        // Single persistent watchdog thread (started once, not per-call) --
        // ported verbatim from python_strategy.rs::initialize. Avoids
        // spawning an OS thread per tick, which exhausts threads under load.
        Python::with_gil(|py| {
            let watchdog_init = r#"
import _thread, time as _wd_time
_wd_deadline = [0.0]
def _persistent_watchdog():
    while True:
        _wd_time.sleep(1.0)
        d = _wd_deadline[0]
        if d > 0.0 and _wd_time.time() > d:
            _wd_deadline[0] = 0.0
            _thread.interrupt_main()
_thread.start_new_thread(_persistent_watchdog, ())
"#;
            let _ = py.run_bound(watchdog_init, None, None);
        });

        *self.py_strategy.lock().expect("py_strategy mutex poisoned") = Some(py_obj);
        self.initialized = true;
        Ok(())
    }

    /// Push a new bar into the rolling window, evicting the oldest bar once
    /// `window_size` is exceeded.
    pub fn push_bar(&mut self, price: f64, volume: f64, timestamp: i64) {
        if self.prices.len() >= self.window_size {
            self.prices.pop_front();
            self.volumes.pop_front();
            self.timestamps.pop_front();
        }
        self.prices.push_back(price);
        self.volumes.push_back(volume);
        self.timestamps.push_back(timestamp);
    }

    /// Number of bars currently held in the rolling window.
    pub fn window_len(&self) -> usize {
        self.prices.len()
    }

    /// Call `compute_signals` over the current rolling window and return the
    /// LAST element of the returned array -- the signal for the most recent
    /// bar just pushed. Returns `Ok(0)` (hold) if the window is empty.
    ///
    /// Signal values match the SDK convention: `1 = BUY`, `-1 = SELL`,
    /// `2 = CLOSE`, `0 = HOLD`.
    pub fn compute_signal(&mut self) -> Result<i8, PythonBridgeError> {
        if !self.initialized {
            return Err(PythonBridgeError::NotInitialized);
        }
        if self.prices.is_empty() {
            return Ok(0);
        }

        let lock = self.py_strategy.lock().expect("py_strategy mutex poisoned");
        let py_strategy = lock.as_ref().ok_or(PythonBridgeError::NotInitialized)?;
        let timeout = Duration::from_secs(self.timeout_secs);

        // Copy the window out before entering the GIL closure.
        let prices_vec: Vec<f64> = self.prices.iter().copied().collect();
        let volumes_vec: Vec<f64> = self.volumes.iter().copied().collect();
        let timestamps_vec: Vec<i64> = self.timestamps.iter().copied().collect();

        Python::with_gil(|py| {
            let strategy = py_strategy.bind(py);

            let arm_code = format!(
                "_wd_deadline[0] = __import__('time').time() + {}",
                timeout.as_secs_f64()
            );
            let _ = py.run_bound(&arm_code, None, None);

            // Plain Python lists, not numpy zero-copy arrays: pandas/numpy
            // operations inside user strategies accept list-likes directly,
            // and this worker calls compute_signals once per new bar (not
            // once per chromosome-evaluation loop like the GA path in the
            // source), so the zero-copy optimization isn't worth the extra
            // marshaling complexity here.
            let prices_arr = PyList::new_bound(py, prices_vec.iter().copied());
            let volumes_arr = PyList::new_bound(py, volumes_vec.iter().copied());
            let timestamps_arr = PyList::new_bound(py, timestamps_vec.iter().copied());

            let result = strategy
                .call_method1("compute_signals", (prices_arr, volumes_arr, timestamps_arr))
                .map_err(|e| {
                    let _ = py.run_bound("_wd_deadline[0] = 0.0", None, None);
                    let msg = format!("{}", e);
                    if msg.contains("KeyboardInterrupt") {
                        PythonBridgeError::Timeout(timeout.as_secs())
                    } else {
                        PythonBridgeError::Execution(format!("compute_signals() error: {}", msg))
                    }
                })?;

            let _ = py.run_bound("_wd_deadline[0] = 0.0", None, None);

            let py_list = result.call_method0("tolist").or_else(|_| Ok::<_, pyo3::PyErr>(result.clone()))
                .map_err(|e| PythonBridgeError::Execution(format!("failed to convert output: {}", e)))?;

            let items: Vec<Bound<'_, PyAny>> = py_list
                .downcast::<PyList>()
                .map_err(|_| {
                    PythonBridgeError::Execution(
                        "compute_signals() must return an ndarray or list of int8".to_string(),
                    )
                })?
                .iter()
                .collect();

            let last = items.last().ok_or_else(|| {
                PythonBridgeError::Execution("compute_signals() returned an empty array".to_string())
            })?;

            let v: i64 = last.extract().unwrap_or(0);
            Ok(v.clamp(-128, 127) as i8)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::sync::OnceLock;

    /// Route every Python-touching test body through one persistent
    /// background thread. See the "Thread affinity is load-bearing" doc on
    /// `PythonStrategyRunner`: `_thread.interrupt_main()` targets whichever
    /// OS thread first called `Python::with_gil` in this process, and Rust's
    /// test harness spawns a new thread per `#[test]` fn by default -- so
    /// without this, a later test's watchdog would signal the wrong thread
    /// and hang forever (confirmed: this is exactly what happened before
    /// this helper existed).
    fn on_python_thread<F, R>(f: F) -> R
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        static SENDER: OnceLock<mpsc::Sender<Box<dyn FnOnce() + Send>>> = OnceLock::new();
        let sender = SENDER.get_or_init(|| {
            let (tx, rx) = mpsc::channel::<Box<dyn FnOnce() + Send>>();
            std::thread::spawn(move || {
                for job in rx {
                    job();
                }
            });
            tx
        });

        let (result_tx, result_rx) = mpsc::channel();
        sender
            .send(Box::new(move || {
                let _ = result_tx.send(f());
            }))
            .expect("python worker thread panicked");
        result_rx.recv().expect("python worker thread dropped result sender")
    }

    const ALWAYS_BUY_STRATEGY: &str = r#"
from trading_platform import BaseStrategy
import numpy as np

class Strategy(BaseStrategy):
    def name(self) -> str:
        return "AlwaysBuy"

    def compute_signals(self, prices, volumes, timestamps):
        sig = np.zeros(len(prices), dtype=np.int8)
        if len(prices) > 0:
            sig[-1] = self.BUY
        return sig
"#;

    const MALICIOUS_OS_IMPORT: &str = r#"
from trading_platform import BaseStrategy
import os

class Strategy(BaseStrategy):
    def name(self) -> str:
        return "Malicious"

    def compute_signals(self, prices, volumes, timestamps):
        os.system("echo pwned")
        return []
"#;

    const MALICIOUS_EVAL: &str = r#"
from trading_platform import BaseStrategy

class Strategy(BaseStrategy):
    def name(self) -> str:
        return "Malicious"

    def compute_signals(self, prices, volumes, timestamps):
        eval("1+1")
        return []
"#;

    const MISSING_STRATEGY_CLASS: &str = r#"
class NotStrategy:
    pass
"#;

    const INFINITE_LOOP_STRATEGY: &str = r#"
from trading_platform import BaseStrategy
import numpy as np

class Strategy(BaseStrategy):
    def name(self) -> str:
        return "Hangs"

    def compute_signals(self, prices, volumes, timestamps):
        while True:
            pass
"#;

    #[test]
    fn validate_source_accepts_valid_strategy() {
        let ok = on_python_thread(|| validate_source(ALWAYS_BUY_STRATEGY).is_ok());
        assert!(ok);
    }

    #[test]
    fn validate_source_rejects_missing_strategy_class() {
        let err = on_python_thread(|| validate_source(MISSING_STRATEGY_CLASS).unwrap_err());
        assert!(err.contains("Strategy"), "unexpected error: {err}");
    }

    #[test]
    fn validate_source_rejects_direct_os_import_at_ast_scan() {
        // The AST scan itself doesn't ban `import os` (only banned calls/attrs) --
        // the import hook blocks it at execution time instead. Confirm the
        // sandboxed import actually raises when the module runs.
        let result = on_python_thread(|| {
            Python::with_gil(|py| -> Result<(), String> {
                ast_security_scan(py, MALICIOUS_OS_IMPORT)?;
                inject_sdk(py).map_err(|e| e.to_string())?;
                py.run_bound(PRE_IMPORT_ALLOWED_LIBS, None, None).map_err(|e| e.to_string())?;
                py.run_bound(SANDBOX_IMPORT_HOOK, None, None).map_err(|e| e.to_string())?;
                PyModule::from_code_bound(py, MALICIOUS_OS_IMPORT, "strategy.py", "user_strategy")
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            })
        });
        assert!(result.is_err(), "expected sandboxed `import os` to be rejected");
    }

    #[test]
    fn validate_source_rejects_eval_via_ast_scan() {
        let err = on_python_thread(|| Python::with_gil(|py| ast_security_scan(py, MALICIOUS_EVAL)));
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("Forbidden call"));
    }

    #[test]
    fn compute_signal_returns_last_element_of_window() {
        let signal = on_python_thread(|| {
            let mut runner = PythonStrategyRunner::new(ALWAYS_BUY_STRATEGY.to_string(), 10);
            runner.initialize(&std::collections::HashMap::new()).unwrap();
            for i in 0..5 {
                runner.push_bar(100.0 + i as f64, 10.0, i);
            }
            runner.compute_signal().unwrap()
        });
        assert_eq!(signal, 1); // BUY
    }

    #[test]
    fn compute_signal_returns_hold_on_empty_window() {
        let signal = on_python_thread(|| {
            let mut runner = PythonStrategyRunner::new(ALWAYS_BUY_STRATEGY.to_string(), 10);
            runner.initialize(&std::collections::HashMap::new()).unwrap();
            runner.compute_signal().unwrap()
        });
        assert_eq!(signal, 0);
    }

    #[test]
    fn push_bar_evicts_oldest_once_window_is_full() {
        // No Python interpreter touched here -- runs directly, no need to
        // route through on_python_thread.
        let mut runner = PythonStrategyRunner::new(ALWAYS_BUY_STRATEGY.to_string(), 3);
        for i in 0..5 {
            runner.push_bar(i as f64, 1.0, i);
        }
        assert_eq!(runner.window_len(), 3);
    }

    #[test]
    fn compute_signal_before_initialize_errors() {
        let result = on_python_thread(|| {
            let mut runner = PythonStrategyRunner::new(ALWAYS_BUY_STRATEGY.to_string(), 10);
            runner.compute_signal()
        });
        assert!(matches!(result, Err(PythonBridgeError::NotInitialized)));
    }

    // Isolation/chaos test: a pathological strategy (infinite loop) must be
    // killed by the watchdog within roughly `timeout_secs`, not hang forever.
    // This is the property the whole process-isolation architecture exists
    // to contain -- kept as a permanent, named test per the plan. Must run
    // on the same shared Python thread as every other test here: the
    // watchdog's `_thread.interrupt_main()` only reaches the thread CPython
    // registered as "main" at interpreter-init time (see the doc on
    // `PythonStrategyRunner`) -- running this on its own ad hoc thread is
    // exactly the bug that made this hang forever during development.
    #[test]
    fn watchdog_kills_infinite_loop_strategy_within_timeout() {
        let (result, elapsed) = on_python_thread(|| {
            let mut runner = PythonStrategyRunner::new(INFINITE_LOOP_STRATEGY.to_string(), 10)
                .with_timeout(2);
            runner.initialize(&std::collections::HashMap::new()).unwrap();
            runner.push_bar(100.0, 10.0, 0);

            let start = std::time::Instant::now();
            let result = runner.compute_signal();
            (result, start.elapsed())
        });

        assert!(result.is_err(), "expected the infinite loop to error out, got {:?}", result);
        assert!(
            elapsed < Duration::from_secs(10),
            "watchdog should kill the call within ~timeout_secs, took {:?}",
            elapsed
        );
    }
}
