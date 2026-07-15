//! IPC protocol between the supervisor (running inside `hostbuilder`, via
//! `PythonBridgeStrategy`) and a `pythonbridge-worker` child process.
//!
//! Transport: newline-delimited JSON over the child's stdin/stdout pipes.
//! Not a Unix domain socket -- the worker is always a subprocess spawned
//! directly by its caller (one process per deployment), so there's no
//! separate daemon to *connect* to; stdio pipes are simpler, need no
//! socket-file cleanup, and work identically on any OS. `serde_json`
//! escapes newlines within string fields (e.g. `source_code`, which always
//! contains real newlines), so a single JSON object never contains a raw
//! newline byte -- newline-delimited framing is safe.
//!
//! JSON over protobuf: this is a low-frequency channel (one call per new
//! bar/signal, not per-tick), so wire efficiency doesn't matter here: a
//! human-readable, trivially-debuggable protocol is worth more than the
//! marginal cost of JSON parsing at this rate.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Request {
    /// Initialize the interpreter and load the strategy. Must be the first
    /// request sent; every other request before this returns `NotInitialized`.
    Initialize {
        source_code: String,
        parameters: HashMap<String, f64>,
        timeout_secs: u64,
        /// Rolling window size (bars). Should be at least the strategy's
        /// declared lookback + 1 -- callers without a better source should
        /// pass a generous default (e.g. 200) rather than guess low.
        window_size: usize,
    },
    /// Push one new bar into the rolling window.
    PushBar {
        price: f64,
        volume: f64,
        timestamp: i64,
    },
    /// Call `compute_signals` over the current window and return the last
    /// element (the signal for the most recently pushed bar).
    ComputeSignal,
    /// Clean shutdown request (also triggered by stdin closing/EOF).
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Response {
    /// `resolved_params` is the strategy's own `self.params` after its
    /// `initialize()` ran (its `__init__` defaults merged with the
    /// `Request::Initialize.parameters` overrides) -- the Python object is
    /// the only reliable source of truth for values like
    /// `position_size_pct` that AI-generated strategies bake directly into
    /// source rather than expose as a separate structured DB field.
    Initialized { resolved_params: HashMap<String, f64> },
    BarPushed,
    /// Signal values match the SDK convention: `1 = BUY`, `-1 = SELL`,
    /// `2 = CLOSE`, `0 = HOLD`.
    Signal { value: i8 },
    Error { message: String },
}
