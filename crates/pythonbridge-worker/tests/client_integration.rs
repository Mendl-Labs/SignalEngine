//! End-to-end IPC test: actually spawns the built `pythonbridge-worker`
//! binary as a child process and drives it over stdin/stdout, exactly like
//! `PythonBridgeStrategy` will in production. `CARGO_BIN_EXE_pythonbridge-worker`
//! (the path to the just-built binary) is only available to integration
//! tests under `tests/`, which is why this lives here rather than inline in
//! `src/client.rs`.

use pythonbridge_worker::client::{WorkerProcess, WorkerProcessError};
use std::collections::HashMap;
use std::path::PathBuf;

fn worker_binary_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_pythonbridge-worker"))
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

const AGGRESSIVE_MEAN_REVERSION_STRATEGY: &str = r#"
from trading_platform import BaseStrategy
import numpy as np
import pandas as pd

class Strategy(BaseStrategy):
    def __init__(self):
        self.params = {"lookback": 5, "entry_z": 1.0, "exit_z": 0.3, "max_holding_bars": 4}

    def name(self) -> str:
        return "TestMeanReversion"

    def compute_signals(self, prices, volumes, timestamps):
        n = len(prices)
        signals = np.zeros(n, dtype=np.int8)
        lookback = int(self.params["lookback"])
        if n < lookback + 1:
            return signals
        p = pd.Series(prices)
        mean = p.rolling(window=lookback).mean()
        std = p.rolling(window=lookback).std()
        entry_z = float(self.params["entry_z"])
        z_last = (prices[-1] - mean.iloc[-1]) / std.iloc[-1] if std.iloc[-1] > 1e-9 else 0.0
        if z_last <= -entry_z:
            signals[-1] = self.BUY
        elif z_last >= entry_z:
            signals[-1] = self.SELL
        return signals
"#;

#[test]
fn spawn_initialize_push_bar_and_compute_signal_end_to_end() {
    let mut worker = WorkerProcess::spawn(&worker_binary_path()).unwrap();
    worker
        .initialize(ALWAYS_BUY_STRATEGY.to_string(), HashMap::new(), 30, 10)
        .unwrap();

    for i in 0..5 {
        worker.push_bar(100.0 + i as f64, 10.0, i).unwrap();
    }

    let signal = worker.compute_signal().unwrap();
    assert_eq!(signal, 1); // BUY
}

#[test]
fn compute_signal_before_initialize_errors() {
    let mut worker = WorkerProcess::spawn(&worker_binary_path()).unwrap();
    let err = worker.compute_signal().unwrap_err();
    assert!(matches!(err, WorkerProcessError::WorkerError(_)));
}

/// Confirms the whole point of this phase end-to-end: a real mean-reversion
/// strategy (shaped like the actual AggressiveMeanReversion strategy found
/// running as a fake market-maker in production) produces a real BUY signal
/// when fed a price series that dips well below its rolling mean, driven
/// entirely through the IPC boundary a live deployment will actually use.
#[test]
fn mean_reversion_strategy_buys_on_a_real_dip_via_ipc() {
    let mut worker = WorkerProcess::spawn(&worker_binary_path()).unwrap();
    let resolved_params = worker
        .initialize(AGGRESSIVE_MEAN_REVERSION_STRATEGY.to_string(), HashMap::new(), 30, 20)
        .unwrap();

    // Confirms the resolved_params round-trip works over the real IPC
    // boundary, not just in the unit-level PyO3 calls -- this is the exact
    // mechanism that fixes position_size_pct silently defaulting to 2%
    // instead of a strategy's real declared value.
    assert_eq!(resolved_params.get("lookback"), Some(&5.0));
    assert_eq!(resolved_params.get("entry_z"), Some(&1.0));

    // Flat prices around 100, then a sharp dip -- should trigger BUY once
    // the dip is the most recent bar.
    for (i, price) in [100.0, 100.1, 99.9, 100.0, 100.1, 100.0, 100.05, 99.95].iter().enumerate() {
        worker.push_bar(*price, 10.0, i as i64).unwrap();
    }
    worker.push_bar(90.0, 10.0, 8).unwrap(); // sharp dip

    let signal = worker.compute_signal().unwrap();
    assert_eq!(signal, 1, "expected BUY on a sharp dip below the rolling mean");
}

#[test]
fn worker_process_is_killed_cleanly_on_drop() {
    let worker = WorkerProcess::spawn(&worker_binary_path()).unwrap();
    drop(worker); // must not hang or panic
}
