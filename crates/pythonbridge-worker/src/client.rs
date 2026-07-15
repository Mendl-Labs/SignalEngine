//! Client side of the IPC protocol: spawns a `pythonbridge-worker` child
//! process and drives it over its stdin/stdout pipes. One `WorkerProcess`
//! per deployment -- never shared across strategies.

use crate::protocol::{Request, Response};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

#[derive(Debug, thiserror::Error)]
pub enum WorkerProcessError {
    #[error("failed to spawn worker process: {0}")]
    Spawn(std::io::Error),
    #[error("failed to write request: {0}")]
    Write(std::io::Error),
    #[error("failed to read response: {0}")]
    Read(std::io::Error),
    #[error("worker process exited (stdout closed) before responding")]
    Closed,
    #[error("malformed response: {0}")]
    Malformed(serde_json::Error),
    #[error("worker returned an error: {0}")]
    WorkerError(String),
    #[error("unexpected response variant for this request")]
    UnexpectedResponse,
}

/// Resolve the `pythonbridge-worker` binary path: `PYTHONBRIDGE_WORKER_BIN`
/// env var if set, otherwise a binary named `pythonbridge-worker` (or
/// `pythonbridge-worker.exe` on Windows) sitting alongside the current
/// executable -- the layout produced by copying both binaries into the same
/// directory in the production Docker image.
pub fn default_binary_path() -> PathBuf {
    if let Ok(p) = std::env::var("PYTHONBRIDGE_WORKER_BIN") {
        return PathBuf::from(p);
    }
    let exe_name = if cfg!(windows) { "pythonbridge-worker.exe" } else { "pythonbridge-worker" };
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join(exe_name)))
        .unwrap_or_else(|| PathBuf::from(exe_name))
}

/// A live handle to one spawned `pythonbridge-worker` child process.
pub struct WorkerProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl WorkerProcess {
    /// Spawn the worker binary at `binary_path`. Does NOT initialize the
    /// strategy yet -- call `initialize()` next.
    pub fn spawn(binary_path: &Path) -> Result<Self, WorkerProcessError> {
        let mut child = Command::new(binary_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(WorkerProcessError::Spawn)?;

        let stdin = child.stdin.take().expect("child spawned with piped stdin");
        let stdout = BufReader::new(child.stdout.take().expect("child spawned with piped stdout"));

        Ok(Self { child, stdin, stdout })
    }

    fn call(&mut self, request: &Request) -> Result<Response, WorkerProcessError> {
        let json = serde_json::to_string(request).expect("Request always serializes");
        writeln!(self.stdin, "{json}").map_err(WorkerProcessError::Write)?;
        self.stdin.flush().map_err(WorkerProcessError::Write)?;

        let mut line = String::new();
        let n = self.stdout.read_line(&mut line).map_err(WorkerProcessError::Read)?;
        if n == 0 {
            return Err(WorkerProcessError::Closed);
        }

        let response: Response = serde_json::from_str(line.trim()).map_err(WorkerProcessError::Malformed)?;
        if let Response::Error { message } = response {
            return Err(WorkerProcessError::WorkerError(message));
        }
        Ok(response)
    }

    /// Returns the strategy's resolved `self.params` after initialization --
    /// see `Response::Initialized`'s doc for why this matters (e.g.
    /// `position_size_pct` for AI-generated strategies).
    pub fn initialize(
        &mut self,
        source_code: String,
        parameters: HashMap<String, f64>,
        timeout_secs: u64,
        window_size: usize,
    ) -> Result<HashMap<String, f64>, WorkerProcessError> {
        match self.call(&Request::Initialize { source_code, parameters, timeout_secs, window_size })? {
            Response::Initialized { resolved_params } => Ok(resolved_params),
            _ => Err(WorkerProcessError::UnexpectedResponse),
        }
    }

    pub fn push_bar(&mut self, price: f64, volume: f64, timestamp: i64) -> Result<(), WorkerProcessError> {
        match self.call(&Request::PushBar { price, volume, timestamp })? {
            Response::BarPushed => Ok(()),
            _ => Err(WorkerProcessError::UnexpectedResponse),
        }
    }

    pub fn compute_signal(&mut self) -> Result<i8, WorkerProcessError> {
        match self.call(&Request::ComputeSignal)? {
            Response::Signal { value } => Ok(value),
            _ => Err(WorkerProcessError::UnexpectedResponse),
        }
    }
}

impl Drop for WorkerProcess {
    fn drop(&mut self) {
        // Best-effort clean shutdown (lets the child exit its stdin-read
        // loop on its own); fall back to killing it if that doesn't work.
        let _ = self.call(&Request::Shutdown);
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Subprocess-spawning tests live in tests/client_integration.rs instead:
    // CARGO_BIN_EXE_pythonbridge-worker (needed to locate the built binary)
    // is only set by cargo for integration tests under tests/, not for
    // unit tests compiled inline into the lib itself.

    #[test]
    fn default_binary_path_respects_env_override() {
        std::env::set_var("PYTHONBRIDGE_WORKER_BIN", "/custom/path/worker");
        assert_eq!(default_binary_path(), PathBuf::from("/custom/path/worker"));
        std::env::remove_var("PYTHONBRIDGE_WORKER_BIN");
    }
}
