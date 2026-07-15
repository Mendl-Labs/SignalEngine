//! `pythonbridge-worker` binary: reads newline-delimited JSON `Request`s
//! from stdin, drives one `PythonStrategyRunner`, writes newline-delimited
//! JSON `Response`s to stdout. Spawned as a child process by
//! `PythonBridgeStrategy` (via `pythonbridge_worker::client::WorkerProcess`)
//! -- one process per deployment, so a crash or hang here is isolated to
//! exactly the one strategy that caused it, never taking down any other
//! tenant's live signals.
//!
//! All requests are handled sequentially, in the order received, on this
//! process's single thread -- satisfying `PythonStrategyRunner`'s thread
//! affinity requirement (the timeout watchdog's `_thread.interrupt_main()`
//! must always target the same OS thread) by construction, with no pooling
//! or concurrency to get wrong.

use pythonbridge_worker::protocol::{Request, Response};
use pythonbridge_worker::PythonStrategyRunner;
use std::io::{self, BufRead, Write};

fn main() {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut runner: Option<PythonStrategyRunner> = None;

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) if l.trim().is_empty() => continue,
            Ok(l) => l,
            Err(_) => break, // stdin closed/errored -- exit cleanly
        };

        let request: Request = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                write_response(&mut stdout, &Response::Error { message: format!("malformed request: {e}") });
                continue;
            }
        };

        let response = handle_request(request, &mut runner);
        let is_shutdown = matches!(response, None);
        if let Some(resp) = response {
            write_response(&mut stdout, &resp);
        }
        if is_shutdown {
            break;
        }
    }
}

/// Returns `None` only for a `Shutdown` request (signals the caller to exit
/// the loop without writing a response).
fn handle_request(request: Request, runner: &mut Option<PythonStrategyRunner>) -> Option<Response> {
    match request {
        Request::Initialize { source_code, parameters, timeout_secs, window_size } => {
            let mut new_runner = PythonStrategyRunner::new(source_code, window_size).with_timeout(timeout_secs);
            Some(match new_runner.initialize(&parameters) {
                Ok(resolved_params) => {
                    *runner = Some(new_runner);
                    Response::Initialized { resolved_params }
                }
                Err(e) => Response::Error { message: e.to_string() },
            })
        }
        Request::PushBar { price, volume, timestamp } => Some(match runner {
            Some(r) => {
                r.push_bar(price, volume, timestamp);
                Response::BarPushed
            }
            None => Response::Error { message: "not initialized".to_string() },
        }),
        Request::ComputeSignal => Some(match runner {
            Some(r) => match r.compute_signal() {
                Ok(value) => Response::Signal { value },
                Err(e) => Response::Error { message: e.to_string() },
            },
            None => Response::Error { message: "not initialized".to_string() },
        }),
        Request::Shutdown => None,
    }
}

fn write_response(stdout: &mut io::Stdout, response: &Response) {
    if let Ok(json) = serde_json::to_string(response) {
        let _ = writeln!(stdout, "{json}");
        let _ = stdout.flush();
    }
}
