//! Placeholder binary entry point. The IPC protocol (stdin/socket request
//! loop wrapping `pythonbridge_worker::PythonStrategyRunner`) is Phase 4's
//! job -- this crate's Phase 3 deliverable is the library only, tested
//! standalone via `cargo test -p pythonbridge-worker`.

fn main() {
    eprintln!("pythonbridge-worker: no IPC entry point yet (Phase 4). See src/lib.rs for the standalone, testable bridge.");
    std::process::exit(1);
}
