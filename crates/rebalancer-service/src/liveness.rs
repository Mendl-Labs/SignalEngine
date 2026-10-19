//! Self-healing liveness for the tick loop. The binary has no HTTP listener, so progress is published as a heartbeat
//! file: the loop writes the current unix time (seconds) at the START and END of every tick, and the Kubernetes
//! liveness probe runs `rebalancer-service liveness-check <stale_secs>`, which fails when the file is missing,
//! unparseable or older than the threshold. A loop that is Running but frozen (the 2026-10-19 incident: 8 hours stuck
//! after tick 1) then gets restarted instead of going unnoticed.
//!
//! The file CONTENT is the clock (not the mtime): it is what the writer meant, and it survives copy/restore.
//! Writes are atomic (temp file in the same directory, then rename) so the probe never reads a half-written value,
//! and a write failure is only ever logged by the caller: liveness reporting must never abort a tick.

use std::io;
use std::path::{Path, PathBuf};

/// Env var naming the heartbeat file.
pub const ENV_LIVENESS_FILE: &str = "REBALANCER_LIVENESS_FILE";
/// Default heartbeat path (the chart mounts an emptyDir at /tmp because the root filesystem is read-only).
pub const DEFAULT_LIVENESS_FILE: &str = "/tmp/rebalancer-last-tick";
/// The staleness floor: three of the default 300s ticks.
pub const MIN_STALE_SECS: u64 = 900;

/// Unset or blank is the default path.
pub fn liveness_path(raw: Option<&str>) -> PathBuf {
    match raw.map(str::trim) {
        Some(p) if !p.is_empty() => PathBuf::from(p),
        _ => PathBuf::from(DEFAULT_LIVENESS_FILE),
    }
}

/// Staleness threshold for a tick interval: max(900, 3 x tick).
pub fn stale_secs_for_tick(tick_secs: u64) -> u64 {
    MIN_STALE_SECS.max(tick_secs.saturating_mul(3))
}

/// Atomically writes `now_unix` to `path` (temp file next to it, then rename).
pub fn write_heartbeat(path: &Path, now_unix: u64) -> io::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    std::fs::write(&tmp, format!("{now_unix}\n"))?;
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(())
}

/// Pure staleness decision on the file's content. A timestamp in the future (clock step) counts as fresh.
pub fn evaluate(content: &str, now_unix: u64, stale_secs: u64) -> Result<(), String> {
    let ts: u64 = content.trim().parse().map_err(|_| format!("heartbeat content {:?} is not a unix timestamp", content.trim()))?;
    let age = now_unix.saturating_sub(ts);
    if age > stale_secs {
        return Err(format!("last heartbeat {age}s ago exceeds the {stale_secs}s threshold: the tick loop is stalled"));
    }
    Ok(())
}

/// Reads the file at `path` and evaluates it.
pub fn check_file(path: &Path, now_unix: u64, stale_secs: u64) -> Result<(), String> {
    let content = std::fs::read_to_string(path).map_err(|e| format!("cannot read heartbeat file {}: {e}", path.display()))?;
    evaluate(&content, now_unix, stale_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("rebalancer-liveness-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn path_defaults_on_unset_or_blank() {
        for raw in [None, Some(""), Some("  ")] {
            assert_eq!(liveness_path(raw), PathBuf::from(DEFAULT_LIVENESS_FILE));
        }
        assert_eq!(liveness_path(Some(" /x/y ")), PathBuf::from("/x/y"));
    }

    #[test]
    fn stale_threshold_is_max_of_floor_and_three_ticks() {
        assert_eq!(stale_secs_for_tick(300), 900);
        assert_eq!(stale_secs_for_tick(60), 900);
        assert_eq!(stale_secs_for_tick(600), 1800);
        assert_eq!(stale_secs_for_tick(u64::MAX), u64::MAX);
    }

    #[test]
    fn evaluate_fresh_boundary_stale_future_and_garbage() {
        assert!(evaluate("1000\n", 1000, 900).is_ok());
        assert!(evaluate("100", 1000, 900).is_ok(), "exactly at the threshold is fresh");
        assert!(evaluate("99", 1000, 900).is_err(), "one second past is stale");
        assert!(evaluate("2000", 1000, 900).is_ok(), "a future stamp is fresh");
        assert!(evaluate("", 1000, 900).is_err());
        assert!(evaluate("abc", 1000, 900).is_err());
    }

    #[test]
    fn write_is_atomic_leaves_no_temp_and_overwrites() {
        let dir = scratch("write");
        let path = dir.join("hb");
        write_heartbeat(&path, 111).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap().trim(), "111");
        write_heartbeat(&path, 222).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap().trim(), "222");
        assert!(!dir.join("hb.tmp").exists(), "the temp file is renamed away");
        assert!(check_file(&path, 222, 900).is_ok());
        assert!(check_file(&path, 5000, 900).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_file_fails_check_and_unwritable_dir_errors_without_panicking() {
        let dir = scratch("missing");
        assert!(check_file(&dir.join("nope"), 1, 900).is_err());
        assert!(write_heartbeat(&dir.join("no-such-dir").join("hb"), 1).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
