//! Hand-written JSON (de)serialization for the small, plain-data types this crate persists that do
//! NOT derive `serde::Serialize`/`Deserialize` upstream (`rebalancer-risk`'s state types are pure,
//! dependency-free data types with no serde dependency at all -- see that crate's own `Cargo.toml`).
//! Deliberately hand-written HERE rather than adding a `serde` feature to `rebalancer-risk`: every
//! field below is a `pub` field or has a public accessor, so nothing here reaches into a private
//! invariant, and it keeps that crate's dependency graph exactly as it was (`rebalancer-store`'s own
//! `Cargo.toml` comment on the diesel/diesel-async versions makes the same "match what exists, don't
//! grow it" call). See `run_store.rs`'s own module doc for the equivalent, larger decision about
//! `RunRecord` itself.

use broker_adapters::Dec;
use chrono::{DateTime, Utc};
use rebalancer_risk::state::{AccountStatus, HaltReason, HaltRecord, ResumeRecord};
use serde_json::{json, Value};

pub fn dt_to_json(d: DateTime<Utc>) -> Value {
    Value::String(d.to_rfc3339())
}

pub fn dt_from_json(v: &Value) -> Result<DateTime<Utc>, String> {
    let s = v.as_str().ok_or("expected a string timestamp")?;
    DateTime::parse_from_rfc3339(s).map(|d| d.with_timezone(&Utc)).map_err(|e| format!("bad timestamp {s:?}: {e}"))
}

pub fn dec_to_json(d: Dec) -> Value {
    Value::String(d.to_string())
}

pub fn dec_from_json(v: &Value) -> Result<Dec, String> {
    let s = v.as_str().ok_or("expected a string decimal")?;
    Dec::parse(s).map_err(|e| format!("bad decimal {s:?}: {e}"))
}

pub fn opt_dec_to_json(d: Option<Dec>) -> Value {
    d.map(dec_to_json).unwrap_or(Value::Null)
}

pub fn opt_dec_from_json(v: &Value) -> Result<Option<Dec>, String> {
    if v.is_null() {
        return Ok(None);
    }
    dec_from_json(v).map(Some)
}

pub fn status_to_str(s: AccountStatus) -> &'static str {
    s.as_str()
}

pub fn status_from_str(s: &str) -> Result<AccountStatus, String> {
    match s {
        "active" => Ok(AccountStatus::Active),
        "shrunk" => Ok(AccountStatus::Shrunk),
        "flattening" => Ok(AccountStatus::Flattening),
        "halted" => Ok(AccountStatus::Halted),
        other => Err(format!("unknown account status {other:?}")),
    }
}

pub fn halt_reason_to_str(r: HaltReason) -> &'static str {
    r.code()
}

pub fn halt_reason_from_str(s: &str) -> Result<HaltReason, String> {
    HaltReason::ALL.into_iter().find(|r| r.code() == s).ok_or_else(|| format!("unknown halt reason {s:?}"))
}

pub fn halt_record_to_json(h: &HaltRecord) -> Value {
    json!({
        "reason": halt_reason_to_str(h.reason),
        "at": dt_to_json(h.at),
        "detail": h.detail,
        "equity_at_halt": opt_dec_to_json(h.equity_at_halt),
        "hwm_at_halt": opt_dec_to_json(h.hwm_at_halt),
        "failed_flatten_attempts": h.failed_flatten_attempts,
    })
}

pub fn halt_record_from_json(v: &Value) -> Result<HaltRecord, String> {
    Ok(HaltRecord {
        reason: halt_reason_from_str(v["reason"].as_str().ok_or("halt.reason missing")?)?,
        at: dt_from_json(&v["at"])?,
        detail: v["detail"].as_str().unwrap_or_default().to_string(),
        equity_at_halt: opt_dec_from_json(&v["equity_at_halt"])?,
        hwm_at_halt: opt_dec_from_json(&v["hwm_at_halt"])?,
        failed_flatten_attempts: v["failed_flatten_attempts"].as_u64().unwrap_or(0) as u32,
    })
}

pub fn resume_record_to_json(r: &ResumeRecord) -> Value {
    json!({
        "approver": r.approver,
        "at": dt_to_json(r.at),
        "note": r.note,
        "equity_at_resume": dec_to_json(r.equity_at_resume),
        "hwm_before": opt_dec_to_json(r.hwm_before),
        "halt_reason": halt_reason_to_str(r.halt_reason),
        "halted_at": dt_to_json(r.halted_at),
    })
}

pub fn resume_record_from_json(v: &Value) -> Result<ResumeRecord, String> {
    Ok(ResumeRecord {
        approver: v["approver"].as_str().unwrap_or_default().to_string(),
        at: dt_from_json(&v["at"])?,
        note: v["note"].as_str().unwrap_or_default().to_string(),
        equity_at_resume: dec_from_json(&v["equity_at_resume"])?,
        hwm_before: opt_dec_from_json(&v["hwm_before"])?,
        halt_reason: halt_reason_from_str(v["halt_reason"].as_str().ok_or("resume.halt_reason missing")?)?,
        halted_at: dt_from_json(&v["halted_at"])?,
    })
}

pub fn resumes_to_json(rs: &[ResumeRecord]) -> Value {
    Value::Array(rs.iter().map(resume_record_to_json).collect())
}

pub fn resumes_from_json(v: &Value) -> Result<Vec<ResumeRecord>, String> {
    v.as_array().ok_or("expected a resumes array")?.iter().map(resume_record_from_json).collect()
}
