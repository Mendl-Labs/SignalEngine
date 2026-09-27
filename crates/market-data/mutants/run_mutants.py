#!/usr/bin/env python3
"""Hand-written mutation runner for `market-data` (same idea as reference-rules/mutants/run_mutants.py).

Each mutant is ONE exact source edit (`old` must occur exactly once in the file). The runner applies it, runs the
crate's whole always-on test suite, records which tests FAIL (or that the mutant does not compile, which makes it
invalid), and ALWAYS restores the file. A mutant is KILLED when at least one test fails; a SURVIVOR means the tests
do not notice the bug and the suite needs a new test.

    python3 run_mutants.py --repo ~/se/SignalEngine-datasource [--only M07,M12] [--cmd '<template>']

`--cmd` is the shell command template that runs the tests; `{id}` is replaced by the mutant id and the command's
combined output is what gets parsed (default: run cargo directly). `--log` is an optional template for a log file
to parse instead of the command's own output (used with the shared build wrapper, which writes its own log).
"""
import argparse
import json
import os
import re
import subprocess
import sys

# (id, file, old, new, what)
MUTANTS = [
    ("M01", "src/time.rs", "    if date >= as_of {\n        return false;", "    if date > as_of {\n        return false;",
     "date filter `<=` for `<`: the bar dated the run date is kept"),
    ("M02", "src/source.rs", "if is_complete(plan.clock, d, as_of, now, plan.settle) {", "if true {",
     "no completeness filter at all: forming and future bars are returned"),
    ("M03", "src/time.rs", "    now >= ready\n", "    true\n",
     "clock rule removed: only the date rule protects against an as_of ahead of real time"),
    ("M04", "src/time.rs", "Some(if date > start && date <= end { 4 } else { 5 })", "Some(5)",
     "wrong timezone conversion: New York treated as always standard time (EST)"),
    ("M05", "src/time.rs", "    let date = dt.date_naive();\n    match clock {", "    let date = (dt - Duration::hours(5)).date_naive();\n    match clock {",
     "wrong timezone conversion: the date is read at a fixed UTC-5 (shifts every daylight-time and every UTC-midnight bar by a day)"),
    ("M06", "src/source.rs", "                if d <= *prev {", "                if d < *prev {",
     "no strict ordering: duplicate dates accepted"),
    ("M07", "src/source.rs", "                if d <= *prev {", "                if false && d <= *prev {",
     "no ascending check at all"),
    ("M08", "src/source.rs", 'let mut url = format!("https://{}{path}?{QUERY}", self.authority);',
     'let mut url = format!("https://{}{path}?{QUERY}&apiKey={}", self.authority, key.expose());',
     "API key sent in the URL"),
    ("M09", "src/secret.rs", 'f.write_str("SecretString(<redacted>)")', 'write!(f, "SecretString({})", self.0)',
     "API key printed by Debug"),
    ("M10", "src/source.rs", "            429 => Ok(Outcome::Retry", "            403 | 429 => Ok(Outcome::Retry",
     "403 is retried"),
    ("M11", "src/runtime.rs", "let capped = raw.min(self.max_delay);", "let capped = raw;",
     "no backoff cap"),
    ("M12", "src/url.rs", "if is_key_param(name) || carries_secret {", "if false {",
     "next_url is not scrubbed of apiKey"),
    ("M13", "src/aggs.rs", "Some(t) if t == expected_ticker => {}", "Some(_) => {}",
     "wrong symbol accepted"),
    ("M14", "src/source.rs",
     '            other => Err(MassiveError::Malformed { instrument: label.to_string(), detail: format!("unexpected HTTP status {other}") }),',
     '            other => Ok(Outcome::Retry { rate_limited: false, detail: format!("HTTP {other}") }),',
     "unexpected HTTP statuses (malformed class) are retried"),
    ("M15", "src/source.rs", "                    self.clock.sleep(self.cfg.retry.delay_after(attempt, self.jitter.factor()));", "                    let _ = self.jitter.factor();",
     "no backoff between retries"),
    ("M16", "src/source.rs", '            429 => Ok(Outcome::Retry { rate_limited: true, detail: "HTTP 429".to_string() }),',
     '            429 => Err(MassiveError::RateLimited { attempts: 1, local_budget: false }),',
     "429 is not retried"),
    ("M17", "src/source.rs", "            500..=599 => Ok(Outcome::Retry", "            600..=699 => Ok(Outcome::Retry",
     "5xx is not retried"),
    ("M18", "src/source.rs", "if !self.budget.take(self.clock.now()) {", "if false && !self.budget.take(self.clock.now()) {",
     "per-tick request budget ignored"),
    ("M19", "src/source.rs", "                    if attempt >= max {", "                    if attempt > max {",
     "one attempt too many"),
    ("M20", "src/aggs.rs", "if !(c.is_finite() && c > 0.0) {", "if !c.is_finite() {",
     "zero and negative closes accepted"),
    ("M21", "src/aggs.rs", 'Some("OK") | Some("DELAYED") => {}', "Some(_) => {}",
     "response status field not checked"),
    ("M22", "src/aggs.rs", "Some(Value::Bool(true)) => {}", "Some(Value::Bool(_)) => {}",
     "an unadjusted series is accepted"),
    ("M23", "src/url.rs", "if !auth.eq_ignore_ascii_case(authority) {", "if false && !auth.eq_ignore_ascii_case(authority) {",
     "next_url on a foreign host is followed (the key would be sent there)"),
    ("M24", "src/source.rs", "                    if page_no == self.cfg.max_pages {", "                    if false && page_no == self.cfg.max_pages {",
     "page cap removed: pages beyond the cap are silently dropped"),
    ("M25", "src/source.rs", "            if !seen.insert(url.clone()) {", "            if false && !seen.insert(url.clone()) {",
     "next_url loops are not detected"),
    ("M26", "src/source.rs", "            for k in (1..=CRYPTO_SMA_DAYS as i64).rev() {", "            for k in (1..=0i64).rev() {",
     "missing crypto days inside the 100-day window are not detected"),
    ("M27", "src/source.rs", "            for d in union {", "            for d in BTreeSet::<NaiveDate>::new() {",
     "missing ETF sessions (cross-ETF alignment) are not detected"),
    ("M28", "src/source.rs", "            if (as_of - newest).num_days() > ETF_MAX_STALE_DAYS {", "            if false {",
     "stale ETF data is accepted by the source"),
    ("M29", "src/source.rs", "            if f.dates.len() < CRYPTO_SMA_DAYS {", "            if f.dates.is_empty() {",
     "too-short crypto history is accepted by the source"),
    ("M30", "src/source.rs", "age.filter(|a| *a > MAX_RESPONSE_AGE_SECS)", "age.filter(|a| *a > u64::MAX)",
     "a cache-served response (Age header) is trusted"),
    ("M31", "src/source.rs", '                ("Cache-Control".to_string(), "no-cache, no-store".to_string()),\n', "",
     "the cache-bypass request header is dropped"),
    ("M32", "src/source.rs", 'format!("Bearer {}", key.expose())', 'format!("Token {}", key.expose())',
     "wrong authorization scheme"),
    ("M33", "src/source.rs", ".and_then(|v| v.get(\"message\").or_else(|| v.get(\"error\")).and_then(Value::as_str).map(|m| bounded(&sc(m))))",
     ".and_then(|v| v.get(\"message\").or_else(|| v.get(\"error\")).and_then(Value::as_str).map(bounded))",
     "the key is not scrubbed from a 403 body that echoes it"),
    ("M34", "src/source.rs", "Err(e) => Outcome::Retry { rate_limited: false, detail: scrub(&e.to_string(), Some(key.expose())) },",
     "Err(e) => Outcome::Retry { rate_limited: false, detail: e.to_string() },",
     "the key is not scrubbed from transport errors"),
    ("M35", "src/source.rs", "let to = as_of.checked_sub_signed(ChronoDuration::days(1)).ok_or_else(unsupported)?;", "let to = as_of.checked_sub_signed(ChronoDuration::days(0)).ok_or_else(unsupported)?;",
     "the request asks for bars up to the run date instead of the day before"),
    ("M36", "src/source.rs", 'if quote != "USD" {', "if false {",
     "non-USD crypto quotes are silently mapped to X:BTC<quote> tickers"),
    ("M37", "src/time.rs", "    let Some(ready) = over.checked_add_signed(settle) else { return false };", "    let Some(ready) = Some(over) else { return false };",
     "the settle margin is ignored"),
    ("M38", "src/time.rs", "pub const STOCK_SESSION_END_HOUR_NY: u32 = 16;", "pub const STOCK_SESSION_END_HOUR_NY: u32 = 4;",
     "a stock session is treated as ending at 04:00 New York"),
    ("M39", "src/url.rs", 'Some((n, _)) if n.eq_ignore_ascii_case("cursor") => format!("{n}=<elided>"),', 'Some((n, _)) if n.eq_ignore_ascii_case("cursor") => seg.to_string(),',
     "the cursor value is recorded in the provenance path"),
    ("M40", "src/url.rs", 'strip_prefix("https://").ok_or_else(|| "the base URL must start with', 'strip_prefix("http://").ok_or_else(|| "the base URL must start with',
     "a clear-text base URL is accepted"),
]


def run(cmd, log):
    p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors="replace")
    text = p.stdout
    if log and os.path.exists(log):
        with open(log, errors="replace") as f:
            text = f.read()
    return p.returncode, text


def parse(text):
    failed = sorted(set(re.findall(r"^test (\S+) \.\.\. FAILED$", text, re.M)))
    compile_error = bool(re.search(r"^error(\[E\d+\])?:", text, re.M)) and not failed
    passed = sum(int(x) for x in re.findall(r"^test result: \w+\. (\d+) passed", text, re.M))
    return failed, compile_error, passed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--crate", default="crates/market-data")
    ap.add_argument("--only", default="")
    ap.add_argument("--cmd", default="cd {repo} && cargo +1.90.0 test -p market-data --no-fail-fast 2>&1")
    ap.add_argument("--log", default="")
    ap.add_argument("--json", default="")
    a = ap.parse_args()
    only = set(x for x in a.only.split(",") if x)
    base = os.path.join(a.repo, a.crate)
    results = []
    for mid, rel, old, new, what in MUTANTS:
        if only and mid not in only:
            continue
        path = os.path.join(base, rel)
        with open(path, newline="") as f:
            original = f.read()
        n = original.count(old)
        if n != 1:
            results.append(dict(id=mid, what=what, status="BAD-PATCH", detail=f"pattern found {n} times in {rel}", failed=[]))
            print(f"{mid} BAD-PATCH ({n} matches) {what}", flush=True)
            continue
        try:
            with open(path, "w", newline="") as f:
                f.write(original.replace(old, new, 1))
            cmd = a.cmd.format(repo=a.repo, id=mid)
            log = a.log.format(id=mid) if a.log else ""
            rc, text = run(cmd, log)
            failed, compile_error, passed = parse(text)
        finally:
            with open(path, "w", newline="") as f:
                f.write(original)
        if compile_error:
            status = "INVALID (does not compile)"
        elif failed:
            status = "KILLED"
        else:
            status = "SURVIVED"
        results.append(dict(id=mid, what=what, file=rel, status=status, failed=failed, passed=passed))
        print(f"{mid} {status} [{len(failed)} failing] {what}", flush=True)
        for t in failed[:6]:
            print(f"      - {t}", flush=True)
    killed = sum(1 for r in results if r["status"] == "KILLED")
    print(f"\n{killed}/{len(results)} killed; survivors: {[r['id'] for r in results if r['status'] == 'SURVIVED']}; invalid: {[r['id'] for r in results if r['status'].startswith(('INVALID', 'BAD'))]}")
    if a.json:
        with open(a.json, "w") as f:
            json.dump(results, f, indent=1)
    sys.exit(0 if all(r["status"] == "KILLED" for r in results) else 1)


if __name__ == "__main__":
    main()
