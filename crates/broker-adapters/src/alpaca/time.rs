//! Minimal RFC 3339 parsing (no chrono dependency in this crate). Alpaca timestamps look like
//! `2021-03-16T18:38:01.937734Z` or `2026-09-21T09:30:00-04:00`.

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(y) => 29,
        2 => 28,
        _ => 0,
    }
}

/// `YYYY-MM-DD` with a real calendar date.
pub fn is_valid_ymd(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return false;
    }
    let num = |r: std::ops::Range<usize>| s.get(r).filter(|t| t.bytes().all(|c| c.is_ascii_digit())).and_then(|t| t.parse::<i64>().ok());
    match (num(0..4), num(5..7), num(8..10)) {
        (Some(y), Some(m), Some(d)) => (1..=12).contains(&m) && d >= 1 && d <= days_in_month(y, m),
        _ => false,
    }
}

/// Seconds since the Unix epoch (fractional seconds kept). `None` if the text is not RFC 3339.
pub fn parse_rfc3339(s: &str) -> Option<f64> {
    let s = s.trim();
    let (date, rest) = s.split_once(['T', 't', ' '])?;
    if !is_valid_ymd(date) {
        return None;
    }
    let y: i64 = date[0..4].parse().ok()?;
    let mo: i64 = date[5..7].parse().ok()?;
    let d: i64 = date[8..10].parse().ok()?;

    // time part, then a zone: Z or +-HH:MM
    let zone_at = rest.find(['Z', 'z', '+', '-'])?;
    let (time, zone) = rest.split_at(zone_at);
    let (hms, frac) = match time.split_once('.') {
        Some((h, f)) => (h, Some(f)),
        None => (time, None),
    };
    let mut it = hms.split(':');
    let h: i64 = it.next()?.parse().ok()?;
    let mi: i64 = it.next()?.parse().ok()?;
    let se: i64 = it.next()?.parse().ok()?;
    if it.next().is_some() || !(0..24).contains(&h) || !(0..60).contains(&mi) || !(0..=60).contains(&se) {
        return None;
    }
    let frac_secs = match frac {
        None => 0.0,
        Some(f) if !f.is_empty() && f.bytes().all(|c| c.is_ascii_digit()) => format!("0.{f}").parse::<f64>().ok()?,
        Some(_) => return None,
    };
    let offset_secs = match zone {
        "Z" | "z" => 0,
        z => {
            let sign = if z.starts_with('-') { -1 } else { 1 };
            let body = &z[1..];
            let (oh, om) = body.split_once(':')?;
            let (oh, om): (i64, i64) = (oh.parse().ok()?, om.parse().ok()?);
            if !(0..24).contains(&oh) || !(0..60).contains(&om) {
                return None;
            }
            sign * (oh * 3600 + om * 60)
        }
    };
    let days = days_from_civil(y, mo, d);
    Some((days * 86_400 + h * 3600 + mi * 60 + se - offset_secs) as f64 + frac_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_and_known_instants() {
        assert_eq!(parse_rfc3339("1970-01-01T00:00:00Z"), Some(0.0));
        assert_eq!(parse_rfc3339("2026-09-21T13:30:00Z"), Some(1_789_997_400.0));
        // the same instant expressed in New York summer time
        assert_eq!(parse_rfc3339("2026-09-21T09:30:00-04:00"), Some(1_789_997_400.0));
        assert_eq!(parse_rfc3339("2024-02-29T00:00:00+00:00"), Some(1_709_164_800.0));
    }

    #[test]
    fn fractional_seconds_are_kept() {
        let v = parse_rfc3339("2021-03-16T18:38:01.937734Z").unwrap();
        assert!((v - 1_615_919_881.937734).abs() < 1e-3, "{v}");
    }

    #[test]
    fn garbage_is_none() {
        for s in ["", "2026-09-21", "2026-13-01T00:00:00Z", "2026-02-30T00:00:00Z", "2026-09-21T25:00:00Z", "2026-09-21T10:00:00", "x"] {
            assert!(parse_rfc3339(s).is_none(), "{s:?}");
        }
    }

    #[test]
    fn ymd_validation() {
        assert!(is_valid_ymd("2024-02-29"));
        assert!(!is_valid_ymd("2023-02-29"));
        assert!(!is_valid_ymd("2024-2-9"));
        assert!(!is_valid_ymd("2024-02-29T"));
    }
}
