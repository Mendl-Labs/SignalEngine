//! The continuous equity monitor's service settings (P1-01). The monitor itself is `rebalancer_run::monitor`; this
//! module only decides whether it runs and how often.
//!
//! * `REBALANCER_MONITOR_ENABLED`: `true` turns the monitor on. Unset, blank or anything else is OFF, so merging the
//!   monitor changes nothing in a deployment until the owner sets the flag.
//! * `REBALANCER_MONITOR_SECS`: the interval between two readings of every protected account, default 300 (every 5
//!   minutes, around the clock). Read only when the monitor is on; anything that is not a positive whole number of
//!   seconds refuses the start, like every other malformed setting of this binary.
//!
//! * `REBALANCER_DAY_START_MAX_LAG_SECS`: with the monitor on, the runs and the monitor anchor the daily-loss limit
//!   at 00:00 UTC (`DayStartRule::UtcMidnight`): the first reading at or after 00:00 UTC is the day's start if it
//!   was taken within this many seconds of midnight (default 1800, at most 43200). Off, the historical rule (first
//!   observation of the trading day) is unchanged. The 00:00 UTC rule needs a reading at night, which only the
//!   monitor takes, so it is never enabled without it.
//!
//! The monitor runs inside the tick loop, so its effective cadence is never finer than `REBALANCER_TICK_SECS`
//! (default also 300). [`MonitorSettings::describe`] says so at startup when the interval is shorter than the tick.

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rebalancer_risk::state::{DayStartRule, DEFAULT_DAY_START_MAX_LAG_SECS};

pub const ENV_MONITOR_ENABLED: &str = "REBALANCER_MONITOR_ENABLED";
pub const ENV_MONITOR_SECS: &str = "REBALANCER_MONITOR_SECS";
pub const DEFAULT_MONITOR_SECS: u64 = 300;
pub const ENV_DAY_START_MAX_LAG_SECS: &str = "REBALANCER_DAY_START_MAX_LAG_SECS";
/// The widest anchor window accepted: 12 hours (a reading at noon is not a midnight value).
pub const MAX_DAY_START_MAX_LAG_SECS: i64 = 43_200;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MonitorSettings {
    pub enabled: bool,
    pub interval_secs: u64,
    /// The 00:00 UTC anchor window (used only when `enabled`).
    pub day_start_max_lag_secs: i64,
}

impl MonitorSettings {
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let enabled = lookup(ENV_MONITOR_ENABLED).is_some_and(|v| v.trim() == "true");
        if !enabled {
            return Ok(Self { enabled: false, interval_secs: DEFAULT_MONITOR_SECS, day_start_max_lag_secs: DEFAULT_DAY_START_MAX_LAG_SECS });
        }
        let interval_secs = match lookup(ENV_MONITOR_SECS).map(|v| v.trim().to_string()).filter(|v| !v.is_empty()) {
            None => DEFAULT_MONITOR_SECS,
            Some(v) => match v.parse::<u64>() {
                Ok(n) if n > 0 => n,
                _ => return Err(format!("{ENV_MONITOR_SECS} is {v:?}; it must be a positive whole number of seconds")),
            },
        };
        let day_start_max_lag_secs = match lookup(ENV_DAY_START_MAX_LAG_SECS).map(|v| v.trim().to_string()).filter(|v| !v.is_empty()) {
            None => DEFAULT_DAY_START_MAX_LAG_SECS,
            Some(v) => match v.parse::<i64>() {
                Ok(n) if n > 0 && n <= MAX_DAY_START_MAX_LAG_SECS => n,
                _ => return Err(format!("{ENV_DAY_START_MAX_LAG_SECS} is {v:?}; it must be a whole number of seconds from 1 to {MAX_DAY_START_MAX_LAG_SECS}")),
            },
        };
        Ok(Self { enabled: true, interval_secs, day_start_max_lag_secs })
    }

    /// The day-start rule the runs and the monitor use: the 00:00 UTC anchor with the monitor on, the historical rule
    /// with it off.
    pub fn day_start_rule(&self) -> DayStartRule {
        if self.enabled {
            DayStartRule::UtcMidnight { max_lag_secs: self.day_start_max_lag_secs }
        } else {
            DayStartRule::FirstObservation
        }
    }

    /// One startup log line.
    pub fn describe(&self, tick_secs: u64) -> String {
        if !self.enabled {
            return format!("{ENV_MONITOR_ENABLED} is not true: continuous equity monitor OFF (equity is read only when a run has a pending decision)");
        }
        let mut s = format!(
            "{ENV_MONITOR_ENABLED}=true: continuous equity monitor ON, every {}s around the clock; it reads every protected account's equity and runs the drawdown ladder and daily-loss limit on each reading, and sends no order except a halt rung's flatten (Live mode)",
            self.interval_secs
        );
        s.push_str(&format!(
            "; the daily-loss limit is measured from the first reading within {}s after 00:00 UTC (a day without one pauses new buys and alerts)",
            self.day_start_max_lag_secs
        ));
        if self.interval_secs < tick_secs {
            s.push_str(&format!("; NOTE: the tick interval is {tick_secs}s, so readings are at most once per tick"));
        }
        s
    }
}

/// When the next monitor pass is due. The first call is always due (the service reads every account once at start).
#[derive(Debug, Clone)]
pub struct MonitorSchedule {
    interval: ChronoDuration,
    last: Option<DateTime<Utc>>,
}

impl MonitorSchedule {
    pub fn new(interval_secs: u64) -> Self {
        Self { interval: ChronoDuration::seconds(interval_secs.min(1_000_000_000_000) as i64), last: None }
    }

    /// True (and the schedule advances) when no pass has run yet or at least one interval has passed since the last.
    pub fn due(&mut self, now: DateTime<Utc>) -> bool {
        if let Some(last) = self.last {
            if now - last < self.interval {
                return false;
            }
        }
        self.last = Some(now);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: BTreeMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        move |k| m.get(k).cloned()
    }

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn the_monitor_is_off_unless_the_flag_is_exactly_true() {
        for raw in [None, Some(""), Some("  "), Some("false"), Some("1"), Some("yes"), Some("TRUE"), Some("on")] {
            let pairs: Vec<(&str, &str)> = raw.map(|v| vec![(ENV_MONITOR_ENABLED, v)]).unwrap_or_default();
            let s = MonitorSettings::from_lookup(lookup(&pairs)).unwrap();
            assert!(!s.enabled, "{raw:?}");
        }
        assert!(MonitorSettings::from_lookup(lookup(&[(ENV_MONITOR_ENABLED, "true")])).unwrap().enabled);
        assert!(MonitorSettings::from_lookup(lookup(&[(ENV_MONITOR_ENABLED, " true ")])).unwrap().enabled);
    }

    #[test]
    fn the_interval_defaults_to_five_minutes_and_a_bad_value_refuses_the_start() {
        let on = |secs: Option<&str>| {
            let mut pairs = vec![(ENV_MONITOR_ENABLED, "true")];
            if let Some(v) = secs {
                pairs.push((ENV_MONITOR_SECS, v));
            }
            MonitorSettings::from_lookup(lookup(&pairs))
        };
        assert_eq!(on(None).unwrap().interval_secs, 300);
        assert_eq!(on(Some("")).unwrap().interval_secs, 300);
        assert_eq!(on(Some(" 60 ")).unwrap().interval_secs, 60);
        for bad in ["0", "-5", "abc", "1.5"] {
            let e = on(Some(bad)).unwrap_err();
            assert!(e.contains(ENV_MONITOR_SECS), "{e}");
        }
        // Off: a bad interval is never even read.
        let off = MonitorSettings::from_lookup(lookup(&[(ENV_MONITOR_SECS, "abc")])).unwrap();
        assert!(!off.enabled);
    }

    #[test]
    fn describe_says_off_or_on_and_warns_when_the_tick_is_coarser() {
        let off = MonitorSettings { enabled: false, interval_secs: 300, day_start_max_lag_secs: 1800 };
        assert!(off.describe(300).contains("OFF"));
        let on = MonitorSettings { enabled: true, interval_secs: 60, day_start_max_lag_secs: 1800 };
        assert!(on.describe(300).contains("within 1800s after 00:00 UTC"));
        assert!(on.describe(300).contains("ON, every 60s"));
        assert!(on.describe(300).contains("at most once per tick"));
        assert!(!on.describe(60).contains("at most once per tick"));
    }

    #[test]
    fn the_midnight_anchor_is_on_exactly_when_the_monitor_is_on() {
        let off = MonitorSettings::from_lookup(lookup(&[(ENV_DAY_START_MAX_LAG_SECS, "600")])).unwrap();
        assert_eq!(off.day_start_rule(), DayStartRule::FirstObservation, "the 00:00 UTC rule needs the monitor's night readings");
        let on = MonitorSettings::from_lookup(lookup(&[(ENV_MONITOR_ENABLED, "true")])).unwrap();
        assert_eq!(on.day_start_rule(), DayStartRule::UtcMidnight { max_lag_secs: 1800 });
        let custom = MonitorSettings::from_lookup(lookup(&[(ENV_MONITOR_ENABLED, "true"), (ENV_DAY_START_MAX_LAG_SECS, " 600 ")])).unwrap();
        assert_eq!(custom.day_start_rule(), DayStartRule::UtcMidnight { max_lag_secs: 600 });
        for bad in ["0", "-1", "43201", "abc", "1.5"] {
            let e = MonitorSettings::from_lookup(lookup(&[(ENV_MONITOR_ENABLED, "true"), (ENV_DAY_START_MAX_LAG_SECS, bad)])).unwrap_err();
            assert!(e.contains(ENV_DAY_START_MAX_LAG_SECS), "{bad}: {e}");
        }
        assert!(MonitorSettings::from_lookup(lookup(&[(ENV_MONITOR_ENABLED, "true"), (ENV_DAY_START_MAX_LAG_SECS, "43200")])).is_ok());
    }

    #[test]
    fn the_schedule_is_due_at_once_then_once_per_interval() {
        let mut s = MonitorSchedule::new(300);
        let t = at("2026-10-23T00:00:00Z");
        assert!(s.due(t), "the first pass runs at start");
        assert!(!s.due(t + ChronoDuration::seconds(299)));
        assert!(s.due(t + ChronoDuration::seconds(300)));
        assert!(!s.due(t + ChronoDuration::seconds(301)));
        assert!(s.due(t + ChronoDuration::seconds(900)), "a late tick is due, never a burst of catch-up passes");
        assert!(!s.due(t + ChronoDuration::seconds(901)));
    }
}
