//! The service's wiring of the first-seen-latency / revision recorder (`rebalancer_run::latency`, W9.1; council
//! R25: "start now, 20+ sessions gate the paper cycle"). Pure configuration and instrument selection here; the tick
//! call itself is in `main.rs` (`observe_tick`), after `run_tick`, wrapped so a panic or an error in the observer can
//! never fail or delay a run.
//!
//! # Flags
//! * `LATENCY_RECORDER_ENABLED` (default `true`): the recorder is a read-only observer the councils asked to start
//!   immediately, so unlike anything that trades it is ON unless switched off. `false` / `0` / `no` switch it off; any
//!   other value refuses to start (this binary's posture: a misread setting is never silently defaulted).
//! * `LATENCY_RECORDER_KINDS` (default `etf_trend,crypto_trend`): sleeve kinds whose instruments are observed even
//!   when no configured account has a sleeve of that kind. The councils want BOTH classes measured (ETF around
//!   20:00Z, crypto's 00:10Z bar is the riskier one), and the pilot has only an ETF sleeve, so by default the two
//!   crypto instruments are observed too (quote `USD`, the only quote the source supports). The instruments of the
//!   configured sleeves are ALWAYS observed in addition. An empty value observes only the sleeves' instruments.
//!
//! # What runs under the kill flag
//! Nothing: the kill flag means "touch nothing external", and the recorder calls the vendor. A tick skipped for the
//! kill flag skips the observation too (sessions are lost; switch the flag off, not the recorder).

use rebalancer_run::data::{SleeveKind, SleeveSpec};
use rebalancer_run::latency::{instruments_for_kind, instruments_for_sleeves, Instrument};

pub const ENV_ENABLED: &str = "LATENCY_RECORDER_ENABLED";
pub const ENV_KINDS: &str = "LATENCY_RECORDER_KINDS";
pub const DEFAULT_KINDS: [SleeveKind; 2] = [SleeveKind::EtfTrend, SleeveKind::CryptoTrend];
/// The crypto quote currency of the baseline instruments (the Massive source supports USD only).
pub const BASELINE_CRYPTO_QUOTE: &str = "USD";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObserverConfig {
    pub enabled: bool,
    /// Kinds observed regardless of the configured sleeves.
    pub kinds: Vec<SleeveKind>,
}

impl Default for ObserverConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            kinds: DEFAULT_KINDS.to_vec(),
        }
    }
}

fn parse_bool(name: &str, raw: &str) -> Result<bool, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        other => Err(format!(
            "{name}={other:?} is not true/false; refusing to start rather than guess"
        )),
    }
}

fn parse_kind(raw: &str) -> Result<SleeveKind, String> {
    match raw.trim() {
        k if k == SleeveKind::EtfTrend.as_str() => Ok(SleeveKind::EtfTrend),
        k if k == SleeveKind::CryptoTrend.as_str() => Ok(SleeveKind::CryptoTrend),
        other => Err(format!(
            "{ENV_KINDS}: unknown sleeve kind {other:?} (expected {} or {})",
            SleeveKind::EtfTrend.as_str(),
            SleeveKind::CryptoTrend.as_str()
        )),
    }
}

impl ObserverConfig {
    /// Read the settings through `lookup` (the process environment in `main`, a map in tests).
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let enabled = match lookup(ENV_ENABLED) {
            None => true,
            Some(v) => parse_bool(ENV_ENABLED, &v)?,
        };
        let kinds = match lookup(ENV_KINDS) {
            None => DEFAULT_KINDS.to_vec(),
            Some(v) => {
                let mut kinds: Vec<SleeveKind> = v
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(parse_kind)
                    .collect::<Result<_, _>>()?;
                kinds.sort();
                kinds.dedup();
                kinds
            }
        };
        Ok(Self { enabled, kinds })
    }

    pub fn from_env() -> Result<Self, String> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// The instruments to observe: the baseline kinds' plus every configured sleeve's, deduplicated and sorted.
    pub fn instruments<'a>(
        &self,
        sleeves: impl IntoIterator<Item = &'a SleeveSpec>,
    ) -> Vec<Instrument> {
        let mut out: Vec<Instrument> = self
            .kinds
            .iter()
            .flat_map(|k| instruments_for_kind(*k, BASELINE_CRYPTO_QUOTE))
            .collect();
        out.extend(instruments_for_sleeves(sleeves));
        out.sort();
        out.dedup();
        out
    }

    pub fn describe(&self) -> String {
        if !self.enabled {
            return format!("latency recorder DISABLED ({ENV_ENABLED}=false)");
        }
        let kinds: Vec<&str> = self.kinds.iter().map(|k| k.as_str()).collect();
        format!("latency recorder enabled: baseline kinds [{}] plus every configured sleeve's instruments, every tick", kinds.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rebalancer_core::Dec;
    use std::collections::BTreeMap;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: BTreeMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| m.get(k).cloned()
    }

    fn sleeve(kind: SleeveKind, quote: &str) -> SleeveSpec {
        SleeveSpec {
            id: "s".into(),
            kind,
            share: Dec::parse("1").unwrap(),
            venue: "v".into(),
            asset_class: "a".into(),
            quote: quote.into(),
        }
    }

    #[test]
    fn defaults_are_on_and_both_kinds() {
        let c = ObserverConfig::from_lookup(lookup(&[])).unwrap();
        assert_eq!(c, ObserverConfig::default());
        assert!(c.enabled);
        assert_eq!(c.kinds, [SleeveKind::EtfTrend, SleeveKind::CryptoTrend]);
        assert!(c.describe().contains("enabled"));
    }

    #[test]
    fn the_flag_parses_true_false_variants_and_refuses_anything_else() {
        for v in ["false", "0", "no", " FALSE "] {
            assert!(
                !ObserverConfig::from_lookup(lookup(&[(ENV_ENABLED, v)]))
                    .unwrap()
                    .enabled,
                "{v}"
            );
        }
        for v in ["true", "1", "yes"] {
            assert!(
                ObserverConfig::from_lookup(lookup(&[(ENV_ENABLED, v)]))
                    .unwrap()
                    .enabled,
                "{v}"
            );
        }
        let e = ObserverConfig::from_lookup(lookup(&[(ENV_ENABLED, "maybe")])).unwrap_err();
        assert!(
            e.contains(ENV_ENABLED) && e.contains("refusing to start"),
            "{e}"
        );
        assert!(
            ObserverConfig::from_lookup(lookup(&[(ENV_ENABLED, "false")]))
                .unwrap()
                .describe()
                .contains("DISABLED")
        );
    }

    #[test]
    fn kinds_parse_sort_dedupe_and_refuse_an_unknown_kind() {
        let c = ObserverConfig::from_lookup(lookup(&[(
            ENV_KINDS,
            "crypto_trend, etf_trend,crypto_trend",
        )]))
        .unwrap();
        assert_eq!(c.kinds, [SleeveKind::EtfTrend, SleeveKind::CryptoTrend]);
        let c = ObserverConfig::from_lookup(lookup(&[(ENV_KINDS, "")])).unwrap();
        assert!(
            c.kinds.is_empty(),
            "an empty value observes only the configured sleeves' instruments"
        );
        let e = ObserverConfig::from_lookup(lookup(&[(ENV_KINDS, "fx_tsmom")])).unwrap_err();
        assert!(e.contains("fx_tsmom"), "{e}");
    }

    #[test]
    fn instruments_are_the_union_of_the_baseline_kinds_and_the_configured_sleeves() {
        let c = ObserverConfig::default();
        let all = c.instruments(&[sleeve(SleeveKind::EtfTrend, "")]);
        let symbols: Vec<&str> = all.iter().map(|i| i.symbol.as_str()).collect();
        assert_eq!(
            all.len(),
            7,
            "five ETFs and two crypto instruments, the ETF sleeve adding nothing new: {symbols:?}"
        );
        assert!(all.windows(2).all(|w| w[0] < w[1]));
        assert!(all
            .iter()
            .filter(|i| i.kind == SleeveKind::CryptoTrend)
            .all(|i| i.quote == BASELINE_CRYPTO_QUOTE));

        let only_sleeves = ObserverConfig {
            enabled: true,
            kinds: vec![],
        };
        assert_eq!(only_sleeves.instruments(&[]).len(), 0);
        assert_eq!(
            only_sleeves
                .instruments(&[sleeve(SleeveKind::CryptoTrend, "USD")])
                .len(),
            2
        );
        // a crypto sleeve on another quote is a distinct instrument from the USD baseline
        let with_eur = c.instruments(&[sleeve(SleeveKind::CryptoTrend, "EUR")]);
        assert_eq!(with_eur.len(), 9);
    }
}
