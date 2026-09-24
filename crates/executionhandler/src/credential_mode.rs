//! How this SignalEngine process obtains exchange credentials, stated once.
//!
//! A live deployment needs a tenant-scoped credential. There are exactly three
//! situations, and the process must know (and say) which one it is in:
//!
//! * [`CredentialMode::None`] -- no provider at all. Live trading is DISABLED;
//!   every live deployment is rejected loudly (see
//!   `strategyloader::reject_live_deployment`). Paper trading is unaffected.
//! * [`CredentialMode::SingleTenant`] -- the public-schema fallback provider
//!   bound to the ONE tenant in `TENANT_ID`. It serves that tenant only.
//! * [`CredentialMode::Injected`] -- a caller passed its own provider to
//!   `HostedObjectBuilder::with_credential_provider`.
//!
//! The decision is a pure function ([`compute_credential_mode`]) so it can be
//! tested exhaustively, and the deployment can DECLARE the mode it expects via
//! the `CREDENTIAL_MODE` env var ([`resolve_credential_mode`]); a mismatch
//! between what was declared and what the process can actually do is an error
//! and downgrades to `None` (fail closed) instead of silently trading (or not
//! trading) under the wrong assumption.

use std::sync::RwLock;
use uuid::Uuid;

/// See the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialMode {
    /// No credential provider: live trading disabled.
    None,
    /// The single-tenant fallback provider, bound to this tenant.
    SingleTenant(Uuid),
    /// A provider was injected by the embedding host.
    Injected,
}

impl CredentialMode {
    /// Stable machine label: `none` | `single_tenant` | `injected`.
    pub fn label(&self) -> &'static str {
        match self {
            CredentialMode::None => "none",
            CredentialMode::SingleTenant(_) => "single_tenant",
            CredentialMode::Injected => "injected",
        }
    }

    /// True when live deployments may resolve credentials at all.
    pub fn live_enabled(&self) -> bool {
        !matches!(self, CredentialMode::None)
    }

    /// The single startup log line, e.g. `credential mode: single_tenant (tenant ...)`.
    pub fn startup_line(&self) -> String {
        match self {
            CredentialMode::None => {
                "credential mode: none (live trading disabled: no provider)".to_string()
            }
            CredentialMode::SingleTenant(t) => {
                format!("credential mode: single_tenant (tenant {})", t)
            }
            CredentialMode::Injected => "credential mode: injected".to_string(),
        }
    }
}

impl std::fmt::Display for CredentialMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// Pure: which mode can this process actually run in?
///
/// * a provider was injected -> `Injected`
/// * else a database URL AND a valid, non-nil `TENANT_ID` -> `SingleTenant`
/// * else -> `None`
pub fn compute_credential_mode(
    injected_provider: bool,
    database_url: Option<&str>,
    tenant_id: Option<&str>,
) -> CredentialMode {
    if injected_provider {
        return CredentialMode::Injected;
    }
    let has_db = database_url.map(|u| !u.trim().is_empty()).unwrap_or(false);
    if !has_db {
        return CredentialMode::None;
    }
    match tenant_id.map(str::trim).and_then(|t| Uuid::parse_str(t).ok()) {
        Some(t) if !t.is_nil() => CredentialMode::SingleTenant(t),
        _ => CredentialMode::None,
    }
}

/// Outcome of cross-checking the computed mode with the declared one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedCredentialMode {
    /// The mode the process will actually use.
    pub mode: CredentialMode,
    /// Set when the declaration disagreed with reality (log at ERROR).
    pub error: Option<String>,
    /// Set when the operator declared `none` although a provider is available
    /// (log at WARN; the provider is deliberately disabled).
    pub warning: Option<String>,
}

/// Pure: cross-check `declared` (the `CREDENTIAL_MODE` env var, `None`/empty =
/// not declared) against the `computed` mode.
///
/// * not declared -> computed
/// * declared `none` -> `None` always (an explicit operator switch-off; a
///   warning when a provider was available)
/// * declared `single_tenant` / `injected` -> only if computed is the same
///   kind, otherwise ERROR and `None`
/// * anything else -> ERROR and `None`
pub fn resolve_credential_mode(
    computed: CredentialMode,
    declared: Option<&str>,
) -> ResolvedCredentialMode {
    let declared = declared.map(|d| d.trim().to_ascii_lowercase()).filter(|d| !d.is_empty());
    let Some(declared) = declared else {
        return ResolvedCredentialMode { mode: computed, error: None, warning: None };
    };
    match declared.as_str() {
        "none" => ResolvedCredentialMode {
            mode: CredentialMode::None,
            error: None,
            warning: if computed.live_enabled() {
                Some(format!(
                    "CREDENTIAL_MODE=none: live trading is switched OFF although a credential \
                     provider is available (computed mode: {}). Set credentialMode to enable it.",
                    computed.label()
                ))
            } else {
                None
            },
        },
        "single_tenant" | "injected" => {
            if computed.label() == declared {
                ResolvedCredentialMode { mode: computed, error: None, warning: None }
            } else {
                ResolvedCredentialMode {
                    mode: CredentialMode::None,
                    error: Some(format!(
                        "CREDENTIAL_MODE={} but this process can only run in mode '{}' \
                         (single_tenant needs DATABASE_URL and a valid TENANT_ID). \
                         Treating credential mode as none: live trading DISABLED.",
                        declared,
                        computed.label()
                    )),
                    warning: None,
                }
            }
        }
        other => ResolvedCredentialMode {
            mode: CredentialMode::None,
            error: Some(format!(
                "CREDENTIAL_MODE='{}' is not one of none|single_tenant|injected. \
                 Treating credential mode as none: live trading DISABLED.",
                other
            )),
            warning: None,
        },
    }
}

// ---------------------------------------------------------------------------
// Exposure through the health / metrics surface (additive, tiny).
// ---------------------------------------------------------------------------

static PUBLISHED_MODE: RwLock<Option<CredentialMode>> = RwLock::new(None);

/// Record the resolved mode so `/health` and `/metrics` can report it.
pub fn publish_credential_mode(mode: CredentialMode) {
    if let Ok(mut g) = PUBLISHED_MODE.write() {
        *g = Some(mode);
    }
}

/// The published mode, if any has been published yet.
pub fn published_credential_mode() -> Option<CredentialMode> {
    PUBLISHED_MODE.read().ok().and_then(|g| *g)
}

/// `/health` JSON value: the label, or `"unset"` before startup published it.
pub fn credential_mode_health_value() -> serde_json::Value {
    let label = published_credential_mode().map(|m| m.label()).unwrap_or("unset");
    serde_json::json!({
        "mode": label,
        "live_trading_enabled": published_credential_mode().map(|m| m.live_enabled()).unwrap_or(false),
    })
}

/// Prometheus text for the mode: one series per mode, 1 for the active one.
pub fn credential_mode_prometheus() -> String {
    let active = published_credential_mode().map(|m| m.label());
    let mut out = String::from(
        "# HELP signalengine_credential_mode Active exchange-credential mode (1 = active)\n\
         # TYPE signalengine_credential_mode gauge\n",
    );
    for label in ["none", "single_tenant", "injected"] {
        out.push_str(&format!(
            "signalengine_credential_mode{{mode=\"{}\"}} {}\n",
            label,
            if active == Some(label) { 1 } else { 0 }
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: &str = "11111111-2222-3333-4444-555555555555";

    fn tid() -> Uuid {
        Uuid::parse_str(T).unwrap()
    }

    #[test]
    fn injected_provider_wins() {
        assert_eq!(compute_credential_mode(true, None, None), CredentialMode::Injected);
        assert_eq!(
            compute_credential_mode(true, Some("postgres://x"), Some(T)),
            CredentialMode::Injected
        );
    }

    #[test]
    fn no_provider_no_tenant_is_none() {
        assert_eq!(compute_credential_mode(false, None, None), CredentialMode::None);
        assert_eq!(
            compute_credential_mode(false, Some("postgres://x"), None),
            CredentialMode::None
        );
    }

    #[test]
    fn tenant_without_database_is_none() {
        assert_eq!(compute_credential_mode(false, None, Some(T)), CredentialMode::None);
        assert_eq!(compute_credential_mode(false, Some("  "), Some(T)), CredentialMode::None);
    }

    #[test]
    fn invalid_or_nil_tenant_is_none() {
        for bad in ["", "not-a-uuid", "00000000-0000-0000-0000-000000000000"] {
            assert_eq!(
                compute_credential_mode(false, Some("postgres://x"), Some(bad)),
                CredentialMode::None,
                "tenant {:?}",
                bad
            );
        }
    }

    #[test]
    fn database_and_valid_tenant_is_single_tenant() {
        assert_eq!(
            compute_credential_mode(false, Some("postgres://x"), Some(T)),
            CredentialMode::SingleTenant(tid())
        );
        assert_eq!(
            compute_credential_mode(false, Some("postgres://x"), Some(&format!(" {} ", T))),
            CredentialMode::SingleTenant(tid())
        );
    }

    #[test]
    fn startup_line_wording() {
        assert_eq!(
            CredentialMode::None.startup_line(),
            "credential mode: none (live trading disabled: no provider)"
        );
        assert_eq!(
            CredentialMode::SingleTenant(tid()).startup_line(),
            format!("credential mode: single_tenant (tenant {})", T)
        );
        assert_eq!(CredentialMode::Injected.startup_line(), "credential mode: injected");
    }

    #[test]
    fn undeclared_keeps_computed() {
        for declared in [None, Some(""), Some("  ")] {
            let r = resolve_credential_mode(CredentialMode::SingleTenant(tid()), declared);
            assert_eq!(r.mode, CredentialMode::SingleTenant(tid()));
            assert!(r.error.is_none() && r.warning.is_none());
        }
    }

    #[test]
    fn declared_single_tenant_without_tenant_is_error_and_none() {
        let r = resolve_credential_mode(CredentialMode::None, Some("single_tenant"));
        assert_eq!(r.mode, CredentialMode::None);
        assert!(r.error.as_deref().unwrap().contains("single_tenant"));
    }

    #[test]
    fn declared_single_tenant_matches() {
        let r = resolve_credential_mode(CredentialMode::SingleTenant(tid()), Some("Single_Tenant"));
        assert_eq!(r.mode, CredentialMode::SingleTenant(tid()));
        assert!(r.error.is_none());
    }

    #[test]
    fn declared_single_tenant_but_injected_is_a_disagreement() {
        let r = resolve_credential_mode(CredentialMode::Injected, Some("single_tenant"));
        assert_eq!(r.mode, CredentialMode::None);
        assert!(r.error.is_some());
    }

    #[test]
    fn declared_injected_without_provider_is_error() {
        let r = resolve_credential_mode(CredentialMode::None, Some("injected"));
        assert_eq!(r.mode, CredentialMode::None);
        assert!(r.error.is_some());
        let ok = resolve_credential_mode(CredentialMode::Injected, Some("injected"));
        assert_eq!(ok.mode, CredentialMode::Injected);
        assert!(ok.error.is_none());
    }

    #[test]
    fn declared_none_switches_live_off_with_warning() {
        let r = resolve_credential_mode(CredentialMode::SingleTenant(tid()), Some("none"));
        assert_eq!(r.mode, CredentialMode::None);
        assert!(r.error.is_none());
        assert!(r.warning.is_some());
        let quiet = resolve_credential_mode(CredentialMode::None, Some("none"));
        assert_eq!(quiet.mode, CredentialMode::None);
        assert!(quiet.error.is_none() && quiet.warning.is_none());
    }

    #[test]
    fn declared_garbage_is_error_and_none() {
        let r = resolve_credential_mode(CredentialMode::SingleTenant(tid()), Some("multi"));
        assert_eq!(r.mode, CredentialMode::None);
        assert!(r.error.is_some());
    }

    #[test]
    fn health_and_prometheus_report_published_mode() {
        // Single test owns the global so parallel tests cannot interleave.
        publish_credential_mode(CredentialMode::None);
        let h = credential_mode_health_value();
        assert_eq!(h["mode"], "none");
        assert_eq!(h["live_trading_enabled"], false);
        let p = credential_mode_prometheus();
        assert!(p.contains("signalengine_credential_mode{mode=\"none\"} 1"));
        assert!(p.contains("signalengine_credential_mode{mode=\"single_tenant\"} 0"));
        publish_credential_mode(CredentialMode::SingleTenant(tid()));
        assert_eq!(credential_mode_health_value()["mode"], "single_tenant");
        assert!(credential_mode_prometheus().contains("{mode=\"single_tenant\"} 1"));
    }
}
