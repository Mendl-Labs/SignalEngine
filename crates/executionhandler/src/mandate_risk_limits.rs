//! Derives `PositionLimits`/`CircuitBreakerConfig` from a tenant's mandate,
//! instead of the fixed, tenant-blind defaults `RiskManager::with_defaults()`
//! has always used.
//!
//! # Scope (see the "properly scope the remaining MVP gaps" plan)
//!
//! - Mandates are per-account (`mandates.account_id NOT NULL REFERENCES
//!   exchange_credentials(id)`), but this platform's pre-trade risk gate is
//!   one shared, tenant-wide `RiskManager`. [`fetch_tenant_mandate`] takes
//!   the first active mandate found for the tenant, across any of its
//!   accounts -- a documented simplification, correct for the common case of
//!   one account per tenant today. Revisit if a tenant ever has multiple
//!   accounts under different mandates.
//! - `CircuitBreakerConfig.max_drawdown_pct` is a single scalar trip-point;
//!   the mandate's `drawdown_ladder` is multi-rung (intermediate `Shrink`
//!   actions, then a final `HaltFlatten`). [`drawdown_trip_fraction`] maps
//!   only the final `HaltFlatten` rung -- the shrink rungs' gradual-reduction
//!   nuance has no equivalent in `CircuitBreakerConfig` today and is dropped,
//!   not silently approximated.
//! - Converting `exposure.max_position`/`max_gross` (fractions of equity)
//!   into `PositionLimits`' dollar fields needs the account's current
//!   equity, which is NOT part of the mandate -- callers must fetch it
//!   separately (see `exchanges::generic::connector::GenericConnector::
//!   get_equity`) and pass it in.
//! - `exposure.max_asset_class` (per-asset-class caps) and
//!   `exposure.max_net` have no field in `PositionLimits` to land in and are
//!   not applied here -- `PositionLimits` only models a single per-symbol
//!   and portfolio-wide gross cap, not per-asset-class or net exposure.
//!   `max_position_size`/`max_order_size` (base-currency UNITS) and
//!   `max_open_positions` (a count) also have no mandate-derived source
//!   (the mandate specifies fractions/notionals, never units or a position
//!   count) and are left at the caller-supplied baseline.

use mandate_core::mandate::{LadderAction, MandateBody};

use crate::risk_controls::{CircuitBreakerConfig, PositionLimits};

/// Raw-SQL fetch of a tenant's first active mandate, across any of its
/// accounts (see module docs for why "first", not "the" mandate). Mirrors
/// `rebalancer-store::account_source::PgAccountSource`'s safety discipline
/// (explicit `sql_types`, tenant-scoped, no dependency on either schema
/// crate's generated models) without depending on that crate directly --
/// that crate's `PgAccountSource` is purpose-built for the rebalancer paper
/// pilot's account-eligibility logic (ETF/Alpaca-only, pilot-plan-bound),
/// not a general-purpose mandate lookup.
#[cfg(feature = "postgres")]
pub mod fetch {
    use diesel::sql_types::{Jsonb, Uuid as SqlUuid};
    use diesel_async::{AsyncPgConnection, RunQueryDsl};
    use mandate_core::mandate::validate_json;

    use super::MandateBody;

    #[derive(diesel::QueryableByName)]
    struct MandateBodyRow {
        #[diesel(sql_type = Jsonb)]
        body: serde_json::Value,
    }

    /// `Ok(None)` means no active mandate exists for this tenant (not an
    /// error -- the risk gate's caller-supplied baseline limits apply as
    /// before). `Err` is reserved for a real query/connection failure or a
    /// stored mandate that fails validation (the latter should never happen
    /// for a mandate the platform itself wrote, but is treated as a hard
    /// error rather than silently falling back, since that would mean
    /// trusting un-validated caps).
    pub async fn fetch_tenant_mandate(
        conn: &mut AsyncPgConnection,
        tenant_id: uuid::Uuid,
    ) -> Result<Option<MandateBody>, String> {
        let rows: Vec<MandateBodyRow> = diesel::sql_query(
            "SELECT body FROM mandates \
             WHERE tenant_id = $1 AND status = 'active' \
             ORDER BY granted_at ASC NULLS LAST, id ASC \
             LIMIT 1",
        )
        .bind::<SqlUuid, _>(tenant_id)
        .get_results(conn)
        .await
        .map_err(|e| format!("mandate lookup failed: {e}"))?;

        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };

        validate_json(&row.body)
            .map(Some)
            .map_err(|violations| {
                let why: Vec<String> =
                    violations.iter().map(|v| format!("{}: {}", v.field, v.message)).collect();
                format!("stored mandate failed validation: {}", why.join("; "))
            })
    }
}

/// The final `HaltFlatten` rung's threshold, or `None` if the ladder has
/// none (mandate validation requires the ladder to end in `HaltFlatten`
/// when non-empty, so `None` only happens for an empty ladder).
fn drawdown_trip_fraction(mandate: &MandateBody) -> Option<f64> {
    mandate
        .loss
        .drawdown_ladder
        .iter()
        .rev()
        .find(|rung| rung.action == LadderAction::HaltFlatten)
        .map(|rung| rung.at)
}

/// `equity` is the account's current equity in the mandate's own currency
/// (see `exchanges::generic::connector::GenericConnector::get_equity`) --
/// fetched separately, not part of the mandate.
pub fn position_limits_from_mandate(mandate: &MandateBody, equity: f64, baseline: PositionLimits) -> PositionLimits {
    let max_order_notional: f64 = mandate
        .exposure
        .max_order_notional
        .amount
        .parse()
        .unwrap_or(baseline.max_order_value);

    PositionLimits {
        max_position_value: mandate.exposure.max_position * equity,
        max_portfolio_value: mandate.exposure.max_gross * equity,
        max_order_value: max_order_notional,
        // No mandate-derived source for these (see module docs) -- keep the
        // caller-supplied baseline.
        max_position_size: baseline.max_position_size,
        max_open_positions: baseline.max_open_positions,
        max_order_size: baseline.max_order_size,
    }
}

pub fn circuit_breaker_config_from_mandate(
    mandate: &MandateBody,
    equity: f64,
    baseline: CircuitBreakerConfig,
) -> CircuitBreakerConfig {
    CircuitBreakerConfig {
        max_drawdown_pct: drawdown_trip_fraction(mandate).unwrap_or(baseline.max_drawdown_pct),
        max_daily_loss: mandate.loss.daily_loss_limit * equity,
        // Rate-limit/failure fields have no mandate-derived source.
        ..baseline
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mandate_core::mandate::{LadderRung, Loss, ResumePolicy};

    fn ladder(rungs: Vec<(f64, LadderAction, Option<f64>)>) -> Loss {
        Loss {
            daily_loss_limit: 0.05,
            drawdown_ladder: rungs
                .into_iter()
                .map(|(at, action, scale)| LadderRung { at, action, scale })
                .collect(),
            resume_after_halt: ResumePolicy::HumanOnly,
        }
    }

    #[test]
    fn drawdown_trip_fraction_takes_the_final_halt_flatten_rung() {
        let loss = ladder(vec![
            (0.05, LadderAction::Shrink, Some(0.5)),
            (0.10, LadderAction::HaltFlatten, None),
        ]);
        let mandate = test_mandate_with_loss(loss);
        assert_eq!(drawdown_trip_fraction(&mandate), Some(0.10));
    }

    #[test]
    fn drawdown_trip_fraction_none_for_empty_ladder() {
        let loss = ladder(vec![]);
        let mandate = test_mandate_with_loss(loss);
        assert_eq!(drawdown_trip_fraction(&mandate), None);
    }

    // Builds a minimal, otherwise-arbitrary valid-shaped MandateBody with
    // only `loss` varied, for the two tests above.
    fn test_mandate_with_loss(loss: Loss) -> MandateBody {
        use mandate_core::mandate::{
            Autonomy, AutonomyLevel, Basis, Capital, CapitalSource, DigestCadence, Exposure, Money,
            Reporting, Universe,
        };

        MandateBody {
            basis: Basis {
                capital_source: CapitalSource::Own,
                jurisdiction: "US-GA".to_string(),
                acknowledgements: vec![],
            },
            capital: Capital {
                allocated: Money { amount: "10000".to_string(), ccy: "USD".to_string() },
                min_cash_reserve: 0.0,
            },
            universe: Universe {
                venues: vec![],
                asset_classes: vec![],
                instrument_allow: vec![],
                instrument_deny: vec![],
                shorting: false,
                derivatives: false,
                leverage_max_gross: 1.0,
            },
            exposure: Exposure {
                max_position: 0.1,
                max_asset_class: Default::default(),
                max_gross: 1.0,
                max_net: 1.0,
                max_order_notional: Money { amount: "1000".to_string(), ccy: "USD".to_string() },
                max_orders_per_day: 100,
                max_turnover_per_day: 1.0,
            },
            loss,
            autonomy: Autonomy {
                level: AutonomyLevel::L0,
                may: vec![],
                needs_confirmation: vec![],
                never: vec![],
            },
            reporting: Reporting { digest: DigestCadence::Daily, alert_channels: vec![] },
        }
    }
}
