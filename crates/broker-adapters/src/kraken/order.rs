//! Pure order preparation: validation, rounding to the pair's precision, minimum-size checks,
//! and the `AddOrder` parameter list.
//!
//! Rules (all deliberate, all tested):
//! * Volume is rounded DOWN to `lot_decimals`. We never send more than requested.
//! * A volume that rounds to zero, or falls below `ordermin`, is REFUSED. We never bump it up:
//!   that would increase risk beyond what the caller asked for.
//! * Limit price is rounded to `pair_decimals` and to the tick, in the direction that is never
//!   worse than requested: buys round down, sells round up.
//! * If `costmin` is known and a price is available (limit price or `reference_price`),
//!   `volume * price < costmin` is refused. Market orders without a reference price skip this
//!   check (recorded in `cost_check_skipped`) and rely on Kraken's own validation.
//! * `reduce_only` is refused unless margin params are explicitly enabled: on a spot account
//!   silently dropping a safety flag is worse than an error.

use crate::decimal::{Dec, Rounding};
use crate::error::BrokerError;
use crate::kraken::pairs::PairInfo;
use crate::types::{OrderKind, OrderRequest, SentOrder, Side, TimeInForce};

#[derive(Debug, Clone, Copy, Default)]
pub struct PrepareOptions {
    /// Permit `reduce_only` (Kraken supports it for margin orders only, FROM-MEMORY-OF-DOCS).
    pub allow_reduce_only: bool,
    /// Force `validate=true` on every order (paper mode).
    pub force_validate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedOrder {
    pub symbol: String,
    pub broker_pair: String,
    pub side: Side,
    pub requested_quantity: Dec,
    pub volume: Dec,
    pub volume_dp: u32,
    pub price: Option<Dec>,
    pub price_dp: u32,
    pub userref: i32,
    pub time_in_force: Option<TimeInForce>,
    pub post_only: bool,
    pub reduce_only: bool,
    pub validate: bool,
    pub cost_check_skipped: bool,
}

fn map_dec(e: crate::decimal::DecError) -> BrokerError {
    BrokerError::InvalidRequest(e.to_string())
}

pub fn prepare_order(
    req: &OrderRequest,
    pair: &PairInfo,
    userref: i32,
    opts: &PrepareOptions,
) -> Result<PreparedOrder, BrokerError> {
    if let Some(status) = &pair.status {
        if status != "online" {
            return Err(BrokerError::PairNotTradable { symbol: pair.canonical.clone(), status: status.clone() });
        }
    }
    if !req.quantity.is_positive() {
        return Err(BrokerError::InvalidRequest("quantity must be positive".into()));
    }
    let is_limit = matches!(req.kind, OrderKind::Limit { .. });
    if req.post_only && !is_limit {
        return Err(BrokerError::InvalidRequest("post_only requires a limit order".into()));
    }
    if req.time_in_force.is_some() && !is_limit {
        return Err(BrokerError::InvalidRequest("time_in_force is only supported on limit orders".into()));
    }
    if req.reduce_only && !opts.allow_reduce_only {
        return Err(BrokerError::Unsupported(
            "reduce_only is not supported for spot orders; enable margin params explicitly or size the order to the held balance"
                .into(),
        ));
    }

    // Volume: round down to lot decimals; refuse zero and sub-minimum.
    let volume = req.quantity.round_dp(pair.lot_decimals, Rounding::Floor).map_err(map_dec)?;
    if volume.is_zero() {
        return Err(BrokerError::QuantityRoundsToZero {
            symbol: pair.canonical.clone(),
            requested: req.quantity,
            rounded: volume,
        });
    }
    if volume < pair.order_min {
        return Err(BrokerError::BelowMinQuantity {
            symbol: pair.canonical.clone(),
            min: pair.order_min,
            rounded: volume,
        });
    }

    // Price: buys round down, sells round up; first to decimals, then to the tick.
    let price = match req.kind {
        OrderKind::Market => None,
        OrderKind::Limit { price } => {
            if !price.is_positive() {
                return Err(BrokerError::InvalidPrice("limit price must be positive".into()));
            }
            let mode = match req.side {
                Side::Buy => Rounding::Floor,
                Side::Sell => Rounding::Ceil,
            };
            let mut p = price.round_dp(pair.pair_decimals, mode).map_err(|e| BrokerError::InvalidPrice(e.to_string()))?;
            if let Some(tick) = pair.tick_size {
                p = p.round_to_multiple(tick, mode).map_err(|e| BrokerError::InvalidPrice(e.to_string()))?;
            }
            if p.decimals() > pair.pair_decimals {
                return Err(BrokerError::Config(format!(
                    "{}: tick size {:?} is finer than pair_decimals {}",
                    pair.canonical, pair.tick_size, pair.pair_decimals
                )));
            }
            if !p.is_positive() {
                return Err(BrokerError::InvalidPrice(format!("limit price {price} rounds to zero")));
            }
            Some(p)
        }
    };

    // Minimum cost.
    let mut cost_check_skipped = false;
    if let Some(min_cost) = pair.cost_min {
        match price.or(req.reference_price) {
            Some(px) => {
                let cost = volume.checked_mul(px).ok_or_else(|| BrokerError::InvalidRequest("cost overflow".into()))?;
                if cost < min_cost {
                    return Err(BrokerError::BelowMinCost { symbol: pair.canonical.clone(), min: min_cost, cost });
                }
            }
            None => cost_check_skipped = true,
        }
    }

    Ok(PreparedOrder {
        symbol: pair.canonical.clone(),
        broker_pair: pair.altname.clone(),
        side: req.side,
        requested_quantity: req.quantity,
        volume,
        volume_dp: pair.lot_decimals,
        price,
        price_dp: pair.pair_decimals,
        userref,
        time_in_force: req.time_in_force,
        post_only: req.post_only,
        reduce_only: req.reduce_only,
        validate: req.validate_only || opts.force_validate,
        cost_check_skipped,
    })
}

impl PreparedOrder {
    /// `AddOrder` form parameters (without `nonce`, which the signer prepends).
    pub fn to_params(&self) -> Result<Vec<(String, String)>, BrokerError> {
        let fixed = |d: &Dec, dp: u32| d.to_fixed(dp).map_err(|e| BrokerError::InvalidRequest(e.to_string()));
        let mut p = vec![
            ("pair".to_string(), self.broker_pair.clone()),
            ("type".to_string(), self.side.as_str().to_string()),
            ("ordertype".to_string(), if self.price.is_some() { "limit" } else { "market" }.to_string()),
            ("volume".to_string(), fixed(&self.volume, self.volume_dp)?),
        ];
        if let Some(px) = &self.price {
            p.push(("price".to_string(), fixed(px, self.price_dp)?));
        }
        if let Some(tif) = self.time_in_force {
            let v = match tif {
                TimeInForce::Gtc => "GTC",
                TimeInForce::Ioc => "IOC",
            };
            p.push(("timeinforce".to_string(), v.to_string()));
        }
        if self.post_only {
            p.push(("oflags".to_string(), "post".to_string()));
        }
        p.push(("userref".to_string(), self.userref.to_string()));
        if self.reduce_only {
            p.push(("reduce_only".to_string(), "true".to_string()));
        }
        if self.validate {
            p.push(("validate".to_string(), "true".to_string()));
        }
        Ok(p)
    }

    pub fn sent(&self) -> SentOrder {
        SentOrder {
            broker_pair: self.broker_pair.clone(),
            side: self.side,
            quantity: self.volume,
            price: self.price,
            userref: self.userref,
            validate_only: self.validate,
        }
    }
}
