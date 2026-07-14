"""
Core type definitions for the TradingPlatform Python Strategy SDK.

These types mirror the Rust types used by the backtesting engine, ensuring
zero-friction interop between Python strategy code and the Rust simulation loop.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from types import SimpleNamespace
from typing import Any, Optional
import uuid

# ── compute_signals() return value constants ──────────────────────────────────
# Use these int8 values in the ndarray returned by compute_signals().
#   signals[i] = BUY    → open/increase long at tick i
#   signals[i] = SELL   → open/increase short at tick i
#   signals[i] = HOLD   → no change at tick i (default, value 0)
#   signals[i] = CLOSE  → close any open position at tick i
BUY: int = 1
SELL: int = -1
HOLD: int = 0
CLOSE: int = 2

# ── Signal Types ──────────────────────────────────────────────────────────────


class SignalType(str, Enum):
    """Type of trading signal."""
    BUY = "Buy"
    SELL = "Sell"
    HOLD = "Hold"
    CLOSE = "Close"
    SCALE_IN = "ScaleIn"
    SCALE_OUT = "ScaleOut"
    STOP_LOSS = "StopLoss"
    TAKE_PROFIT = "TakeProfit"
    CANCEL_QUOTES = "CancelQuotes"
    TWO_SIDED_QUOTE = "TwoSidedQuote"
    CROSS_ASSET = "CrossAsset"

    # Derivatives
    BUY_OPTION = "BuyOption"
    SELL_OPTION = "SellOption"
    BUY_FUTURE = "BuyFuture"
    SELL_FUTURE = "SellFuture"
    OPTION_SPREAD = "OptionSpread"
    ROLL_CONTRACT = "RollContract"
    EXERCISE_OPTION = "ExerciseOption"
    HEDGE_DELTA = "HedgeDelta"

    @classmethod
    def custom(cls, name: str) -> str:
        """Create a custom signal type."""
        return f"Custom:{name}"


class SignalStrength(str, Enum):
    """Strength/confidence of a signal."""
    WEAK = "Weak"
    MEDIUM = "Medium"
    STRONG = "Strong"

    @classmethod
    def custom(cls, value: float) -> float:
        """Create a custom signal strength value (0.0 to 1.0)."""
        return max(0.0, min(1.0, value))


class SignalReason(str, Enum):
    """Reason/source for a signal."""
    TECHNICAL_ANALYSIS = "TechnicalAnalysis"
    FUNDAMENTAL_ANALYSIS = "FundamentalAnalysis"
    RISK_MANAGEMENT = "RiskManagement"
    REGIME_CHANGE = "RegimeChange"
    VOLUME_ANALYSIS = "VolumeAnalysis"
    PRICE_ACTION = "PriceAction"
    MEAN_REVERSION = "MeanReversion"
    MOMENTUM = "Momentum"
    ARBITRAGE = "Arbitrage"
    CUSTOM = "Custom"


# ── Instrument & Derivatives Types ─────────────────────────────────────────────


class InstrumentKind(str, Enum):
    """Classification of a financial instrument."""
    SPOT = "Spot"
    PERPETUAL = "Perpetual"
    FUTURE = "Future"
    CALL = "Call"
    PUT = "Put"


@dataclass
class DerivativeMetadata:
    """Static metadata describing a derivative contract.

    Examples:
        # BTC perpetual on Deribit
        DerivativeMetadata("BTC-PERPETUAL", "BTC", InstrumentKind.PERPETUAL, exchange="deribit")

        # BTC call option
        DerivativeMetadata(
            symbol="BTC-30APR26-90000-C", underlying="BTC",
            instrument_kind=InstrumentKind.CALL,
            strike=90_000.0, expiry="2026-04-30T08:00:00Z",
            exchange="deribit",
        )
    """
    symbol: str
    underlying: str
    instrument_kind: InstrumentKind
    contract_multiplier: float = 1.0
    settlement_currency: str = "USD"
    exchange: str = ""
    strike: Optional[float] = None
    expiry: Optional[str] = None  # ISO-8601 datetime string

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for Rust interop."""
        d: dict[str, Any] = {
            "symbol": self.symbol,
            "underlying": self.underlying,
            "instrument_kind": self.instrument_kind.value,
            "contract_multiplier": self.contract_multiplier,
            "settlement_currency": self.settlement_currency,
            "exchange": self.exchange,
        }
        if self.strike is not None:
            d["strike"] = self.strike
        if self.expiry is not None:
            d["expiry"] = self.expiry
        return d


@dataclass(frozen=True)
class Greeks:
    """Black-Scholes option price sensitivities."""
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0
    rho: float = 0.0


@dataclass
class SpreadLeg:
    """A single leg in a multi-leg derivative spread.

    ``ratio`` is signed: positive = long, negative = short.
    Example: a long straddle has two legs with ratio +1 each (one call, one put).
    """
    instrument: DerivativeMetadata
    action: SignalType  # BUY_OPTION, SELL_OPTION, BUY_FUTURE, SELL_FUTURE
    ratio: int = 1
    limit_price: Optional[float] = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "instrument": self.instrument.to_dict(),
            "action": self.action.value if isinstance(self.action, SignalType) else self.action,
            "ratio": self.ratio,
        }
        if self.limit_price is not None:
            d["limit_price"] = self.limit_price
        return d


@dataclass(frozen=True)
class IvPoint:
    """A single point on the implied volatility surface."""
    strike: float
    expiry: str  # ISO-8601
    iv: float
    bid_iv: float = 0.0
    ask_iv: float = 0.0


@dataclass
class IvSurface:
    """Snapshot of the implied volatility surface for one underlying."""
    underlying: str
    timestamp: str  # ISO-8601
    points: list[IvPoint] = field(default_factory=list)

    def nearest_iv(self, strike: float, expiry: str) -> Optional[IvPoint]:
        """Find the closest IV point for a given strike and expiry."""
        if not self.points:
            return None
        from datetime import datetime as _dt
        try:
            target_exp = _dt.fromisoformat(expiry.replace("Z", "+00:00"))
        except Exception:
            return self.points[0] if self.points else None
        def _dist(p: IvPoint) -> float:
            try:
                p_exp = _dt.fromisoformat(p.expiry.replace("Z", "+00:00"))
                days_diff = abs((p_exp - target_exp).total_seconds()) / 86400.0
            except Exception:
                days_diff = 1e6
            return abs(p.strike - strike) + days_diff
        return min(self.points, key=_dist)


@dataclass(frozen=True)
class FuturesCurvePoint:
    """A single row in the futures term structure."""
    expiry: str  # ISO-8601
    mark_price: float
    basis_pct: float = 0.0
    open_interest: float = 0.0


@dataclass
class FuturesCurve:
    """Term structure of futures prices for one underlying."""
    underlying: str
    spot_price: float
    timestamp: str  # ISO-8601
    points: list[FuturesCurvePoint] = field(default_factory=list)

    def front_month(self) -> Optional[FuturesCurvePoint]:
        """Returns the nearest-expiry futures contract."""
        if not self.points:
            return None
        return min(self.points, key=lambda p: p.expiry)


@dataclass(frozen=True)
class FundingRate:
    """Funding rate for a perpetual swap instrument."""
    symbol: str
    rate: float
    next_settlement: str  # ISO-8601
    predicted_rate: Optional[float] = None
    annualised_rate: Optional[float] = None


@dataclass
class Signal:
    """
    A trading signal generated by a strategy.

    This is the primary output of `compute_signals()`. The backtesting engine
    converts these to orders and processes them through the simulation.

    Examples:
        # Simple buy signal
        Signal.buy(price=50000.0, strength="strong", reason="Price above SMA")

        # Sell with custom metadata
        Signal(
            signal_type=SignalType.SELL,
            strength=SignalStrength.MEDIUM,
            price=49500.0,
            quantity=0.1,
            reason=SignalReason.TECHNICAL_ANALYSIS,
            reason_detail="RSI overbought at 75",
            metadata={"rsi": 75.0, "sma_20": 49800.0},
        )
    """

    signal_type: SignalType | str
    strength: SignalStrength | float
    price: Optional[float] = None
    quantity: Optional[float] = None
    reason: SignalReason = SignalReason.CUSTOM
    reason_detail: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: Optional[datetime] = None

    @classmethod
    def buy(
        cls,
        price: Optional[float] = None,
        quantity: Optional[float] = None,
        strength: SignalStrength | str | float = SignalStrength.MEDIUM,
        reason: str = "",
        **metadata: Any,
    ) -> Signal:
        """Create a buy signal with convenience defaults."""
        if isinstance(strength, str):
            strength = SignalStrength(strength.capitalize()) if strength.capitalize() in [e.value for e in SignalStrength] else SignalStrength.MEDIUM
        return cls(
            signal_type=SignalType.BUY,
            strength=strength,
            price=price,
            quantity=quantity,
            reason=SignalReason.CUSTOM,
            reason_detail=reason,
            metadata=metadata,
        )

    @classmethod
    def sell(
        cls,
        price: Optional[float] = None,
        quantity: Optional[float] = None,
        strength: SignalStrength | str | float = SignalStrength.MEDIUM,
        reason: str = "",
        **metadata: Any,
    ) -> Signal:
        """Create a sell signal with convenience defaults."""
        if isinstance(strength, str):
            strength = SignalStrength(strength.capitalize()) if strength.capitalize() in [e.value for e in SignalStrength] else SignalStrength.MEDIUM
        return cls(
            signal_type=SignalType.SELL,
            strength=strength,
            price=price,
            quantity=quantity,
            reason=SignalReason.CUSTOM,
            reason_detail=reason,
            metadata=metadata,
        )

    @classmethod
    def hold(cls, reason: str = "") -> Signal:
        """Create a hold/no-action signal."""
        return cls(
            signal_type=SignalType.HOLD,
            strength=SignalStrength.WEAK,
            reason=SignalReason.CUSTOM,
            reason_detail=reason,
        )

    @classmethod
    def close(cls, reason: str = "", **metadata: Any) -> Signal:
        """Create a close-position signal."""
        return cls(
            signal_type=SignalType.CLOSE,
            strength=SignalStrength.MEDIUM,
            reason=SignalReason.CUSTOM,
            reason_detail=reason,
            metadata=metadata,
        )

    @classmethod
    def quote(
        cls,
        bid_price: float,
        ask_price: float,
        bid_size: float,
        ask_size: float,
        reason: str = "",
        **metadata: Any,
    ) -> Signal:
        """Create a two-sided market-making quote signal."""
        payload = {
            "bid_price": bid_price,
            "ask_price": ask_price,
            "bid_size": bid_size,
            "ask_size": ask_size,
            **metadata,
        }
        return cls(
            signal_type=SignalType.TWO_SIDED_QUOTE,
            strength=SignalStrength.MEDIUM,
            reason=SignalReason.CUSTOM,
            reason_detail=reason,
            metadata=payload,
        )

    @classmethod
    def cancel_quotes(cls, reason: str = "", **metadata: Any) -> Signal:
        """Cancel all outstanding market-making quotes."""
        return cls(
            signal_type=SignalType.CANCEL_QUOTES,
            strength=SignalStrength.MEDIUM,
            reason=SignalReason.CUSTOM,
            reason_detail=reason,
            metadata=metadata,
        )

    @classmethod
    def multi(
        cls,
        target_symbol: str,
        action: str,
        target_exchange: str = "",
        reason: str = "",
        **metadata: Any,
    ) -> Signal:
        """Create a cross-asset signal routed to a target symbol/exchange."""
        payload = {
            "target_symbol": target_symbol,
            "target_exchange": target_exchange,
            "action": action,
            **metadata,
        }
        return cls(
            signal_type=SignalType.CROSS_ASSET,
            strength=SignalStrength.MEDIUM,
            reason=SignalReason.CUSTOM,
            reason_detail=reason,
            metadata=payload,
        )

    # ── Derivatives signal factories ──────────────────────────────────

    @classmethod
    def buy_option(
        cls,
        instrument: DerivativeMetadata,
        premium: Optional[float] = None,
        quantity: Optional[float] = None,
        strength: SignalStrength | str | float = SignalStrength.MEDIUM,
        reason: str = "",
    ) -> Signal:
        """Buy (go long) an option contract.

        Args:
            instrument: Option contract metadata (symbol, underlying, strike, expiry).
            premium: Maximum premium to pay per contract. None = market order.
            quantity: Number of contracts.
            strength: Signal confidence.
            reason: Human-readable reason.
        """
        if isinstance(strength, str):
            strength = SignalStrength(strength.capitalize()) if strength.capitalize() in [e.value for e in SignalStrength] else SignalStrength.MEDIUM
        return cls(
            signal_type=SignalType.BUY_OPTION,
            strength=strength,
            price=premium,
            quantity=quantity,
            reason=SignalReason.CUSTOM,
            reason_detail=reason,
            metadata={"instrument": instrument.to_dict(), "premium": premium},
        )

    @classmethod
    def sell_option(
        cls,
        instrument: DerivativeMetadata,
        premium: Optional[float] = None,
        quantity: Optional[float] = None,
        strength: SignalStrength | str | float = SignalStrength.MEDIUM,
        reason: str = "",
    ) -> Signal:
        """Sell (write) an option contract.

        Args:
            instrument: Option contract metadata.
            premium: Minimum acceptable premium. None = market order.
            quantity: Number of contracts.
            strength: Signal confidence.
            reason: Human-readable reason.
        """
        if isinstance(strength, str):
            strength = SignalStrength(strength.capitalize()) if strength.capitalize() in [e.value for e in SignalStrength] else SignalStrength.MEDIUM
        return cls(
            signal_type=SignalType.SELL_OPTION,
            strength=strength,
            price=premium,
            quantity=quantity,
            reason=SignalReason.CUSTOM,
            reason_detail=reason,
            metadata={"instrument": instrument.to_dict(), "premium": premium},
        )

    @classmethod
    def buy_future(
        cls,
        instrument: DerivativeMetadata,
        price: Optional[float] = None,
        quantity: Optional[float] = None,
        strength: SignalStrength | str | float = SignalStrength.MEDIUM,
        reason: str = "",
    ) -> Signal:
        """Buy (go long) a futures or perpetual contract."""
        if isinstance(strength, str):
            strength = SignalStrength(strength.capitalize()) if strength.capitalize() in [e.value for e in SignalStrength] else SignalStrength.MEDIUM
        return cls(
            signal_type=SignalType.BUY_FUTURE,
            strength=strength,
            price=price,
            quantity=quantity,
            reason=SignalReason.CUSTOM,
            reason_detail=reason,
            metadata={"instrument": instrument.to_dict()},
        )

    @classmethod
    def sell_future(
        cls,
        instrument: DerivativeMetadata,
        price: Optional[float] = None,
        quantity: Optional[float] = None,
        strength: SignalStrength | str | float = SignalStrength.MEDIUM,
        reason: str = "",
    ) -> Signal:
        """Sell (go short) a futures or perpetual contract."""
        if isinstance(strength, str):
            strength = SignalStrength(strength.capitalize()) if strength.capitalize() in [e.value for e in SignalStrength] else SignalStrength.MEDIUM
        return cls(
            signal_type=SignalType.SELL_FUTURE,
            strength=strength,
            price=price,
            quantity=quantity,
            reason=SignalReason.CUSTOM,
            reason_detail=reason,
            metadata={"instrument": instrument.to_dict()},
        )

    @classmethod
    def option_spread(
        cls,
        legs: list[SpreadLeg],
        strength: SignalStrength | str | float = SignalStrength.MEDIUM,
        reason: str = "",
    ) -> Signal:
        """Multi-leg spread order (straddle, condor, butterfly, etc.).

        All legs are executed atomically: all-or-nothing.

        Args:
            legs: List of SpreadLeg objects defining each leg.
            strength: Signal confidence.
            reason: Human-readable reason.
        """
        if isinstance(strength, str):
            strength = SignalStrength(strength.capitalize()) if strength.capitalize() in [e.value for e in SignalStrength] else SignalStrength.MEDIUM
        return cls(
            signal_type=SignalType.OPTION_SPREAD,
            strength=strength,
            reason=SignalReason.CUSTOM,
            reason_detail=reason,
            metadata={"legs": [leg.to_dict() for leg in legs]},
        )

    @classmethod
    def roll_contract(
        cls,
        from_instrument: DerivativeMetadata,
        to_instrument: DerivativeMetadata,
        strength: SignalStrength | str | float = SignalStrength.MEDIUM,
        reason: str = "",
    ) -> Signal:
        """Roll an expiring position to the next contract."""
        if isinstance(strength, str):
            strength = SignalStrength(strength.capitalize()) if strength.capitalize() in [e.value for e in SignalStrength] else SignalStrength.MEDIUM
        return cls(
            signal_type=SignalType.ROLL_CONTRACT,
            strength=strength,
            reason=SignalReason.CUSTOM,
            reason_detail=reason,
            metadata={
                "from_instrument": from_instrument.to_dict(),
                "to_instrument": to_instrument.to_dict(),
                "from_symbol": from_instrument.symbol,
                "to_symbol": to_instrument.symbol,
            },
        )

    @classmethod
    def exercise_option(
        cls,
        instrument: DerivativeMetadata,
        reason: str = "",
    ) -> Signal:
        """Exercise an in-the-money option."""
        return cls(
            signal_type=SignalType.EXERCISE_OPTION,
            strength=SignalStrength.STRONG,
            reason=SignalReason.CUSTOM,
            reason_detail=reason,
            metadata={"instrument": instrument.to_dict()},
        )

    @classmethod
    def hedge_delta(
        cls,
        target_delta: float,
        current_delta: float,
        hedge_instrument: str = "",
        strength: SignalStrength | str | float = SignalStrength.MEDIUM,
        reason: str = "",
    ) -> Signal:
        """Adjust the hedge position to reach a target portfolio delta.

        Args:
            target_delta: Desired net portfolio delta after hedging.
            current_delta: Current portfolio delta before hedging.
            hedge_instrument: Symbol used to hedge (e.g. "BTC-PERPETUAL").
            strength: Signal confidence.
            reason: Human-readable reason.
        """
        if isinstance(strength, str):
            strength = SignalStrength(strength.capitalize()) if strength.capitalize() in [e.value for e in SignalStrength] else SignalStrength.MEDIUM
        return cls(
            signal_type=SignalType.HEDGE_DELTA,
            strength=strength,
            reason=SignalReason.CUSTOM,
            reason_detail=reason,
            metadata={
                "target_delta": target_delta,
                "current_delta": current_delta,
                "hedge_instrument": hedge_instrument,
            },
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for Rust interop."""
        d: dict[str, Any] = {
            "id": self.id,
            "signal_type": self.signal_type.value if isinstance(self.signal_type, SignalType) else self.signal_type,
            "strength": self.strength.value if isinstance(self.strength, SignalStrength) else self.strength,
            "reason": self.reason.value if isinstance(self.reason, SignalReason) else str(self.reason),
            "reason_detail": self.reason_detail,
            "metadata": self.metadata,
        }
        if self.price is not None:
            d["price"] = self.price
        if self.quantity is not None:
            d["quantity"] = self.quantity
        if self.timestamp is not None:
            d["timestamp"] = self.timestamp.isoformat()

        # Bridge compatibility: quote/cross-asset fields are expected at the top level
        # by the Rust parser, not only inside metadata.
        signal_type = d["signal_type"]
        if signal_type in ("TwoSidedQuote", "Quote"):
            for key in ("bid_price", "ask_price", "bid_size", "ask_size"):
                if key in self.metadata:
                    d[key] = self.metadata[key]
        elif signal_type in ("CrossAsset", "Multi"):
            for key in ("target_symbol", "target_exchange", "action"):
                if key in self.metadata:
                    d[key] = self.metadata[key]
        return d


# ── Market Data Types ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class TradeData:
    """A single trade tick from the market."""

    timestamp: datetime
    symbol: str
    exchange: str
    price: float
    quantity: float
    side: str  # "buy" or "sell"
    trade_id: str = ""
    value: float = 0.0  # price * quantity, pre-computed

    def __post_init__(self):
        if self.value == 0.0:
            object.__setattr__(self, "value", self.price * self.quantity)


@dataclass(frozen=True)
class CandleData:
    """OHLCV candle data."""

    timestamp: datetime
    symbol: str
    exchange: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int = 0


@dataclass
class MarketData:
    """
    Market data snapshot provided to strategies at each simulation step.

    In trade-level simulation, `trade` is populated. In candle-based simulation,
    `candle` is populated. At least one will always be present.

    Attributes:
        trade: Trade tick data (if trade-level simulation)
        candle: OHLCV candle data (if candle-based simulation)
        price: Convenience accessor for current price (trade price or candle close)
        volume: Current volume (trade quantity or candle volume)
        timestamp: Current timestamp
        symbol: Trading pair symbol
        exchange: Exchange name
    """

    trade: Optional[TradeData] = None
    candle: Optional[CandleData] = None

    @property
    def price(self) -> float:
        """Current price (trade price or candle close)."""
        if self.trade is not None:
            return self.trade.price
        if self.candle is not None:
            return self.candle.close
        return 0.0

    @property
    def volume(self) -> float:
        """Current volume."""
        if self.trade is not None:
            return self.trade.quantity
        if self.candle is not None:
            return self.candle.volume
        return 0.0

    @property
    def timestamp(self) -> Optional[datetime]:
        """Current timestamp."""
        if self.trade is not None:
            return self.trade.timestamp
        if self.candle is not None:
            return self.candle.timestamp
        return None

    @property
    def symbol(self) -> str:
        """Trading pair symbol."""
        if self.trade is not None:
            return self.trade.symbol
        if self.candle is not None:
            return self.candle.symbol
        return ""

    @property
    def exchange(self) -> str:
        """Exchange name."""
        if self.trade is not None:
            return self.trade.exchange
        if self.candle is not None:
            return self.candle.exchange
        return ""

    @property
    def is_trade(self) -> bool:
        """Whether this is trade-level data."""
        return self.trade is not None

    @property
    def is_candle(self) -> bool:
        """Whether this is candle data."""
        return self.candle is not None

    # ── Dict-compat accessors (so both data.price and data["price"] work) ──

    def __getitem__(self, key: str) -> Any:
        """Support dict-style access for backward compatibility with raw-dict strategies."""
        return getattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        """Dict-compatible .get() for backward compatibility."""
        return getattr(self, key, default)

    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)

    @classmethod
    def _from_dict(cls, d: dict) -> "MarketData":
        """Construct from the raw dict passed by the Rust bridge."""
        def _parse_ts(raw: Any) -> datetime:
            if isinstance(raw, datetime):
                return raw
            if isinstance(raw, (int, float)):
                return datetime.fromtimestamp(raw / 1000, tz=timezone.utc)
            if isinstance(raw, str):
                try:
                    return datetime.fromisoformat(raw.replace("Z", "+00:00"))
                except Exception:
                    return datetime.now(tz=timezone.utc)
            return datetime.now(tz=timezone.utc)

        # Prefer timestamp_ms (int) — avoids string parsing overhead
        ts_raw = d.get("timestamp_ms") or d.get("timestamp", 0)
        ts = _parse_ts(ts_raw)
        dtype = d.get("type", "trade")

        trade = None
        candle = None

        if dtype == "candle":
            candle = CandleData(
                timestamp=ts,
                symbol=d.get("symbol", ""),
                exchange=d.get("exchange", ""),
                open=d.get("open", 0.0),
                high=d.get("high", 0.0),
                low=d.get("low", 0.0),
                close=d.get("close", 0.0),
                volume=d.get("volume", 0.0),
                trade_count=d.get("trade_count", 0),
            )
        else:
            trade = TradeData(
                timestamp=ts,
                symbol=d.get("symbol", ""),
                exchange=d.get("exchange", ""),
                price=d.get("price", 0.0),
                quantity=d.get("quantity", d.get("volume", 0.0)),
                side=d.get("side", "buy"),
                trade_id=d.get("trade_id", ""),
            )

        return cls(trade=trade, candle=candle)


# ── Strategy Context ──────────────────────────────────────────────────────────


@dataclass
class PortfolioState:
    """Current portfolio state provided to the strategy."""

    cash: float = 0.0
    equity: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    position_size: float = 0.0
    position_value: float = 0.0
    avg_entry_price: float = 0.0
    num_open_positions: int = 0
    total_commission_paid: float = 0.0

    # Aliases expected by dict-style code from the Rust bridge
    @property
    def balance(self) -> float:
        return self.cash

    @property
    def net_position(self) -> float:
        return self.position_size

    @property
    def fees_paid(self) -> float:
        return self.total_commission_paid

    def __getitem__(self, key: str) -> Any:
        """Support dict-style access: portfolio[\"cash\"], portfolio[\"equity\"], etc."""
        return getattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)


@dataclass(frozen=True)
class PricePoint:
    """Historical price point."""

    timestamp: datetime
    price: float
    source: str = "Trade"  # Trade, BestBid, BestAsk, MidPrice, VWAP


@dataclass(frozen=True)
class VolumePoint:
    """Historical volume point."""

    timestamp: datetime
    volume: float
    side: Optional[str] = None  # "buy", "sell", or None


@dataclass
class StrategyContext:
    """
    Execution context provided to the strategy at each step.

    Contains portfolio state, price/volume history, and market session info.
    Also provides convenience methods for common technical analysis calculations.

    Attributes:
        timestamp: Current simulation timestamp
        portfolio: Current portfolio state (cash, equity, positions)
        price_history: Recent price history (list of PricePoint)
        volume_history: Recent volume history (list of VolumePoint)
        spread: Current bid-ask spread (if available)
        market_session_open: Whether the market session is currently open
    """

    timestamp: datetime = field(default_factory=datetime.utcnow)
    portfolio: PortfolioState = field(default_factory=PortfolioState)
    price_history: list[PricePoint] = field(default_factory=list)
    volume_history: list[VolumePoint] = field(default_factory=list)
    spread: Optional[float] = None
    market_session_open: bool = True

    @property
    def prices(self) -> list[float]:
        """Extract price values from history as a flat list."""
        cache = getattr(self, '_prices_cache', None)
        if cache is not None:
            return cache
        raw = getattr(self, '_prices_raw', None)
        if raw is not None:
            result = raw.tolist() if hasattr(raw, 'tolist') else raw
            object.__setattr__(self, '_prices_cache', result)
            return result
        return [p.price for p in self.price_history]

    @property
    def volumes(self) -> list[float]:
        """Extract volume values from history as a flat list."""
        cache = getattr(self, '_volumes_cache', None)
        if cache is not None:
            return cache
        raw = getattr(self, '_volumes_raw', None)
        if raw is not None:
            result = raw.tolist() if hasattr(raw, 'tolist') else raw
            object.__setattr__(self, '_volumes_cache', result)
            return result
        return [v.volume for v in self.volume_history]

    def sma(self, period: int) -> Optional[float]:
        """Simple Moving Average of recent prices."""
        prices = self.prices
        if len(prices) < period:
            return None
        window = prices[-period:]
        return sum(window) / period

    def ema(self, period: int) -> Optional[float]:
        """Exponential Moving Average of recent prices."""
        prices = self.prices
        if len(prices) < period:
            return None
        multiplier = 2.0 / (period + 1)
        ema_val = prices[-period]
        for price in prices[-period + 1:]:
            ema_val = (price - ema_val) * multiplier + ema_val
        return ema_val

    def rsi(self, period: int = 14) -> Optional[float]:
        """Relative Strength Index."""
        prices = self.prices
        if len(prices) < period + 1:
            return None
        changes = [prices[i] - prices[i - 1] for i in range(-period, 0)]
        gains = [c for c in changes if c > 0]
        losses = [-c for c in changes if c < 0]
        avg_gain = sum(gains) / period if gains else 0.0
        avg_loss = sum(losses) / period if losses else 0.0
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def bollinger_bands(
        self, period: int = 20, std_dev: float = 2.0
    ) -> Optional[tuple[float, float, float]]:
        """Bollinger Bands (upper, middle, lower)."""
        prices = self.prices
        if len(prices) < period:
            return None
        window = prices[-period:]
        middle = sum(window) / period
        variance = sum((p - middle) ** 2 for p in window) / period
        std = variance**0.5
        return (middle + std_dev * std, middle, middle - std_dev * std)

    def vwap(self, lookback: Optional[int] = None) -> Optional[float]:
        """Volume-Weighted Average Price."""
        prices = self.prices
        volumes = self.volumes
        n = min(len(prices), len(volumes))
        if n == 0:
            return None
        if lookback is not None:
            n = min(n, lookback)
        p = prices[-n:]
        v = volumes[-n:]
        total_vol = sum(v)
        if total_vol == 0:
            return None
        return sum(p[i] * v[i] for i in range(n)) / total_vol

    def atr(self, period: int = 14) -> Optional[float]:
        """Average True Range (approximation using price history only)."""
        prices = self.prices
        if len(prices) < period + 1:
            return None
        true_ranges = []
        for i in range(-period, 0):
            high_low = abs(prices[i] - prices[i - 1])  # simplified
            true_ranges.append(high_low)
        return sum(true_ranges) / period

    # ── Dict-compat accessors ─────────────────────────────────────────────

    def __getitem__(self, key: str) -> Any:
        """Support dict-style access: context["prices"], context["portfolio"], etc."""
        # Map legacy dict keys to dataclass attributes/properties
        _ALIASES: dict[str, str] = {
            "tick_number": "_tick_number",
            "elapsed_seconds": "_elapsed_seconds",
            "market_session_open": "_market_session_open",
        }
        real_key = _ALIASES.get(key, key)
        try:
            return getattr(self, real_key)
        except AttributeError:
            raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        """Dict-compatible .get() for backward compatibility."""
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)

    @classmethod
    def _from_dict(cls, d: dict) -> "StrategyContext":
        """Construct from the raw dict passed by the Rust bridge.

        Performance-critical: called once per tick (~691k ticks per backtest).
        Avoids constructing PricePoint/VolumePoint objects; raw arrays are
        cached and exposed via the ``prices`` / ``volumes`` properties.
        """
        # Portfolio state (cheap — single dict)
        port = d.get("portfolio", {})
        portfolio = PortfolioState(
            cash=port.get("cash", 0.0),
            equity=port.get("total_value", port.get("equity", port.get("cash", 0.0))),
            unrealized_pnl=port.get("unrealized_pnl", 0.0),
            realized_pnl=port.get("realized_pnl", 0.0),
            total_commission_paid=port.get("fees_paid", 0.0),
            position_size=port.get("net_position", 0.0),
        )

        # Timestamp — prefer epoch_ms (int) to avoid string parsing
        ts_raw = d.get("timestamp", 0)
        if isinstance(ts_raw, (int, float)) and ts_raw > 0:
            timestamp = datetime.fromtimestamp(ts_raw / 1000, tz=timezone.utc)
        elif isinstance(ts_raw, str) and ts_raw:
            try:
                timestamp = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            except Exception:
                timestamp = datetime.now(tz=timezone.utc)
        else:
            timestamp = datetime.now(tz=timezone.utc)

        # Use __new__ to skip dataclass __init__ default_factory overhead
        ctx = cls.__new__(cls)
        ctx.timestamp = timestamp
        ctx.portfolio = portfolio
        ctx.price_history = []   # lazy — use ctx.prices instead
        ctx.volume_history = []  # lazy — use ctx.volumes instead
        ctx.spread = d.get("spread")
        ctx.market_session_open = d.get("market_session_open", True)

        # Store raw arrays for fast property access (no PricePoint construction)
        ctx._prices_raw = d.get("prices", [])
        ctx._volumes_raw = d.get("volumes", [])
        ctx._timestamps_raw = d.get("timestamps", [])
        ctx._prices_cache = None
        ctx._volumes_cache = None

        # Stash extra fields that dict-style code expects
        ctx._tick_number = d.get("tick_number", 0)  # type: ignore[attr-defined]
        ctx._elapsed_seconds = d.get("elapsed_seconds", 0.0)  # type: ignore[attr-defined]
        ctx._market_session_open = d.get("market_session_open", True)  # type: ignore[attr-defined]
        # Wrap orderbook dict in SimpleNamespace so both attribute and dict access work:
        #   context.orderbook.spread  AND  context.orderbook["spread"]
        ob_raw = d.get("orderbook")
        if ob_raw and isinstance(ob_raw, dict):
            ob_ns = SimpleNamespace(**ob_raw)
            ob_ns.get = lambda key, default=None: getattr(ob_ns, key, default)  # type: ignore[attr-defined]
            ctx._orderbook = ob_ns  # type: ignore[attr-defined]
        else:
            ctx._orderbook = ob_raw  # type: ignore[attr-defined]
        ctx._custom_data = d.get("custom_data", {})  # type: ignore[attr-defined]
        # Extract Rust-precomputed indicators from custom_data (keys prefixed with "ind.")
        _indicators = {}  # type: ignore[var-annotated]
        for k, v in ctx._custom_data.items():
            if k.startswith("ind."):
                _indicators[k[4:]] = v  # strip "ind." prefix
        ctx._indicators = _indicators  # type: ignore[attr-defined]
        ctx._assets = d.get("assets", {})  # type: ignore[attr-defined]

        # ── Derivatives context fields ────────────────────────────────────
        ctx._iv_surface = _parse_iv_surface(d.get("iv_surface"))  # type: ignore[attr-defined]
        ctx._futures_curve = _parse_futures_curve(d.get("futures_curve"))  # type: ignore[attr-defined]
        ctx._funding_rates = _parse_funding_rates(d.get("funding_rates"))  # type: ignore[attr-defined]
        ctx._portfolio_greeks = _parse_greeks(d.get("portfolio_greeks"))  # type: ignore[attr-defined]

        ctx._raw = d  # keep raw dict for any keys we missed  # type: ignore[attr-defined]
        return ctx

    @property
    def tick_number(self) -> int:
        return getattr(self, "_tick_number", 0)

    @property
    def elapsed_seconds(self) -> float:
        return getattr(self, "_elapsed_seconds", 0.0)

    @property
    def orderbook(self) -> Any:
        return getattr(self, "_orderbook", None)

    @property
    def custom_data(self) -> dict:
        return getattr(self, "_custom_data", {})

    @property
    def indicators(self) -> dict:
        """Pre-computed Rust indicator values (from indicators() declaration).

        Returns a dict of {name: value} for single-value indicators,
        and {name.field: value} for multi-value indicators (MACD, Bollinger).
        Empty dict if no indicators were declared or pre-computed.
        """
        return getattr(self, "_indicators", {})

    @property
    def iv_surface(self) -> Optional[IvSurface]:
        """Implied volatility surface (populated when ``DataRequirements.needs_iv_surface`` is True)."""
        return getattr(self, "_iv_surface", None)

    @property
    def futures_curve(self) -> Optional[FuturesCurve]:
        """Futures term structure (populated when ``DataRequirements.needs_futures_curve`` is True)."""
        return getattr(self, "_futures_curve", None)

    @property
    def funding_rates(self) -> Optional[dict[str, FundingRate]]:
        """Funding rates keyed by symbol (populated when ``DataRequirements.needs_funding_rates`` is True)."""
        return getattr(self, "_funding_rates", None)

    @property
    def portfolio_greeks(self) -> Optional[Greeks]:
        """Aggregate portfolio-level Greeks (delta, gamma, theta, vega, rho)."""
        return getattr(self, "_portfolio_greeks", None)


# ── Strategy Configuration Types ──────────────────────────────────────────────


class StrategyCategory(str, Enum):
    """Category of strategy, affects data loading behavior."""
    MARKET_MAKING = "MarketMaking"
    MOMENTUM = "Momentum"
    ARBITRAGE = "Arbitrage"
    STATISTICAL_ARBITRAGE = "StatisticalArbitrage"
    OPTIONS = "Options"
    FUTURES = "Futures"
    VOLATILITY = "Volatility"
    SPREAD = "Spread"
    CUSTOM = "Custom"


@dataclass
class DataRequirements:
    """
    Declares what data the strategy needs.

    The backtesting engine uses this to fetch the appropriate data types
    from Tardis. Market-making strategies get order flow + trades;
    momentum strategies get candles.
    """

    category: StrategyCategory = StrategyCategory.CUSTOM
    needs_order_flow: bool = False
    needs_trades: bool = True
    needs_orderbook_snapshots: bool = False
    needs_candles: bool = False
    lookback_window: int = 100
    needs_depth: bool = False
    max_data_age_secs: Optional[int] = None
    needs_iv_surface: bool = False
    needs_futures_curve: bool = False
    needs_funding_rates: bool = False

    @classmethod
    def market_making(cls) -> DataRequirements:
        """Data requirements for market-making strategies."""
        return cls(
            category=StrategyCategory.MARKET_MAKING,
            needs_order_flow=True,
            needs_trades=True,
            needs_orderbook_snapshots=True,
            lookback_window=1000,
            needs_depth=True,
            max_data_age_secs=60,
        )

    @classmethod
    def momentum(cls) -> DataRequirements:
        """Data requirements for momentum/trend strategies."""
        return cls(
            category=StrategyCategory.MOMENTUM,
            needs_candles=True,
            lookback_window=200,
        )

    @classmethod
    def arbitrage(cls) -> DataRequirements:
        """Data requirements for arbitrage strategies."""
        return cls(
            category=StrategyCategory.ARBITRAGE,
            needs_order_flow=True,
            needs_trades=True,
            needs_orderbook_snapshots=True,
            lookback_window=100,
            needs_depth=True,
            max_data_age_secs=1,
        )

    @classmethod
    def options(cls) -> DataRequirements:
        """Data requirements for options strategies (IV surface, orderbook)."""
        return cls(
            category=StrategyCategory.OPTIONS,
            needs_trades=True,
            needs_orderbook_snapshots=True,
            lookback_window=200,
            needs_iv_surface=True,
        )

    @classmethod
    def futures(cls) -> DataRequirements:
        """Data requirements for dated futures strategies (term structure)."""
        return cls(
            category=StrategyCategory.FUTURES,
            needs_trades=True,
            lookback_window=200,
            needs_futures_curve=True,
        )

    @classmethod
    def perpetual(cls) -> DataRequirements:
        """Data requirements for perpetual swap strategies (funding rates)."""
        return cls(
            category=StrategyCategory.FUTURES,
            needs_trades=True,
            lookback_window=100,
            needs_funding_rates=True,
        )

    @classmethod
    def volatility(cls) -> DataRequirements:
        """Data requirements for volatility strategies (IV surface, deep history)."""
        return cls(
            category=StrategyCategory.VOLATILITY,
            needs_trades=True,
            needs_orderbook_snapshots=True,
            lookback_window=500,
            needs_iv_surface=True,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for Rust interop."""
        return {
            "category": self.category.value,
            "needs_order_flow": self.needs_order_flow,
            "needs_trades": self.needs_trades,
            "needs_orderbook_snapshots": self.needs_orderbook_snapshots,
            "needs_candles": self.needs_candles,
            "lookback_window": self.lookback_window,
            "needs_depth": self.needs_depth,
            "max_data_age_secs": self.max_data_age_secs,
            "needs_iv_surface": self.needs_iv_surface,
            "needs_futures_curve": self.needs_futures_curve,
            "needs_funding_rates": self.needs_funding_rates,
        }


@dataclass
class RiskLimits:
    """Risk limits for the strategy."""

    max_position_size: float = 1.0
    max_order_size: float = 0.1
    max_daily_loss: float = 1000.0
    max_drawdown_pct: float = 0.10
    max_open_orders: int = 10
    max_leverage: float = 1.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for Rust interop."""
        return {
            "max_position_size": self.max_position_size,
            "max_order_size": self.max_order_size,
            "max_daily_loss": self.max_daily_loss,
            "max_drawdown_pct": self.max_drawdown_pct,
            "max_open_orders": self.max_open_orders,
            "max_leverage": self.max_leverage,
        }


# Type alias for strategy parameters
ParameterValue = int | float | str | bool


# ── Derivatives dict→dataclass helpers (used by StrategyContext._from_dict) ──


def _parse_greeks(raw: Any) -> Optional[Greeks]:
    if not raw or not isinstance(raw, dict):
        return None
    return Greeks(
        delta=raw.get("delta", 0.0),
        gamma=raw.get("gamma", 0.0),
        theta=raw.get("theta", 0.0),
        vega=raw.get("vega", 0.0),
        rho=raw.get("rho", 0.0),
    )


def _parse_iv_surface(raw: Any) -> Optional[IvSurface]:
    if not raw or not isinstance(raw, dict):
        return None
    pts = []
    for p in raw.get("points", []):
        pts.append(IvPoint(
            strike=p.get("strike", 0.0),
            expiry=p.get("expiry", ""),
            iv=p.get("iv", 0.0),
            bid_iv=p.get("bid_iv", 0.0),
            ask_iv=p.get("ask_iv", 0.0),
        ))
    return IvSurface(
        underlying=raw.get("underlying", ""),
        timestamp=raw.get("timestamp", ""),
        points=pts,
    )


def _parse_futures_curve(raw: Any) -> Optional[FuturesCurve]:
    if not raw or not isinstance(raw, dict):
        return None
    pts = []
    for p in raw.get("points", []):
        pts.append(FuturesCurvePoint(
            expiry=p.get("expiry", ""),
            mark_price=p.get("mark_price", 0.0),
            basis_pct=p.get("basis_pct", 0.0),
            open_interest=p.get("open_interest", 0.0),
        ))
    return FuturesCurve(
        underlying=raw.get("underlying", ""),
        spot_price=raw.get("spot_price", 0.0),
        timestamp=raw.get("timestamp", ""),
        points=pts,
    )


def _parse_funding_rates(raw: Any) -> Optional[dict[str, FundingRate]]:
    if not raw or not isinstance(raw, dict):
        return None
    result = {}
    for sym, fr in raw.items():
        if isinstance(fr, dict):
            result[sym] = FundingRate(
                symbol=fr.get("symbol", sym),
                rate=fr.get("rate", 0.0),
                next_settlement=fr.get("next_settlement", ""),
                predicted_rate=fr.get("predicted_rate"),
                annualised_rate=fr.get("annualised_rate"),
            )
    return result if result else None
