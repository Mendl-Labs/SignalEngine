"""
Base strategy class that all user strategies must inherit from.

All user strategies must implement:
    - compute_signals(prices, volumes, timestamps) -> np.ndarray

compute_signals() is called ONCE per evaluation with the full tick array.
Use numpy/pandas for vectorized indicator computation — this is the fast path.

Return an int8 array with one entry per tick:
    1  = BUY   — open or increase long position
   -1  = SELL  — open or increase short position
    0  = HOLD  — no change (default)
    2  = CLOSE — close any open position

Optional overrides:
    - name() -> str
    - initialize(params) -> None
    - update_parameters(params) -> None
    - get_parameters() -> dict
    - parameter_space() -> dict
    - state_schema() -> dict
    - indicators() -> dict
    - model() -> dict
    - features() -> list
    - compute_features(prices, volumes, timestamps) -> dict[str, np.ndarray]
    - reset() -> None
    - validate_config(params) -> None
    - data_requirements() -> DataRequirements
    - get_risk_limits() -> RiskLimits
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional

import numpy as np

from .types import (
    DataRequirements,
    DerivativeMetadata,
    RiskLimits,
    ParameterValue,
)


class BaseStrategy(ABC):
    """
    Base class for all Python trading strategies.

    Subclass this and implement ``compute_signals()`` to create a custom strategy.
    The backtesting engine calls your strategy ONCE per evaluation with the complete
    tick array, so you can use fast vectorized numpy/pandas operations.

    Signal constants — use these in your compute_signals() return array:
        BaseStrategy.BUY   =  1  (go long)
        BaseStrategy.SELL  = -1  (go short)
        BaseStrategy.HOLD  =  0  (no change, default)
        BaseStrategy.CLOSE =  2  (close any open position)

    ``compute_signals`` receives the CLOSE series as ``prices``. To use full
    OHLC, add optional ``opens``/``highs``/``lows`` parameters to your signature
    and the engine passes them automatically (for tick data they equal price).

    Example (pure vectorized — fastest path):
        import numpy as np
        import pandas as pd

        class MomentumCrossover(BaseStrategy):
            def name(self) -> str:
                return "MomentumCrossover"

            def compute_signals(self, prices, volumes, timestamps):
                s = pd.Series(prices)
                fast = s.rolling(self.params.get("fast_period", 10)).mean()
                slow = s.rolling(self.params.get("slow_period", 50)).mean()
                sig = np.zeros(len(prices), dtype=np.int8)
                sig[fast > slow] = self.BUY
                sig[fast < slow] = self.SELL
                return sig

    Example (stateful loop — for sequential logic like trailing stops):
        class TrailingStop(BaseStrategy):
            def compute_signals(self, prices, volumes, timestamps):
                sig = np.zeros(len(prices), dtype=np.int8)
                in_pos = False
                entry = 0.0
                for i, price in enumerate(prices):
                    if not in_pos and prices[i] > 0:  # your entry condition
                        sig[i] = self.BUY
                        in_pos = True
                        entry = price
                    elif in_pos and price < entry * 0.97:
                        sig[i] = self.CLOSE
                        in_pos = False
                return sig
    """

    # Signal constants — assign these values in compute_signals() output arrays
    BUY: "np.int8" = np.int8(1)
    SELL: "np.int8" = np.int8(-1)
    HOLD: "np.int8" = np.int8(0)
    CLOSE: "np.int8" = np.int8(2)

    def name(self) -> str:
        """
        Return the strategy name.

        Defaults to the class name if not overridden. Override to set a
        human-readable display name used for logging and results identification.
        """
        return self.__class__.__name__

    def version(self) -> str:
        """Return the strategy version. Override for versioning."""
        return "1.0.0"

    def description(self) -> str:
        """Return a human-readable description of the strategy."""
        return "Custom Python strategy"

    def initialize(self, parameters: dict[str, ParameterValue]) -> None:
        """
        Initialize the strategy with parameters.

        Called once before the simulation starts. Use this to set up any
        internal state based on the provided parameters (which may come from
        genetic algorithm optimization).

        Args:
            parameters: Dict of parameter name -> value. Values can be
                        int, float, str, or bool.
        """
        if not hasattr(self, 'params'):
            self.params = {}
        # Seed declared parameter_space() defaults BEFORE applying the
        # engine-supplied override. The quick/Stage-0/Stage-1 backtest passes
        # an empty `parameters` dict (real values only arrive once GA runs),
        # so a strategy that declares a parameter here but reads it via
        # `self.params[key]` (rather than `.get(key, default)`) would
        # otherwise KeyError before GA ever gets a chance to inject it.
        if hasattr(self, 'parameter_space') and callable(self.parameter_space):
            try:
                space = self.parameter_space()
            except Exception:
                space = None
            if isinstance(space, dict):
                for key, spec in space.items():
                    if key not in self.params and isinstance(spec, dict) and 'default' in spec:
                        self.params[key] = spec['default']
        if parameters:
            self.params.update(parameters)

    @abstractmethod
    def compute_signals(
        self,
        prices: "np.ndarray",
        volumes: "np.ndarray",
        timestamps: "np.ndarray",
        opens: "np.ndarray | None" = None,
        highs: "np.ndarray | None" = None,
        lows: "np.ndarray | None" = None,
    ) -> "np.ndarray":
        """
        Generate trading signals for the entire tick array.

        Called ONCE per evaluation with the full market data. Use numpy/pandas
        for fast vectorized indicator computation. Return an int8 array with
        one entry per tick:
            1  = BUY   — open or increase long position (BaseStrategy.BUY)
           -1  = SELL  — open or increase short position (BaseStrategy.SELL)
            0  = HOLD  — no change, default (BaseStrategy.HOLD)
            2  = CLOSE — close any open position (BaseStrategy.CLOSE)

        Args:
            prices:     float64 ndarray, shape (N,) — CLOSE price per tick
            volumes:    float64 ndarray, shape (N,) — one volume per tick
            timestamps: int64 ndarray, shape (N,)   — Unix millisecond timestamps
            opens:      float64 ndarray (N,) or None — bar OPEN. OPTIONAL: declare
                        these params to receive them. For tick/trade data (no real
                        candles) open=high=low=close=price. `None` only when the
                        engine called the legacy 3-arg form.
            highs:      float64 ndarray (N,) or None — bar HIGH (see `opens`).
            lows:       float64 ndarray (N,) or None — bar LOW (see `opens`).

        Returns:
            int8 ndarray, shape (N,)

        Note:
            `prices` is the CLOSE series. To use full OHLC, add the optional
            opens/highs/lows parameters to your signature and the engine will
            pass them automatically (e.g. a high-breakout entry uses `highs`).

        Example (vectorized SMA crossover — close only):
            def compute_signals(self, prices, volumes, timestamps):
                import pandas as pd
                s = pd.Series(prices)
                fast = s.rolling(self.params.get("fast_period", 10)).mean()
                slow = s.rolling(self.params.get("slow_period", 50)).mean()
                sig = np.zeros(len(prices), dtype=np.int8)
                sig[fast > slow] = self.BUY
                sig[fast < slow] = self.SELL
                return sig

        Example (OHLC — high-breakout entry, low-stop exit):
            def compute_signals(self, prices, volumes, timestamps,
                                opens=None, highs=None, lows=None):
                import pandas as pd
                n = len(prices)
                sig = np.zeros(n, dtype=np.int8)
                window = self.params.get("lookback", 20)
                # prior rolling high/low (shifted to avoid look-ahead)
                roll_high = pd.Series(highs).rolling(window).max().shift(1)
                roll_low = pd.Series(lows).rolling(window).min().shift(1)
                in_pos = False
                for i in range(n):
                    if not in_pos and highs[i] > roll_high[i]:
                        sig[i] = self.BUY
                        in_pos = True
                    elif in_pos and lows[i] < roll_low[i]:
                        sig[i] = self.CLOSE
                        in_pos = False
                return sig
        """
        ...

    # compute_features(self, prices, volumes, timestamps) -> dict[str, np.ndarray]
    #
    # Optional, not defined on this base class (same treatment as
    # parameter_space() — detected via hasattr, not required). Implement it
    # to expose the intermediate signal(s) your strategy actually trades on
    # — e.g. an RSI value, a z-score, a spread — as a dict of numpy arrays,
    # one entry per tick, aligned 1:1 with `prices`. When present, the engine
    # uses this to compute information coefficient (IC) and quantile
    # analysis: how predictive the feature actually is of forward returns,
    # independent of the entry/exit rules built on top of it. This is
    # diagnostic context, gated behind ENABLE_COMPUTE_FEATURES — it never
    # blocks a backtest, and a strategy that omits it validates exactly as
    # it did before this hook existed.
    #
    # Example:
    #     def compute_features(self, prices, volumes, timestamps):
    #         import pandas as pd
    #         s = pd.Series(prices)
    #         delta = s.diff()
    #         gain = delta.clip(lower=0).rolling(14).mean()
    #         loss = (-delta.clip(upper=0)).rolling(14).mean()
    #         rsi = 100 - (100 / (1 + gain / loss.replace(0, float("nan"))))
    #         return {"rsi_14": rsi.to_numpy()}

    def update_state(self, data: Any, context: Any) -> None:
        """Deprecated per-tick hook — no longer called by the engine. No-op."""
        pass

    def get_parameters(self) -> dict[str, ParameterValue]:
        """
        Return current strategy parameters.

        Used by the genetic algorithm optimizer to read the current parameter
        set. Override this if your strategy has tunable parameters.

        Returns:
            Dict of parameter name -> current value.
        """
        return {}

    def update_parameters(self, parameters: dict[str, ParameterValue]) -> None:
        """
        Update strategy parameters.

        Called by the genetic algorithm when testing new parameter combinations.
        Update your internal state to use the new values.

        Args:
            parameters: Dict of parameter name -> new value.
        """
        pass

    def get_risk_limits(self) -> RiskLimits:
        """
        Return risk limits for this strategy.

        Override to set custom position limits, max drawdown, etc.
        """
        return RiskLimits()

    def is_ready(self) -> bool:
        """Whether the strategy is ready to generate signals."""
        return True

    def required_data_window(self) -> int:
        """Number of historical data points needed before generating signals."""
        return 100

    def data_requirements(self) -> DataRequirements:
        """
        Declare what data this strategy needs.

        Override to specify whether you need trades, candles, orderbook
        snapshots, etc. This affects what data the engine fetches from Tardis.

        Returns:
            DataRequirements specifying needed data types.
        """
        return DataRequirements()

    def warmup(self, historical_data: Any, context: Any) -> None:
        """Deprecated warmup hook — no longer called by the engine. No-op."""
        pass

    def reset(self) -> None:
        """Reset strategy state. Called between optimization runs."""
        pass

    def validate_config(
        self, parameters: dict[str, ParameterValue]
    ) -> Optional[str]:
        """
        Validate a parameter configuration.

        Called before running a backtest to check parameter validity.

        Args:
            parameters: Parameters to validate.

        Returns:
            None if valid, or an error message string if invalid.
        """
        return None

    # ── Derivatives lifecycle hooks (opt-in) ─────────────────────────────

    def on_expiry(self, instrument: DerivativeMetadata, context: Any) -> list:
        """Called when a derivative position reaches expiry. Override to handle rolls."""
        return []

    def on_funding_rate_change(
        self, symbol: str, old_rate: float, new_rate: float, context: Any
    ) -> list:
        """Called when a perpetual swap funding rate changes. Override to react."""
        return []

    def required_instruments(self) -> list[DerivativeMetadata]:
        """Declare specific derivative contracts this strategy requires.

        Override to tell the data loader which exact contracts to stream.
        Default: empty list (engine infers from symbol + exchange).
        """
        return []

    def target_net_delta(self) -> Optional[float]:
        """Portfolio-level target net delta for automatic delta-hedging.

        Return None to disable auto-hedging.
        Return 0.0 for a delta-neutral portfolio.
        The engine will call ``compute_signals`` when delta drifts
        beyond the configured tolerance.
        """
        return None

    # ── Internal serialization (used by Rust bridge) ─────────────────────

    def __init_subclass__(cls, **kwargs):
        """Auto-wrap subclass __init__ to populate state_schema fields as attributes."""
        super().__init_subclass__(**kwargs)
        original_init = cls.__dict__.get("__init__")
        if original_init is None:
            return

        def _wrapped_init(self, *args, **kw):
            original_init(self, *args, **kw)
            # After user __init__, apply state_schema defaults as Python attributes
            if hasattr(self, "state_schema") and callable(self.state_schema):
                try:
                    schema = self.state_schema()
                except Exception:
                    return
                if not isinstance(schema, dict):
                    return
                for field_name, spec in schema.items():
                    if hasattr(self, field_name):
                        continue  # user already set it
                    if not isinstance(spec, dict):
                        continue
                    ftype = spec.get("type", "")
                    if ftype.startswith("window"):
                        continue  # windows are managed by Rust
                    default = spec.get("default")
                    if ftype == "bool":
                        setattr(self, field_name, bool(default) if default is not None else False)
                    elif ftype == "float":
                        setattr(self, field_name, float(default) if default is not None else 0.0)
                    elif ftype == "int":
                        setattr(self, field_name, int(default) if default is not None else 0)
                    elif ftype == "enum":
                        setattr(self, field_name, str(default) if default is not None else "")
                    elif default is not None:
                        setattr(self, field_name, default)

        cls.__init__ = _wrapped_init

    def _to_dict(self) -> dict[str, Any]:
        """Serialize strategy metadata for Rust interop."""
        return {
            "name": self.name(),
            "version": self.version(),
            "description": self.description(),
            "parameters": self.get_parameters(),
            "risk_limits": self.get_risk_limits().to_dict(),
            "data_requirements": self.data_requirements().to_dict(),
            "is_ready": self.is_ready(),
            "required_data_window": self.required_data_window(),
        }


# Alias for market-making strategies.
# MarketMakingStrategy is identical to BaseStrategy — both use the same
# vectorized `compute_signals(prices, volumes, timestamps) -> np.ndarray[int8]` API.
# The MM simulation engine interprets the directional signals (BUY/SELL/HOLD/CLOSE)
# as inventory targets and wraps them in two-sided quotes automatically.
MarketMakingStrategy = BaseStrategy
