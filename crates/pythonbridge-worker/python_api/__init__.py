"""
TradingPlatform Python Strategy SDK

Write custom trading strategies in Python and run them on the institutional-grade
backtesting engine with sub-millisecond simulation, genetic optimization,
walk-forward analysis, and Monte Carlo validation.

Quick Start:
    from trading_platform import BaseStrategy
    import numpy as np
    import pandas as pd

    class MyStrategy(BaseStrategy):
        def name(self) -> str:
            return "MyCustomStrategy"

        def compute_signals(self, prices, volumes, timestamps):
            # Vectorized SMA crossover — called ONCE with full tick array
            s = pd.Series(prices)
            fast = s.rolling(10).mean()
            slow = s.rolling(50).mean()
            sig = np.zeros(len(prices), dtype=np.int8)
            sig[fast > slow] = self.BUY   # 1
            sig[fast < slow] = self.SELL  # -1
            return sig

Signal constants (also available as module-level: BUY, SELL, HOLD, CLOSE):
    BUY   =  1  — open/increase long position
    SELL  = -1  — open/increase short position
    HOLD  =  0  — no change (default)
    CLOSE =  2  — close any open position
"""

from .strategy import BaseStrategy, MarketMakingStrategy
from .types import (
    # Signal constants
    BUY,
    SELL,
    HOLD,
    CLOSE,
    # Signal types
    Signal,
    SignalType,
    SignalStrength,
    SignalReason,
    MarketData,
    TradeData,
    CandleData,
    StrategyContext,
    PortfolioState,
    PricePoint,
    VolumePoint,
    DataRequirements,
    StrategyCategory,
    RiskLimits,
    ParameterValue,
    # Derivatives
    InstrumentKind,
    DerivativeMetadata,
    Greeks,
    SpreadLeg,
    IvPoint,
    IvSurface,
    FuturesCurvePoint,
    FuturesCurve,
    FundingRate,
)
from .indicators import Indicators

__version__ = "0.2.0"
__all__ = [
    "BaseStrategy",
    "MarketMakingStrategy",
    # Signal constants
    "BUY",
    "SELL",
    "HOLD",
    "CLOSE",
    # Signal types
    "Signal",
    "SignalType",
    "SignalStrength",
    "SignalReason",
    "MarketData",
    "TradeData",
    "CandleData",
    "StrategyContext",
    "PortfolioState",
    "PricePoint",
    "VolumePoint",
    "DataRequirements",
    "StrategyCategory",
    "RiskLimits",
    "ParameterValue",
    "Indicators",
    # Derivatives
    "InstrumentKind",
    "DerivativeMetadata",
    "Greeks",
    "SpreadLeg",
    "IvPoint",
    "IvSurface",
    "FuturesCurvePoint",
    "FuturesCurve",
    "FundingRate",
]
