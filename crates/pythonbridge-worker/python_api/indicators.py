"""
Technical analysis indicators for Python strategies.

These are pure-Python implementations of common indicators.
The Rust engine uses SIMD-optimized versions internally, but these
provide the same calculations for use in strategy logic.

Usage:
    from trading_platform import Indicators

    prices = [100.0, 101.5, 102.0, 101.0, 103.0]
    sma = Indicators.sma(prices, period=3)
    rsi = Indicators.rsi(prices, period=14)
"""

from __future__ import annotations

from typing import Optional
import math


class Indicators:
    """Static methods for common technical analysis indicators."""

    @staticmethod
    def sma(prices: list[float], period: int) -> Optional[float]:
        """
        Simple Moving Average.

        Args:
            prices: List of price values (most recent last).
            period: Number of periods to average.

        Returns:
            SMA value, or None if insufficient data.
        """
        if len(prices) < period or period <= 0:
            return None
        return sum(prices[-period:]) / period

    @staticmethod
    def ema(prices: list[float], period: int) -> Optional[float]:
        """
        Exponential Moving Average.

        Args:
            prices: List of price values (most recent last).
            period: Number of periods.

        Returns:
            EMA value, or None if insufficient data.
        """
        if len(prices) < period or period <= 0:
            return None
        multiplier = 2.0 / (period + 1)
        ema_val = prices[-period]
        for price in prices[-period + 1:]:
            ema_val = (price - ema_val) * multiplier + ema_val
        return ema_val

    @staticmethod
    def rsi(prices: list[float], period: int = 14) -> Optional[float]:
        """
        Relative Strength Index.

        Args:
            prices: List of price values (most recent last).
            period: RSI period (default 14).

        Returns:
            RSI value (0-100), or None if insufficient data.
        """
        if len(prices) < period + 1:
            return None
        changes = [prices[i] - prices[i - 1] for i in range(len(prices) - period, len(prices))]
        gains = [c for c in changes if c > 0]
        losses = [-c for c in changes if c < 0]
        avg_gain = sum(gains) / period if gains else 0.0
        avg_loss = sum(losses) / period if losses else 0.0
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def bollinger_bands(
        prices: list[float], period: int = 20, std_dev: float = 2.0
    ) -> Optional[tuple[float, float, float]]:
        """
        Bollinger Bands.

        Args:
            prices: List of price values.
            period: Moving average period (default 20).
            std_dev: Number of standard deviations (default 2.0).

        Returns:
            Tuple of (upper_band, middle_band, lower_band), or None.
        """
        if len(prices) < period:
            return None
        window = prices[-period:]
        middle = sum(window) / period
        variance = sum((p - middle) ** 2 for p in window) / period
        std = math.sqrt(variance)
        return (middle + std_dev * std, middle, middle - std_dev * std)

    @staticmethod
    def macd(
        prices: list[float],
        fast_period: int = 12,
        slow_period: int = 26,
        signal_period: int = 9,
    ) -> Optional[tuple[float, float, float]]:
        """
        Moving Average Convergence Divergence.

        Args:
            prices: List of price values.
            fast_period: Fast EMA period (default 12).
            slow_period: Slow EMA period (default 26).
            signal_period: Signal line EMA period (default 9).

        Returns:
            Tuple of (macd_line, signal_line, histogram), or None.
        """
        if len(prices) < slow_period + signal_period:
            return None

        fast_ema = Indicators.ema(prices, fast_period)
        slow_ema = Indicators.ema(prices, slow_period)

        if fast_ema is None or slow_ema is None:
            return None

        # Compute MACD line history for signal line
        macd_values = []
        for i in range(signal_period):
            idx = len(prices) - signal_period + i
            sub_prices = prices[: idx + 1]
            f = Indicators.ema(sub_prices, fast_period)
            s = Indicators.ema(sub_prices, slow_period)
            if f is not None and s is not None:
                macd_values.append(f - s)

        if len(macd_values) < signal_period:
            return None

        macd_line = fast_ema - slow_ema
        signal_line = Indicators.ema(macd_values, signal_period)
        if signal_line is None:
            return None

        histogram = macd_line - signal_line
        return (macd_line, signal_line, histogram)

    @staticmethod
    def vwap(prices: list[float], volumes: list[float], lookback: Optional[int] = None) -> Optional[float]:
        """
        Volume-Weighted Average Price.

        Args:
            prices: List of price values.
            volumes: List of volume values (same length as prices).
            lookback: Number of recent periods to use (None = all).

        Returns:
            VWAP value, or None if insufficient data.
        """
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

    @staticmethod
    def atr(
        highs: list[float], lows: list[float], closes: list[float], period: int = 14
    ) -> Optional[float]:
        """
        Average True Range.

        Args:
            highs: List of high prices.
            lows: List of low prices.
            closes: List of close prices.
            period: ATR period (default 14).

        Returns:
            ATR value, or None if insufficient data.
        """
        n = min(len(highs), len(lows), len(closes))
        if n < period + 1:
            return None
        true_ranges = []
        for i in range(n - period, n):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
            true_ranges.append(tr)
        return sum(true_ranges) / period

    @staticmethod
    def stochastic(
        highs: list[float],
        lows: list[float],
        closes: list[float],
        k_period: int = 14,
        d_period: int = 3,
    ) -> Optional[tuple[float, float]]:
        """
        Stochastic Oscillator (%K, %D).

        Args:
            highs: List of high prices.
            lows: List of low prices.
            closes: List of close prices.
            k_period: %K period (default 14).
            d_period: %D smoothing period (default 3).

        Returns:
            Tuple of (%K, %D), or None if insufficient data.
        """
        n = min(len(highs), len(lows), len(closes))
        if n < k_period + d_period - 1:
            return None

        k_values = []
        for i in range(d_period):
            end = n - d_period + i + 1
            start = end - k_period
            period_high = max(highs[start:end])
            period_low = min(lows[start:end])
            if period_high == period_low:
                k_values.append(50.0)
            else:
                k_values.append(
                    100.0 * (closes[end - 1] - period_low) / (period_high - period_low)
                )

        k = k_values[-1]
        d = sum(k_values) / d_period
        return (k, d)

    @staticmethod
    def price_change(prices: list[float], period: int = 1) -> Optional[float]:
        """
        Price change over N periods.

        Returns:
            Absolute price change, or None if insufficient data.
        """
        if len(prices) < period + 1:
            return None
        return prices[-1] - prices[-1 - period]

    @staticmethod
    def price_change_pct(prices: list[float], period: int = 1) -> Optional[float]:
        """
        Percentage price change over N periods.

        Returns:
            Percentage change as decimal (0.05 = 5%), or None.
        """
        if len(prices) < period + 1:
            return None
        prev = prices[-1 - period]
        if prev == 0:
            return None
        return (prices[-1] - prev) / prev

    @staticmethod
    def stddev(prices: list[float], period: int) -> Optional[float]:
        """
        Standard deviation of prices over a period.

        Returns:
            Population standard deviation, or None.
        """
        if len(prices) < period:
            return None
        window = prices[-period:]
        mean = sum(window) / period
        variance = sum((p - mean) ** 2 for p in window) / period
        return math.sqrt(variance)
