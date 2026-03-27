# -*- coding: utf-8 -*-
"""
Pair Trade Tracking Module

Tracks 10 long/short pairs with:
- Current spread vs 30/60/90 day historical spread
- Spread z-score (actionable if abs(z) > 2)
- Momentum direction per leg
- Conviction-tiered signal (HIGH/MEDIUM/LOW/WATCH)

Usage:
    tracker = PairTracker()
    results = tracker.analyze_all_pairs()
    for pair in results:
        print(pair.summary())
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class PairLeg:
    """One leg of a pair trade."""
    tickers: List[str]  # Can be multiple (e.g. ["NEM", "GOLD"] for composite leg)
    name: str  # Display name


@dataclass
class PairDefinition:
    """Definition of a long/short pair trade."""
    long_leg: PairLeg
    short_leg: PairLeg
    thesis: str
    id: str  # Unique identifier


@dataclass
class PairAnalysis:
    """Analysis result for a single pair."""
    pair_id: str
    long_name: str
    short_name: str
    thesis: str

    # Spread data
    current_spread: Optional[float] = None
    spread_30d_avg: Optional[float] = None
    spread_60d_avg: Optional[float] = None
    spread_90d_avg: Optional[float] = None
    spread_30d_std: Optional[float] = None

    # Z-score
    z_score_30d: Optional[float] = None
    z_score_60d: Optional[float] = None

    # Momentum
    long_return_5d: Optional[float] = None
    long_return_20d: Optional[float] = None
    short_return_5d: Optional[float] = None
    short_return_20d: Optional[float] = None
    stronger_leg: str = "NEUTRAL"  # LONG / SHORT / NEUTRAL

    # Spread direction
    spread_direction: str = "NEUTRAL"  # WIDENING / NARROWING / NEUTRAL

    # Signal
    signal: str = "HOLD"  # ENTER / ADD / HOLD / CLOSE / FLIP
    conviction: str = "WATCH"  # HIGH / MEDIUM / LOW / WATCH

    # Error
    error: Optional[str] = None

    def summary(self) -> str:
        """One-line summary for report."""
        if self.error:
            return f"{self.long_name}/{self.short_name}: ERROR - {self.error}"
        z = f"z={self.z_score_30d:+.1f}" if self.z_score_30d is not None else "z=N/A"
        return (
            f"{self.long_name}/{self.short_name}: "
            f"spread={self.current_spread:+.1f}% " if self.current_spread is not None else f"{self.long_name}/{self.short_name}: "
            f"({z}) | {self.spread_direction} | "
            f"{self.signal} [{self.conviction}]"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for JSON serialization."""
        return {
            "pair_id": self.pair_id,
            "long": self.long_name,
            "short": self.short_name,
            "thesis": self.thesis,
            "current_spread_pct": round(self.current_spread, 2) if self.current_spread is not None else None,
            "spread_30d_avg": round(self.spread_30d_avg, 2) if self.spread_30d_avg is not None else None,
            "z_score_30d": round(self.z_score_30d, 2) if self.z_score_30d is not None else None,
            "z_score_60d": round(self.z_score_60d, 2) if self.z_score_60d is not None else None,
            "spread_direction": self.spread_direction,
            "stronger_leg": self.stronger_leg,
            "signal": self.signal,
            "conviction": self.conviction,
            "long_return_5d": round(self.long_return_5d, 2) if self.long_return_5d is not None else None,
            "short_return_5d": round(self.short_return_5d, 2) if self.short_return_5d is not None else None,
            "error": self.error,
        }


# === Pair Definitions ===

PAIR_DEFINITIONS: List[PairDefinition] = [
    PairDefinition(
        id="CRWD_CSCO",
        long_leg=PairLeg(tickers=["CRWD"], name="CRWD"),
        short_leg=PairLeg(tickers=["CSCO"], name="CSCO"),
        thesis="AI-native cyber vs legacy network security",
    ),
    PairDefinition(
        id="ZS_PANW",
        long_leg=PairLeg(tickers=["ZS"], name="ZS"),
        short_leg=PairLeg(tickers=["PANW"], name="PANW"),
        thesis="Zero-trust pure-play vs overpriced platform bundler",
    ),
    PairDefinition(
        id="TJX_M",
        long_leg=PairLeg(tickers=["TJX"], name="TJX"),
        short_leg=PairLeg(tickers=["M"], name="M"),
        thesis="Trade-down winner vs mid-tier retail loser",
    ),
    PairDefinition(
        id="DLTR_WMT",
        long_leg=PairLeg(tickers=["DLTR"], name="DLTR"),
        short_leg=PairLeg(tickers=["WMT"], name="WMT"),
        thesis="Ultra-discount vs full-price discount",
    ),
    PairDefinition(
        id="LNG_XOM",
        long_leg=PairLeg(tickers=["LNG"], name="LNG"),
        short_leg=PairLeg(tickers=["XOM"], name="XOM"),
        thesis="Pure LNG infrastructure vs integrated oil",
    ),
    PairDefinition(
        id="RYAAY_AFPA",
        long_leg=PairLeg(tickers=["RYAAY"], name="RYAAY"),
        short_leg=PairLeg(tickers=["AF.PA"], name="AF.PA"),
        thesis="Low-cost EU aviation vs legacy carrier",
    ),
    PairDefinition(
        id="NEMGOLD_TLT",
        long_leg=PairLeg(tickers=["NEM", "GOLD"], name="NEM+GOLD"),
        short_leg=PairLeg(tickers=["TLT"], name="TLT"),
        thesis="Gold miners vs long-duration bonds",
    ),
    PairDefinition(
        id="ASML_INTC",
        long_leg=PairLeg(tickers=["ASML"], name="ASML"),
        short_leg=PairLeg(tickers=["INTC"], name="INTC"),
        thesis="Technology war winner vs technology war loser",
    ),
    PairDefinition(
        id="KSAUAE_EEM",
        long_leg=PairLeg(tickers=["KSA", "UAE"], name="KSA+UAE"),
        short_leg=PairLeg(tickers=["EEM"], name="EEM"),
        thesis="Gulf reconstruction vs broader EM basket",
    ),
    PairDefinition(
        id="FLOWAS_GS",
        long_leg=PairLeg(tickers=["FLOW.AS"], name="FLOW.AS"),
        short_leg=PairLeg(tickers=["GS"], name="GS"),
        thesis="Volatility profiteer vs traditional market maker",
    ),
]


class PairTracker:
    """Analyzes long/short pairs using historical price data."""

    def __init__(self, lookback_days: int = 120):
        """
        Args:
            lookback_days: Number of calendar days of history to fetch (default 120
                           to ensure ~90 trading days for z-score calculation).
        """
        self.lookback_days = lookback_days

    def analyze_all_pairs(self) -> List[PairAnalysis]:
        """Analyze all defined pairs. Returns list of PairAnalysis."""
        results = []
        for pair_def in PAIR_DEFINITIONS:
            try:
                result = self._analyze_pair(pair_def)
                results.append(result)
            except Exception as e:
                logger.error(f"[PairTracker] Failed to analyze {pair_def.id}: {e}")
                results.append(PairAnalysis(
                    pair_id=pair_def.id,
                    long_name=pair_def.long_leg.name,
                    short_name=pair_def.short_leg.name,
                    thesis=pair_def.thesis,
                    error=str(e),
                ))
        return results

    def _analyze_pair(self, pair_def: PairDefinition) -> PairAnalysis:
        """Analyze a single pair."""
        result = PairAnalysis(
            pair_id=pair_def.id,
            long_name=pair_def.long_leg.name,
            short_name=pair_def.short_leg.name,
            thesis=pair_def.thesis,
        )

        # Fetch price history for both legs
        long_prices = self._get_composite_prices(pair_def.long_leg.tickers)
        short_prices = self._get_composite_prices(pair_def.short_leg.tickers)

        if long_prices is None or short_prices is None:
            result.error = "Failed to fetch price data for one or both legs"
            return result

        # Align dates
        combined = pd.DataFrame({
            "long": long_prices,
            "short": short_prices,
        }).dropna()

        if len(combined) < 20:
            result.error = f"Insufficient data: only {len(combined)} trading days"
            return result

        # Calculate spread: log(long) - log(short) normalized to percentage
        combined["spread"] = (
            (combined["long"] / combined["long"].iloc[0]) -
            (combined["short"] / combined["short"].iloc[0])
        ) * 100  # percentage points of relative performance

        current_spread = combined["spread"].iloc[-1]
        result.current_spread = float(current_spread)

        # 30/60/90 day averages and z-scores
        for window, attr_avg, attr_std, attr_z in [
            (30, "spread_30d_avg", "spread_30d_std", "z_score_30d"),
            (60, "spread_60d_avg", None, "z_score_60d"),
            (90, "spread_90d_avg", None, None),
        ]:
            if len(combined) >= window:
                window_data = combined["spread"].iloc[-window:]
                avg = float(window_data.mean())
                std = float(window_data.std())
                setattr(result, attr_avg, avg)
                if attr_std:
                    setattr(result, attr_std, std)
                if attr_z and std > 0.01:
                    setattr(result, attr_z, float((current_spread - avg) / std))

        # Momentum per leg (5d and 20d returns)
        if len(combined) >= 5:
            result.long_return_5d = float(
                (combined["long"].iloc[-1] / combined["long"].iloc[-5] - 1) * 100
            )
            result.short_return_5d = float(
                (combined["short"].iloc[-1] / combined["short"].iloc[-5] - 1) * 100
            )
        if len(combined) >= 20:
            result.long_return_20d = float(
                (combined["long"].iloc[-1] / combined["long"].iloc[-20] - 1) * 100
            )
            result.short_return_20d = float(
                (combined["short"].iloc[-1] / combined["short"].iloc[-20] - 1) * 100
            )

        # Determine stronger leg
        if result.long_return_5d is not None and result.short_return_5d is not None:
            diff = (result.long_return_5d or 0) - (result.short_return_5d or 0)
            if diff > 1.0:
                result.stronger_leg = "LONG"
            elif diff < -1.0:
                result.stronger_leg = "SHORT"
            else:
                result.stronger_leg = "NEUTRAL"

        # Spread direction (5-day trend)
        if len(combined) >= 5:
            spread_5d_ago = combined["spread"].iloc[-5]
            spread_change = current_spread - spread_5d_ago
            if spread_change > 0.5:
                result.spread_direction = "WIDENING"
            elif spread_change < -0.5:
                result.spread_direction = "NARROWING"
            else:
                result.spread_direction = "NEUTRAL"

        # Generate signal based on z-score
        z = result.z_score_30d
        if z is not None:
            abs_z = abs(z)
            if abs_z >= 2.5:
                result.signal = "ENTER" if z > 0 else "CLOSE"
                result.conviction = "HIGH"
            elif abs_z >= 2.0:
                result.signal = "ADD" if z > 0 else "CLOSE"
                result.conviction = "HIGH"
            elif abs_z >= 1.5:
                result.signal = "ADD" if z > 0 else "HOLD"
                result.conviction = "MEDIUM"
            elif abs_z >= 1.0:
                result.signal = "HOLD"
                result.conviction = "LOW"
            else:
                result.signal = "HOLD"
                result.conviction = "WATCH"
        else:
            result.signal = "HOLD"
            result.conviction = "WATCH"

        logger.info(
            f"[PairTracker] {pair_def.id}: spread={current_spread:+.1f}% "
            f"z30={result.z_score_30d or 'N/A'} signal={result.signal} [{result.conviction}]"
        )
        return result

    def _get_composite_prices(self, tickers: List[str]) -> Optional[pd.Series]:
        """
        Get composite closing prices for a leg (equal-weighted if multiple tickers).
        Returns a Series indexed by date.
        """
        from data_provider.base import DataFetcherManager

        manager = DataFetcherManager()
        all_prices = []

        for ticker in tickers:
            try:
                df, source = manager.get_daily_data(
                    stock_code=ticker,
                    days=self.lookback_days,
                )
                if df is not None and not df.empty and "close" in df.columns:
                    series = df.set_index("date")["close"]
                    series.name = ticker
                    all_prices.append(series)
                else:
                    logger.warning(f"[PairTracker] No data for {ticker}")
                    return None
            except Exception as e:
                logger.warning(f"[PairTracker] Failed to fetch {ticker}: {e}")
                return None

        if not all_prices:
            return None

        if len(all_prices) == 1:
            return all_prices[0]

        # Equal-weighted composite: normalize each to 100, then average
        combined = pd.DataFrame(all_prices).T.dropna()
        if combined.empty:
            return None
        normalized = combined.div(combined.iloc[0]) * 100
        return normalized.mean(axis=1)


def format_pair_tracker_report(pairs: List[PairAnalysis]) -> str:
    """Format pair analysis results into a Telegram-friendly report section."""
    lines = ["**LONG/SHORT PAIR TRACKER**", ""]

    for p in pairs:
        if p.error:
            lines.append(f"- {p.long_name}/{p.short_name}: DATA ERROR")
            continue

        z_str = f"z={p.z_score_30d:+.1f}" if p.z_score_30d is not None else "z=N/A"
        spread_str = f"{p.current_spread:+.1f}%" if p.current_spread is not None else "N/A"
        avg_str = f"avg={p.spread_30d_avg:+.1f}%" if p.spread_30d_avg is not None else ""

        lines.append(
            f"- {p.long_name}/{p.short_name}: "
            f"{spread_str} ({z_str}) {avg_str} | "
            f"{p.spread_direction} | "
            f"{p.signal} [{p.conviction}]"
        )

    return "\n".join(lines)
