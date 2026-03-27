# -*- coding: utf-8 -*-
"""
Thesis Health Check Module

Monitors 3 macro investment theses and flags status changes:
- Iran Settlement (4-8 week horizon)
- Stagflation / Middle Class Erosion (12-24 month horizon)
- Dalio Stage 6 World Disorder (10-20 year horizon)

Each thesis has:
- Proxy tickers whose price action signals thesis health
- Confirming/disconfirming indicators
- Status: INTACT / STRENGTHENING / WEAKENING / INVALIDATED

Usage:
    checker = ThesisHealthChecker()
    results = checker.check_all()
    print(format_thesis_report(results))
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ThesisProxy:
    """A market proxy that confirms or disconfirms a thesis."""
    ticker: str
    name: str
    direction: str  # "UP" = confirming if rising, "DOWN" = confirming if falling
    weight: float = 1.0  # Relative importance


@dataclass
class ThesisDefinition:
    """Definition of a macro investment thesis."""
    id: str
    name: str
    horizon: str  # e.g. "4-8 weeks", "12-24 months"
    description: str
    confirming_proxies: List[ThesisProxy]
    disconfirming_proxies: List[ThesisProxy]
    portfolio_impact: List[str]  # Affected bucket/ticker names


@dataclass
class ThesisStatus:
    """Health check result for a single thesis."""
    thesis_id: str
    thesis_name: str
    horizon: str
    description: str

    # Status
    status: str = "INTACT"  # STRENGTHENING / INTACT / WEAKENING / INVALIDATED
    confidence: str = "MEDIUM"  # HIGH / MEDIUM / LOW

    # Proxy scores
    confirming_score: float = 0.0  # [-1, +1] composite of confirming proxies
    disconfirming_score: float = 0.0  # [-1, +1] composite of disconfirming proxies
    net_score: float = 0.0  # Combined

    # Detail
    confirming_signals: List[str] = field(default_factory=list)
    disconfirming_signals: List[str] = field(default_factory=list)
    portfolio_impact: List[str] = field(default_factory=list)

    # Error
    error: Optional[str] = None

    def summary(self) -> str:
        """One-line summary for report."""
        if self.error:
            return f"{self.thesis_name}: ERROR - {self.error}"
        return (
            f"{self.thesis_name} [{self.horizon}]: "
            f"{self.status} ({self.confidence}) | "
            f"net={self.net_score:+.2f}"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for JSON serialization."""
        return {
            "thesis_id": self.thesis_id,
            "thesis_name": self.thesis_name,
            "horizon": self.horizon,
            "status": self.status,
            "confidence": self.confidence,
            "net_score": round(self.net_score, 3),
            "confirming_signals": self.confirming_signals,
            "disconfirming_signals": self.disconfirming_signals,
            "portfolio_impact": self.portfolio_impact,
            "error": self.error,
        }


# === Thesis Definitions ===

THESIS_DEFINITIONS: List[ThesisDefinition] = [
    ThesisDefinition(
        id="IRAN_SETTLEMENT",
        name="Iran Settlement",
        horizon="4-8 weeks",
        description=(
            "Diplomatic progress toward Iran nuclear deal or sanctions relief. "
            "If confirmed: energy prices drop (short energy winners), "
            "Gulf reconstruction plays rally (KSA, UAE). "
            "If invalidated: energy stays bid, defense names rally."
        ),
        confirming_proxies=[
            # Settlement = oil drops, Gulf ETFs rally, defense eases
            ThesisProxy("BZ=F", "Brent Crude", "DOWN", weight=1.5),
            ThesisProxy("KSA", "iShares MSCI Saudi", "UP", weight=1.0),
            ThesisProxy("UAE", "iShares MSCI UAE", "UP", weight=1.0),
        ],
        disconfirming_proxies=[
            # No deal = oil stays high, defense rallies
            ThesisProxy("BZ=F", "Brent Crude", "UP", weight=1.0),
            ThesisProxy("LNG", "Cheniere Energy", "UP", weight=0.8),
            ThesisProxy("^VIX", "VIX", "UP", weight=0.5),
        ],
        portfolio_impact=["LNG", "EQNR", "KSA", "UAE", "RENK.DE"],
    ),
    ThesisDefinition(
        id="STAGFLATION",
        name="Stagflation / Middle Class Erosion",
        horizon="12-24 months",
        description=(
            "Persistent above-target inflation combined with slowing growth. "
            "Consumer purchasing power erodes, benefiting discount retailers, "
            "hard assets (gold, BTC, TIPS), and hurting growth/tech multiples."
        ),
        confirming_proxies=[
            # Stagflation = gold up, TIPS outperform, discount retail strong
            ThesisProxy("GC=F", "Gold Futures", "UP", weight=1.5),
            ThesisProxy("TIP", "TIPS ETF", "UP", weight=1.0),
            ThesisProxy("BTC-USD", "Bitcoin", "UP", weight=0.8),
            ThesisProxy("TJX", "TJX Companies", "UP", weight=1.0),
            ThesisProxy("DLTR", "Dollar Tree", "UP", weight=0.8),
        ],
        disconfirming_proxies=[
            # No stagflation = growth rallies, gold drops, yields normalize
            ThesisProxy("^GSPC", "S&P 500", "UP", weight=0.8),
            ThesisProxy("GC=F", "Gold Futures", "DOWN", weight=1.0),
            ThesisProxy("TLT", "20Y Treasury", "UP", weight=1.0),
        ],
        portfolio_impact=[
            "BTC-USD", "PHYS", "NEM", "GOLD", "TIP",
            "TJX", "DLTR", "FCFS", "CXW",
        ],
    ),
    ThesisDefinition(
        id="DALIO_STAGE6",
        name="Dalio Stage 6 World Disorder",
        horizon="10-20 years",
        description=(
            "Ray Dalio's big cycle framework: rising great power conflict, "
            "debt monetization, internal political polarization. "
            "Favors defense spending, hard assets, commodity producers, "
            "and geopolitical hedge positions."
        ),
        confirming_proxies=[
            # Disorder = defense up, gold up, VIX elevated, EM divergence
            ThesisProxy("GC=F", "Gold Futures", "UP", weight=1.5),
            ThesisProxy("BTC-USD", "Bitcoin", "UP", weight=1.0),
            ThesisProxy("CCJ", "Cameco (Uranium)", "UP", weight=0.8),
            ThesisProxy("ASML", "ASML", "UP", weight=1.0),
            ThesisProxy("^VIX", "VIX", "UP", weight=0.5),
        ],
        disconfirming_proxies=[
            # Order restored = risk-on, VIX drops, growth leads
            ThesisProxy("^VIX", "VIX", "DOWN", weight=1.0),
            ThesisProxy("EEM", "Emerging Markets", "UP", weight=0.8),
            ThesisProxy("TLT", "20Y Treasury", "UP", weight=0.8),
        ],
        portfolio_impact=[
            "ASML", "RENK.DE", "HENS.DE", "KGSY",
            "CCJ", "CEG", "VST", "NEM", "GOLD",
        ],
    ),
]


class ThesisHealthChecker:
    """Checks health of macro investment theses using price proxies."""

    def __init__(self, lookback_days: int = 30):
        """
        Args:
            lookback_days: Number of calendar days for return calculation.
        """
        self.lookback_days = lookback_days

    def check_all(self) -> List[ThesisStatus]:
        """Check all defined theses. Returns list of ThesisStatus."""
        results = []
        for thesis_def in THESIS_DEFINITIONS:
            try:
                result = self._check_thesis(thesis_def)
                results.append(result)
            except Exception as e:
                logger.error(f"[ThesisHealth] Failed to check {thesis_def.id}: {e}")
                results.append(ThesisStatus(
                    thesis_id=thesis_def.id,
                    thesis_name=thesis_def.name,
                    horizon=thesis_def.horizon,
                    description=thesis_def.description,
                    portfolio_impact=thesis_def.portfolio_impact,
                    error=str(e),
                ))
        return results

    def _check_thesis(self, thesis_def: ThesisDefinition) -> ThesisStatus:
        """Check a single thesis using proxy price data."""
        result = ThesisStatus(
            thesis_id=thesis_def.id,
            thesis_name=thesis_def.name,
            horizon=thesis_def.horizon,
            description=thesis_def.description,
            portfolio_impact=thesis_def.portfolio_impact,
        )

        # Score confirming proxies
        confirm_scores = []
        for proxy in thesis_def.confirming_proxies:
            score, signal = self._score_proxy(proxy)
            if score is not None:
                confirm_scores.append(score * proxy.weight)
                if signal:
                    result.confirming_signals.append(signal)

        # Score disconfirming proxies
        disconfirm_scores = []
        for proxy in thesis_def.disconfirming_proxies:
            score, signal = self._score_proxy(proxy)
            if score is not None:
                disconfirm_scores.append(score * proxy.weight)
                if signal:
                    result.disconfirming_signals.append(signal)

        # Compute composite scores
        if confirm_scores:
            total_weight = sum(
                p.weight for p in thesis_def.confirming_proxies
                if any(True for _ in [])  # placeholder
            )
            result.confirming_score = sum(confirm_scores) / max(
                sum(p.weight for p in thesis_def.confirming_proxies), 1
            )
        if disconfirm_scores:
            result.disconfirming_score = sum(disconfirm_scores) / max(
                sum(p.weight for p in thesis_def.disconfirming_proxies), 1
            )

        # Net score: confirming minus disconfirming
        result.net_score = result.confirming_score - result.disconfirming_score

        # Determine status from net score
        net = result.net_score
        if net >= 0.3:
            result.status = "STRENGTHENING"
            result.confidence = "HIGH" if net >= 0.5 else "MEDIUM"
        elif net >= -0.1:
            result.status = "INTACT"
            result.confidence = "MEDIUM" if net >= 0.1 else "LOW"
        elif net >= -0.3:
            result.status = "WEAKENING"
            result.confidence = "MEDIUM"
        else:
            result.status = "INVALIDATED"
            result.confidence = "HIGH" if net <= -0.5 else "MEDIUM"

        logger.info(
            f"[ThesisHealth] {thesis_def.id}: {result.status} ({result.confidence}) "
            f"net={result.net_score:+.2f}"
        )
        return result

    def _score_proxy(self, proxy: ThesisProxy) -> tuple:
        """
        Score a single proxy ticker.

        Returns:
            (score, signal_text) where score is in [-1, +1]
            score > 0 means proxy is confirming its thesis direction
            signal_text is a human-readable description, or None if data unavailable
        """
        try:
            returns = self._get_returns(proxy.ticker)
            if returns is None:
                return (None, None)

            ret_5d, ret_20d = returns

            # Normalize: if direction is "UP", positive return = confirming
            # If direction is "DOWN", negative return = confirming
            multiplier = 1.0 if proxy.direction == "UP" else -1.0

            # Weighted blend: 60% 20d, 40% 5d (longer-term matters more for theses)
            if ret_20d is not None and ret_5d is not None:
                raw = 0.4 * ret_5d + 0.6 * ret_20d
            elif ret_20d is not None:
                raw = ret_20d
            elif ret_5d is not None:
                raw = ret_5d
            else:
                return (None, None)

            # Apply direction multiplier
            directed = raw * multiplier

            # Normalize to [-1, +1] using tanh-like scaling (5% move = ~0.5 score)
            score = float(np.tanh(directed / 5.0))

            # Generate signal text
            direction_word = "up" if raw > 0 else "down"
            strength = "strongly " if abs(raw) > 3 else ""
            confirming = "confirming" if score > 0.1 else "disconfirming" if score < -0.1 else "neutral"
            signal = f"{proxy.name} {strength}{direction_word} {raw:+.1f}% ({confirming})"

            return (score, signal)

        except Exception as e:
            logger.warning(f"[ThesisHealth] Failed to score {proxy.ticker}: {e}")
            return (None, None)

    def _get_returns(self, ticker: str) -> Optional[tuple]:
        """
        Get 5-day and 20-day returns for a ticker.

        Returns:
            (return_5d_pct, return_20d_pct) or None if data unavailable
        """
        from data_provider.base import DataFetcherManager

        manager = DataFetcherManager()
        try:
            df, source = manager.get_daily_data(
                stock_code=ticker,
                days=self.lookback_days,
            )
            if df is None or df.empty or "close" not in df.columns:
                logger.warning(f"[ThesisHealth] No data for {ticker}")
                return None

            prices = df.set_index("date")["close"]
            if len(prices) < 5:
                return None

            ret_5d = float((prices.iloc[-1] / prices.iloc[-5] - 1) * 100) if len(prices) >= 5 else None
            ret_20d = float((prices.iloc[-1] / prices.iloc[-20] - 1) * 100) if len(prices) >= 20 else None

            return (ret_5d, ret_20d)

        except Exception as e:
            logger.warning(f"[ThesisHealth] Failed to fetch {ticker}: {e}")
            return None


def format_thesis_report(theses: List[ThesisStatus]) -> str:
    """Format thesis health results into a Telegram-friendly report section."""
    lines = ["**MACRO THESIS HEALTH CHECK**", ""]

    for t in theses:
        if t.error:
            lines.append(f"- {t.thesis_name} [{t.horizon}]: DATA ERROR")
            continue

        # Status emoji
        status_emoji = {
            "STRENGTHENING": "+",
            "INTACT": "=",
            "WEAKENING": "-",
            "INVALIDATED": "X",
        }.get(t.status, "?")

        lines.append(
            f"- [{status_emoji}] {t.thesis_name} ({t.horizon}): "
            f"{t.status} [{t.confidence}]"
        )

        # Top confirming signals (max 2)
        for sig in t.confirming_signals[:2]:
            lines.append(f"    + {sig}")

        # Top disconfirming signals (max 2)
        for sig in t.disconfirming_signals[:2]:
            lines.append(f"    - {sig}")

        lines.append("")

    return "\n".join(lines)
