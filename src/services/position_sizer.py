# -*- coding: utf-8 -*-
"""
Position Sizing Calculator

Calculates position sizes based on:
- Conviction level (HIGH/MEDIUM/LOW)
- Thesis bucket membership
- Current market regime
- Kill switch status
- Bucket capacity constraints

Returns EUR amounts and portfolio percentages with rationale.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple

logger = logging.getLogger(__name__)


# === Portfolio Bucket Definitions ===
# Maps tickers to their bucket and target allocation

BUCKET_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "HARD_ASSETS": {
        "name": "Hard Assets & Inflation Protection",
        "target_pct": (20, 25),  # min, max
        "tickers": ["BTC-USD", "PHYS", "NEM", "GOLD", "TIP", "CCJ"],
        "thesis_id": ["STAGFLATION", "DALIO_STAGE6"],
    },
    "ENERGY_NUCLEAR": {
        "name": "Energy & Nuclear Infrastructure",
        "target_pct": (15, 20),
        "tickers": ["LNG", "FLNG", "EQNR", "CEG", "VST"],
        "thesis_id": ["IRAN_SETTLEMENT", "STAGFLATION"],
    },
    "DEFENSE_SUPPLY": {
        "name": "Defense Supply Chain Tier 2/3",
        "target_pct": (15, 20),
        "tickers": ["ASML", "KGSY", "HENS.DE", "RENK.DE", "JDDE.DE", "CHG.L", "AMG.AS"],
        "thesis_id": ["DALIO_STAGE6"],
    },
    "CONSUMER_STRESS": {
        "name": "Consumer Stress & Domestic Policy",
        "target_pct": (10, 15),
        "tickers": ["TJX", "DLTR", "FCFS", "CXW", "FLOW.AS"],
        "thesis_id": ["STAGFLATION"],
    },
    "GEOPOLITICAL": {
        "name": "Geopolitical Reconstruction & Multipolar",
        "target_pct": (10, 15),
        "tickers": ["KSA", "UAE", "MP", "MELI", "RYAAY", "NVO"],
        "thesis_id": ["IRAN_SETTLEMENT", "DALIO_STAGE6"],
    },
}

# Industry watch tickers - no position sizing
INDUSTRY_WATCH_TICKERS = {"CRM", "NOW", "HUBS", "CRWD", "ZS"}

# Conviction to base size mapping
CONVICTION_SIZING: Dict[str, Tuple[float, float]] = {
    "HIGH": (3.0, 5.0),     # 3-5% of portfolio
    "MEDIUM": (1.5, 3.0),   # 1.5-3% of portfolio
    "LOW": (0.5, 1.5),      # 0.5-1.5% of portfolio
}

# Market regime modifiers
REGIME_MODIFIERS: Dict[str, float] = {
    "RISK_ON": 1.0,
    "RISK_OFF": 0.7,
    "TRANSITIONING": 0.7,
    "STAGFLATION": 1.0,
}


@dataclass
class PositionSizeResult:
    """Result of position sizing calculation."""
    ticker: str
    conviction: str
    bucket_name: str
    bucket_id: str

    # Sizing output
    min_amount_eur: float
    max_amount_eur: float
    min_pct: float
    max_pct: float

    # Context
    regime: str
    regime_modifier: float
    kill_switch_modifier: float
    bucket_available_capacity_pct: float

    # Rationale
    rationale: List[str] = field(default_factory=list)

    # Flags
    rejected: bool = False
    rejection_reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ticker": self.ticker,
            "conviction": self.conviction,
            "bucket": self.bucket_name,
            "min_amount_eur": round(self.min_amount_eur, 2),
            "max_amount_eur": round(self.max_amount_eur, 2),
            "min_pct": round(self.min_pct, 2),
            "max_pct": round(self.max_pct, 2),
            "regime": self.regime,
            "rationale": self.rationale,
            "rejected": self.rejected,
            "rejection_reason": self.rejection_reason,
        }

    def summary(self) -> str:
        if self.rejected:
            return f"{self.ticker}: REJECTED - {self.rejection_reason}"
        return (
            f"{self.ticker} [{self.conviction}]: "
            f"EUR {self.min_amount_eur:,.0f}-{self.max_amount_eur:,.0f} "
            f"({self.min_pct:.1f}-{self.max_pct:.1f}%) | {self.bucket_name}"
        )


@dataclass
class PairSizeResult:
    """Result of pair trade sizing calculation."""
    long_ticker: str
    short_ticker: str
    conviction: str
    thesis: str

    # Long leg sizing
    long_amount_eur: float
    long_pct: float

    # Short leg sizing (60% of long)
    short_amount_eur: float
    short_pct: float

    # Context
    regime: str
    rationale: List[str] = field(default_factory=list)

    # Flags
    rejected: bool = False
    rejection_reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "long": self.long_ticker,
            "short": self.short_ticker,
            "conviction": self.conviction,
            "long_amount_eur": round(self.long_amount_eur, 2),
            "long_pct": round(self.long_pct, 2),
            "short_amount_eur": round(self.short_amount_eur, 2),
            "short_pct": round(self.short_pct, 2),
            "rejected": self.rejected,
        }

    def summary(self) -> str:
        if self.rejected:
            return f"{self.long_ticker}/{self.short_ticker}: REJECTED - {self.rejection_reason}"
        return (
            f"{self.long_ticker}/{self.short_ticker} [{self.conviction}]: "
            f"Long EUR {self.long_amount_eur:,.0f} ({self.long_pct:.1f}%) / "
            f"Short EUR {self.short_amount_eur:,.0f} ({self.short_pct:.1f}%)"
        )


class PositionSizer:
    """
    Calculate position sizes based on conviction, regime, and constraints.
    """

    def __init__(
        self,
        portfolio_value_eur: float = 500_000.0,
        current_allocations: Optional[Dict[str, float]] = None,
        regime: str = "RISK_ON",
        kill_switch_status: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize position sizer.

        Args:
            portfolio_value_eur: Total portfolio value in EUR
            current_allocations: Dict mapping bucket_id -> current % allocation
            regime: Current market regime (RISK_ON/RISK_OFF/TRANSITIONING/STAGFLATION)
            kill_switch_status: Dict mapping thesis_id -> status (ACTIVE/WARNING/TRIGGERED)
        """
        self.portfolio_value = portfolio_value_eur
        self.current_allocations = current_allocations or {}
        self.regime = regime.upper() if regime else "RISK_ON"
        self.kill_switch_status = kill_switch_status or {}

    def get_bucket_for_ticker(self, ticker: str) -> Optional[str]:
        """Get bucket ID for a ticker, or None if not in any bucket."""
        ticker_upper = ticker.upper().strip()
        for bucket_id, bucket_def in BUCKET_DEFINITIONS.items():
            if ticker_upper in [t.upper() for t in bucket_def["tickers"]]:
                return bucket_id
        return None

    def is_industry_watch(self, ticker: str) -> bool:
        """Check if ticker is industry watch (no sizing)."""
        return ticker.upper().strip() in INDUSTRY_WATCH_TICKERS

    def calculate_position_size(
        self,
        ticker: str,
        conviction: str,
    ) -> PositionSizeResult:
        """
        Calculate position size for a single ticker.

        Args:
            ticker: Stock ticker
            conviction: Conviction level (HIGH/MEDIUM/LOW)

        Returns:
            PositionSizeResult with sizing recommendation
        """
        ticker = ticker.upper().strip()
        conviction = conviction.upper().strip()
        rationale = []

        # Check if industry watch
        if self.is_industry_watch(ticker):
            return PositionSizeResult(
                ticker=ticker,
                conviction=conviction,
                bucket_name="INDUSTRY WATCH",
                bucket_id="",
                min_amount_eur=0,
                max_amount_eur=0,
                min_pct=0,
                max_pct=0,
                regime=self.regime,
                regime_modifier=0,
                kill_switch_modifier=0,
                bucket_available_capacity_pct=0,
                rejected=True,
                rejection_reason="Industry watch only - no position sizing",
            )

        # Get bucket
        bucket_id = self.get_bucket_for_ticker(ticker)
        if not bucket_id:
            return PositionSizeResult(
                ticker=ticker,
                conviction=conviction,
                bucket_name="UNKNOWN",
                bucket_id="",
                min_amount_eur=0,
                max_amount_eur=0,
                min_pct=0,
                max_pct=0,
                regime=self.regime,
                regime_modifier=0,
                kill_switch_modifier=0,
                bucket_available_capacity_pct=0,
                rejected=True,
                rejection_reason="Ticker not in any defined bucket",
            )

        bucket = BUCKET_DEFINITIONS[bucket_id]
        bucket_name = bucket["name"]
        bucket_max_pct = bucket["target_pct"][1]
        bucket_thesis_ids = bucket["thesis_id"]

        # Get base sizing from conviction
        if conviction not in CONVICTION_SIZING:
            conviction = "MEDIUM"
        base_min_pct, base_max_pct = CONVICTION_SIZING[conviction]
        rationale.append(f"Base sizing for {conviction} conviction: {base_min_pct}-{base_max_pct}%")

        # Apply regime modifier
        regime_modifier = REGIME_MODIFIERS.get(self.regime, 1.0)
        if regime_modifier != 1.0:
            rationale.append(f"Regime {self.regime}: {regime_modifier:.0%} modifier applied")

        # Apply kill switch modifier
        kill_switch_modifier = 1.0
        for thesis_id in bucket_thesis_ids:
            status = self.kill_switch_status.get(thesis_id, "ACTIVE")
            if status == "TRIGGERED":
                kill_switch_modifier = min(kill_switch_modifier, 0.5)
                rationale.append(f"Kill switch TRIGGERED for {thesis_id}: 50% reduction")
            elif status == "WARNING":
                kill_switch_modifier = min(kill_switch_modifier, 0.7)
                rationale.append(f"Kill switch WARNING for {thesis_id}: 30% reduction")

        # Calculate final percentages
        final_min_pct = base_min_pct * regime_modifier * kill_switch_modifier
        final_max_pct = base_max_pct * regime_modifier * kill_switch_modifier

        # Cap at 5% single position
        final_max_pct = min(final_max_pct, 5.0)

        # Check bucket capacity
        current_bucket_pct = self.current_allocations.get(bucket_id, 0.0)
        available_capacity = max(0, bucket_max_pct - current_bucket_pct)

        if available_capacity <= 0:
            return PositionSizeResult(
                ticker=ticker,
                conviction=conviction,
                bucket_name=bucket_name,
                bucket_id=bucket_id,
                min_amount_eur=0,
                max_amount_eur=0,
                min_pct=0,
                max_pct=0,
                regime=self.regime,
                regime_modifier=regime_modifier,
                kill_switch_modifier=kill_switch_modifier,
                bucket_available_capacity_pct=available_capacity,
                rationale=rationale,
                rejected=True,
                rejection_reason=f"Bucket {bucket_name} at capacity ({current_bucket_pct:.1f}% >= {bucket_max_pct}%)",
            )

        # Cap by available capacity
        if final_max_pct > available_capacity:
            rationale.append(f"Capped by bucket capacity: {available_capacity:.1f}% available")
            final_max_pct = available_capacity

        # Calculate EUR amounts
        min_amount = self.portfolio_value * final_min_pct / 100
        max_amount = self.portfolio_value * final_max_pct / 100

        rationale.append(f"Final sizing: EUR {min_amount:,.0f}-{max_amount:,.0f}")

        return PositionSizeResult(
            ticker=ticker,
            conviction=conviction,
            bucket_name=bucket_name,
            bucket_id=bucket_id,
            min_amount_eur=min_amount,
            max_amount_eur=max_amount,
            min_pct=final_min_pct,
            max_pct=final_max_pct,
            regime=self.regime,
            regime_modifier=regime_modifier,
            kill_switch_modifier=kill_switch_modifier,
            bucket_available_capacity_pct=available_capacity,
            rationale=rationale,
        )

    def calculate_pair_size(
        self,
        long_ticker: str,
        short_ticker: str,
        conviction: str,
        thesis: str = "",
        total_pair_exposure_pct: float = 0.0,
    ) -> PairSizeResult:
        """
        Calculate sizing for a long/short pair trade.

        The short leg is sized at 60% of the long leg.
        Total pair exposure is capped at 30% of portfolio.

        Args:
            long_ticker: Long leg ticker
            short_ticker: Short leg ticker
            conviction: Conviction level
            thesis: Pair thesis description
            total_pair_exposure_pct: Current total pair trade exposure %

        Returns:
            PairSizeResult with sizing for both legs
        """
        conviction = conviction.upper().strip()
        rationale = []

        # Get base sizing from conviction
        if conviction not in CONVICTION_SIZING:
            conviction = "MEDIUM"
        base_min_pct, base_max_pct = CONVICTION_SIZING[conviction]

        # Use midpoint for pairs
        base_pct = (base_min_pct + base_max_pct) / 2
        rationale.append(f"Base pair sizing for {conviction}: {base_pct:.1f}%")

        # Apply regime modifier
        regime_modifier = REGIME_MODIFIERS.get(self.regime, 1.0)
        if regime_modifier != 1.0:
            rationale.append(f"Regime {self.regime}: {regime_modifier:.0%} modifier")

        long_pct = base_pct * regime_modifier
        short_pct = long_pct * 0.6  # Short at 60% of long

        # Check total pair exposure cap (30%)
        new_total_exposure = total_pair_exposure_pct + long_pct + short_pct
        max_pair_exposure = 30.0

        if new_total_exposure > max_pair_exposure:
            available = max_pair_exposure - total_pair_exposure_pct
            if available <= 0:
                return PairSizeResult(
                    long_ticker=long_ticker,
                    short_ticker=short_ticker,
                    conviction=conviction,
                    thesis=thesis,
                    long_amount_eur=0,
                    long_pct=0,
                    short_amount_eur=0,
                    short_pct=0,
                    regime=self.regime,
                    rationale=rationale,
                    rejected=True,
                    rejection_reason=f"Pair exposure at capacity ({total_pair_exposure_pct:.1f}% >= {max_pair_exposure}%)",
                )
            # Scale down to fit
            scale = available / (long_pct + short_pct)
            long_pct *= scale
            short_pct *= scale
            rationale.append(f"Scaled down to fit pair exposure cap")

        # Calculate EUR amounts
        long_amount = self.portfolio_value * long_pct / 100
        short_amount = self.portfolio_value * short_pct / 100

        rationale.append(f"Long: EUR {long_amount:,.0f} ({long_pct:.1f}%)")
        rationale.append(f"Short: EUR {short_amount:,.0f} ({short_pct:.1f}%)")

        return PairSizeResult(
            long_ticker=long_ticker,
            short_ticker=short_ticker,
            conviction=conviction,
            thesis=thesis,
            long_amount_eur=long_amount,
            long_pct=long_pct,
            short_amount_eur=short_amount,
            short_pct=short_pct,
            regime=self.regime,
            rationale=rationale,
        )


def format_position_sizing_report(
    results: List[PositionSizeResult],
    portfolio_value_eur: float = 500_000.0,
    regime: str = "RISK_ON",
) -> str:
    """
    Format position sizing results for Telegram report.

    Args:
        results: List of PositionSizeResult
        portfolio_value_eur: Portfolio value for header
        regime: Current regime for context

    Returns:
        Formatted markdown string
    """
    lines = [
        f"**POSITION SIZING** (EUR {portfolio_value_eur:,.0f} portfolio)",
        f"Regime: {regime}",
        "",
    ]

    # Group by conviction
    high = [r for r in results if r.conviction == "HIGH" and not r.rejected]
    medium = [r for r in results if r.conviction == "MEDIUM" and not r.rejected]
    low = [r for r in results if r.conviction == "LOW" and not r.rejected]
    rejected = [r for r in results if r.rejected]

    if high:
        lines.append("**HIGH conviction signals:**")
        for r in high:
            lines.append(
                f"  {r.ticker}: EUR {r.min_amount_eur:,.0f}-{r.max_amount_eur:,.0f} "
                f"({r.min_pct:.1f}-{r.max_pct:.1f}%) - {r.bucket_name}"
            )
        lines.append("")

    if medium:
        lines.append("**MEDIUM conviction signals:**")
        for r in medium:
            lines.append(
                f"  {r.ticker}: EUR {r.min_amount_eur:,.0f}-{r.max_amount_eur:,.0f} "
                f"({r.min_pct:.1f}-{r.max_pct:.1f}%) - {r.bucket_name}"
            )
        lines.append("")

    if low:
        lines.append("**LOW conviction signals:**")
        for r in low:
            lines.append(
                f"  {r.ticker}: EUR {r.min_amount_eur:,.0f}-{r.max_amount_eur:,.0f} "
                f"({r.min_pct:.1f}-{r.max_pct:.1f}%)"
            )
        lines.append("")

    if rejected:
        lines.append("**Rejected (capacity/constraints):**")
        for r in rejected:
            lines.append(f"  {r.ticker}: {r.rejection_reason}")
        lines.append("")

    # Add regime note if not RISK_ON
    if regime != "RISK_ON":
        modifier = REGIME_MODIFIERS.get(regime, 1.0)
        lines.append(f"Note: Sizes reduced {(1-modifier)*100:.0f}% due to {regime} regime")

    return "\n".join(lines)
