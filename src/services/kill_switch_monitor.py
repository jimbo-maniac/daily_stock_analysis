# -*- coding: utf-8 -*-
"""
Thesis Kill Switch Monitor

Checks specific numeric conditions that would invalidate each macro thesis.
Unlike thesis_health.py which uses relative performance scores, this module
checks hard thresholds (e.g., "Brent >$115 for 3 consecutive days").

States:
- ACTIVE: No kill switch triggered, thesis intact
- WARNING: One condition approaching threshold (within 20% of trigger)
- TRIGGERED: Kill switch fired, thesis declared dead

State is persisted to JSON to detect when a switch NEWLY triggers.
"""

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple

import numpy as np

logger = logging.getLogger(__name__)

# State file path (relative to project root)
_STATE_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "kill_switch_state.json",
)


@dataclass
class KillSwitchCondition:
    """A single kill switch condition definition."""
    id: str
    description: str
    check_fn: str  # Function name to call for checking
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class KillSwitchResult:
    """Result of checking a single kill switch condition."""
    condition_id: str
    description: str
    status: str  # "ACTIVE" / "WARNING" / "TRIGGERED"
    current_value: Optional[float] = None
    threshold: Optional[float] = None
    detail: str = ""
    newly_triggered: bool = False  # True if status changed to TRIGGERED this run


@dataclass
class ThesisKillSwitchStatus:
    """Aggregated kill switch status for a single thesis."""
    thesis_id: str
    thesis_name: str
    horizon: str
    overall_status: str  # "ACTIVE" / "WARNING" / "TRIGGERED"
    conditions: List[KillSwitchResult] = field(default_factory=list)
    affected_positions: List[str] = field(default_factory=list)
    newly_triggered: bool = False  # True if any condition newly triggered

    def to_dict(self) -> Dict[str, Any]:
        return {
            "thesis_id": self.thesis_id,
            "thesis_name": self.thesis_name,
            "horizon": self.horizon,
            "overall_status": self.overall_status,
            "newly_triggered": self.newly_triggered,
            "conditions": [
                {
                    "id": c.condition_id,
                    "description": c.description,
                    "status": c.status,
                    "current_value": c.current_value,
                    "threshold": c.threshold,
                    "detail": c.detail,
                    "newly_triggered": c.newly_triggered,
                }
                for c in self.conditions
            ],
            "affected_positions": self.affected_positions,
        }


# === Kill Switch Definitions ===

THESIS_KILL_SWITCHES: Dict[str, Dict[str, Any]] = {
    "IRAN_SETTLEMENT": {
        "thesis_name": "Iran Settlement",
        "horizon": "4-8 weeks",
        "affected_positions": ["RYAAY", "LNG", "KSA", "UAE", "FLNG"],
        "conditions": [
            {
                "id": "brent_115_3d",
                "description": "Brent crude >$115 for 3 consecutive trading days",
                "check_type": "consecutive_above",
                "ticker": "BZ=F",
                "threshold": 115.0,
                "consecutive_days": 3,
            },
            {
                "id": "vix_40_spike",
                "description": "VIX spikes >40 on Middle East news",
                "check_type": "threshold_above",
                "ticker": "^VIX",
                "threshold": 40.0,
            },
            {
                "id": "lng_xom_underperform",
                "description": "LNG underperforms XOM by >10% over 5 trading days",
                "check_type": "relative_underperform",
                "long_ticker": "LNG",
                "short_ticker": "XOM",
                "threshold_pct": -10.0,
                "window_days": 5,
            },
        ],
    },
    "STAGFLATION": {
        "thesis_name": "Stagflation / Middle Class Erosion",
        "horizon": "12-24 months",
        "affected_positions": ["TJX", "DLTR", "FCFS", "CXW", "TIP", "PHYS", "NEM", "GOLD", "EQNR", "CEG", "VST"],
        "conditions": [
            {
                "id": "cpi_below_2_5_2m",
                "description": "Core CPI prints below 2.5% for two consecutive months",
                "check_type": "macro_indicator",
                "indicator": "CPIAUCSL",  # Would need FRED API
                "threshold": 2.5,
                "note": "Requires manual confirmation or FRED API",
            },
            {
                "id": "10y_below_3_5",
                "description": "10-year Treasury yield drops below 3.5%",
                "check_type": "threshold_below",
                "ticker": "^TNX",
                "threshold": 3.5,
            },
            {
                "id": "consumer_confidence_100",
                "description": "Consumer confidence index rebounds above 100",
                "check_type": "macro_indicator",
                "indicator": "UMCSENT",  # Would need FRED API
                "threshold": 100.0,
                "note": "Requires manual confirmation or FRED API",
            },
        ],
    },
    "DALIO_STAGE6": {
        "thesis_name": "Dalio Stage 6 World Disorder",
        "horizon": "10-20 years",
        "affected_positions": [
            "ASML", "KGSY", "HENS.DE", "RENK.DE", "JDDE.DE", "CHG.L", "AMG.AS",
            "BTC-USD", "PHYS", "NEM", "MP", "MELI",
        ],
        "conditions": [
            {
                "id": "peace_framework",
                "description": "Credible global peace framework announced",
                "check_type": "manual",
                "note": "Requires manual assessment based on news",
            },
            {
                "id": "dxy_115_1m",
                "description": "USD DXY strengthens above 115 for 1 month",
                "check_type": "consecutive_above",
                "ticker": "DX-Y.NYB",
                "threshold": 115.0,
                "consecutive_days": 20,  # ~1 month of trading days
            },
            {
                "id": "gold_20pct_drop",
                "description": "Gold drops >20% from peak",
                "check_type": "drawdown_from_peak",
                "ticker": "GC=F",
                "threshold_pct": 20.0,
                "lookback_days": 252,  # 1 year
            },
        ],
    },
}


class KillSwitchMonitor:
    """
    Monitors kill switch conditions for all macro theses.

    Fetches market data and checks specific numeric thresholds.
    Persists state to detect when switches newly trigger.
    """

    def __init__(self, lookback_days: int = 60):
        self.lookback_days = lookback_days
        self._previous_state = self._load_state()

    def check_all(self) -> List[ThesisKillSwitchStatus]:
        """Check all thesis kill switches. Returns list of status objects."""
        results = []

        for thesis_id, config in THESIS_KILL_SWITCHES.items():
            try:
                status = self._check_thesis(thesis_id, config)
                results.append(status)
            except Exception as e:
                logger.error(f"[KillSwitch] Failed to check {thesis_id}: {e}")
                results.append(ThesisKillSwitchStatus(
                    thesis_id=thesis_id,
                    thesis_name=config["thesis_name"],
                    horizon=config["horizon"],
                    overall_status="ERROR",
                    affected_positions=config["affected_positions"],
                ))

        # Save state for next run
        self._save_state(results)

        return results

    def _check_thesis(
        self, thesis_id: str, config: Dict[str, Any]
    ) -> ThesisKillSwitchStatus:
        """Check all conditions for a single thesis."""
        condition_results = []
        any_triggered = False
        any_warning = False
        any_newly_triggered = False

        for cond in config["conditions"]:
            result = self._check_condition(thesis_id, cond)
            condition_results.append(result)

            if result.status == "TRIGGERED":
                any_triggered = True
                if result.newly_triggered:
                    any_newly_triggered = True
            elif result.status == "WARNING":
                any_warning = True

        # Determine overall status
        if any_triggered:
            overall = "TRIGGERED"
        elif any_warning:
            overall = "WARNING"
        else:
            overall = "ACTIVE"

        return ThesisKillSwitchStatus(
            thesis_id=thesis_id,
            thesis_name=config["thesis_name"],
            horizon=config["horizon"],
            overall_status=overall,
            conditions=condition_results,
            affected_positions=config["affected_positions"],
            newly_triggered=any_newly_triggered,
        )

    def _check_condition(
        self, thesis_id: str, cond: Dict[str, Any]
    ) -> KillSwitchResult:
        """Check a single kill switch condition."""
        cond_id = cond["id"]
        check_type = cond.get("check_type", "manual")
        description = cond["description"]

        # Check if this was already triggered in previous state
        was_triggered = self._was_previously_triggered(thesis_id, cond_id)

        try:
            if check_type == "threshold_above":
                return self._check_threshold_above(cond, was_triggered)
            elif check_type == "threshold_below":
                return self._check_threshold_below(cond, was_triggered)
            elif check_type == "consecutive_above":
                return self._check_consecutive_above(cond, was_triggered)
            elif check_type == "relative_underperform":
                return self._check_relative_underperform(cond, was_triggered)
            elif check_type == "drawdown_from_peak":
                return self._check_drawdown_from_peak(cond, was_triggered)
            elif check_type in ("manual", "macro_indicator"):
                # Manual checks cannot be automated
                return KillSwitchResult(
                    condition_id=cond_id,
                    description=description,
                    status="ACTIVE",
                    detail=cond.get("note", "Requires manual verification"),
                )
            else:
                return KillSwitchResult(
                    condition_id=cond_id,
                    description=description,
                    status="ACTIVE",
                    detail=f"Unknown check type: {check_type}",
                )
        except Exception as e:
            logger.warning(f"[KillSwitch] Error checking {cond_id}: {e}")
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="ACTIVE",
                detail=f"Check failed: {e}",
            )

    def _check_threshold_above(
        self, cond: Dict[str, Any], was_triggered: bool
    ) -> KillSwitchResult:
        """Check if current price is above threshold."""
        ticker = cond["ticker"]
        threshold = cond["threshold"]
        cond_id = cond["id"]
        description = cond["description"]

        prices = self._get_prices(ticker, days=5)
        if prices is None or len(prices) == 0:
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="ACTIVE",
                detail="No price data available",
            )

        current = prices[-1]
        warning_threshold = threshold * 0.8  # 20% buffer for warning

        if current > threshold:
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="TRIGGERED",
                current_value=current,
                threshold=threshold,
                detail=f"{ticker} at {current:.2f} > {threshold}",
                newly_triggered=not was_triggered,
            )
        elif current > warning_threshold:
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="WARNING",
                current_value=current,
                threshold=threshold,
                detail=f"{ticker} at {current:.2f}, approaching {threshold}",
            )
        else:
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="ACTIVE",
                current_value=current,
                threshold=threshold,
                detail=f"{ticker} at {current:.2f}, safe below {threshold}",
            )

    def _check_threshold_below(
        self, cond: Dict[str, Any], was_triggered: bool
    ) -> KillSwitchResult:
        """Check if current price is below threshold."""
        ticker = cond["ticker"]
        threshold = cond["threshold"]
        cond_id = cond["id"]
        description = cond["description"]

        prices = self._get_prices(ticker, days=5)
        if prices is None or len(prices) == 0:
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="ACTIVE",
                detail="No price data available",
            )

        current = prices[-1]
        warning_threshold = threshold * 1.2  # 20% buffer for warning

        if current < threshold:
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="TRIGGERED",
                current_value=current,
                threshold=threshold,
                detail=f"{ticker} at {current:.2f} < {threshold}",
                newly_triggered=not was_triggered,
            )
        elif current < warning_threshold:
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="WARNING",
                current_value=current,
                threshold=threshold,
                detail=f"{ticker} at {current:.2f}, approaching {threshold}",
            )
        else:
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="ACTIVE",
                current_value=current,
                threshold=threshold,
                detail=f"{ticker} at {current:.2f}, safe above {threshold}",
            )

    def _check_consecutive_above(
        self, cond: Dict[str, Any], was_triggered: bool
    ) -> KillSwitchResult:
        """Check if price has been above threshold for N consecutive days."""
        ticker = cond["ticker"]
        threshold = cond["threshold"]
        consecutive_days = cond["consecutive_days"]
        cond_id = cond["id"]
        description = cond["description"]

        prices = self._get_prices(ticker, days=consecutive_days + 10)
        if prices is None or len(prices) < consecutive_days:
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="ACTIVE",
                detail=f"Insufficient data (need {consecutive_days} days)",
            )

        # Check last N days
        recent = prices[-consecutive_days:]
        days_above = sum(1 for p in recent if p > threshold)
        all_above = all(p > threshold for p in recent)

        if all_above:
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="TRIGGERED",
                current_value=recent[-1],
                threshold=threshold,
                detail=f"{ticker} above {threshold} for {consecutive_days} consecutive days",
                newly_triggered=not was_triggered,
            )
        elif days_above >= consecutive_days * 0.6:  # 60% = warning
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="WARNING",
                current_value=recent[-1],
                threshold=threshold,
                detail=f"{ticker} above {threshold} for {days_above}/{consecutive_days} days",
            )
        else:
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="ACTIVE",
                current_value=recent[-1],
                threshold=threshold,
                detail=f"{ticker} above {threshold} for only {days_above}/{consecutive_days} days",
            )

    def _check_relative_underperform(
        self, cond: Dict[str, Any], was_triggered: bool
    ) -> KillSwitchResult:
        """Check if long ticker underperforms short ticker by threshold %."""
        long_ticker = cond["long_ticker"]
        short_ticker = cond["short_ticker"]
        threshold_pct = cond["threshold_pct"]  # Negative = underperform
        window_days = cond["window_days"]
        cond_id = cond["id"]
        description = cond["description"]

        long_prices = self._get_prices(long_ticker, days=window_days + 5)
        short_prices = self._get_prices(short_ticker, days=window_days + 5)

        if (
            long_prices is None
            or short_prices is None
            or len(long_prices) < window_days
            or len(short_prices) < window_days
        ):
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="ACTIVE",
                detail="Insufficient data for relative comparison",
            )

        # Calculate relative performance over window
        long_return = (long_prices[-1] / long_prices[-window_days] - 1) * 100
        short_return = (short_prices[-1] / short_prices[-window_days] - 1) * 100
        relative_perf = long_return - short_return

        if relative_perf < threshold_pct:  # threshold is negative
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="TRIGGERED",
                current_value=relative_perf,
                threshold=threshold_pct,
                detail=f"{long_ticker} vs {short_ticker}: {relative_perf:+.1f}% (threshold: {threshold_pct}%)",
                newly_triggered=not was_triggered,
            )
        elif relative_perf < threshold_pct * 0.8:  # Within 20% of trigger
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="WARNING",
                current_value=relative_perf,
                threshold=threshold_pct,
                detail=f"{long_ticker} vs {short_ticker}: {relative_perf:+.1f}%, approaching {threshold_pct}%",
            )
        else:
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="ACTIVE",
                current_value=relative_perf,
                threshold=threshold_pct,
                detail=f"{long_ticker} vs {short_ticker}: {relative_perf:+.1f}% (safe)",
            )

    def _check_drawdown_from_peak(
        self, cond: Dict[str, Any], was_triggered: bool
    ) -> KillSwitchResult:
        """Check if price has dropped X% from peak within lookback period."""
        ticker = cond["ticker"]
        threshold_pct = cond["threshold_pct"]
        lookback_days = cond.get("lookback_days", 252)
        cond_id = cond["id"]
        description = cond["description"]

        prices = self._get_prices(ticker, days=lookback_days)
        if prices is None or len(prices) < 20:
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="ACTIVE",
                detail="Insufficient data for drawdown calculation",
            )

        peak = max(prices)
        current = prices[-1]
        drawdown_pct = (peak - current) / peak * 100

        if drawdown_pct > threshold_pct:
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="TRIGGERED",
                current_value=drawdown_pct,
                threshold=threshold_pct,
                detail=f"{ticker} down {drawdown_pct:.1f}% from peak {peak:.2f} (current: {current:.2f})",
                newly_triggered=not was_triggered,
            )
        elif drawdown_pct > threshold_pct * 0.8:  # Within 20% of trigger
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="WARNING",
                current_value=drawdown_pct,
                threshold=threshold_pct,
                detail=f"{ticker} down {drawdown_pct:.1f}% from peak, approaching {threshold_pct}%",
            )
        else:
            return KillSwitchResult(
                condition_id=cond_id,
                description=description,
                status="ACTIVE",
                current_value=drawdown_pct,
                threshold=threshold_pct,
                detail=f"{ticker} down {drawdown_pct:.1f}% from peak (safe)",
            )

    def _get_prices(self, ticker: str, days: int) -> Optional[List[float]]:
        """Fetch closing prices for ticker. Returns list of floats or None."""
        try:
            import yfinance as yf
            from datetime import datetime, timedelta

            end_date = datetime.now()
            start_date = end_date - timedelta(days=days * 2)  # Buffer for non-trading days

            t = yf.Ticker(ticker)
            hist = t.history(
                start=start_date.strftime("%Y-%m-%d"),
                end=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
                auto_adjust=True,
            )

            if hist is None or hist.empty:
                logger.warning(f"[KillSwitch] No yfinance data for {ticker}")
                return None

            prices = hist["Close"].dropna().tolist()
            return prices[-days:] if len(prices) >= days else prices

        except Exception as e:
            logger.warning(f"[KillSwitch] Failed to fetch {ticker}: {e}")
            return None

    def _was_previously_triggered(self, thesis_id: str, condition_id: str) -> bool:
        """Check if this condition was already triggered in previous state."""
        if not self._previous_state:
            return False
        thesis_state = self._previous_state.get(thesis_id, {})
        conditions = thesis_state.get("conditions", {})
        return conditions.get(condition_id, {}).get("status") == "TRIGGERED"

    def _load_state(self) -> Dict[str, Any]:
        """Load previous state from JSON file."""
        try:
            if os.path.exists(_STATE_FILE):
                with open(_STATE_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"[KillSwitch] Failed to load state: {e}")
        return {}

    def _save_state(self, results: List[ThesisKillSwitchStatus]) -> None:
        """Save current state to JSON file."""
        try:
            os.makedirs(os.path.dirname(_STATE_FILE), exist_ok=True)
            state = {}
            for thesis in results:
                state[thesis.thesis_id] = {
                    "overall_status": thesis.overall_status,
                    "last_checked": datetime.now().isoformat(),
                    "conditions": {
                        c.condition_id: {
                            "status": c.status,
                            "current_value": c.current_value,
                            "threshold": c.threshold,
                        }
                        for c in thesis.conditions
                    },
                }
            with open(_STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
            logger.info(f"[KillSwitch] State saved to {_STATE_FILE}")
        except Exception as e:
            logger.warning(f"[KillSwitch] Failed to save state: {e}")


def format_kill_switch_alerts(results: List[ThesisKillSwitchStatus]) -> str:
    """
    Format kill switch results for Telegram report.

    Returns alert section text (empty string if no alerts).
    Only shows TRIGGERED or newly WARNING conditions.
    """
    lines = []
    has_triggered = any(r.overall_status == "TRIGGERED" for r in results)
    has_warning = any(r.overall_status == "WARNING" for r in results)

    if not has_triggered and not has_warning:
        return ""

    for thesis in results:
        if thesis.overall_status == "TRIGGERED":
            for cond in thesis.conditions:
                if cond.status == "TRIGGERED":
                    newly = " (NEW)" if cond.newly_triggered else ""
                    lines.append(f"**KILL SWITCH TRIGGERED{newly}: {thesis.thesis_name}**")
                    lines.append(f"Condition: {cond.description}")
                    lines.append(f"Detail: {cond.detail}")
                    lines.append(f"Action required: Review all positions in affected bucket immediately.")
                    lines.append(f"Affected: {', '.join(thesis.affected_positions)}")
                    lines.append("")
        elif thesis.overall_status == "WARNING":
            for cond in thesis.conditions:
                if cond.status == "WARNING":
                    lines.append(f"**KILL SWITCH WARNING: {thesis.thesis_name}**")
                    lines.append(f"Condition: {cond.description}")
                    lines.append(f"Detail: {cond.detail}")
                    lines.append("")

    return "\n".join(lines)


def format_kill_switch_summary(results: List[ThesisKillSwitchStatus]) -> str:
    """Format a brief summary of all thesis kill switch statuses."""
    lines = ["**THESIS KILL SWITCH STATUS**", ""]

    for thesis in results:
        emoji = {
            "ACTIVE": "+",
            "WARNING": "!",
            "TRIGGERED": "X",
            "ERROR": "?",
        }.get(thesis.overall_status, "?")

        lines.append(f"[{emoji}] **{thesis.thesis_name}** ({thesis.horizon}): {thesis.overall_status}")

        # Show any non-ACTIVE conditions
        for cond in thesis.conditions:
            if cond.status != "ACTIVE":
                lines.append(f"    - {cond.description}: {cond.status}")
                if cond.detail:
                    lines.append(f"      {cond.detail}")

    return "\n".join(lines)
