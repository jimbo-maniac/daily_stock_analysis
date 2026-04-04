# -*- coding: utf-8 -*-
"""
Correlation Monitor & Crowding Risk Detector

Analyzes 60-day rolling correlations across portfolio positions.
Flags hidden concentration risk when:
- 3+ positions have average pairwise correlation >0.7

Calculates effective portfolio concentration (not just position count).
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from itertools import combinations
from typing import Dict, List, Optional, Any, Tuple, Set

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# Default portfolio tickers to monitor
DEFAULT_PORTFOLIO_TICKERS: List[str] = [
    # Bucket 1: Hard Assets
    "BTC-USD", "PHYS", "NEM", "GOLD", "TIP", "CCJ",
    # Bucket 2: Energy/Nuclear
    "LNG", "FLNG", "EQNR", "CEG", "VST",
    # Bucket 3: Defense Supply Chain
    "ASML", "KGSY", "HENS.DE", "RENK.DE", "JDDE.DE", "CHG.L", "AMG.AS",
    # Bucket 4: Consumer Stress
    "TJX", "DLTR", "FCFS", "CXW", "FLOW.AS",
    # Bucket 5: Geopolitical
    "KSA", "UAE", "MP", "MELI", "RYAAY", "NVO",
]

# Bucket name mapping for reporting
BUCKET_NAMES: Dict[str, str] = {
    "BTC-USD": "Hard Assets", "PHYS": "Hard Assets", "NEM": "Hard Assets",
    "GOLD": "Hard Assets", "TIP": "Hard Assets", "CCJ": "Hard Assets",
    "LNG": "Energy/Nuclear", "FLNG": "Energy/Nuclear", "EQNR": "Energy/Nuclear",
    "CEG": "Energy/Nuclear", "VST": "Energy/Nuclear",
    "ASML": "Defense", "KGSY": "Defense", "HENS.DE": "Defense",
    "RENK.DE": "Defense", "JDDE.DE": "Defense", "CHG.L": "Defense", "AMG.AS": "Defense",
    "TJX": "Consumer", "DLTR": "Consumer", "FCFS": "Consumer",
    "CXW": "Consumer", "FLOW.AS": "Consumer",
    "KSA": "Geopolitical", "UAE": "Geopolitical", "MP": "Geopolitical",
    "MELI": "Geopolitical", "RYAAY": "Geopolitical", "NVO": "Geopolitical",
}


@dataclass
class CorrelationCluster:
    """A cluster of highly correlated positions."""
    tickers: List[str]
    avg_correlation: float
    max_correlation: float
    min_correlation: float
    bucket_breakdown: Dict[str, int]  # bucket -> count of tickers

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tickers": self.tickers,
            "avg_correlation": round(self.avg_correlation, 3),
            "max_correlation": round(self.max_correlation, 3),
            "bucket_breakdown": self.bucket_breakdown,
        }


@dataclass
class CorrelationAlert:
    """Alert for hidden concentration risk."""
    cluster: CorrelationCluster
    effective_exposure_pct: float  # What % the cluster acts like
    target_exposure_pct: float  # What % it should be
    severity: str  # "HIGH" / "MEDIUM"
    recommendation: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tickers": self.cluster.tickers,
            "avg_correlation": self.cluster.avg_correlation,
            "effective_exposure_pct": round(self.effective_exposure_pct, 1),
            "target_exposure_pct": round(self.target_exposure_pct, 1),
            "severity": self.severity,
            "recommendation": self.recommendation,
        }


@dataclass
class CorrelationReport:
    """Full correlation analysis report."""
    as_of_date: str
    tickers_analyzed: int
    tickers_with_data: int
    correlation_matrix: Optional[pd.DataFrame] = None
    clusters: List[CorrelationCluster] = field(default_factory=list)
    alerts: List[CorrelationAlert] = field(default_factory=list)
    effective_positions: float = 0.0  # Effective number of independent positions
    error: Optional[str] = None

    def has_alerts(self) -> bool:
        return len(self.alerts) > 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "as_of_date": self.as_of_date,
            "tickers_analyzed": self.tickers_analyzed,
            "tickers_with_data": self.tickers_with_data,
            "clusters": [c.to_dict() for c in self.clusters],
            "alerts": [a.to_dict() for a in self.alerts],
            "effective_positions": round(self.effective_positions, 1),
            "error": self.error,
        }


class CorrelationMonitor:
    """
    Monitors portfolio correlations and detects crowding risk.
    """

    def __init__(
        self,
        tickers: Optional[List[str]] = None,
        lookback_days: int = 60,
        correlation_threshold: float = 0.7,
        min_cluster_size: int = 3,
    ):
        """
        Initialize correlation monitor.

        Args:
            tickers: List of tickers to monitor (defaults to all portfolio tickers)
            lookback_days: Number of trading days for correlation calculation
            correlation_threshold: Correlation level to flag as concerning
            min_cluster_size: Minimum positions to form a cluster alert
        """
        self.tickers = tickers or DEFAULT_PORTFOLIO_TICKERS
        self.lookback_days = lookback_days
        self.correlation_threshold = correlation_threshold
        self.min_cluster_size = min_cluster_size

    def analyze(
        self,
        position_weights: Optional[Dict[str, float]] = None,
    ) -> CorrelationReport:
        """
        Run full correlation analysis.

        Args:
            position_weights: Optional dict mapping ticker -> weight % (for effective exposure calc)

        Returns:
            CorrelationReport with matrix, clusters, and alerts
        """
        report = CorrelationReport(
            as_of_date=datetime.now().strftime("%Y-%m-%d"),
            tickers_analyzed=len(self.tickers),
            tickers_with_data=0,
        )

        try:
            # Fetch returns data
            returns_df = self._fetch_returns()
            if returns_df is None or returns_df.empty:
                report.error = "Failed to fetch price data"
                return report

            report.tickers_with_data = len(returns_df.columns)

            # Calculate correlation matrix
            corr_matrix = returns_df.corr()
            report.correlation_matrix = corr_matrix

            # Find clusters of highly correlated positions
            clusters = self._find_clusters(corr_matrix)
            report.clusters = clusters

            # Generate alerts for concerning clusters
            alerts = self._generate_alerts(clusters, position_weights)
            report.alerts = alerts

            # Calculate effective portfolio concentration
            report.effective_positions = self._calculate_effective_positions(
                corr_matrix, position_weights
            )

            logger.info(
                f"[Correlation] Analyzed {report.tickers_with_data} tickers, "
                f"found {len(clusters)} clusters, {len(alerts)} alerts"
            )

        except Exception as e:
            logger.error(f"[Correlation] Analysis failed: {e}")
            report.error = str(e)

        return report

    def _fetch_returns(self) -> Optional[pd.DataFrame]:
        """Fetch daily returns for all tickers."""
        try:
            import yfinance as yf

            end_date = datetime.now()
            start_date = end_date - timedelta(days=self.lookback_days * 2)

            # Fetch all tickers at once
            data = yf.download(
                self.tickers,
                start=start_date.strftime("%Y-%m-%d"),
                end=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
                auto_adjust=True,
                progress=False,
            )

            if data.empty:
                logger.warning("[Correlation] No data returned from yfinance")
                return None

            # Handle single vs multiple tickers
            if isinstance(data.columns, pd.MultiIndex):
                prices = data["Close"]
            else:
                prices = data[["Close"]]
                prices.columns = self.tickers[:1]

            # Calculate returns
            returns = prices.pct_change().dropna()

            # Filter to lookback period
            returns = returns.tail(self.lookback_days)

            # Drop tickers with insufficient data
            returns = returns.dropna(axis=1, how="all")
            returns = returns.loc[:, returns.count() >= self.lookback_days * 0.8]

            return returns

        except Exception as e:
            logger.warning(f"[Correlation] Failed to fetch returns: {e}")
            return None

    def _find_clusters(
        self, corr_matrix: pd.DataFrame
    ) -> List[CorrelationCluster]:
        """
        Find clusters of highly correlated positions.

        Uses a simple approach: for each pair above threshold,
        expand to include all other tickers also correlated with both.
        """
        tickers = list(corr_matrix.columns)
        n = len(tickers)

        # Build adjacency list of highly correlated pairs
        high_corr_pairs: Set[Tuple[str, str]] = set()
        for i in range(n):
            for j in range(i + 1, n):
                if abs(corr_matrix.iloc[i, j]) >= self.correlation_threshold:
                    high_corr_pairs.add((tickers[i], tickers[j]))

        # Find connected components (clusters)
        clusters = []
        visited: Set[str] = set()

        def expand_cluster(seed: str) -> Set[str]:
            """BFS to find all connected tickers."""
            cluster = {seed}
            queue = [seed]
            while queue:
                current = queue.pop(0)
                for t1, t2 in high_corr_pairs:
                    neighbor = None
                    if t1 == current and t2 not in cluster:
                        neighbor = t2
                    elif t2 == current and t1 not in cluster:
                        neighbor = t1
                    if neighbor:
                        cluster.add(neighbor)
                        queue.append(neighbor)
            return cluster

        for ticker in tickers:
            if ticker not in visited:
                # Check if this ticker is in any high-corr pair
                in_pair = any(ticker in pair for pair in high_corr_pairs)
                if in_pair:
                    cluster_tickers = expand_cluster(ticker)
                    visited.update(cluster_tickers)

                    # Only report clusters >= min_cluster_size
                    if len(cluster_tickers) >= self.min_cluster_size:
                        cluster_list = sorted(cluster_tickers)
                        corrs = []
                        for t1, t2 in combinations(cluster_list, 2):
                            if t1 in corr_matrix.columns and t2 in corr_matrix.columns:
                                corrs.append(abs(corr_matrix.loc[t1, t2]))

                        if corrs:
                            bucket_breakdown = {}
                            for t in cluster_list:
                                bucket = BUCKET_NAMES.get(t, "Other")
                                bucket_breakdown[bucket] = bucket_breakdown.get(bucket, 0) + 1

                            clusters.append(CorrelationCluster(
                                tickers=cluster_list,
                                avg_correlation=float(np.mean(corrs)),
                                max_correlation=float(np.max(corrs)),
                                min_correlation=float(np.min(corrs)),
                                bucket_breakdown=bucket_breakdown,
                            ))

        return clusters

    def _generate_alerts(
        self,
        clusters: List[CorrelationCluster],
        position_weights: Optional[Dict[str, float]],
    ) -> List[CorrelationAlert]:
        """Generate alerts for concerning clusters."""
        alerts = []

        for cluster in clusters:
            # Calculate effective exposure if we have weights
            if position_weights:
                cluster_weight = sum(
                    position_weights.get(t, 0) for t in cluster.tickers
                )
                # With high correlation, these positions act as ~one position
                # Effective exposure = cluster_weight * (1 + avg_corr) / 2
                # At corr=1, effective = full weight; at corr=0, effective = weight/n
                effective_exposure = cluster_weight * (
                    1 + (cluster.avg_correlation * (len(cluster.tickers) - 1))
                ) / len(cluster.tickers)
            else:
                # Assume equal weight
                cluster_weight = len(cluster.tickers) * 5.0  # 5% each
                effective_exposure = cluster_weight

            # Determine target based on predominant bucket
            predominant_bucket = max(cluster.bucket_breakdown.items(), key=lambda x: x[1])[0]
            target_exposure = {
                "Hard Assets": 25.0,
                "Energy/Nuclear": 20.0,
                "Defense": 20.0,
                "Consumer": 15.0,
                "Geopolitical": 15.0,
            }.get(predominant_bucket, 20.0)

            # Determine severity
            if effective_exposure > target_exposure * 1.5:
                severity = "HIGH"
            elif effective_exposure > target_exposure * 1.2:
                severity = "MEDIUM"
            else:
                continue  # No alert needed

            # Generate recommendation
            excess = effective_exposure - target_exposure
            tickers_to_trim = cluster.tickers[-2:]  # Last 2 alphabetically
            recommendation = (
                f"Effective {predominant_bucket.lower()} exposure is {effective_exposure:.0f}%, "
                f"target is {target_exposure:.0f}%. "
                f"Consider trimming {' or '.join(tickers_to_trim)}."
            )

            alerts.append(CorrelationAlert(
                cluster=cluster,
                effective_exposure_pct=effective_exposure,
                target_exposure_pct=target_exposure,
                severity=severity,
                recommendation=recommendation,
            ))

        return alerts

    def _calculate_effective_positions(
        self,
        corr_matrix: pd.DataFrame,
        position_weights: Optional[Dict[str, float]],
    ) -> float:
        """
        Calculate effective number of independent positions.

        Uses the formula: n_eff = n / (1 + (n-1) * avg_abs_corr)
        This gives the equivalent number of uncorrelated positions.
        """
        n = len(corr_matrix.columns)
        if n < 2:
            return float(n)

        # Calculate average absolute correlation (excluding diagonal)
        corr_values = []
        for i in range(n):
            for j in range(i + 1, n):
                corr_values.append(abs(corr_matrix.iloc[i, j]))

        if not corr_values:
            return float(n)

        avg_abs_corr = np.mean(corr_values)

        # Effective positions formula
        n_eff = n / (1 + (n - 1) * avg_abs_corr)
        return float(n_eff)


def format_correlation_report(
    report: CorrelationReport,
    show_full: bool = False,
) -> str:
    """
    Format correlation report for Telegram.

    Args:
        report: CorrelationReport object
        show_full: If True, show all clusters. If False, only show alerts.

    Returns:
        Formatted markdown string (empty if no alerts and not show_full)
    """
    if report.error:
        return f"**CORRELATION ANALYSIS ERROR**: {report.error}"

    if not report.has_alerts() and not show_full:
        return ""  # No alerts, nothing to show in daily report

    lines = ["**CORRELATION RISK ANALYSIS**", ""]

    # Summary
    lines.append(
        f"Analyzed {report.tickers_with_data} positions | "
        f"Effective positions: {report.effective_positions:.1f}"
    )
    lines.append("")

    # Alerts (always show)
    if report.alerts:
        lines.append("**Hidden concentration detected:**")
        for alert in report.alerts:
            severity_emoji = "!!" if alert.severity == "HIGH" else "!"
            lines.append(
                f"  [{severity_emoji}] {' + '.join(alert.cluster.tickers)}: "
                f"avg corr {alert.cluster.avg_correlation:.2f}"
            )
            lines.append(f"      -> {alert.recommendation}")
        lines.append("")

    # Full cluster list (weekly report only)
    if show_full and report.clusters:
        lines.append("**All correlation clusters (>0.7):**")
        for cluster in report.clusters:
            buckets = ", ".join(f"{k}:{v}" for k, v in cluster.bucket_breakdown.items())
            lines.append(
                f"  {' + '.join(cluster.tickers)}: "
                f"avg={cluster.avg_correlation:.2f} [{buckets}]"
            )
        lines.append("")

    return "\n".join(lines)
