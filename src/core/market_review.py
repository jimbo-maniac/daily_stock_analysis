# -*- coding: utf-8 -*-
"""
===================================
stockintelligentanalyzingsystem - market reviewmodule（support A stocks / US stock）
===================================

Responsibilities:
1. based on MARKET_REVIEW_REGION configurationselectmarketzonedomain（cn / us / both）
2. execute market reviewanalyzingandgeneratingreviewreport
3. savingandsendingreviewreport
"""

import logging
from datetime import datetime
from typing import Optional

from src.config import get_config
from src.notification import NotificationService
from src.market_analyzer import MarketAnalyzer
from src.search_service import SearchService
from src.analyzer import GeminiAnalyzer


logger = logging.getLogger(__name__)


def _build_kill_switch_section() -> tuple:
    """
    Build kill switch alerts section.

    Returns:
        (alerts_text, kill_switch_status_dict) where:
        - alerts_text: Formatted alerts (empty if none)
        - kill_switch_status_dict: Dict mapping thesis_id -> status for position sizing
    """
    alerts_text = ""
    status_dict = {}

    try:
        from src.services.kill_switch_monitor import (
            KillSwitchMonitor,
            format_kill_switch_alerts,
            format_kill_switch_summary,
        )
        monitor = KillSwitchMonitor()
        results = monitor.check_all()

        # Build status dict for position sizer
        for r in results:
            status_dict[r.thesis_id] = r.overall_status

        # Format alerts (only shown if any triggered or warning)
        alerts_text = format_kill_switch_alerts(results)

        # Also get summary for thesis health section
        summary_text = format_kill_switch_summary(results)
        return alerts_text, status_dict, summary_text

    except Exception as e:
        logger.warning(f"Kill switch monitor failed (skipping): {e}")
        return "", {}, ""


def _build_position_sizing_section(
    kill_switch_status: dict,
    regime: str = "RISK_ON",
) -> str:
    """
    Build position sizing section for HIGH/MEDIUM conviction signals.

    This is a placeholder that would integrate with actual analysis results.
    For now, returns empty string as sizing needs signal context.
    """
    # Position sizing is contextual - it needs actual signals from analysis
    # This section would be populated when integrating with individual stock analysis
    return ""


def _build_correlation_section(is_weekly: bool = False) -> str:
    """
    Build correlation/crowding risk section.

    Args:
        is_weekly: If True, show full cluster list. If False, only show alerts.

    Returns:
        Formatted correlation report (empty if no alerts in daily mode)
    """
    try:
        from src.services.correlation_monitor import (
            CorrelationMonitor,
            format_correlation_report,
        )
        monitor = CorrelationMonitor()
        report = monitor.analyze()
        return format_correlation_report(report, show_full=is_weekly)
    except Exception as e:
        logger.warning(f"Correlation monitor failed (skipping): {e}")
        return ""


def _build_global_extra_sections(is_weekly: bool = False) -> str:
    """
    Build all extra sections for global reports.

    Section order:
    1. Kill switch alerts (if any triggered - FIRST)
    2. Thesis health with kill switch status
    3. Pair tracker
    4. Correlation/crowding risk (if any alerts)

    Args:
        is_weekly: If True, show full correlation clusters

    Returns:
        Combined markdown sections
    """
    sections = []
    priority_sections = []  # Kill switch alerts go first

    # Kill switch monitoring (provides alerts + status for other modules)
    kill_switch_alerts, kill_switch_status, kill_switch_summary = _build_kill_switch_section()

    # Kill switch alerts go at the very top if any
    if kill_switch_alerts:
        priority_sections.append(kill_switch_alerts)

    # Thesis health (with kill switch status integrated)
    try:
        from src.services.thesis_health import ThesisHealthChecker, format_thesis_report
        checker = ThesisHealthChecker()
        theses = checker.check_all()
        if theses:
            sections.append(format_thesis_report(theses))
    except Exception as e:
        logger.warning(f"Thesis health check failed (skipping): {e}")

    # Add kill switch summary after thesis health if we have it
    if kill_switch_summary and not kill_switch_alerts:
        # Only show summary if no active alerts (alerts contain more detail)
        sections.append(kill_switch_summary)

    # Pair tracker
    try:
        from src.services.pair_tracker import PairTracker, format_pair_tracker_report
        tracker = PairTracker()
        pairs = tracker.analyze_all_pairs()
        if pairs:
            sections.append(format_pair_tracker_report(pairs))
    except Exception as e:
        logger.warning(f"Pair tracker failed (skipping): {e}")

    # Correlation/crowding risk
    corr_section = _build_correlation_section(is_weekly=is_weekly)
    if corr_section:
        sections.append(corr_section)

    # Combine: priority sections first (kill switch alerts), then regular sections
    all_sections = priority_sections + sections
    return "\n\n---\n\n".join(all_sections)


def run_market_review(
    notifier: NotificationService,
    analyzer: Optional[GeminiAnalyzer] = None,
    search_service: Optional[SearchService] = None,
    send_notification: bool = True,
    merge_notification: bool = False,
    override_region: Optional[str] = None,
) -> Optional[str]:
    """
    execute market reviewanalyzing

    Args:
        notifier: notificationservice
        analyzer: AIanalyzinghandler（optional）
        search_service: searchservice（optional）
        send_notification: whethersendingnotification
        merge_notification: whethermergingpush（skip thispush，by main layermergingindividual stock+unified after marketsending，Issue #190）
        override_region: override config  market_review_region（Issue #373 trading dayfilteringaftervalidsub-set）

    Returns:
        reviewreporttext
    """
    logger.info("Starting market review analysis...")
    config = get_config()
    region = (
        override_region
        if override_region is not None
        else (getattr(config, 'market_review_region', 'global') or 'global')
    )
    if region not in ('cn', 'us', 'eu', 'global', 'both'):
        region = 'global'

    try:
        if region == 'both':
            # Execute A-share + US stock reviews, merge report
            cn_analyzer = MarketAnalyzer(
                search_service=search_service, analyzer=analyzer, region='cn'
            )
            us_analyzer = MarketAnalyzer(
                search_service=search_service, analyzer=analyzer, region='us'
            )
            logger.info("Generating A-share market review...")
            cn_report = cn_analyzer.run_daily_review()
            logger.info("Generating US market review...")
            us_report = us_analyzer.run_daily_review()
            review_report = ''
            if cn_report:
                review_report = f"# A-share Market Review\n\n{cn_report}"
            if us_report:
                if review_report:
                    review_report += "\n\n---\n\n"
                review_report += f"# US Market Review\n\n{us_report}"
            if not review_report:
                review_report = None
        elif region in ('global', 'eu'):
            # Global macro review with pair tracker and thesis health
            market_analyzer = MarketAnalyzer(
                search_service=search_service,
                analyzer=analyzer,
                region=region,
            )
            review_report = market_analyzer.run_daily_review()

            # Check if this is a weekly run (Sunday) for full correlation report
            is_weekly = datetime.now().weekday() == 6  # Sunday

            # Append pair tracker, thesis health, kill switches, correlation sections
            extra_sections = _build_global_extra_sections(is_weekly=is_weekly)
            if extra_sections and review_report:
                review_report += "\n\n---\n\n" + extra_sections
            elif extra_sections:
                review_report = extra_sections
        else:
            market_analyzer = MarketAnalyzer(
                search_service=search_service,
                analyzer=analyzer,
                region=region,
            )
            review_report = market_analyzer.run_daily_review()
        
        if review_report:
            # savingreporttofile
            date_str = datetime.now().strftime('%Y%m%d')
            report_filename = f"market_review_{date_str}.md"
            filepath = notifier.save_report_to_file(
                f"# 🎯 market review\n\n{review_report}", 
                report_filename
            )
            logger.info(f"market reviewreportalreadysaving: {filepath}")
            
            # pushnotification（mergingmodebelowskip，by main layerunifiedsending）
            if merge_notification and send_notification:
                logger.info("mergingpushmode：skipmarket reviewseparatepush，willinindividual stock+market reviewafterunifiedsending")
            elif send_notification and notifier.is_available():
                # addtitle
                report_content = f"🎯 market review\n\n{review_report}"

                success = notifier.send(report_content, email_send_to_all=True)
                if success:
                    logger.info("market reviewpushsuccessful")
                else:
                    logger.warning("market reviewpushfailed")
            elif not send_notification:
                logger.info("skippedpushnotification (--no-notify)")
            
            return review_report
        
    except Exception as e:
        logger.error(f"market reviewanalyzingfailed: {e}")
    
    return None
