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


def _build_global_extra_sections() -> str:
    """Build pair tracker + thesis health sections for global reports."""
    sections = []

    # Pair tracker
    try:
        from src.services.pair_tracker import PairTracker, format_pair_tracker_report
        tracker = PairTracker()
        pairs = tracker.analyze_all_pairs()
        if pairs:
            sections.append(format_pair_tracker_report(pairs))
    except Exception as e:
        logger.warning(f"Pair tracker failed (skipping): {e}")

    # Thesis health
    try:
        from src.services.thesis_health import ThesisHealthChecker, format_thesis_report
        checker = ThesisHealthChecker()
        theses = checker.check_all()
        if theses:
            sections.append(format_thesis_report(theses))
    except Exception as e:
        logger.warning(f"Thesis health check failed (skipping): {e}")

    return "\n\n".join(sections)


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

            # Append pair tracker and thesis health sections
            extra_sections = _build_global_extra_sections()
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
