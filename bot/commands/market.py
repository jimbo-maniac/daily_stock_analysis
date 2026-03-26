# -*- coding: utf-8 -*-
"""
===================================
market reviewcommand
===================================

execute market reviewanalyzing，generatingmarketoverviewreport。
"""

import logging
import threading
from typing import List

from bot.commands.base import BotCommand
from bot.models import BotMessage, BotResponse

logger = logging.getLogger(__name__)


class MarketCommand(BotCommand):
    """
    market reviewcommand
    
    execute market reviewanalyzing，packagebracket：
    - mainindexperformance
    - sectorhotspot
    - market sentiment
    - aftermarket outlook
    
    usage：
        /market - execute market review
    """

    @property
    def name(self) -> str:
        return "market"

    @property
    def aliases(self) -> List[str]:
        return ["m", "market index", "review", "quote/market data"]

    @property
    def description(self) -> str:
        return "market reviewanalyzing"

    @property
    def usage(self) -> str:
        return "/market"

    def execute(self, message: BotMessage, args: List[str]) -> BotResponse:
        """execute market reviewcommand"""
        logger.info(f"[MarketCommand] startingmarket reviewanalyzing")

        # in backgroundthreadinexecutereview（avoidblocking）
        thread = threading.Thread(
            target=self._run_market_review,
            args=(message,),
            daemon=True
        )
        thread.start()

        return BotResponse.markdown_response(
            "✅ **market reviewtaskstarted**\n\n"
            "currentlyanalyzing：\n"
            "• mainindexperformance\n"
            "• sectorhotspotanalyzing\n"
            "• market sentimentdetermine\n"
            "• aftermarket outlook\n\n"
            "analyzingcompletedwill automatically afterpushresult。"
        )

    def _run_market_review(self, message: BotMessage) -> None:
        """afterplatformexecute market review"""
        try:
            from src.config import get_config
            from src.notification import NotificationService
            from src.market_analyzer import MarketAnalyzer
            from src.search_service import SearchService
            from src.analyzer import GeminiAnalyzer

            config = get_config()
            notifier = NotificationService(source_message=message)

            # initializingsearchservice
            search_service = None
            if config.has_search_capability_enabled():
                search_service = SearchService(
                    bocha_keys=config.bocha_api_keys,
                    tavily_keys=config.tavily_api_keys,
                    brave_keys=config.brave_api_keys,
                    serpapi_keys=config.serpapi_keys,
                    minimax_keys=config.minimax_api_keys,
                    searxng_base_urls=config.searxng_base_urls,
                    searxng_public_instances_enabled=config.searxng_public_instances_enabled,
                    news_max_age_days=config.news_max_age_days,
                )

            # initializing AI analyzinghandler
            analyzer = None
            if config.gemini_api_key or config.openai_api_key:
                analyzer = GeminiAnalyzer()

            # readingconfigurationinmarketzonedomain，withscheduled task/CLI keep consistent
            region = getattr(config, 'market_review_region', 'cn')

            # executereview
            market_analyzer = MarketAnalyzer(
                search_service=search_service,
                analyzer=analyzer,
                region=region,
            )

            review_report = market_analyzer.run_daily_review()

            if review_report:
                # pushresult
                report_content = f"🎯 **market review**\n\n{review_report}"
                notifier.send(report_content, email_send_to_all=True)
                logger.info("[MarketCommand] market reviewcompletedandalreadypush")
            else:
                logger.warning("[MarketCommand] market reviewreturn empty result")

        except Exception as e:
            logger.error(f"[MarketCommand] market reviewfailed: {e}")
            logger.exception(e)
