# -*- coding: utf-8 -*-
"""
===================================
A-share Stock Intelligent Analysis System - coreanalyzingpipeline
===================================

Responsibilities:
1. managefullcountanalyzingprocess
2. coordinatedataget、storage、search、analyzing、notificationetcmodule
3. implementconcurrencycontrolandabnormalprocessing
4. providestockanalyzingcorefeature
"""

import logging
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd

from src.config import get_config, Config
from src.storage import get_db
from data_provider import DataFetcherManager
from data_provider.realtime_types import ChipDistribution
from src.analyzer import GeminiAnalyzer, AnalysisResult, fill_chip_structure_if_needed, fill_price_position_if_needed
from src.data.stock_mapping import STOCK_NAME_MAP
from src.notification import NotificationService, NotificationChannel
from src.report_language import (
    get_unknown_text,
    localize_confidence_level,
    normalize_report_language,
)
from src.search_service import SearchService
from src.services.social_sentiment_service import SocialSentimentService
from src.enums import ReportType
from src.stock_analyzer import StockTrendAnalyzer, TrendAnalysisResult
from src.core.trading_calendar import get_market_for_stock, is_market_open
from data_provider.us_index_mapping import is_us_stock_code
from bot.models import BotMessage
from data_provider.fmp_client import FMPClient
from src.services.apify_reddit_client import ApifyRedditClient


logger = logging.getLogger(__name__)


class StockAnalysisPipeline:
    """
    stockanalyzingmainprocessscheduler
    
    Responsibilities:
    1. managefullcountanalyzingprocess
    2. coordinatedataget、storage、search、analyzing、notificationetcmodule
    3. implementconcurrencycontrolandabnormalprocessing
    """
    
    def __init__(
        self,
        config: Optional[Config] = None,
        max_workers: Optional[int] = None,
        source_message: Optional[BotMessage] = None,
        query_id: Optional[str] = None,
        query_source: Optional[str] = None,
        save_context_snapshot: Optional[bool] = None
    ):
        """
        initializingscheduler
        
        Args:
            config: configurationobject（optional，defaultuseglobalconfiguration）
            max_workers: maxconcurrencythread count（optional，defaultfromconfigurationreading）
        """
        self.config = config or get_config()
        self.max_workers = max_workers or self.config.max_workers
        self.source_message = source_message
        self.query_id = query_id
        self.query_source = self._resolve_query_source(query_source)
        self.save_context_snapshot = (
            self.config.save_context_snapshot if save_context_snapshot is None else save_context_snapshot
        )
        
        # initializingeachmodule
        self.db = get_db()
        self.fetcher_manager = DataFetcherManager()
        # notagainseparatecreating akshare_fetcher，unifieduse fetcher_manager get enhanceddata
        self.trend_analyzer = StockTrendAnalyzer()  # trend analysishandler
        self.analyzer = GeminiAnalyzer()
        self.notifier = NotificationService(source_message=source_message)
        
        # initializingsearchservice
        self.search_service = SearchService(
            bocha_keys=self.config.bocha_api_keys,
            tavily_keys=self.config.tavily_api_keys,
            brave_keys=self.config.brave_api_keys,
            serpapi_keys=self.config.serpapi_keys,
            minimax_keys=self.config.minimax_api_keys,
            searxng_base_urls=self.config.searxng_base_urls,
            searxng_public_instances_enabled=self.config.searxng_public_instances_enabled,
            news_max_age_days=self.config.news_max_age_days,
            news_strategy_profile=getattr(self.config, "news_strategy_profile", "short"),
        )
        
        logger.info(f"schedulerinitializingcompleted，maxconcurrencycount: {self.max_workers}")
        logger.info("alreadyenabledtrend analysishandler (MA5>MA10>MA20 long positiondetermine)")
        # printrealtimequote/market data/chipconfigurationstatus
        if self.config.enable_realtime_quote:
            logger.info(f"realtimequote/market dataalreadyenabled (priority: {self.config.realtime_source_priority})")
        else:
            logger.info("realtimequote/market dataalreadydisabled，willusehistoricalclosing price")
        if self.config.enable_chip_distribution:
            logger.info("chip distributionanalyzingalreadyenabled")
        else:
            logger.info("chip distributionanalyzingalreadydisabled")
        if self.search_service.is_available:
            logger.info("searchservicealreadyenabled")
        else:
            logger.warning("searchservicenotenabled（notconfigurationsearchcapability）")

        # FMP client (optional) — None when FMP_API_KEY is not set
        fmp_key = getattr(self.config, "fmp_api_key", None)
        self.fmp_client: Optional[FMPClient] = FMPClient(fmp_key) if fmp_key else None
        if self.fmp_client:
            logger.info("FMP fundamentals client enabled")

        # Apify Reddit client (optional) — None when APIFY_API_KEY is not set
        apify_key = getattr(self.config, "apify_api_key", None)
        self.apify_reddit_client: Optional[ApifyRedditClient] = (
            ApifyRedditClient(apify_key) if apify_key else None
        )
        if self.apify_reddit_client:
            logger.info("Apify Reddit sentiment client enabled")

        # initializingcommunityexchangepublic sentimentservice（onlyUS stock）
        self.social_sentiment_service = SocialSentimentService(
            api_key=self.config.social_sentiment_api_key,
            api_url=self.config.social_sentiment_api_url,
        )
        if self.social_sentiment_service.is_available:
            logger.info("Social sentiment service enabled (Reddit/X/Polymarket, US stocks only)")

    def fetch_and_save_stock_data(
        self, 
        code: str,
        force_refresh: bool = False
    ) -> Tuple[bool, Optional[str]]:
        """
        getandsavingsinglestockdata
        
        resumable transferlogic：
        1. checkdatabasewhetherexistingtodaydata
        2. if existsandnotmandatoryrefresh，thenskipnetworkrequest
        3. otherwisefromdatasource fetchandsaving
        
        Args:
            code: stock code
            force_refresh: whethermandatoryrefresh（ignorelocalcache）
            
        Returns:
            Tuple[whethersuccessful, error message]
        """
        stock_name = code
        try:
            # firstfirstgetstockname
            stock_name = self.fetcher_manager.get_stock_name(code)

            today = date.today()
            # Note：hereusenaturalday date.today() do“resumable transfer”determine。
            # ifinweekend/holidayday/non-trading dayrunning，ormachinehandlerwhenzonenotinChina，possiblyappear：
            # - databaseexistinglatesttrading daydatabutstillwillduplicatepull（has_today_data return False）
            # - orincrossday/whenzonebiasedmovewhenmisjudge“todayexistingdata”
            # this behavior is currently preserved（logic not changed per requirements），bute.g.needmorerigorouscanmodifyas“latesttrading day/datasourcelatestdate”determine。
            
            # resumable transfercheck：iftodaydataalready exists，skip
            if not force_refresh and self.db.has_today_data(code, today):
                logger.info(f"{stock_name}({code}) todaydataalready exists，skipget（resumable transfer）")
                return True, None

            # fromdatasource fetchdata
            logger.info(f"{stock_name}({code}) startingfromdatasource fetchdata...")
            df, source_name = self.fetcher_manager.get_daily_data(code, days=30)

            if df is None or df.empty:
                return False, "getdata is empty"

            # savingtodatabase
            saved_count = self.db.save_daily_data(df, code, source_name)
            logger.info(f"{stock_name}({code}) datasave successful（source: {source_name}，add new {saved_count} items）")

            return True, None

        except Exception as e:
            error_msg = f"get/savingdatafailed: {str(e)}"
            logger.error(f"{stock_name}({code}) {error_msg}")
            return False, error_msg
    
    def analyze_stock(self, code: str, report_type: ReportType, query_id: str) -> Optional[AnalysisResult]:
        """
        analyzingsinglestock（enhancedversion：includevolume ratio、turnover rate、chipanalyzing、multi-dimensional intelligence）
        
        process：
        1. get realtimequote/market data（volume ratio、turnover rate）- via DataFetcherManager automatic failover
        2. get chip distribution - via DataFetcherManager withcircuit breakprotect
        3. proceedtrend analysis（based ontrading philosophy）
        4. multi-dimensional intelligencesearch（latestmessage+risktroubleshoot+performanceexpected）
        5. fromdatabasegetanalyzingcontext
        6. call AI proceedcompositeanalyzing
        
        Args:
            query_id: queryinglinkassociation id
            code: stock code
            report_type: report type
            
        Returns:
            AnalysisResult or None（ifanalyzingfailed）
        """
        try:
            # getstockname（priorityfromrealtimequote/market dataget realname）
            stock_name = self.fetcher_manager.get_stock_name(code)

            # Step 1: get realtimequote/market data（volume ratio、turnover rateetc）- useunifiedentry，automatic failover
            realtime_quote = None
            try:
                realtime_quote = self.fetcher_manager.get_realtime_quote(code)
                if realtime_quote:
                    # userealtimequote/market datareturnedrealstockname
                    if realtime_quote.name:
                        stock_name = realtime_quote.name
                    # compatible with differentdatasource'sfield（hassomedatasourcepossiblynohas volume_ratio）
                    volume_ratio = getattr(realtime_quote, 'volume_ratio', None)
                    turnover_rate = getattr(realtime_quote, 'turnover_rate', None)
                    logger.info(f"{stock_name}({code}) realtimequote/market data: price={realtime_quote.price}, "
                              f"volume ratio={volume_ratio}, turnover rate={turnover_rate}% "
                              f"(source: {realtime_quote.source.value if hasattr(realtime_quote, 'source') else 'unknown'})")
                else:
                    logger.info(f"{stock_name}({code}) realtimequote/market datafetch failedoralreadydisabled，willusehistoricaldataproceedanalyzing")
            except Exception as e:
                logger.warning(f"{stock_name}({code}) get realtimequote/market datafailed: {e}")

            # ifstillisnohasname，usecodeact asasname
            if not stock_name:
                stock_name = f'stock{code}'

            # Step 2: get chip distribution - useunifiedentry，withcircuit breakprotect
            chip_data = None
            try:
                chip_data = self.fetcher_manager.get_chip_distribution(code)
                if chip_data:
                    logger.info(f"{stock_name}({code}) chip distribution: profitproportion={chip_data.profit_ratio:.1%}, "
                              f"90%concentration={chip_data.concentration_90:.2%}")
                else:
                    logger.debug(f"{stock_name}({code}) chip distributionfetch failedoralreadydisabled")
            except Exception as e:
                logger.warning(f"{stock_name}({code}) get chip distributionfailed: {e}")

            # If agent mode is explicitly enabled, or specific agent skills are configured, use the Agent analysis pipeline.
            # NOTE: use config.agent_mode (explicit opt-in) instead of
            # config.is_agent_available() so that users who only configured an
            # API Key for the traditional analysis path are not silently
            # switched to Agent mode (which is slower and more expensive).
            use_agent = getattr(self.config, 'agent_mode', False)
            if not use_agent:
                # Auto-enable agent mode when specific skills are configured (e.g., scheduled task with strategy)
                configured_skills = getattr(self.config, 'agent_skills', [])
                if configured_skills and configured_skills != ['all']:
                    use_agent = True
                    logger.info(f"{stock_name}({code}) Auto-enabled agent mode due to configured skills: {configured_skills}")

            # Step 2.5: fundamentalscapabilityaggregation（unifiedentry，abnormalfallback）
            # - failedreturn when partial/failed，notimpact existinghastechnicals/newslink
            # - closetogglewhenstillreturn not_supported structure
            fundamental_context = None
            try:
                fundamental_context = self.fetcher_manager.get_fundamental_context(
                    code,
                    budget_seconds=getattr(self.config, 'fundamental_stage_timeout_seconds', 1.5),
                )
            except Exception as e:
                logger.warning(f"{stock_name}({code}) fundamentalsaggregationfailed: {e}")
                fundamental_context = self.fetcher_manager.build_failed_fundamental_context(code, str(e))

            # P0: write-only snapshot, fail-open, no read dependency on this table.
            try:
                self.db.save_fundamental_snapshot(
                    query_id=query_id,
                    code=code,
                    payload=fundamental_context,
                    source_chain=fundamental_context.get("source_chain", []),
                    coverage=fundamental_context.get("coverage", {}),
                )
            except Exception as e:
                logger.debug(f"{stock_name}({code}) fundamental snapshotwritingfailed: {e}")

            # Step 3: trend analysis（based ontrading philosophy）— in Agent minutesupportbeforeexecute，providetwoitemspathtotaluse
            trend_result: Optional[TrendAnalysisResult] = None
            try:
                end_date = date.today()
                start_date = end_date - timedelta(days=89)  # ~60 trading days for MA60
                historical_bars = self.db.get_data_range(code, start_date, end_date)
                if historical_bars:
                    df = pd.DataFrame([bar.to_dict() for bar in historical_bars])
                    # Issue #234: Augment with realtime for intraday MA calculation
                    if self.config.enable_realtime_quote and realtime_quote:
                        df = self._augment_historical_with_realtime(df, realtime_quote, code)
                    trend_result = self.trend_analyzer.analyze(df, code)
                    logger.info(f"{stock_name}({code}) trend analysis: {trend_result.trend_status.value}, "
                              f"buy signal={trend_result.buy_signal.value}, score={trend_result.signal_score}")
            except Exception as e:
                logger.warning(f"{stock_name}({code}) trend analysisfailed: {e}", exc_info=True)

            if use_agent:
                logger.info(f"{stock_name}({code}) enabled Agent modeproceedanalyzing")
                return self._analyze_with_agent(
                    code,
                    report_type,
                    query_id,
                    stock_name,
                    realtime_quote,
                    chip_data,
                    fundamental_context,
                    trend_result,
                )

            # Step 4: multi-dimensional intelligencesearch（latestmessage+risktroubleshoot+performanceexpected）
            news_context = None
            if self.search_service.is_available:
                logger.info(f"{stock_name}({code}) startingmulti-dimensional intelligencesearch...")

                # usemultipledimensionsearch（at most5timessearch）
                intel_results = self.search_service.search_comprehensive_intel(
                    stock_code=code,
                    stock_name=stock_name,
                    max_searches=5
                )

                # formattingintelligencereport
                if intel_results:
                    news_context = self.search_service.format_intel_report(intel_results, stock_name)
                    total_results = sum(
                        len(r.results) for r in intel_results.values() if r.success
                    )
                    logger.info(f"{stock_name}({code}) intelligencesearchcompleted: total {total_results} results")
                    logger.debug(f"{stock_name}({code}) intelligencesearchresult:\n{news_context}")

                    # savingnewsintelligence todatabase（foraftercontinuereviewwithquerying）
                    try:
                        query_context = self._build_query_context(query_id=query_id)
                        for dim_name, response in intel_results.items():
                            if response and response.success and response.results:
                                self.db.save_news_intel(
                                    code=code,
                                    name=stock_name,
                                    dimension=dim_name,
                                    query=response.query,
                                    response=response,
                                    query_context=query_context
                                )
                    except Exception as e:
                        logger.warning(f"{stock_name}({code}) savingnewsintelligencefailed: {e}")
            else:
                logger.info(f"{stock_name}({code}) searchservice unavailable，skipintelligencesearch")

            # Step 4.5: Social sentiment intelligence (US stocks only)
            if self.social_sentiment_service.is_available and is_us_stock_code(code):
                try:
                    social_context = self.social_sentiment_service.get_social_context(code)
                    if social_context:
                        logger.info(f"{stock_name}({code}) Social sentiment data retrieved")
                        if news_context:
                            news_context = news_context + "\n\n" + social_context
                        else:
                            news_context = social_context
                except Exception as e:
                    logger.warning(f"{stock_name}({code}) Social sentiment fetch failed: {e}")

            # Step 5: getanalyzingcontext（technicalsdata）
            context = self.db.get_analysis_context(code)

            if context is None:
                logger.warning(f"{stock_name}({code}) unable toget historicalquote/market datadata，willonlybased onnewsandrealtimequote/market dataanalyzing")
                context = {
                    'code': code,
                    'stock_name': stock_name,
                    'date': date.today().isoformat(),
                    'data_missing': True,
                    'today': {},
                    'yesterday': {}
                }
            
            # Step 5.5: FMP fundamentals (non-fatal, cached per run)
            fmp_fundamentals = None
            if self.fmp_client:
                try:
                    fmp_fundamentals = self.fmp_client.get_fundamentals(code)
                except Exception as e:
                    logger.warning(f"{stock_name}({code}) FMP fetch failed: {e}")

            # Step 5.6: Apify Reddit sentiment (selected tickers, non-fatal)
            # Runs in the current thread-pool worker — MAX_WORKERS caps concurrency.
            reddit_sentiment = None
            if self.apify_reddit_client:
                try:
                    reddit_sentiment = self.apify_reddit_client.get_sentiment(code)
                except Exception as e:
                    logger.warning(f"{stock_name}({code}) Apify Reddit fetch failed: {e}")

            # Step 6: enhancedcontextdata（addrealtimequote/market data、chip、trendanalysis result、stockname）
            enhanced_context = self._enhance_context(
                context,
                realtime_quote,
                chip_data,
                trend_result,
                stock_name,  # pass instockname
                fundamental_context,
                fmp_fundamentals=fmp_fundamentals,
                reddit_sentiment=reddit_sentiment,
            )
            
            # Step 7: call AI analyzing（pass inenhancedcontextandnews）
            result = self.analyzer.analyze(enhanced_context, news_context=news_context)

            # Step 7.5: fillanalyzingwhenpriceinfoto result
            if result:
                result.query_id = query_id
                realtime_data = enhanced_context.get('realtime', {})
                result.current_price = realtime_data.get('price')
                result.change_pct = realtime_data.get('change_pct')

            # Step 7.6: chip_structure fallback (Issue #589)
            if result and chip_data:
                fill_chip_structure_if_needed(result, chip_data)

            # Step 7.7: price_position fallback
            if result:
                fill_price_position_if_needed(result, trend_result, realtime_quote)

            # Step 8: savinganalyzinghistoricalrecord
            if result:
                try:
                    context_snapshot = self._build_context_snapshot(
                        enhanced_context=enhanced_context,
                        news_content=news_context,
                        realtime_quote=realtime_quote,
                        chip_data=chip_data
                    )
                    self.db.save_analysis_history(
                        result=result,
                        query_id=query_id,
                        report_type=report_type.value,
                        news_content=news_context,
                        context_snapshot=context_snapshot,
                        save_snapshot=self.save_context_snapshot
                    )
                except Exception as e:
                    logger.warning(f"{stock_name}({code}) savinganalyzinghistoricalfailed: {e}")

            return result

        except Exception as e:
            logger.error(f"{stock_name}({code}) analyzingfailed: {e}")
            logger.exception(f"{stock_name}({code}) detailederror message:")
            return None
    
    def _enhance_context(
        self,
        context: Dict[str, Any],
        realtime_quote,
        chip_data: Optional[ChipDistribution],
        trend_result: Optional[TrendAnalysisResult],
        stock_name: str = "",
        fundamental_context: Optional[Dict[str, Any]] = None,
        fmp_fundamentals: Optional[Dict[str, Any]] = None,
        reddit_sentiment: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        enhancedanalyzingcontext
        
        willrealtimequote/market data、chip distribution、trendanalysis result、stocknameaddtocontextin
        
        Args:
            context: rawcontext
            realtime_quote: realtimequote/market datadata（UnifiedRealtimeQuote or None）
            chip_data: chip distributiondata
            trend_result: trendanalysis result
            stock_name: stockname
            
        Returns:
            enhancedaftercontext
        """
        enhanced = context.copy()
        enhanced["report_language"] = normalize_report_language(getattr(self.config, "report_language", "zh"))
        
        # addstockname
        if stock_name:
            enhanced['stock_name'] = stock_name
        elif realtime_quote and getattr(realtime_quote, 'name', None):
            enhanced['stock_name'] = realtime_quote.name

        # willrunningwhensearchwindowpass-throughgive analyzer，avoidwithglobalconfigurationre-readingproducegeneratewindowinconsistent
        enhanced['news_window_days'] = getattr(self.search_service, "news_window_days", 3)
        
        # addrealtimequote/market data（compatible with differentdatasource'sfielddifference）
        if realtime_quote:
            # use getattr safegetfield，missingfieldreturn None ordefault value
            volume_ratio = getattr(realtime_quote, 'volume_ratio', None)
            enhanced['realtime'] = {
                'name': getattr(realtime_quote, 'name', ''),
                'price': getattr(realtime_quote, 'price', None),
                'change_pct': getattr(realtime_quote, 'change_pct', None),
                'volume_ratio': volume_ratio,
                'volume_ratio_desc': self._describe_volume_ratio(volume_ratio) if volume_ratio else 'nodata',
                'turnover_rate': getattr(realtime_quote, 'turnover_rate', None),
                'pe_ratio': getattr(realtime_quote, 'pe_ratio', None),
                'pb_ratio': getattr(realtime_quote, 'pb_ratio', None),
                'total_mv': getattr(realtime_quote, 'total_mv', None),
                'circ_mv': getattr(realtime_quote, 'circ_mv', None),
                'change_60d': getattr(realtime_quote, 'change_60d', None),
                'source': getattr(realtime_quote, 'source', None),
            }
            # remove None valuewithreducecontextsize
            enhanced['realtime'] = {k: v for k, v in enhanced['realtime'].items() if v is not None}
        
        # addchip distribution
        if chip_data:
            current_price = getattr(realtime_quote, 'price', 0) if realtime_quote else 0
            enhanced['chip'] = {
                'profit_ratio': chip_data.profit_ratio,
                'avg_cost': chip_data.avg_cost,
                'concentration_90': chip_data.concentration_90,
                'concentration_70': chip_data.concentration_70,
                'chip_status': chip_data.get_chip_status(current_price or 0),
            }
        
        # addtrendanalysis result
        if trend_result:
            enhanced['trend_analysis'] = {
                'trend_status': trend_result.trend_status.value,
                'ma_alignment': trend_result.ma_alignment,
                'trend_strength': trend_result.trend_strength,
                'bias_ma5': trend_result.bias_ma5,
                'bias_ma10': trend_result.bias_ma10,
                'volume_status': trend_result.volume_status.value,
                'volume_trend': trend_result.volume_trend,
                'buy_signal': trend_result.buy_signal.value,
                'signal_score': trend_result.signal_score,
                'signal_reasons': trend_result.signal_reasons,
                'risk_factors': trend_result.risk_factors,
            }

        # Issue #234: Override today with realtime OHLC + trend MA for intraday analysis
        # Guard: trend_result.ma5 > 0 ensures MA calculation succeeded (data sufficient)
        if realtime_quote and trend_result and trend_result.ma5 > 0:
            price = getattr(realtime_quote, 'price', None)
            if price is not None and price > 0:
                yesterday_close = None
                if enhanced.get('yesterday') and isinstance(enhanced['yesterday'], dict):
                    yesterday_close = enhanced['yesterday'].get('close')
                orig_today = enhanced.get('today') or {}
                open_p = getattr(realtime_quote, 'open_price', None) or getattr(
                    realtime_quote, 'pre_close', None
                ) or yesterday_close or orig_today.get('open') or price
                high_p = getattr(realtime_quote, 'high', None) or price
                low_p = getattr(realtime_quote, 'low', None) or price
                vol = getattr(realtime_quote, 'volume', None)
                amt = getattr(realtime_quote, 'amount', None)
                pct = getattr(realtime_quote, 'change_pct', None)
                realtime_today = {
                    'close': price,
                    'open': open_p,
                    'high': high_p,
                    'low': low_p,
                    'ma5': trend_result.ma5,
                    'ma10': trend_result.ma10,
                    'ma20': trend_result.ma20,
                }
                if vol is not None:
                    realtime_today['volume'] = vol
                if amt is not None:
                    realtime_today['amount'] = amt
                if pct is not None:
                    realtime_today['pct_chg'] = pct
                for k, v in orig_today.items():
                    if k not in realtime_today and v is not None:
                        realtime_today[k] = v
                enhanced['today'] = realtime_today
                enhanced['ma_status'] = self._compute_ma_status(
                    price, trend_result.ma5, trend_result.ma10, trend_result.ma20
                )
                enhanced['date'] = date.today().isoformat()
                if yesterday_close is not None:
                    try:
                        yc = float(yesterday_close)
                        if yc > 0:
                            enhanced['price_change_ratio'] = round(
                                (price - yc) / yc * 100, 2
                            )
                    except (TypeError, ValueError):
                        pass
                if vol is not None and enhanced.get('yesterday'):
                    yest_vol = enhanced['yesterday'].get('volume') if isinstance(
                        enhanced['yesterday'], dict
                    ) else None
                    if yest_vol is not None:
                        try:
                            yv = float(yest_vol)
                            if yv > 0:
                                enhanced['volume_change_ratio'] = round(
                                    float(vol) / yv, 2
                                )
                        except (TypeError, ValueError):
                            pass

        # ETF/index flag for analyzer prompt (Fixes #274)
        enhanced['is_index_etf'] = SearchService.is_index_or_etf(
            context.get('code', ''), enhanced.get('stock_name', stock_name)
        )

        # P0: append unified fundamental block; keep as additional context only
        enhanced["fundamental_context"] = (
            fundamental_context
            if isinstance(fundamental_context, dict)
            else self.fetcher_manager.build_failed_fundamental_context(
                context.get("code", ""),
                "invalid fundamental context",
            )
        )

        # FMP fundamentals block (None when FMP is disabled or fetch failed)
        if fmp_fundamentals is not None:
            enhanced["fmp_fundamentals"] = fmp_fundamentals

        # Apify Reddit sentiment (None when disabled, ineligible ticker, or fetch failed)
        if reddit_sentiment is not None:
            enhanced["reddit_sentiment"] = reddit_sentiment

        return enhanced

    def _analyze_with_agent(
        self, 
        code: str, 
        report_type: ReportType, 
        query_id: str,
        stock_name: str,
        realtime_quote: Any,
        chip_data: Optional[ChipDistribution],
        fundamental_context: Optional[Dict[str, Any]] = None,
        trend_result: Optional[TrendAnalysisResult] = None,
    ) -> Optional[AnalysisResult]:
        """
        use Agent modeanalyzingsinglestock。
        """
        try:
            from src.agent.factory import build_agent_executor
            report_language = normalize_report_language(getattr(self.config, "report_language", "zh"))

            # Build executor from shared factory (ToolRegistry and SkillManager prototype are cached)
            executor = build_agent_executor(self.config, getattr(self.config, 'agent_skills', None) or None)

            # Build initial context to avoid redundant tool calls
            initial_context = {
                "stock_code": code,
                "stock_name": stock_name,
                "report_type": report_type.value,
                "report_language": report_language,
                "fundamental_context": fundamental_context,
            }
            
            if realtime_quote:
                initial_context["realtime_quote"] = self._safe_to_dict(realtime_quote)
            if chip_data:
                initial_context["chip_distribution"] = self._safe_to_dict(chip_data)
            if trend_result:
                initial_context["trend_result"] = self._safe_to_dict(trend_result)

            # Agent path: inject social sentiment as news_context so both
            # executor (_build_user_message) and orchestrator (ctx.set_data)
            # can consume it through the existing news_context channel
            if self.social_sentiment_service.is_available and is_us_stock_code(code):
                try:
                    social_context = self.social_sentiment_service.get_social_context(code)
                    if social_context:
                        existing = initial_context.get("news_context")
                        if existing:
                            initial_context["news_context"] = existing + "\n\n" + social_context
                        else:
                            initial_context["news_context"] = social_context
                        logger.info(f"[{code}] Agent mode: social sentiment data injected into news_context")
                except Exception as e:
                    logger.warning(f"[{code}] Agent mode: social sentiment fetch failed: {e}")

            # running Agent
            if report_language == "en":
                message = f"Analyze stock {code} ({stock_name}) and return the full decision dashboard JSON in English."
            else:
                message = f"pleaseanalyzingstock {code} ({stock_name})，andgeneratingdecisiondashboardreport。"
            agent_result = executor.run(message, context=initial_context)

            # convertingas AnalysisResult
            result = self._agent_result_to_analysis_result(agent_result, code, stock_name, report_type, query_id)
            if result:
                result.query_id = query_id
            # Agent weak integrity: placeholder fill only, no LLM retry
            if result and getattr(self.config, "report_integrity_enabled", False):
                from src.analyzer import check_content_integrity, apply_placeholder_fill

                pass_integrity, missing = check_content_integrity(result)
                if not pass_integrity:
                    apply_placeholder_fill(result, missing)
                    logger.info(
                        "[LLMcomplete-ness] integrity_mode=agent_weak requiredfieldmissing %s，alreadyaccount fordigitcomplete",
                        missing,
                    )
            # chip_structure fallback (Issue #589), before save_analysis_history
            if result and chip_data:
                fill_chip_structure_if_needed(result, chip_data)

            # price_position fallback (same as non-agent path Step 7.7)
            if result:
                fill_price_position_if_needed(result, trend_result, realtime_quote)

            resolved_stock_name = result.name if result and result.name else stock_name

            # savingnewsintelligence todatabase（Agent toolresultonlyfor LLM context，notpersist，Fixes #396）
            # use search_stock_news（with Agent toolcalllogicconsistent），only 1 times API call，noextradelay
            if self.search_service.is_available:
                try:
                    news_response = self.search_service.search_stock_news(
                        stock_code=code,
                        stock_name=resolved_stock_name,
                        max_results=5
                    )
                    if news_response.success and news_response.results:
                        query_context = self._build_query_context(query_id=query_id)
                        self.db.save_news_intel(
                            code=code,
                            name=resolved_stock_name,
                            dimension="latest_news",
                            query=news_response.query,
                            response=news_response,
                            query_context=query_context
                        )
                        logger.info(f"[{code}] Agent mode: newsintelligencealreadysaving {len(news_response.results)} items")
                except Exception as e:
                    logger.warning(f"[{code}] Agent modesavingnewsintelligencefailed: {e}")

            # savinganalyzinghistoricalrecord
            if result:
                try:
                    initial_context["stock_name"] = resolved_stock_name
                    self.db.save_analysis_history(
                        result=result,
                        query_id=query_id,
                        report_type=report_type.value,
                        news_content=None,
                        context_snapshot=initial_context,
                        save_snapshot=self.save_context_snapshot
                    )
                except Exception as e:
                    logger.warning(f"[{code}] saving Agent analyzinghistoricalfailed: {e}")

            return result

        except Exception as e:
            logger.error(f"[{code}] Agent analyzingfailed: {e}")
            logger.exception(f"[{code}] Agent detailederror message:")
            return None

    def _agent_result_to_analysis_result(
        self, agent_result, code: str, stock_name: str, report_type: ReportType, query_id: str
    ) -> AnalysisResult:
        """
        will AgentResult convertingas AnalysisResult。
        """
        report_language = normalize_report_language(getattr(self.config, "report_language", "zh"))
        result = AnalysisResult(
            code=code,
            name=stock_name,
            sentiment_score=50,
            trend_prediction="Unknown" if report_language == "en" else "unknown",
            operation_advice="Watch" if report_language == "en" else "wait and see",
            confidence_level=localize_confidence_level("medium", report_language),
            report_language=report_language,
            success=agent_result.success,
            error_message=agent_result.error or None,
            data_sources=f"agent:{agent_result.provider}",
            model_used=agent_result.model or None,
        )

        if agent_result.success and agent_result.dashboard:
            dash = agent_result.dashboard
            ai_stock_name = str(dash.get("stock_name", "")).strip()
            if ai_stock_name and self._is_placeholder_stock_name(stock_name, code):
                result.name = ai_stock_name
            result.sentiment_score = self._safe_int(dash.get("sentiment_score"), 50)
            result.trend_prediction = dash.get("trend_prediction", "Unknown" if report_language == "en" else "unknown")
            raw_advice = dash.get("operation_advice", "Watch" if report_language == "en" else "wait and see")
            if isinstance(raw_advice, dict):
                # LLM may return {"no_position": "...", "has_position": "..."}
                # Derive a short string from decision_type for the scalar field
                _signal_to_advice = {
                    "buy": "Buy" if report_language == "en" else "buy",
                    "sell": "Sell" if report_language == "en" else "sell",
                    "hold": "Hold" if report_language == "en" else "hold",
                    "strong_buy": "Strong Buy" if report_language == "en" else "strong buy",
                    "strong_sell": "Strong Sell" if report_language == "en" else "strong sell",
                }
                # Normalize decision_type (strip/lower) before lookup so
                # variants like "BUY" or " Buy " map correctly.
                raw_dt = str(dash.get("decision_type") or "hold").strip().lower()
                result.operation_advice = _signal_to_advice.get(raw_dt, "Watch" if report_language == "en" else "wait and see")
            else:
                result.operation_advice = str(raw_advice) if raw_advice else ("Watch" if report_language == "en" else "wait and see")
            from src.agent.protocols import normalize_decision_signal

            result.decision_type = normalize_decision_signal(
                dash.get("decision_type", "hold")
            )
            result.confidence_level = localize_confidence_level(
                dash.get("confidence_level", result.confidence_level),
                report_language,
            )
            result.analysis_summary = dash.get("analysis_summary", "")
            # The AI returns a top-level dict that contains a nested 'dashboard' sub-key
            # with core_conclusion / battle_plan / intelligence.  AnalysisResult's helper
            # methods (get_sniper_points, get_core_conclusion, etc.) expect that inner
            # structure, so we unwrap it here.
            result.dashboard = dash.get("dashboard") or dash
        else:
            result.sentiment_score = 50
            result.operation_advice = "Watch" if report_language == "en" else "wait and see"
            if not result.error_message:
                result.error_message = "Agent failed to generate a valid decision dashboard" if report_language == "en" else "Agent notcangeneratingvaliddecisiondashboard"

        return result

    @staticmethod
    def _is_placeholder_stock_name(name: str, code: str) -> bool:
        """Return True when the stock name is missing or placeholder-like."""
        if not name:
            return True
        normalized = str(name).strip()
        if not normalized:
            return True
        if normalized == code:
            return True
        if normalized.startswith("stock"):
            return True
        if "Unknown" in normalized:
            return True
        return False

    @staticmethod
    def _safe_int(value: Any, default: int = 50) -> int:
        """safeadverb markerwillvalueconvertingasinteger。"""
        if value is None:
            return default
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            import re
            match = re.search(r'-?\d+', value)
            if match:
                return int(match.group())
        return default
    
    def _describe_volume_ratio(self, volume_ratio: float) -> str:
        """
        volume ratiodescription
        
        volume ratio = currenttrading volume / remove5daily averagetrading volume
        """
        if volume_ratio < 0.5:
            return "extremelyshrinkage"
        elif volume_ratio < 0.8:
            return "obviousshrinkage"
        elif volume_ratio < 1.2:
            return "normal"
        elif volume_ratio < 2.0:
            return "warmandvolume increase"
        elif volume_ratio < 3.0:
            return "obviousvolume increase"
        else:
            return "hugevolume"

    @staticmethod
    def _compute_ma_status(close: float, ma5: float, ma10: float, ma20: float) -> str:
        """
        Compute MA alignment status from price and MA values.
        Logic mirrors storage._analyze_ma_status (Issue #234).
        """
        close = close or 0
        ma5 = ma5 or 0
        ma10 = ma10 or 0
        ma20 = ma20 or 0
        if close > ma5 > ma10 > ma20 > 0:
            return "long positionarrange 📈"
        elif close < ma5 < ma10 < ma20 and ma20 > 0:
            return "short positionarrange 📉"
        elif close > ma5 and ma5 > ma10:
            return "short-termtogood 🔼"
        elif close < ma5 and ma5 < ma10:
            return "short-termweakening 🔽"
        else:
            return "oscillation consolidation ↔️"

    def _augment_historical_with_realtime(
        self, df: pd.DataFrame, realtime_quote: Any, code: str
    ) -> pd.DataFrame:
        """
        Augment historical OHLCV with today's realtime quote for intraday MA calculation.
        Issue #234: Use realtime price instead of yesterday's close for technical indicators.
        """
        if df is None or df.empty or 'close' not in df.columns:
            return df
        if realtime_quote is None:
            return df
        price = getattr(realtime_quote, 'price', None)
        if price is None or not (isinstance(price, (int, float)) and price > 0):
            return df

        # Optional: skip augmentation on non-trading days (fail-open)
        enable_realtime_tech = getattr(
            self.config, 'enable_realtime_technical_indicators', True
        )
        if not enable_realtime_tech:
            return df
        market = get_market_for_stock(code)
        if market and not is_market_open(market, date.today()):
            return df

        last_val = df['date'].max()
        last_date = (
            last_val.date() if hasattr(last_val, 'date') else
            (last_val if isinstance(last_val, date) else pd.Timestamp(last_val).date())
        )
        yesterday_close = float(df.iloc[-1]['close']) if len(df) > 0 else price
        open_p = getattr(realtime_quote, 'open_price', None) or getattr(
            realtime_quote, 'pre_close', None
        ) or yesterday_close
        high_p = getattr(realtime_quote, 'high', None) or price
        low_p = getattr(realtime_quote, 'low', None) or price
        vol = getattr(realtime_quote, 'volume', None) or 0
        amt = getattr(realtime_quote, 'amount', None)
        pct = getattr(realtime_quote, 'change_pct', None)

        if last_date >= date.today():
            # Update last row with realtime close (copy to avoid mutating caller's df)
            df = df.copy()
            idx = df.index[-1]
            df.loc[idx, 'close'] = price
            if open_p is not None:
                df.loc[idx, 'open'] = open_p
            if high_p is not None:
                df.loc[idx, 'high'] = high_p
            if low_p is not None:
                df.loc[idx, 'low'] = low_p
            if vol:
                df.loc[idx, 'volume'] = vol
            if amt is not None:
                df.loc[idx, 'amount'] = amt
            if pct is not None:
                df.loc[idx, 'pct_chg'] = pct
        else:
            # Append virtual today row
            new_row = {
                'code': code,
                'date': date.today(),
                'open': open_p,
                'high': high_p,
                'low': low_p,
                'close': price,
                'volume': vol,
                'amount': amt if amt is not None else 0,
                'pct_chg': pct if pct is not None else 0,
            }
            new_df = pd.DataFrame([new_row])
            df = pd.concat([df, new_df], ignore_index=True)
        return df

    def _build_context_snapshot(
        self,
        enhanced_context: Dict[str, Any],
        news_content: Optional[str],
        realtime_quote: Any,
        chip_data: Optional[ChipDistribution]
    ) -> Dict[str, Any]:
        """
        buildanalyzingcontextsnapshot
        """
        return {
            "enhanced_context": enhanced_context,
            "news_content": news_content,
            "realtime_quote_raw": self._safe_to_dict(realtime_quote),
            "chip_distribution_raw": self._safe_to_dict(chip_data),
        }

    @staticmethod
    def _safe_to_dict(value: Any) -> Optional[Dict[str, Any]]:
        """
        safeconvertingasdictionary
        """
        if value is None:
            return None
        if hasattr(value, "to_dict"):
            try:
                return value.to_dict()
            except Exception:
                return None
        if hasattr(value, "__dict__"):
            try:
                return dict(value.__dict__)
            except Exception:
                return None
        return None

    def _resolve_query_source(self, query_source: Optional[str]) -> str:
        """
        parsingrequestsource。

        priority（fromhightolow）：
        1. explicitly pass query_source：callmethodcleardeterminespecifiedwhenprefer to use，for convenienceoverrideinferresultorcompatiblenotfrom source_message fromnon- bot scenario
        2. exists source_message wheninferas "bot"：currentapproximatelyfixedasbotsessioncontext
        3. exists query_id wheninferas "web"：Web triggerrequestwillwithabove/upper query_id
        4. default "system"：scheduled taskor CLI etcnoabove-mentionedcontextwhen

        Args:
            query_source: callmethodexplicitspecifiedsource，e.g. "bot" / "web" / "cli" / "system"

        Returns:
            normalizeaftersourceidentifierstring，e.g. "bot" / "web" / "cli" / "system"
        """
        if query_source:
            return query_source
        if self.source_message:
            return "bot"
        if self.query_id:
            return "web"
        return "system"

    def _build_query_context(self, query_id: Optional[str] = None) -> Dict[str, str]:
        """
        generatinguserqueryingassociationinfo
        """
        effective_query_id = query_id or self.query_id or ""

        context: Dict[str, str] = {
            "query_id": effective_query_id,
            "query_source": self.query_source or "",
        }

        if self.source_message:
            context.update({
                "requester_platform": self.source_message.platform or "",
                "requester_user_id": self.source_message.user_id or "",
                "requester_user_name": self.source_message.user_name or "",
                "requester_chat_id": self.source_message.chat_id or "",
                "requester_message_id": self.source_message.message_id or "",
                "requester_query": self.source_message.content or "",
            })

        return context
    
    def process_single_stock(
        self,
        code: str,
        skip_analysis: bool = False,
        single_stock_notify: bool = False,
        report_type: ReportType = ReportType.SIMPLE,
        analysis_query_id: Optional[str] = None,
    ) -> Optional[AnalysisResult]:
        """
        processingsinglestockcompleteprocess

        packagebracket：
        1. getdata
        2. savingdata
        3. AI analyzing
        4. single stockpush（optional，#55）

        thismethodwillbythreadpoolcall，needprocessinggoodabnormal

        Args:
            analysis_query_id: queryinglinkassociation id
            code: stock code
            skip_analysis: whetherskip AI analyzing
            single_stock_notify: whetherenabledsingle stockpushmode（eachanalyzingcomplete one stockimmediatelypush）
            report_type: Report type enum（fromconfigurationreading，Issue #119）

        Returns:
            AnalysisResult or None
        """
        logger.info(f"========== startingprocessing {code} ==========")
        
        try:
            # Step 1: getandsavingdata
            success, error = self.fetch_and_save_stock_data(code)
            
            if not success:
                logger.warning(f"[{code}] datafetch failed: {error}")
                # i.e.usefetch failed，alsotryuseexistingdataanalyzing
            
            # Step 2: AI analyzing
            if skip_analysis:
                logger.info(f"[{code}] skip AI analyzing（dry-run mode）")
                return None
            
            effective_query_id = analysis_query_id or self.query_id or uuid.uuid4().hex
            result = self.analyze_stock(code, report_type, query_id=effective_query_id)
            
            if result:
                if not result.success:
                    logger.warning(
                        f"[{code}] analyzingnotsuccessful: {result.error_message or 'unknownerror'}"
                    )
                else:
                    logger.info(
                        f"[{code}] analyzingcompleted: {result.operation_advice}, "
                        f"score {result.sentiment_score}"
                    )
                
                # single stockpushmode（#55）：eachanalyzingcomplete one stockstockimmediatelypush
                if single_stock_notify and self.notifier.is_available():
                    try:
                        # based onreport typeselectgeneratingmethod
                        if report_type == ReportType.FULL:
                            report_content = self.notifier.generate_dashboard_report([result])
                            logger.info(f"[{code}] useFull Reportformat")
                        elif report_type == ReportType.BRIEF:
                            report_content = self.notifier.generate_brief_report([result])
                            logger.info(f"[{code}] useBrief Reportformat")
                        else:
                            report_content = self.notifier.generate_single_stock_report(result)
                            logger.info(f"[{code}] useSimple Reportformat")
                        
                        if self.notifier.send(report_content, email_stock_codes=[code]):
                            logger.info(f"[{code}] single stockpushsuccessful")
                        else:
                            logger.warning(f"[{code}] single stockpushfailed")
                    except Exception as e:
                        logger.error(f"[{code}] single stockpushabnormal: {e}")
            
            return result
            
        except Exception as e:
            # captureallabnormal，ensuresingle stockfailednotaffects overall
            logger.exception(f"[{code}] processingprocesssendgenerateunknownabnormal: {e}")
            return None
    
    def run(
        self,
        stock_codes: Optional[List[str]] = None,
        dry_run: bool = False,
        send_notification: bool = True,
        merge_notification: bool = False
    ) -> List[AnalysisResult]:
        """
        runningcompleteanalyzingprocess

        process：
        1. getpendinganalyzingstocklist
        2. usethreadpoolconcurrencyprocessing
        3. collectanalysis result
        4. sendingnotification

        Args:
            stock_codes: stock codelist（optional，defaultuseconfigurationwatchlist stocks in）
            dry_run: whetheronly getdatanotanalyzing
            send_notification: whethersendingpushnotification
            merge_notification: whethermergingpush（skip thispush，by main layermergingindividual stock+unified after marketsending，Issue #190）

        Returns:
            analysis resultlist
        """
        start_time = time.time()
        
        # useconfigurationinstocklist
        if stock_codes is None:
            self.config.refresh_stock_list()
            stock_codes = self.config.stock_list
        
        if not stock_codes:
            logger.error("notconfigurationwatchlist stockslist，please in .env fileinsettings STOCK_LIST")
            return []
        
        logger.info(f"===== startinganalyzing {len(stock_codes)} onlystock =====")
        logger.info(f"stocklist: {', '.join(stock_codes)}")
        logger.info(f"concurrencycount: {self.max_workers}, mode: {'only getdata' if dry_run else 'completeanalyzing'}")
        
        # === batch prefetch realtimequote/market data（optimize：avoideachonlystockalltrigger fullpull）===
        # onlyhasstockquantity >= 5 whenonly thenproceedprefetch，fewvolumestockdirectlyone by onequeryingmorehigheffect
        if len(stock_codes) >= 5:
            prefetch_count = self.fetcher_manager.prefetch_realtime_quotes(stock_codes)
            if prefetch_count > 0:
                logger.info(f"alreadyenabledbatch prefetcharchitecture：oncepull allmarketdata，{len(stock_codes)} onlystocktotalenjoycache")

        # Issue #455: prefetchstockname，avoidconcurrencyanalyzingwhendisplay「stockxxxxx」
        # dry_run onlydodatapull，no neednameprefetch，avoidextranetworkoverhead
        if not dry_run:
            self.fetcher_manager.prefetch_stock_names(stock_codes, use_bulk=False)

        # single stockpushmode（#55）：fromconfigurationreading
        single_stock_notify = getattr(self.config, 'single_stock_notify', False)
        # Issue #119: fromconfigurationreadingreport type
        report_type_str = getattr(self.config, 'report_type', 'simple').lower()
        if report_type_str == 'brief':
            report_type = ReportType.BRIEF
        elif report_type_str == 'full':
            report_type = ReportType.FULL
        else:
            report_type = ReportType.SIMPLE
        # Issue #128: fromconfigurationreadinganalyzinginterval
        analysis_delay = getattr(self.config, 'analysis_delay', 0)

        if single_stock_notify:
            logger.info(f"alreadyenabledsingle stockpushmode：eachanalyzingcomplete one stockstockimmediatelypush（report type: {report_type_str}）")
        
        results: List[AnalysisResult] = []
        
        # usethreadpoolconcurrencyprocessing
        # Note：max_workers default 5，can via MAX_WORKERS environment variableadjust
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # submittask
            future_to_code = {
                executor.submit(
                    self.process_single_stock,
                    code,
                    skip_analysis=dry_run,
                    single_stock_notify=single_stock_notify and send_notification,
                    report_type=report_type,  # Issue #119: passreport type
                    analysis_query_id=uuid.uuid4().hex,
                ): code
                for code in stock_codes
            }
            
            # collectresult
            for idx, future in enumerate(as_completed(future_to_code)):
                code = future_to_code[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)

                    # Issue #128: analyzinginterval - inindividual stock analysisandmarket index analysisbetweenadddelay
                    if idx < len(stock_codes) - 1 and analysis_delay > 0:
                        # Note：this sleep sendgeneratein“mainthreadcollect future loop”in，
                        # andnotwillstopthreadpoolintasksimultaneouslysendstartnetworkrequest。
                        # becausethisittodecreaselowconcurrencyrequestpeakvalueeffecthaslimit；realpositivepeakvaluemainby max_workers decidefixed。
                        # this behavior is currently preserved（logic not changed per requirements）。
                        logger.debug(f"waiting {analysis_delay} secondsaftercontinuingbelowoneonlystock...")
                        time.sleep(analysis_delay)

                except Exception as e:
                    logger.error(f"[{code}] taskexecutefailed: {e}")
        
        # statistics
        elapsed_time = time.time() - start_time
        
        # dry-run modebelow，datafetch successfuli.e.view asassuccessful
        if dry_run:
            # checkwhich onesstockdatatodayalready exists
            success_count = sum(1 for code in stock_codes if self.db.has_today_data(code))
            fail_count = len(stock_codes) - success_count
        else:
            success_count = len(results)
            fail_count = len(stock_codes) - success_count
        
        logger.info("===== analyzingcompleted =====")
        logger.info(f"successful: {success_count}, failed: {fail_count}, elapsed: {elapsed_time:.2f} seconds")
        
        # savingreportto localfile（notheorywhetherpushnotificationallsaving）
        if results and not dry_run:
            self._save_local_report(results, report_type)

        # sendingnotification（single stockpushmodebelowskipsummarypush，avoid duplicate）
        if results and send_notification and not dry_run:
            if single_stock_notify:
                # single stockpushmode：onlysavingsummaryreport，notagainduplicatepush
                logger.info("single stockpushmode：skipsummarypush，onlysavingreportto local")
                self._send_notifications(results, report_type, skip_push=True)
            elif merge_notification:
                # mergingmode（Issue #190）：onlysaving，notpush，by main layermergingindividual stock+unified after marketsending
                logger.info("mergingpushmode：skip thispush，willinindividual stock+market reviewafterunifiedsending")
                self._send_notifications(results, report_type, skip_push=True)
            else:
                self._send_notifications(results, report_type)
        
        return results
    
    def _save_local_report(
        self,
        results: List[AnalysisResult],
        report_type: ReportType = ReportType.SIMPLE,
    ) -> None:
        """savinganalysis reportto localfile（withnotificationpushdecoupling）"""
        try:
            report = self._generate_aggregate_report(results, report_type)
            filepath = self.notifier.save_report_to_file(report)
            logger.info(f"decisiondashboarddaily reportalreadysaving: {filepath}")
        except Exception as e:
            logger.error(f"savinglocalreportfailed: {e}")

    def _send_notifications(
        self,
        results: List[AnalysisResult],
        report_type: ReportType = ReportType.SIMPLE,
        skip_push: bool = False,
    ) -> None:
        """
        sendinganalysis resultnotification
        
        generatingdecisiondashboardformatreport
        
        Args:
            results: analysis resultlist
            skip_push: whetherskippush（onlysavingto local，forsingle stockpushmode）
        """
        try:
            logger.info("generatingdecisiondashboarddaily report...")
            report = self._generate_aggregate_report(results, report_type)
            
            # skippush（single stockpushmode / mergingmode：reportalreadyby _save_local_report saving）
            if skip_push:
                return
            
            # pushnotification
            if self.notifier.is_available():
                channels = self.notifier.get_available_channels()
                context_success = self.notifier.send_to_context(report)

                # Issue #455: Markdown convertimage（with notification.send logicconsistent）
                from src.md2img import markdown_to_image

                channels_needing_image = {
                    ch for ch in channels
                    if ch.value in self.notifier._markdown_to_image_channels
                }
                non_wechat_channels_needing_image = {
                    ch for ch in channels_needing_image if ch != NotificationChannel.WECHAT
                }

                def _get_md2img_hint() -> str:
                    try:
                        engine = getattr(get_config(), "md2img_engine", "wkhtmltoimage")
                    except Exception:
                        engine = "wkhtmltoimage"
                    return (
                        "npm i -g markdown-to-file" if engine == "markdown-to-file"
                        else "wkhtmltopdf (apt install wkhtmltopdf / brew install wkhtmltopdf)"
                    )

                image_bytes = None
                if non_wechat_channels_needing_image:
                    image_bytes = markdown_to_image(
                        report, max_chars=self.notifier._markdown_to_image_max_chars
                    )
                    if image_bytes:
                        logger.info(
                            "Markdown alreadyconvertingasimage，willto %s sendingimage",
                            [ch.value for ch in non_wechat_channels_needing_image],
                        )
                    else:
                        logger.warning(
                            "Markdown convertimagefailed，willrollbackas textsending。pleasecheck MARKDOWN_TO_IMAGE_CHANNELS configurationandsetup %s",
                            _get_md2img_hint(),
                        )

                # Enterprise WeChat：onlysendsimplifiedversion（platformconstraint）
                wechat_success = False
                if NotificationChannel.WECHAT in channels:
                    if report_type == ReportType.BRIEF:
                        dashboard_content = self.notifier.generate_brief_report(results)
                    else:
                        dashboard_content = self.notifier.generate_wechat_dashboard(results)
                    logger.info(f"Enterprise WeChatdashboardlength: {len(dashboard_content)} character")
                    logger.debug(f"Enterprise WeChatpushcontent:\n{dashboard_content}")
                    wechat_image_bytes = None
                    if NotificationChannel.WECHAT in channels_needing_image:
                        wechat_image_bytes = markdown_to_image(
                            dashboard_content,
                            max_chars=self.notifier._markdown_to_image_max_chars,
                        )
                        if wechat_image_bytes is None:
                            logger.warning(
                                "Enterprise WeChat Markdown convertimagefailed，willrollbackas textsending。pleasecheck MARKDOWN_TO_IMAGE_CHANNELS configurationandsetup %s",
                                _get_md2img_hint(),
                            )
                    use_image = self.notifier._should_use_image_for_channel(
                        NotificationChannel.WECHAT, wechat_image_bytes
                    )
                    if use_image:
                        wechat_success = self.notifier._send_wechat_image(wechat_image_bytes)
                    else:
                        wechat_success = self.notifier.send_to_wechat(dashboard_content)

                # otherchannel：sendFull Report（avoidcustom Webhook by wechat truncatelogicpollute）
                non_wechat_success = False
                stock_email_groups = getattr(self.config, 'stock_email_groups', []) or []
                for channel in channels:
                    if channel == NotificationChannel.WECHAT:
                        continue
                    if channel == NotificationChannel.FEISHU:
                        non_wechat_success = self.notifier.send_to_feishu(report) or non_wechat_success
                    elif channel == NotificationChannel.TELEGRAM:
                        use_image = self.notifier._should_use_image_for_channel(
                            channel, image_bytes
                        )
                        if use_image:
                            result = self.notifier._send_telegram_photo(image_bytes)
                        else:
                            result = self.notifier.send_to_telegram(report)
                        non_wechat_success = result or non_wechat_success
                    elif channel == NotificationChannel.EMAIL:
                        if stock_email_groups:
                            code_to_emails: Dict[str, Optional[List[str]]] = {}
                            for r in results:
                                if r.code not in code_to_emails:
                                    emails = []
                                    for stocks, emails_list in stock_email_groups:
                                        if r.code in stocks:
                                            emails.extend(emails_list)
                                    code_to_emails[r.code] = list(dict.fromkeys(emails)) if emails else None
                            emails_to_results: Dict[Optional[Tuple], List] = defaultdict(list)
                            for r in results:
                                recs = code_to_emails.get(r.code)
                                key = tuple(recs) if recs else None
                                emails_to_results[key].append(r)
                            for key, group_results in emails_to_results.items():
                                grp_report = self._generate_aggregate_report(group_results, report_type)
                                grp_image_bytes = None
                                if channel.value in self.notifier._markdown_to_image_channels:
                                    grp_image_bytes = markdown_to_image(
                                        grp_report,
                                        max_chars=self.notifier._markdown_to_image_max_chars,
                                    )
                                use_image = self.notifier._should_use_image_for_channel(
                                    channel, grp_image_bytes
                                )
                                receivers = list(key) if key is not None else None
                                if use_image:
                                    result = self.notifier._send_email_with_inline_image(
                                        grp_image_bytes, receivers=receivers
                                    )
                                else:
                                    result = self.notifier.send_to_email(
                                        grp_report, receivers=receivers
                                    )
                                non_wechat_success = result or non_wechat_success
                        else:
                            use_image = self.notifier._should_use_image_for_channel(
                                channel, image_bytes
                            )
                            if use_image:
                                result = self.notifier._send_email_with_inline_image(image_bytes)
                            else:
                                result = self.notifier.send_to_email(report)
                            non_wechat_success = result or non_wechat_success
                    elif channel == NotificationChannel.CUSTOM:
                        use_image = self.notifier._should_use_image_for_channel(
                            channel, image_bytes
                        )
                        if use_image:
                            result = self.notifier._send_custom_webhook_image(
                                image_bytes, fallback_content=report
                            )
                        else:
                            result = self.notifier.send_to_custom(report)
                        non_wechat_success = result or non_wechat_success
                    elif channel == NotificationChannel.PUSHPLUS:
                        non_wechat_success = self.notifier.send_to_pushplus(report) or non_wechat_success
                    elif channel == NotificationChannel.SERVERCHAN3:
                        non_wechat_success = self.notifier.send_to_serverchan3(report) or non_wechat_success
                    elif channel == NotificationChannel.DISCORD:
                        non_wechat_success = self.notifier.send_to_discord(report) or non_wechat_success
                    elif channel == NotificationChannel.PUSHOVER:
                        non_wechat_success = self.notifier.send_to_pushover(report) or non_wechat_success
                    elif channel == NotificationChannel.ASTRBOT:
                        non_wechat_success = self.notifier.send_to_astrbot(report) or non_wechat_success
                    elif channel == NotificationChannel.SLACK:
                        use_image = self.notifier._should_use_image_for_channel(
                            channel, image_bytes
                        )
                        if use_image and self.notifier._slack_bot_token and self.notifier._slack_channel_id:
                            result = self.notifier._send_slack_image(
                                image_bytes, fallback_content=report
                            )
                        else:
                            result = self.notifier.send_to_slack(report)
                        non_wechat_success = result or non_wechat_success
                    else:
                        logger.warning(f"unknownnotification channel: {channel}")

                success = wechat_success or non_wechat_success or context_success
                if success:
                    logger.info("decisiondashboardpushsuccessful")
                else:
                    logger.warning("decisiondashboardpushfailed")
            else:
                logger.info("notification channelnotconfiguration，skippush")
                
        except Exception as e:
            import traceback
            logger.error(f"sendingnotificationfailed: {e}\n{traceback.format_exc()}")

    def _generate_aggregate_report(
        self,
        results: List[AnalysisResult],
        report_type: ReportType,
    ) -> str:
        """Generate aggregate report with backward-compatible notifier fallback."""
        generator = getattr(self.notifier, "generate_aggregate_report", None)
        if callable(generator):
            return generator(results, report_type)
        if report_type == ReportType.BRIEF and hasattr(self.notifier, "generate_brief_report"):
            return self.notifier.generate_brief_report(results)
        return self.notifier.generate_dashboard_report(results)
