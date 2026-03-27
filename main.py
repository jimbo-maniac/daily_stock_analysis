# -*- coding: utf-8 -*-
"""
===================================
A-share Stock Intelligent Analysis System - mainscheduleprogram
===================================

Responsibilities:
1. coordinateeachmodulecompletedstockanalyzingprocess
2. implementlowconcurrencythreadpoolschedule
3. globalabnormalprocessing，ensuresingle stockfailednotaffects overall
4. providecommandrowentry

usage：
    python main.py              # normalrunning
    python main.py --debug      # debugmode
    python main.py --dry-run    # only getdatanotanalyzing

trading philosophy（alreadymergeenteranalyzing）：
- strict entry strategy：don't chase highs，BIAS ratio > 5% notbuy
- trendtrade：onlydo MA5>MA10>MA20 long positionarrange
- efficiency first：monitorchip concentrationgoodstock
- buy point preference：volume contraction pullback MA5/MA10 support
"""
import os
from src.config import setup_env
setup_env()

# proxyconfiguration - via USE_PROXY environment variablecontrol，defaultclose
# GitHub Actions environmentautomaticskipproxyconfiguration
if os.getenv("GITHUB_ACTIONS") != "true" and os.getenv("USE_PROXY", "false").lower() == "true":
    # localdevelopmentenvironment，enabledproxy（canin .env inconfiguration PROXY_HOST and PROXY_PORT）
    proxy_host = os.getenv("PROXY_HOST", "127.0.0.1")
    proxy_port = os.getenv("PROXY_PORT", "10809")
    proxy_url = f"http://{proxy_host}:{proxy_port}"
    os.environ["http_proxy"] = proxy_url
    os.environ["https_proxy"] = proxy_url

import argparse
import logging
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple

from data_provider.base import canonical_stock_code
from src.core.pipeline import StockAnalysisPipeline
from src.core.market_review import run_market_review
from src.webui_frontend import prepare_webui_frontend_assets
from src.config import get_config, Config
from src.logging_config import setup_logging


logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """parsingcommandrowparameter"""
    parser = argparse.ArgumentParser(
        description='A-share Stock Intelligent Analysis System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Example:
  python main.py                    # normalrunning
  python main.py --debug            # debugmode
  python main.py --dry-run          # only getdata，notproceed AI analyzing
  python main.py --stocks 600519,000001  # specifiedanalyzingspecificstock
  python main.py --no-notify        # notsendingpushnotification
  python main.py --single-notify    # enabledsingle stockpushmode（eachanalyzingcomplete one stockimmediatelypush）
  python main.py --schedule         # enabledscheduled taskmode
  python main.py --market-review    # onlyrunningmarket review
        '''
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='enableddebugmode，outputdetailedlog'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='only getdata，notproceed AI analyzing'
    )

    parser.add_argument(
        '--stocks',
        type=str,
        help='specifiedneedanalyzingstock code，comma-separated（overrideconfiguration file）'
    )

    parser.add_argument(
        '--no-notify',
        action='store_true',
        help='notsendingpushnotification'
    )

    parser.add_argument(
        '--single-notify',
        action='store_true',
        help='enabledsingle stockpushmode：eachanalyzingcomplete one stockstockimmediatelypush，andis notsummarypush'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=None,
        help='concurrencythread count（defaultuseconfiguration value）'
    )

    parser.add_argument(
        '--schedule',
        action='store_true',
        help='enabledscheduled taskmode，dailyfixedwhenexecute'
    )

    parser.add_argument(
        '--no-run-immediately',
        action='store_true',
        help='scheduled taskstartwhennotimmediatelyexecuteonce'
    )

    parser.add_argument(
        '--report-mode',
        type=str,
        choices=['brief', 'full'],
        default=None,
        help='Report mode: brief (macro + thesis only) or full (complete analysis)'
    )

    parser.add_argument(
        '--market-review',
        action='store_true',
        help='onlyrunningmarket reviewanalyzing'
    )

    parser.add_argument(
        '--no-market-review',
        action='store_true',
        help='skipmarket reviewanalyzing'
    )

    parser.add_argument(
        '--force-run',
        action='store_true',
        help='skiptrading daycheck，mandatoryexecutefullanalyzing（Issue #373）'
    )

    parser.add_argument(
        '--webui',
        action='store_true',
        help='start Web manageboundaryaspect'
    )

    parser.add_argument(
        '--webui-only',
        action='store_true',
        help='only start Web service，notexecuteautomaticanalyzing'
    )

    parser.add_argument(
        '--serve',
        action='store_true',
        help='start FastAPI backendservice（simultaneouslyexecuteanalyzingtask）'
    )

    parser.add_argument(
        '--serve-only',
        action='store_true',
        help='only start FastAPI backendservice，notautomaticexecuteanalyzing'
    )

    parser.add_argument(
        '--port',
        type=int,
        default=8000,
        help='FastAPI serviceport（default 8000）'
    )

    parser.add_argument(
        '--host',
        type=str,
        default='0.0.0.0',
        help='FastAPI servicelistening address（default 0.0.0.0）'
    )

    parser.add_argument(
        '--no-context-snapshot',
        action='store_true',
        help='notsavinganalyzingcontextsnapshot'
    )

    # === Backtest ===
    parser.add_argument(
        '--backtest',
        action='store_true',
        help='runningbacktest（tohistoricalanalysis resultproceedevaluation）'
    )

    parser.add_argument(
        '--backtest-code',
        type=str,
        default=None,
        help='onlybacktestspecifiedstock code'
    )

    parser.add_argument(
        '--backtest-days',
        type=int,
        default=None,
        help='backtestevaluationwindow（trading daycount，defaultuseconfiguration）'
    )

    parser.add_argument(
        '--backtest-force',
        action='store_true',
        help='mandatorybacktest（i.e.useexistingbacktest resultalsore-calculating）'
    )

    return parser.parse_args()


def _compute_trading_day_filter(
    config: Config,
    args: argparse.Namespace,
    stock_codes: List[str],
) -> Tuple[List[str], Optional[str], bool]:
    """
    Compute filtered stock list and effective market review region (Issue #373).

    Returns:
        (filtered_codes, effective_region, should_skip_all)
        - effective_region None = use config default (check disabled)
        - effective_region '' = all relevant markets closed, skip market review
        - should_skip_all: skip entire run when no stocks and no market review to run
    """
    force_run = getattr(args, 'force_run', False)
    if force_run or not getattr(config, 'trading_day_check_enabled', True):
        return (stock_codes, None, False)

    from src.core.trading_calendar import (
        get_market_for_stock,
        get_open_markets_today,
        compute_effective_region,
    )

    open_markets = get_open_markets_today()
    filtered_codes = []
    for code in stock_codes:
        mkt = get_market_for_stock(code)
        if mkt in open_markets or mkt is None:
            filtered_codes.append(code)

    if config.market_review_enabled and not getattr(args, 'no_market_review', False):
        effective_region = compute_effective_region(
            getattr(config, 'market_review_region', 'global') or 'global', open_markets
        )
    else:
        effective_region = None

    should_skip_all = (not filtered_codes) and (effective_region or '') == ''
    return (filtered_codes, effective_region, should_skip_all)


def run_full_analysis(
    config: Config,
    args: argparse.Namespace,
    stock_codes: Optional[List[str]] = None
):
    """
    executecompleteanalyzingprocess（individual stock + market review）

    thisisscheduled taskcallmainfunction
    """
    try:
        # Issue #529: Hot-reload STOCK_LIST from .env on each scheduled run
        if stock_codes is None:
            config.refresh_stock_list()

        # Issue #373: Trading day filter (per-stock, per-market)
        effective_codes = stock_codes if stock_codes is not None else config.stock_list
        filtered_codes, effective_region, should_skip = _compute_trading_day_filter(
            config, args, effective_codes
        )
        if should_skip:
            logger.info(
                "todayallrelatedmarketaverageasnon-trading day，skipexecute。canuse --force-run mandatoryexecute。"
            )
            return
        if set(filtered_codes) != set(effective_codes):
            skipped = set(effective_codes) - set(filtered_codes)
            logger.info("todayrestmarketstockskipped: %s", skipped)
        stock_codes = filtered_codes

        # commandrowparameter --single-notify overrideconfiguration（#55）
        if getattr(args, 'single_notify', False):
            config.single_stock_notify = True

        # Issue #190: individual stockwithmarket reviewmergingpush
        merge_notification = (
            getattr(config, 'merge_email_notification', False)
            and config.market_review_enabled
            and not getattr(args, 'no_market_review', False)
            and not config.single_stock_notify
        )

        # creatingscheduler
        save_context_snapshot = None
        if getattr(args, 'no_context_snapshot', False):
            save_context_snapshot = False
        query_id = uuid.uuid4().hex
        pipeline = StockAnalysisPipeline(
            config=config,
            max_workers=args.workers,
            query_id=query_id,
            query_source="cli",
            save_context_snapshot=save_context_snapshot
        )

        # 1. runningindividual stock analysis
        results = pipeline.run(
            stock_codes=stock_codes,
            dry_run=args.dry_run,
            send_notification=not args.no_notify,
            merge_notification=merge_notification
        )

        # Issue #128: analyzinginterval - inindividual stock analysisandmarket index analysisbetweenadddelay
        analysis_delay = getattr(config, 'analysis_delay', 0)
        if (
            analysis_delay > 0
            and config.market_review_enabled
            and not args.no_market_review
            and effective_region != ''
        ):
            logger.info(f"waiting {analysis_delay} secondsafterexecute market review（avoidAPIrate limiting）...")
            time.sleep(analysis_delay)

        # 2. runningmarket review（ifenabledandis notonlyindividual stockmode）
        market_report = ""
        if (
            config.market_review_enabled
            and not args.no_market_review
            and effective_region != ''
        ):
            review_result = run_market_review(
                notifier=pipeline.notifier,
                analyzer=pipeline.analyzer,
                search_service=pipeline.search_service,
                send_notification=not args.no_notify,
                merge_notification=merge_notification,
                override_region=effective_region,
            )
            # if existsresult，assignvaluegive market_report foraftercontinueFeishudocumentgenerating
            if review_result:
                market_report = review_result

        # Issue #190: mergingpush（individual stock+market review）
        if merge_notification and (results or market_report) and not args.no_notify:
            parts = []
            if market_report:
                parts.append(f"# 📈 market review\n\n{market_report}")
            if results:
                dashboard_content = pipeline.notifier.generate_aggregate_report(
                    results,
                    getattr(config, 'report_type', 'simple'),
                )
                parts.append(f"# 🚀 individual stockdecisiondashboard\n\n{dashboard_content}")
            if parts:
                combined_content = "\n\n---\n\n".join(parts)
                if pipeline.notifier.is_available():
                    if pipeline.notifier.send(combined_content, email_send_to_all=True):
                        logger.info("alreadymergingpush（individual stock+market review）")
                    else:
                        logger.warning("mergingpushfailed")

        # outputsummary
        if results:
            logger.info("\n===== analysis resultsummary =====")
            for r in sorted(results, key=lambda x: x.sentiment_score, reverse=True):
                emoji = r.get_emoji()
                logger.info(
                    f"{emoji} {r.name}({r.code}): {r.operation_advice} | "
                    f"score {r.sentiment_score} | {r.trend_prediction}"
                )

        logger.info("\ntaskexecutecompleted")

        # === add new：generatingFeishucloud document ===
        try:
            from src.feishu_doc import FeishuDocManager

            feishu_doc = FeishuDocManager()
            if feishu_doc.is_configured() and (results or market_report):
                logger.info("currentlycreatingFeishucloud document...")

                # 1. preparetitle "01-01 13:01market review"
                tz_cn = timezone(timedelta(hours=8))
                now = datetime.now(tz_cn)
                doc_title = f"{now.strftime('%Y-%m-%d %H:%M')} market review"

                # 2. preparecontent (concatenateindividual stock analysisandmarket review)
                full_content = ""

                # addmarket reviewcontent（if exists）
                if market_report:
                    full_content += f"# 📈 market review\n\n{market_report}\n\n---\n\n"

                # addindividual stockdecisiondashboard（use NotificationService generating，by report_type minutesupport）
                if results:
                    dashboard_content = pipeline.notifier.generate_aggregate_report(
                        results,
                        getattr(config, 'report_type', 'simple'),
                    )
                    full_content += f"# 🚀 individual stockdecisiondashboard\n\n{dashboard_content}"

                # 3. creatingdocument
                doc_url = feishu_doc.create_daily_doc(doc_title, full_content)
                if doc_url:
                    logger.info(f"Feishucloud documentcreatingsuccessful: {doc_url}")
                    # optional：willdocumentlinkalsopushtogroupinside
                    if not args.no_notify:
                        pipeline.notifier.send(f"[{now.strftime('%Y-%m-%d %H:%M')}] reviewdocumentcreatingsuccessful: {doc_url}")

        except Exception as e:
            logger.error(f"Feishudocumentgeneratingfailed: {e}")

        # === Auto backtest ===
        try:
            if getattr(config, 'backtest_enabled', False):
                from src.services.backtest_service import BacktestService

                logger.info("startingauto backtest...")
                service = BacktestService()
                stats = service.run_backtest(
                    force=False,
                    eval_window_days=getattr(config, 'backtest_eval_window_days', 10),
                    min_age_days=getattr(config, 'backtest_min_age_days', 14),
                    limit=200,
                )
                logger.info(
                    f"auto backtestcompleted: processed={stats.get('processed')} saved={stats.get('saved')} "
                    f"completed={stats.get('completed')} insufficient={stats.get('insufficient')} errors={stats.get('errors')}"
                )
        except Exception as e:
            logger.warning(f"auto backtestfailed（alreadyignore）: {e}")

    except Exception as e:
        logger.exception(f"analyzingprocessexecutefailed: {e}")


def start_api_server(host: str, port: int, config: Config) -> None:
    """
    in backgroundthreadstart FastAPI service
    
    Args:
        host: listening address
        port: listenport
        config: configurationobject
    """
    import threading
    import uvicorn

    def run_server():
        level_name = (config.log_level or "INFO").lower()
        uvicorn.run(
            "api.app:app",
            host=host,
            port=port,
            log_level=level_name,
            log_config=None,
        )

    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()
    logger.info(f"FastAPI servicestarted: http://{host}:{port}")


def _is_truthy_env(var_name: str, default: str = "true") -> bool:
    """Parse common truthy / falsy environment values."""
    value = os.getenv(var_name, default).strip().lower()
    return value not in {"0", "false", "no", "off"}

def start_bot_stream_clients(config: Config) -> None:
    """Start bot stream clients when enabled in config."""
    # startDingTalk Stream client
    if config.dingtalk_stream_enabled:
        try:
            from bot.platforms import start_dingtalk_stream_background, DINGTALK_STREAM_AVAILABLE
            if DINGTALK_STREAM_AVAILABLE:
                if start_dingtalk_stream_background():
                    logger.info("[Main] Dingtalk Stream client started in background.")
                else:
                    logger.warning("[Main] Dingtalk Stream client failed to start.")
            else:
                logger.warning("[Main] Dingtalk Stream enabled but SDK is missing.")
                logger.warning("[Main] Run: pip install dingtalk-stream")
        except Exception as exc:
            logger.error(f"[Main] Failed to start Dingtalk Stream client: {exc}")

    # startFeishu Stream client
    if getattr(config, 'feishu_stream_enabled', False):
        try:
            from bot.platforms import start_feishu_stream_background, FEISHU_SDK_AVAILABLE
            if FEISHU_SDK_AVAILABLE:
                if start_feishu_stream_background():
                    logger.info("[Main] Feishu Stream client started in background.")
                else:
                    logger.warning("[Main] Feishu Stream client failed to start.")
            else:
                logger.warning("[Main] Feishu Stream enabled but SDK is missing.")
                logger.warning("[Main] Run: pip install lark-oapi")
        except Exception as exc:
            logger.error(f"[Main] Failed to start Feishu Stream client: {exc}")


def main() -> int:
    """
    mainentryfunction

    Returns:
        logoutcode（0 indicatessuccessful）
    """
    # parsingcommandrowparameter
    args = parse_arguments()

    # loadingconfiguration（insettingslogbeforeloading，withgetlogdirectory）
    config = get_config()

    # Configure logging（outputtoconsoleandfile）
    setup_logging(log_prefix="stock_analysis", debug=args.debug, log_dir=config.log_dir)

    logger.info("=" * 60)
    logger.info("A-share Stock Intelligent Analysis System start")
    logger.info(f"runningtime: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # verificationconfiguration
    warnings = config.validate()
    for warning in warnings:
        logger.warning(warning)

    # parsingstocklist（unifiedasuppercase Issue #355）
    stock_codes = None
    if args.stocks:
        stock_codes = [canonical_stock_code(c) for c in args.stocks.split(',') if (c or "").strip()]
        logger.info(f"usecommandrowspecifiedstocklist: {stock_codes}")

    # === processing --webui / --webui-only parameter，mappingto --serve / --serve-only ===
    if args.webui:
        args.serve = True
    if args.webui_only:
        args.serve_only = True

    # backward compatible WEBUI_ENABLED environment variable
    if config.webui_enabled and not (args.serve or args.serve_only):
        args.serve = True

    # === start Web service (ifenabled) ===
    start_serve = (args.serve or args.serve_only) and os.getenv("GITHUB_ACTIONS") != "true"

    # backward compatible WEBUI_HOST/WEBUI_PORT：ifusernotvia --host/--port specified，thenuseoldchangevolume
    if start_serve:
        if args.host == '0.0.0.0' and os.getenv('WEBUI_HOST'):
            args.host = os.getenv('WEBUI_HOST')
        if args.port == 8000 and os.getenv('WEBUI_PORT'):
            args.port = int(os.getenv('WEBUI_PORT'))

    bot_clients_started = False
    if start_serve:
        if not prepare_webui_frontend_assets():
            logger.warning("frontendstaticcapitalsourcenotready，continuingstart FastAPI service（Web pagepossiblyunavailable）")
        try:
            start_api_server(host=args.host, port=args.port, config=config)
            bot_clients_started = True
        except Exception as e:
            logger.error(f"start FastAPI servicefailed: {e}")

    if bot_clients_started:
        start_bot_stream_clients(config)

    # === only Web servicemode：notautomaticexecuteanalyzing ===
    if args.serve_only:
        logger.info("mode: only Web service")
        logger.info(f"Web servicerunning: http://{args.host}:{args.port}")
        logger.info("via /api/v1/analysis/analyze API/interfacetriggeranalyzing")
        logger.info(f"API document: http://{args.host}:{args.port}/docs")
        logger.info("by Ctrl+C logout...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("\nuserindisconnect，programlogout")
        return 0

    try:
        # mode0: backtest
        if getattr(args, 'backtest', False):
            logger.info("mode: backtest")
            from src.services.backtest_service import BacktestService

            service = BacktestService()
            stats = service.run_backtest(
                code=getattr(args, 'backtest_code', None),
                force=getattr(args, 'backtest_force', False),
                eval_window_days=getattr(args, 'backtest_days', None),
            )
            logger.info(
                f"backtestcompleted: processed={stats.get('processed')} saved={stats.get('saved')} "
                f"completed={stats.get('completed')} insufficient={stats.get('insufficient')} errors={stats.get('errors')}"
            )
            return 0

        # mode1: onlymarket review
        if args.market_review:
            from src.analyzer import GeminiAnalyzer
            from src.core.market_review import run_market_review
            from src.notification import NotificationService
            from src.search_service import SearchService

            # Issue #373: Trading day check for market-review-only mode.
            # Do NOT use _compute_trading_day_filter here: that helper checks
            # config.market_review_enabled, which would wrongly block an
            # explicit --market-review invocation when the flag is disabled.
            effective_region = None
            if not getattr(args, 'force_run', False) and getattr(config, 'trading_day_check_enabled', True):
                from src.core.trading_calendar import get_open_markets_today, compute_effective_region as _compute_region
                open_markets = get_open_markets_today()
                effective_region = _compute_region(
                    getattr(config, 'market_review_region', 'global') or 'global', open_markets
                )
                if effective_region == '':
                    logger.info("todaymarket reviewrelatedmarketaverageasnon-trading day，skipexecute。canuse --force-run mandatoryexecute。")
                    return 0

            logger.info("mode: onlymarket review")
            notifier = NotificationService()

            # initializingsearchserviceandanalyzinghandler（if existsconfiguration）
            search_service = None
            analyzer = None

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
                    news_strategy_profile=getattr(config, "news_strategy_profile", "short"),
                )

            if config.gemini_api_key or config.openai_api_key:
                analyzer = GeminiAnalyzer(api_key=config.gemini_api_key)
                if not analyzer.is_available():
                    logger.warning("AI analyzinghandlerinitializingafterunavailable，pleasecheck API Key configuration")
                    analyzer = None
            else:
                logger.warning("not detected API Key (Gemini/OpenAI)，willonlyuseTemplategeneratingreport")

            run_market_review(
                notifier=notifier,
                analyzer=analyzer,
                search_service=search_service,
                send_notification=not args.no_notify,
                override_region=effective_region,
            )
            return 0

        # mode2: scheduled taskmode
        if args.schedule or config.schedule_enabled:
            logger.info("mode: scheduled task")
            logger.info(f"dailyexecution time: {config.schedule_time}")

            # Determine whether to run immediately:
            # Command line arg --no-run-immediately overrides config if present.
            # Otherwise use config (defaults to True).
            should_run_immediately = config.schedule_run_immediately
            if getattr(args, 'no_run_immediately', False):
                should_run_immediately = False

            logger.info(f"startwhenimmediatelyexecute: {should_run_immediately}")

            from src.scheduler import run_with_schedule

            def scheduled_task():
                run_full_analysis(config, args, stock_codes)

            run_with_schedule(
                task=scheduled_task,
                schedule_time=config.schedule_time,
                run_immediately=should_run_immediately
            )
            return 0

        # mode3: normalsinglerunning
        if config.run_immediately:
            run_full_analysis(config, args, stock_codes)
        else:
            logger.info("configurationasnotimmediatelyrunninganalyzing (RUN_IMMEDIATELY=false)")

        logger.info("\nprogramexecutecompleted")

        # ifenabledserviceandisnon-scheduled taskmode，maintainprogramrunning
        keep_running = start_serve and not (args.schedule or config.schedule_enabled)
        if keep_running:
            logger.info("API servicerunning (by Ctrl+C logout)...")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass

        return 0

    except KeyboardInterrupt:
        logger.info("\nuserindisconnect，programlogout")
        return 130

    except Exception as e:
        logger.exception(f"programexecutefailed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
