# -*- coding: utf-8 -*-
"""
===================================
A-share Stock Intelligent Analysis System - environmentverificationtesting
===================================

forverification .env configurationwhethercorrect，packagebracket：
1. configurationloadingtesting
2. databaseview
3. datasourcetesting
4. LLM calltesting
5. notificationpushtesting

usemethod：
    python test_env.py              # runningalltesting
    python test_env.py --db         # onlyviewdatabase
    python test_env.py --llm        # onlytesting LLM
    python test_env.py --fetch      # onlytestingdataget
    python test_env.py --notify     # onlytestingnotification

"""
import os
# Proxy config - controlled by USE_PROXY env var, off by default.
# Set USE_PROXY=true in .env if you need a local proxy (e.g. mainland China).
# GitHub Actions always skips this regardless of USE_PROXY.
if os.getenv("GITHUB_ACTIONS") != "true" and os.getenv("USE_PROXY", "false").lower() == "true":
    proxy_host = os.getenv("PROXY_HOST", "127.0.0.1")
    proxy_port = os.getenv("PROXY_PORT", "10809")
    proxy_url = f"http://{proxy_host}:{proxy_port}"
    os.environ["http_proxy"] = proxy_url
    os.environ["https_proxy"] = proxy_url

import argparse
import logging
import sys
from datetime import datetime, date, timedelta
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def print_header(title: str):
    """printtitle"""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def print_section(title: str):
    """printsmallholiday"""
    print(f"\n--- {title} ---")


def test_config():
    """testingconfigurationloading"""
    print_header("1. configurationloadingtesting")
    
    from src.config import get_config
    config = get_config()
    
    print_section("basicconfiguration")
    print(f"  stocklist: {config.stock_list}")
    print(f"  databasepath: {config.database_path}")
    print(f"  maxconcurrencycount: {config.max_workers}")
    print(f"  debugmode: {config.debug}")
    
    print_section("API configuration")
    print(f"  Tushare Token: {'alreadyconfiguration ✓' if config.tushare_token else 'notconfiguration ✗'}")
    if config.tushare_token:
        print(f"    Token before8digit: {config.tushare_token[:8]}...")
    
    print(f"  Gemini API Key: {'alreadyconfiguration ✓' if config.gemini_api_key else 'notconfiguration ✗'}")
    if config.gemini_api_key:
        print(f"    Key before8digit: {config.gemini_api_key[:8]}...")
    print(f"  Gemini mainmodel: {config.gemini_model}")
    print(f"  Gemini alternativemodel: {config.gemini_model_fallback}")
    
    print(f"  Enterprise WeChat Webhook: {'alreadyconfiguration ✓' if config.wechat_webhook_url else 'notconfiguration ✗'}")
    
    print_section("configurationverification")
    issues = config.validate_structured()
    _prefix = {"error": "  ✗", "warning": "  ⚠", "info": "  ·"}
    for issue in issues:
        print(f"{_prefix.get(issue.severity, '  ?')} [{issue.severity.upper()}] {issue.message}")
    if not any(i.severity in ("error", "warning") for i in issues):
        print("  ✓ keyconfiguration itemverificationvia")
    
    return True


def view_database():
    """viewdatabasecontent"""
    print_header("2. databasecontentview")
    
    from src.storage import get_db
    from sqlalchemy import text
    
    db = get_db()
    
    print_section("databaseconnecting")
    print(f"  ✓ connectingsuccessful")
    
    # useindependent session querying
    session = db.get_session()
    try:
        # statistics
        result = session.execute(text("""
            SELECT 
                code,
                COUNT(*) as count,
                MIN(date) as min_date,
                MAX(date) as max_date,
                data_source
            FROM stock_daily 
            GROUP BY code
            ORDER BY code
        """))
        stocks = result.fetchall()
        
        print_section(f"alreadystoragestockdata (total {len(stocks)} only)")
        if stocks:
            print(f"  {'code':<10} {'recordcount':<8} {'startdate':<12} {'latestdate':<12} {'datasource'}")
            print("  " + "-" * 60)
            for row in stocks:
                print(f"  {row[0]:<10} {row[1]:<8} {row[2]!s:<12} {row[3]!s:<12} {row[4] or 'Unknown'}")
        else:
            print("  temporarilynodata")
        
        # queryingtodaydata
        today = date.today()
        result = session.execute(text("""
            SELECT code, date, open, high, low, close, pct_chg, volume, ma5, ma10, ma20, volume_ratio
            FROM stock_daily 
            WHERE date = :today
            ORDER BY code
        """), {"today": today})
        today_data = result.fetchall()
        
        print_section(f"todaydata ({today})")
        if today_data:
            for row in today_data:
                code, dt, open_, high, low, close, pct_chg, volume, ma5, ma10, ma20, vol_ratio = row
                print(f"\n  【{code}】")
                print(f"    open: {open_:.2f}  highest: {high:.2f}  lowest: {low:.2f}  close: {close:.2f}")
                print(f"    price change percentage: {pct_chg:.2f}%  trading volume: {volume/10000:.2f}ten thousandstocks")
                print(f"    MA5: {ma5:.2f}  MA10: {ma10:.2f}  MA20: {ma20:.2f}  volume ratio: {vol_ratio:.2f}")
        else:
            print("  todaytemporarilynodata")
        
        # queryingrecent10itemsdata
        result = session.execute(text("""
            SELECT code, date, close, pct_chg, volume, data_source
            FROM stock_daily 
            ORDER BY date DESC, code
            LIMIT 10
        """))
        recent = result.fetchall()
        
        print_section("recent10itemsrecord")
        if recent:
            print(f"  {'code':<10} {'date':<12} {'close':<10} {'price change%':<8} {'trading volume':<15} {'source'}")
            print("  " + "-" * 70)
            for row in recent:
                vol_str = f"{row[4]/10000:.2f}ten thousand" if row[4] else "N/A"
                print(f"  {row[0]:<10} {row[1]!s:<12} {row[2]:<10.2f} {row[3]:<8.2f} {vol_str:<15} {row[5] or 'Unknown'}")
    finally:
        session.close()
    
    return True


def test_data_fetch(stock_code: str = "600519"):
    """testingdataget"""
    print_header("3. datagettesting")
    
    from data_provider import DataFetcherManager
    
    manager = DataFetcherManager()
    
    print_section("datasourcelist")
    for i, name in enumerate(manager.available_fetchers, 1):
        print(f"  {i}. {name}")
    
    print_section(f"get {stock_code} data")
    print(f"  currentlyget（possiblyneedseveralsecondsclock）...")
    
    try:
        df, source = manager.get_daily_data(stock_code, days=5)
        
        print(f"  ✓ fetch successful")
        print(f"    datasource: {source}")
        print(f"    recordcount: {len(df)}")
        
        print_section("datapreview（recent5items）")
        if not df.empty:
            preview_cols = ['date', 'open', 'high', 'low', 'close', 'pct_chg', 'volume']
            existing_cols = [c for c in preview_cols if c in df.columns]
            print(df[existing_cols].tail().to_string(index=False))
        
        return True
        
    except Exception as e:
        print(f"  ✗ fetch failed: {e}")
        return False


def test_llm():
    """testing LLM call"""
    print_header("4. LLM (Gemini) calltesting")
    
    from src.analyzer import GeminiAnalyzer
    from src.config import get_config
    import time
    
    config = get_config()
    
    print_section("modelconfiguration")
    print(f"  mainmodel: {config.gemini_model}")
    print(f"  alternativemodel: {config.gemini_model_fallback}")
    
    # checknetworkconnecting
    print_section("networkconnectingcheck")
    try:
        import socket
        socket.setdefaulttimeout(10)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect(("generativelanguage.googleapis.com", 443))
        print(f"  ✓ canconnectingto Google API servicehandler")
    except Exception as e:
        print(f"  ✗ unable toconnectingto Google API servicehandler: {e}")
        print(f"  Tip: pleasechecknetworkconnectingorconfigurationproxy")
        print(f"  Tip: cansettingsenvironment variable HTTPS_PROXY=http://your-proxy:port")
        return False
    
    analyzer = GeminiAnalyzer()
    
    print_section("modelinitializing")
    if analyzer.is_available():
        print(f"  ✓ modelinitializingsuccessful")
    else:
        print(f"  ✗ modelinitializingfailed（pleasecheck API Key）")
        return False
    
    # constructtestingcontext
    test_context = {
        'code': '600519',
        'date': date.today().isoformat(),
        'today': {
            'open': 1420.0,
            'high': 1435.0,
            'low': 1415.0,
            'close': 1428.0,
            'volume': 5000000,
            'amount': 7140000000,
            'pct_chg': 0.56,
            'ma5': 1425.0,
            'ma10': 1418.0,
            'ma20': 1410.0,
            'volume_ratio': 1.1,
        },
        'ma_status': 'long positionarrange 📈',
        'volume_change_ratio': 1.05,
        'price_change_ratio': 0.56,
    }
    
    print_section("sendingtestingrequest")
    print(f"  testingstock: Kweichow Moutai (600519)")
    print(f"  currentlycall Gemini API（timeout: 60seconds）...")
    
    start_time = time.time()
    
    try:
        result = analyzer.analyze(test_context)
        
        elapsed = time.time() - start_time
        print(f"\n  ✓ API callsuccessful (elapsed: {elapsed:.2f}seconds)")
        
        print_section("analysis result")
        print(f"  sentimentscore: {result.sentiment_score}/100")
        print(f"  trendprediction: {result.trend_prediction}")
        print(f"  operationrecommended: {result.operation_advice}")
        print(f"  technical analysis: {result.technical_analysis[:80]}..." if len(result.technical_analysis) > 80 else f"  technical analysis: {result.technical_analysis}")
        print(f"  messageaspect: {result.news_summary[:80]}..." if len(result.news_summary) > 80 else f"  messageaspect: {result.news_summary}")
        print(f"  compositesummary: {result.analysis_summary}")
        
        if not result.success:
            print(f"\n  ⚠ Note: {result.error_message}")
        
        return result.success
        
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"\n  ✗ API callfailed (elapsed: {elapsed:.2f}seconds)")
        print(f"  error: {e}")
        
        # providemoredetailederror message
        error_str = str(e).lower()
        if 'timeout' in error_str or 'unavailable' in error_str:
            print(f"\n  diagnose: networktimeout，possiblyreason:")
            print(f"    1. networknotthrough（needproxyaccess Google）")
            print(f"    2. API servicetemporarilywhenunavailable")
            print(f"    3. requestlarge volumebyrate limiting")
        elif 'invalid' in error_str or 'api key' in error_str:
            print(f"\n  diagnose: API Key possiblyinvalid")
        elif 'model' in error_str:
            print(f"\n  diagnose: modelnamepossiblynotcorrect，trymodify .env in GEMINI_MODEL")
        
        return False


def test_notification():
    """testingnotificationpush"""
    print_header("5. notificationpushtesting")
    
    from src.notification import NotificationService
    from src.config import get_config
    
    config = get_config()
    service = NotificationService()
    
    print_section("configurationcheck")
    if service.is_available():
        print(f"  ✓ Enterprise WeChat Webhook alreadyconfiguration")
        webhook_preview = config.wechat_webhook_url[:50] + "..." if len(config.wechat_webhook_url) > 50 else config.wechat_webhook_url
        print(f"    URL: {webhook_preview}")
    else:
        print(f"  ✗ Enterprise WeChat Webhook notconfiguration")
        return False
    
    print_section("sendingtestingmessage")
    
    test_message = f"""## 🧪 systemtestingmessage

thisisoneitemsfrom **A-share Stock Intelligent Analysis System** testingmessage。

- testingtime: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- testingitem: verificationEnterprise WeChat Webhook configuration

ifyou (formal)receivedthismessage，Descriptionnotificationfeatureconfigurationcorrect ✓"""
    
    print(f"  currentlysending...")
    
    try:
        success = service.send_to_wechat(test_message)
        
        if success:
            print(f"  ✓ messagesendingsuccessful，pleasecheckEnterprise WeChat")
        else:
            print(f"  ✗ messagesendingfailed")
        
        return success
        
    except Exception as e:
        print(f"  ✗ sendingabnormal: {e}")
        return False


def run_all_tests():
    """runningalltesting"""
    print("\n" + "🚀" * 20)
    print("  A-share Stock Intelligent Analysis System - environmentverification")
    print("  " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print("🚀" * 20)
    
    results = {}
    
    # 1. configurationtesting
    try:
        results['configurationloading'] = test_config()
    except Exception as e:
        print(f"  ✗ configurationtestingfailed: {e}")
        results['configurationloading'] = False
    
    # 2. databaseview
    try:
        results['database'] = view_database()
    except Exception as e:
        print(f"  ✗ databasetestingfailed: {e}")
        results['database'] = False
    
    # 3. dataget（skip，avoidtooslow）
    # results['dataget'] = test_data_fetch()
    
    # 4. LLM testing（optional）
    # results['LLMcall'] = test_llm()
    
    # summary
    print_header("testingresultsummary")
    for name, passed in results.items():
        status = "✓ via" if passed else "✗ failed"
        print(f"  {status}: {name}")
    
    print(f"\nTip: use --llm parameterseparatetesting LLM call")
    print(f"Tip: use --fetch parameterseparatetestingdataget")
    print(f"Tip: use --notify parameterseparatetestingnotificationpush")


def query_stock_data(stock_code: str, days: int = 10):
    """queryingspecifiedstockdata"""
    print_header(f"queryingstockdata: {stock_code}")
    
    from src.storage import get_db
    from sqlalchemy import text
    
    db = get_db()
    
    session = db.get_session()
    try:
        result = session.execute(text("""
            SELECT date, open, high, low, close, pct_chg, volume, amount, ma5, ma10, ma20, volume_ratio
            FROM stock_daily 
            WHERE code = :code
            ORDER BY date DESC
            LIMIT :limit
        """), {"code": stock_code, "limit": days})
        
        rows = result.fetchall()
        
        if rows:
            print(f"\n  recent {len(rows)} itemsrecord:\n")
            print(f"  {'date':<12} {'open':<10} {'highest':<10} {'lowest':<10} {'close':<10} {'price change%':<8} {'MA5':<10} {'MA10':<10} {'volume ratio':<8}")
            print("  " + "-" * 100)
            for row in rows:
                dt, open_, high, low, close, pct_chg, vol, amt, ma5, ma10, ma20, vol_ratio = row
                print(f"  {dt!s:<12} {open_:<10.2f} {high:<10.2f} {low:<10.2f} {close:<10.2f} {pct_chg:<8.2f} {ma5:<10.2f} {ma10:<10.2f} {vol_ratio:<8.2f}")
        else:
            print(f"  not found {stock_code} data")
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(
        description='A-share Stock Intelligent Analysis System - environmentverificationtesting',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument('--db', action='store_true', help='viewdatabasecontent')
    parser.add_argument('--llm', action='store_true', help='testing LLM call')
    parser.add_argument('--fetch', action='store_true', help='testingdataget')
    parser.add_argument('--notify', action='store_true', help='testingnotificationpush')
    parser.add_argument('--config', action='store_true', help='viewconfiguration')
    parser.add_argument('--stock', type=str, help='queryingspecifiedstockdata，e.g. --stock 600519')
    parser.add_argument('--all', action='store_true', help='runningalltesting（packagebracket LLM）')
    
    args = parser.parse_args()
    
    # if nospecifiedanyparameter，runningbasictesting
    if not any([args.db, args.llm, args.fetch, args.notify, args.config, args.stock, args.all]):
        run_all_tests()
        return 0
    
    # based onparameterrunningspecifiedtesting
    if args.config:
        test_config()
    
    if args.db:
        view_database()
    
    if args.stock:
        query_stock_data(args.stock)
    
    if args.fetch:
        test_data_fetch()
    
    if args.llm:
        test_llm()
    
    if args.notify:
        test_notification()
    
    if args.all:
        test_config()
        view_database()
        test_data_fetch()
        test_llm()
        test_notification()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
