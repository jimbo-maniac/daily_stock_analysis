# -*- coding: utf-8 -*-
"""
===================================
TushareFetcher - backup data source 1 (Priority 2)
===================================

Data sources:Tushare Pro API（mineadverb markerrabbit）
features：need Token、hasrequestquotaconstraint
advantages：dataqualityvolumehigh、API/interfacestable

streamcontrolstrategy：
1. implement"eachminutescallcounter"
2. exceedfreequota（80times/minute）when，mandatorysleeptobelowoneminutes
3. Use tenacity for exponential backoff retry
"""

import json as _json
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict, Any

import pandas as pd
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from .base import BaseFetcher, DataFetchError, RateLimitError, STANDARD_COLUMNS,is_bse_code, is_st_stock, is_kc_cy_stock, normalize_stock_code, _is_hk_market
from .realtime_types import UnifiedRealtimeQuote, ChipDistribution
from src.config import get_config
import os
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


# ETF code prefixes by exchange
# Shanghai: 51xxxx, 52xxxx, 56xxxx, 58xxxx
# Shenzhen: 15xxxx, 16xxxx, 18xxxx
_ETF_SH_PREFIXES = ('51', '52', '56', '58')
_ETF_SZ_PREFIXES = ('15', '16', '18')
_ETF_ALL_PREFIXES = _ETF_SH_PREFIXES + _ETF_SZ_PREFIXES


def _is_etf_code(stock_code: str) -> bool:
    """
    Check if the code is an ETF fund code.

    ETF code ranges:
    - Shanghai ETF: 51xxxx, 52xxxx, 56xxxx, 58xxxx
    - Shenzhen ETF: 15xxxx, 16xxxx, 18xxxx
    """
    code = stock_code.strip().split('.')[0]
    return code.startswith(_ETF_ALL_PREFIXES) and len(code) == 6


def _is_us_code(stock_code: str) -> bool:
    """
    check if code isUS stock
    
    US stockcode rules：
    - 1-5uppercase letters，e.g. 'AAPL', 'TSLA'
    - possiblypackageinclude '.'，e.g. 'BRK.B'
    """
    code = stock_code.strip().upper()
    return bool(re.match(r'^[A-Z]{1,5}(\.[A-Z])?$', code))


class TushareFetcher(BaseFetcher):
    """
    Tushare Pro datasource implementation
    
    priority：2
    Data sources:Tushare Pro API
    
    key strategy：
    - eachminutescallcounter，preventexceedquota
    - exceed 80 times/minuteswhenmandatorywaiting
    - failedafterindexbackoffretry
    
    quotaDescription（Tushare freeuser）：
    - eachminutesat most 80 timesrequest
    - eachdaysat most 500 timesrequest
    """
    
    name = "TushareFetcher"
    priority = int(os.getenv("TUSHARE_PRIORITY", "2"))  # defaultpriority，willin __init__ inbased onconfigurationdynamicadjust

    def __init__(self, rate_limit_per_minute: int = 80):
        """
        initializing TushareFetcher

        Args:
            rate_limit_per_minute: eachminutesmaxrequest count（default80，Tusharefreequota）
        """
        self.rate_limit_per_minute = rate_limit_per_minute
        self._call_count = 0  # currentminutesincallcount
        self._minute_start: Optional[float] = None  # currentcountperiodstart time
        self._api: Optional[object] = None  # Tushare API instance
        self.date_list: Optional[List[str]] = None  # trading daylistcache（reverse order，latestdateinbefore）
        self._date_list_end: Optional[str] = None  # cachetoshoulddeadlinedate，forcrossdayrefresh

        # tryinitializing API
        self._init_api()

        # based on API initializingresultdynamicadjustpriority
        self.priority = self._determine_priority()
    
    def _init_api(self) -> None:
        """
        initializing Tushare API
        
        if Token notconfiguration，thisdatasourcewillunavailable
        """
        config = get_config()
        
        if not config.tushare_token:
            logger.warning("Tushare Token notconfiguration，thisdata source unavailable")
            return
        
        try:
            import tushare as ts
            
            # Set Token
            ts.set_token(config.tushare_token)
            
            # Get API instance
            self._api = ts.pro_api()
            
            # Fix: tushare SDK 1.4.x hardcodes api.waditu.com/dataapi which may
            # be unavailable (503). Monkey-patch the query method to use the
            # official api.tushare.pro endpoint which posts to root URL.
            self._patch_api_endpoint(config.tushare_token)

            logger.info("Tushare API initializingsuccessful")
            
        except Exception as e:
            logger.error(f"Tushare API initializingfailed: {e}")
            self._api = None

    def _patch_api_endpoint(self, token: str) -> None:
        """
        Patch tushare SDK to use the official api.tushare.pro endpoint.

        The SDK (v1.4.x) hardcodes http://api.waditu.com/dataapi and appends
        /{api_name} to the URL. That endpoint may return 503, causing silent
        empty-DataFrame failures. This method replaces the query method to
        POST directly to http://api.tushare.pro (root URL, no path suffix).
        """
        import types

        TUSHARE_API_URL = "http://api.tushare.pro"
        _token = token
        _timeout = getattr(self._api, '_DataApi__timeout', 30)

        def patched_query(self_api, api_name, fields='', **kwargs):
            req_params = {
                'api_name': api_name,
                'token': _token,
                'params': kwargs,
                'fields': fields,
            }
            res = requests.post(TUSHARE_API_URL, json=req_params, timeout=_timeout)
            if res.status_code != 200:
                raise Exception(f"Tushare API HTTP {res.status_code}")
            result = _json.loads(res.text)
            if result['code'] != 0:
                raise Exception(result['msg'])
            data = result['data']
            columns = data['fields']
            items = data['items']
            return pd.DataFrame(items, columns=columns)

        self._api.query = types.MethodType(patched_query, self._api)
        logger.debug(f"Tushare API endpoint patched to {TUSHARE_API_URL}")

    def _determine_priority(self) -> int:
        """
        based on Token configurationand API initializingstatusdeterminepriority

        strategy：
        - Token configurationand API initializingsuccessful：priority -1（absolutelytohighest，better than efinance）
        - other cases：priority 2（default）

        Returns:
            prioritycountcharacter（0=highest，countcharactermorelargeprioritymorelow）
        """
        config = get_config()

        if config.tushare_token and self._api is not None:
            # Token configurationand API initializingsuccessful，improveashighest priority
            logger.info("✅ detectto TUSHARE_TOKEN and API initializingsuccessful，Tushare datasource priorityimproveashighest (Priority -1)")
            return -1

        # Token notconfigurationor API initializingfailed，maintaindefaultpriority
        return 2

    def is_available(self) -> bool:
        """
        checkdatasourcewhetheravailable

        Returns:
            True indicatesavailable，False indicatesunavailable
        """
        return self._api is not None

    def _check_rate_limit(self) -> None:
        """
        checkandexecution rateconstraint
        
        streamcontrolstrategy：
        1. checkwhetherenternewoneminutes
        2. if it is，resetcounter
        3. ifcurrentminutescallcountexceedconstraint，mandatorysleep
        """
        current_time = time.time()
        
        # checkwhetherneedresetcounter（newoneminutes）
        if self._minute_start is None:
            self._minute_start = current_time
            self._call_count = 0
        elif current_time - self._minute_start >= 60:
            # alreadythroughoneminutes，resetcounter
            self._minute_start = current_time
            self._call_count = 0
            logger.debug("rateconstraintcounteralreadyreset")
        
        # checkwhetherexceedquota
        if self._call_count >= self.rate_limit_per_minute:
            # calculatingneedwaitingtime（tobelowoneminutes）
            elapsed = current_time - self._minute_start
            sleep_time = max(0, 60 - elapsed) + 1  # +1 secondsbuffer
            
            logger.warning(
                f"Tushare reachtorateconstraint ({self._call_count}/{self.rate_limit_per_minute} times/minutes)，"
                f"waiting {sleep_time:.1f} seconds..."
            )
            
            time.sleep(sleep_time)
            
            # resetcounter
            self._minute_start = time.time()
            self._call_count = 0
        
        # increasecallcount
        self._call_count += 1
        logger.debug(f"Tushare currentminutescallcount: {self._call_count}/{self.rate_limit_per_minute}")

    def _call_api_with_rate_limit(self, method_name: str, **kwargs) -> pd.DataFrame:
        """unifiedviarateconstraintpackageinstall Tushare API call。"""
        if self._api is None:
            raise DataFetchError("Tushare API notinitializing，pleasecheck Token configuration")

        self._check_rate_limit()
        method = getattr(self._api, method_name)
        return method(**kwargs)

    def _get_china_now(self) -> datetime:
        """returnShanghaiwhenzonecurrenttime，methodconvenienttestingoverridecrossdayrefreshlogic。"""
        return datetime.now(ZoneInfo("Asia/Shanghai"))

    def _get_trade_dates(self, end_date: Optional[str] = None) -> List[str]:
        """bynaturaldayrefreshtrading calendarcache，avoidservicecrossdayaftercontinuingreuseolddayhistory。"""
        if self._api is None:
            return []

        china_now = self._get_china_now()
        requested_end_date = end_date or china_now.strftime("%Y%m%d")

        if self.date_list is not None and self._date_list_end == requested_end_date:
            return self.date_list

        start_date = (china_now - timedelta(days=20)).strftime("%Y%m%d")
        df_cal = self._call_api_with_rate_limit(
            "trade_cal",
            exchange="SSE",
            start_date=start_date,
            end_date=requested_end_date,
        )

        if df_cal is None or df_cal.empty or "cal_date" not in df_cal.columns:
            logger.warning("[Tushare] trade_cal returnis empty，unable toupdatingtrading calendarcache")
            self.date_list = []
            self._date_list_end = requested_end_date
            return self.date_list

        trade_dates = sorted(
            df_cal[df_cal["is_open"] == 1]["cal_date"].astype(str).tolist(),
            reverse=True,
        )
        self.date_list = trade_dates
        self._date_list_end = requested_end_date
        return trade_dates

    @staticmethod
    def _pick_trade_date(trade_dates: List[str], use_today: bool) -> Optional[str]:
        """based onavailabletrading daylistselectwhendaysorbeforeonetrading day。"""
        if not trade_dates:
            return None
        if use_today or len(trade_dates) == 1:
            return trade_dates[0]
        return trade_dates[1]
    
    def _convert_stock_code(self, stock_code: str) -> str:
        """
        convertingstock codeas Tushare format
        
        Tushare needrequestformat：
        - Shanghai marketstock：600519.SH
        - Shenzhen marketstock：000001.SZ
        - Shanghai market ETF：510050.SH, 563230.SH
        - Shenzhen market ETF：159919.SZ
        
        Args:
            stock_code: original code，e.g. '600519', '000001', '563230'
            
        Returns:
            Tushare formatcode，e.g. '600519.SH', '000001.SZ', '563230.SH'
        """
        code = stock_code.strip()
        
        # Already has suffix
        if '.' in code:
            return code.upper()

        # HK stocks are not supported by Tushare
        if _is_hk_market(code):
            raise DataFetchError(f"TushareFetcher not supportedHK stock {code}，please use AkshareFetcher")

        # ETF: determine exchange by prefix
        if code.startswith(_ETF_SH_PREFIXES) and len(code) == 6:
            return f"{code}.SH"
        if code.startswith(_ETF_SZ_PREFIXES) and len(code) == 6:
            return f"{code}.SZ"
        
        # BSE (Beijing Stock Exchange): 8xxxxx, 4xxxxx, 920xxx
        if is_bse_code(code):
            return f"{code}.BJ"
        
        # Regular stocks
        # Shanghai: 600xxx, 601xxx, 603xxx, 688xxx (STAR Market)
        # Shenzhen: 000xxx, 002xxx, 300xxx (ChiNext)
        if code.startswith(('600', '601', '603', '688')):
            return f"{code}.SH"
        elif code.startswith(('000', '002', '300')):
            return f"{code}.SZ"
        else:
            logger.warning(f"cannot determinestock {code} market，defaultuse Shenzhen market")
            return f"{code}.SZ"
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        from Tushare get rawdata
        
        based on codetypeselectnotsameAPI/interface：
        - normalstock：daily()
        - ETF fund：fund_daily()
        
        process：
        1. check API whetheravailable
        2. checkwhether isUS stock（not supported）
        3. execution rateconstraintcheck
        4. convertingstock codeformat
        5. based on codetypeselectAPI/interfaceand call
        """
        if self._api is None:
            raise DataFetchError("Tushare API notinitializing，pleasecheck Token configuration")
        
        # US stocks not supported
        if _is_us_code(stock_code):
            raise DataFetchError(f"TushareFetcher not supportedUS stock {stock_code}，please use AkshareFetcher or YfinanceFetcher")

        # HK stocks not supported
        if _is_hk_market(stock_code):
            raise DataFetchError(f"TushareFetcher not supportedHK stock {stock_code}，please use AkshareFetcher")
        
        # Rate-limit check
        self._check_rate_limit()
        
        # Convert code format
        ts_code = self._convert_stock_code(stock_code)
        
        # Convert date format (Tushare requires YYYYMMDD)
        ts_start = start_date.replace('-', '')
        ts_end = end_date.replace('-', '')
        
        is_etf = _is_etf_code(stock_code)
        api_name = "fund_daily" if is_etf else "daily"
        logger.debug(f"call Tushare {api_name}({ts_code}, {ts_start}, {ts_end})")
        
        try:
            if is_etf:
                # ETF uses fund_daily interface
                df = self._api.fund_daily(
                    ts_code=ts_code,
                    start_date=ts_start,
                    end_date=ts_end,
                )
            else:
                # Regular stocks use daily interface
                df = self._api.daily(
                    ts_code=ts_code,
                    start_date=ts_start,
                    end_date=ts_end,
                )
            
            return df
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # detectquotaover limit
            if any(keyword in error_msg for keyword in ['quota', 'quota', 'limit', 'permission']):
                logger.warning(f"Tushare quotapossiblyover limit: {e}")
                raise RateLimitError(f"Tushare quotaover limit: {e}") from e
            
            raise DataFetchError(f"Tushare getdatafailed: {e}") from e
    
    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        standardize Tushare data
        
        Tushare daily returned column names：
        ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount
        
        need to map to standard column names：
        date, open, high, low, close, volume, amount, pct_chg
        """
        df = df.copy()
        
        # column name mapping
        column_mapping = {
            'trade_date': 'date',
            'vol': 'volume',
            # open, high, low, close, amount, pct_chg column namesame
        }
        
        df = df.rename(columns=column_mapping)
        
        # convertingdateformat（YYYYMMDD -> YYYY-MM-DD）
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        
        # trading volumesingledigitconverting（Tushare  vol singledigitishand，needconvertingasstocks）
        if 'volume' in df.columns:
            df['volume'] = df['volume'] * 100
        
        # trading amountsingledigitconverting（Tushare  amount singledigitisthousandyuan，convertingasyuan）
        if 'amount' in df.columns:
            df['amount'] = df['amount'] * 1000
        
        # addstock codecolumn
        df['code'] = stock_code
        
        # keep only needed columns
        keep_cols = ['code'] + STANDARD_COLUMNS
        existing_cols = [col for col in keep_cols if col in df.columns]
        df = df[existing_cols]
        
        return df

    def get_stock_name(self, stock_code: str) -> Optional[str]:
        """
        getstockname
        
        use Tushare  stock_basic API/interfacegetstockbasicinfo
        
        Args:
            stock_code: stock code
            
        Returns:
            stockname，failedreturn None
        """
        if self._api is None:
            logger.warning("Tushare API notinitializing，cannot getstockname")
            return None

        # HK stocks not supported by Tushare stock_basic
        if _is_hk_market(stock_code):
            return None

        # checkcache
        if hasattr(self, '_stock_name_cache') and stock_code in self._stock_name_cache:
            return self._stock_name_cache[stock_code]
        
        # initializingcache
        if not hasattr(self, '_stock_name_cache'):
            self._stock_name_cache = {}
        
        try:
            # rateconstraintcheck
            self._check_rate_limit()
            
            # convertingcodeformat
            ts_code = self._convert_stock_code(stock_code)
            
            # ETF uses fund_basic, regular stocks use stock_basic
            if _is_etf_code(stock_code):
                df = self._api.fund_basic(
                    ts_code=ts_code,
                    fields='ts_code,name'
                )
            else:
                df = self._api.stock_basic(
                    ts_code=ts_code,
                    fields='ts_code,name'
                )
            
            if df is not None and not df.empty:
                name = df.iloc[0]['name']
                self._stock_name_cache[stock_code] = name
                logger.debug(f"Tushare getstocknamesuccessful: {stock_code} -> {name}")
                return name
            
        except Exception as e:
            logger.warning(f"Tushare getstocknamefailed {stock_code}: {e}")
        
        return None
    
    def get_stock_list(self) -> Optional[pd.DataFrame]:
        """
        getstocklist
        
        use Tushare  stock_basic API/interfacegetallstocklist
        
        Returns:
            packageinclude code, name column DataFrame，failedreturn None
        """
        if self._api is None:
            logger.warning("Tushare API notinitializing，cannot getstocklist")
            return None
        
        try:
            # rateconstraintcheck
            self._check_rate_limit()
            
            # call stock_basic API/interfaceget allstock
            df = self._api.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,name,industry,area,market'
            )
            
            if df is not None and not df.empty:
                # converting ts_code asstandardcodeformat
                df['code'] = df['ts_code'].apply(lambda x: x.split('.')[0])
                
                # updatingcache
                if not hasattr(self, '_stock_name_cache'):
                    self._stock_name_cache = {}
                for _, row in df.iterrows():
                    self._stock_name_cache[row['code']] = row['name']
                
                logger.info(f"Tushare getstocklistsuccessful: {len(df)} items")
                return df[['code', 'name', 'industry', 'area', 'market']]
            
        except Exception as e:
            logger.warning(f"Tushare getstocklistfailed: {e}")
        
        return None
    
    def get_realtime_quote(self, stock_code: str) -> Optional[UnifiedRealtimeQuote]:
        """
        get realtimequote/market data

        strategy：
        1. try first Pro API/interface（need2000points）：dataall，stable-nesshigh
        2. failedfallbacktoold versionAPI/interface：thresholdlow，datafewer

        Args:
            stock_code: stock code

        Returns:
            UnifiedRealtimeQuote object，failedreturn None
        """
        if self._api is None:
            return None

        # HK stocks not supported by Tushare
        if _is_hk_market(stock_code):
            logger.debug(f"TushareFetcher skipHK stockrealtimequote/market data {stock_code}")
            return None

        from .realtime_types import (
            RealtimeSource,
            safe_float, safe_int
        )

        # rateconstraintcheck
        self._check_rate_limit()

        # try Pro API/interface
        try:
            ts_code = self._convert_stock_code(stock_code)
            # trycall Pro realtimeAPI/interface (needpoints)
            df = self._api.quotation(ts_code=ts_code)

            if df is not None and not df.empty:
                row = df.iloc[0]
                logger.debug(f"Tushare Pro realtimequote/market datafetch successful: {stock_code}")

                return UnifiedRealtimeQuote(
                    code=stock_code,
                    name=str(row.get('name', '')),
                    source=RealtimeSource.TUSHARE,
                    price=safe_float(row.get('price')),
                    change_pct=safe_float(row.get('pct_chg')),  # Pro API/interfaceusuallyreturn directlyprice change percentage
                    change_amount=safe_float(row.get('change')),
                    volume=safe_int(row.get('vol')),
                    amount=safe_float(row.get('amount')),
                    high=safe_float(row.get('high')),
                    low=safe_float(row.get('low')),
                    open_price=safe_float(row.get('open')),
                    pre_close=safe_float(row.get('pre_close')),
                    turnover_rate=safe_float(row.get('turnover_ratio')), # Pro API/interfacepossiblyhasturnover rate
                    pe_ratio=safe_float(row.get('pe')),
                    pb_ratio=safe_float(row.get('pb')),
                    total_mv=safe_float(row.get('total_mv')),
                )
        except Exception as e:
            # onlyrecorddebuglog，noterror report，continuingtryfallback
            logger.debug(f"Tushare Pro realtimequote/market dataunavailable (possiblyispointsinsufficient): {e}")

        # fallback：tryold versionAPI/interface
        try:
            import tushare as ts

            # Tushare old versionAPI/interfaceuse 6 digitcode
            code_6 = stock_code.split('.')[0] if '.' in stock_code else stock_code

            # specialprocessingindexcode：old versionAPI/interfaceneedprefix (sh000001, sz399001)
            # simpleindexdeterminelogic
            if code_6 == '000001':  # Shanghai Compositeindex
                symbol = 'sh000001'
            elif code_6 == '399001':  # Shenzhen Component Index
                symbol = 'sz399001'
            elif code_6 == '399006':  # ChiNextrefers to
                symbol = 'sz399006'
            elif code_6 == '000300':  # Shanghai-Shenzhen300
                symbol = 'sh000300'
            elif is_bse_code(code_6):  # BSE
                symbol = f"bj{code_6}"
            else:
                symbol = code_6

            # callold versionrealtimeAPI/interface (ts.get_realtime_quotes)
            df = ts.get_realtime_quotes(symbol)

            if df is None or df.empty:
                return None

            row = df.iloc[0]

            # calculatingprice change percentage
            price = safe_float(row['price'])
            pre_close = safe_float(row['pre_close'])
            change_pct = 0.0
            change_amount = 0.0

            if price and pre_close and pre_close > 0:
                change_amount = price - pre_close
                change_pct = (change_amount / pre_close) * 100

            # buildunifiedobject
            return UnifiedRealtimeQuote(
                code=stock_code,
                name=str(row['name']),
                source=RealtimeSource.TUSHARE,
                price=price,
                change_pct=round(change_pct, 2),
                change_amount=round(change_amount, 2),
                volume=safe_int(row['volume']) // 100,  # convertingashand
                amount=safe_float(row['amount']),
                high=safe_float(row['high']),
                low=safe_float(row['low']),
                open_price=safe_float(row['open']),
                pre_close=pre_close,
            )

        except Exception as e:
            logger.warning(f"Tushare (old version) get realtimequote/market datafailed {stock_code}: {e}")
            return None

    def get_main_indices(self, region: str = "cn") -> Optional[List[dict]]:
        """
        get mainindexrealtimequote/market data (Tushare Pro)，only support A stocks
        """
        if region != "cn":
            return None
        if self._api is None:
            return None

        from .realtime_types import safe_float

        # indexmapping：Tusharecode -> name
        indices_map = {
            '000001.SH': 'Shanghai Compositeindex',
            '399001.SZ': 'Shenzhen Component Index',
            '399006.SZ': 'ChiNextrefers to',
            '000688.SH': 'STAR50',
            '000016.SH': 'Shanghai Composite50',
            '000300.SH': 'Shanghai-Shenzhen300',
        }

        try:
            self._check_rate_limit()

            # Tushare index_daily get historicaldata，realtimedataneeduseotherAPI/interfaceorestimate
            # byat Tushare freeuserpossiblycannot getindexrealtimequote/market data，hereact asasalternative
            # use index_daily get recenttrading daydata

            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - pd.Timedelta(days=5)).strftime('%Y%m%d')

            results = []

            # batchget allindexdata
            for ts_code, name in indices_map.items():
                try:
                    df = self._api.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
                    if df is not None and not df.empty:
                        row = df.iloc[0] # latestonedays

                        current = safe_float(row['close'])
                        prev_close = safe_float(row['pre_close'])

                        results.append({
                            'code': ts_code.split('.')[0], # compatible sh000001 formatneedconverting，heremaintainpurecountcharacter
                            'name': name,
                            'current': current,
                            'change': safe_float(row['change']),
                            'change_pct': safe_float(row['pct_chg']),
                            'open': safe_float(row['open']),
                            'high': safe_float(row['high']),
                            'low': safe_float(row['low']),
                            'prev_close': prev_close,
                            'volume': safe_float(row['vol']),
                            'amount': safe_float(row['amount']) * 1000, # thousandyuanconvertyuan
                            'amplitude': 0.0 # Tushare index_daily do not return directlyamplitude
                        })
                except Exception as e:
                    logger.debug(f"Tushare getindex {name} failed: {e}")
                    continue

            if results:
                return results
            else:
                logger.warning("[Tushare] failed to getindexquote/market datadata")

        except Exception as e:
            logger.error(f"[Tushare] getindexquote/market datafailed: {e}")

        return None

    def get_market_stats(self) -> Optional[dict]:
        """
        get market gain/loss statistics (Tushare Pro)
        2000points eachdaysaccessthisAPI/interface ts.pro_api().rt_k twice
        API/interfaceconstraintsee：https://tushare.pro/document/1?doc_id=108
        """
        if self._api is None:
            return None

        try:
            logger.info("[Tushare] ts.pro_api() get market statistics...")
            
            # getcurrentChinatime，determinewhether intradetimein
            china_now = self._get_china_now()
            current_clock = china_now.strftime("%H:%M")
            current_date = china_now.strftime("%Y%m%d")

            trade_dates = self._get_trade_dates(current_date)
            if not trade_dates:
                return None

            if current_date in trade_dates:
                if current_clock < '09:30' or current_clock > '16:30':
                    use_realtime = False
                else:
                    use_realtime = True
            else:
                use_realtime = False

            # iflive tradingwhenwaituse thenuseothercanlive tradinggetdatasource akshare、efinance
            if use_realtime:
                try:
                    df = self._call_api_with_rate_limit("rt_k", ts_code='3*.SZ,6*.SH,0*.SZ,92*.BJ')
                    if df is not None and not df.empty:
                        return self._calc_market_stats(df)
                    
                except Exception as e:
                    logger.error(f"[Tushare] ts.pro_api().rt_k tryget realtimedatafailed: {e}")
                    return None
            else:

                if current_date not in trade_dates:
                    last_date = self._pick_trade_date(trade_dates, use_today=True)  # takerecentdate
                else:
                    if current_clock < '09:30': 
                        last_date = self._pick_trade_date(trade_dates, use_today=False)  # retrievebeforeonedaysdata
                    else:  # i.e. '> 16:30'                  
                        last_date = self._pick_trade_date(trade_dates, use_today=True)  # retrievewhendaysdata

                if last_date is None:
                    return None

                try:
                    df = self._call_api_with_rate_limit(
                        "daily",
                        ts_code='3*.SZ,6*.SH,0*.SZ,92*.BJ',
                        start_date=last_date,
                        end_date=last_date,
                    )
                    # aspreventnotsameAPI/interfacereturned column namessizewriteinconsistent（for example rt_k returnsmallwrite，daily returnuppercase），unifiedwillcolumn nameconvertassmallwrite
                    df.columns = [col.lower() for col in df.columns]

                    # getstockbasicinfo（packageincludecodeandname）
                    df_basic = self._call_api_with_rate_limit("stock_basic", fields='ts_code,name')
                    df = pd.merge(df, df_basic, on='ts_code', how='left')
                    # will daily amount columnvaluemultiplywith 1000 fromandotherdatasourcekeep consistent
                    if 'amount' in df.columns:
                        df['amount'] = df['amount'] * 1000

                    if df is not None and not df.empty:
                        return self._calc_market_stats(df)
                except Exception as e:
                    logger.error(f"[Tushare] ts.pro_api().daily getdatafailed: {e}")
                    

            
        except Exception as e:
            logger.error(f"[Tushare] get market statisticsfailed: {e}")

        return None
    
    def _calc_market_stats(
            self,
            df: pd.DataFrame,
            ) -> Optional[Dict[str, Any]]:
            """fromquote/market data DataFrame calculatinggain/loss statistics。"""
            import numpy as np

            df = df.copy()
            
            # 1. extractbasiccomparedata：latest price、yesterday close
            # compatible with differentAPI/interfacereturned column names sina/em efinance tushare xtdata
            code_col = next((c for c in ['code', 'stock code', 'ts_code','stock_code'] if c in df.columns), None)
            name_col = next((c for c in ['name', 'stockname','name','name'] if c in df.columns), None)
            close_col = next((c for c in ['latest price', 'latest price', 'close','lastPrice'] if c in df.columns), None)
            pre_close_col = next((c for c in ['yesterday close', 'prev close', 'pre_close','lastClose'] if c in df.columns), None)
            amount_col = next((c for c in ['trading amount', 'trading amount', 'amount','amount'] if c in df.columns), None) 
            
            limit_up_count = 0
            limit_down_count = 0
            up_count = 0
            down_count = 0
            flat_count = 0

            for code, name, current_price, pre_close, amount in zip(
                df[code_col], df[name_col], df[close_col], df[pre_close_col], df[amount_col]
            ):
                
                # trading suspendedfiltering efinance trading suspendeddatasometimes missing price shown as '-'，em shown asnone
                if pd.isna(current_price) or pd.isna(pre_close) or current_price in ['-'] or pre_close in ['-'] or amount == 0:
                    continue
                
                # em、efinance asstr needconvertingasfloat
                current_price = float(current_price)
                pre_close = float(pre_close)
                
                # get numeric code without prefix
                pure_code = normalize_stock_code(str(code)) 

                # A. determine eachstockprice change percentageproportion (use numeric code to determine)
                if is_bse_code(pure_code): 
                    ratio = 0.30
                elif is_kc_cy_stock(pure_code): #pure_code.startswith(('688', '30')):
                    ratio = 0.20
                elif is_st_stock(name): #'ST' in str_name:
                    ratio = 0.05
                else:
                    ratio = 0.10

                # B. strictaccording to A stock rulescalculatinglimit up/down price：yesterday close * (1 ± proportion) -> round to2decimal places
                limit_up_price = np.floor(pre_close * (1 + ratio) * 100 + 0.5) / 100.0
                limit_down_price = np.floor(pre_close * (1 - ratio) * 100 + 0.5) / 100.0

                limit_up_price_Tolerance = round(abs(pre_close * (1 + ratio) - limit_up_price), 10)
                limit_down_price_Tolerance = round(abs(pre_close * (1 - ratio) - limit_down_price), 10)

                # C. exactcompare
                if current_price > 0 :
                    is_limit_up = (current_price > 0) and (abs(current_price - limit_up_price) <= limit_up_price_Tolerance)
                    is_limit_down = (current_price > 0) and (abs(current_price - limit_down_price) <= limit_down_price_Tolerance)

                    if is_limit_up:
                        limit_up_count += 1
                    if is_limit_down:
                        limit_down_count += 1

                    if current_price > pre_close:
                        up_count += 1
                    elif current_price < pre_close:
                        down_count += 1
                    else:
                        flat_count += 1
                    
            # count
            stats = {
                'up_count': up_count,
                'down_count': down_count,
                'flat_count': flat_count,
                'limit_up_count': limit_up_count,
                'limit_down_count': limit_down_count,
                'total_amount': 0.0,
            }
            
            # trading amountstatistics
            if amount_col and amount_col in df.columns:
                df[amount_col] = pd.to_numeric(df[amount_col], errors='coerce')
                stats['total_amount'] = (df[amount_col].sum() / 1e8)
                
            return stats

    def get_trade_time(self,early_time='09:30',late_time='16:30') -> Optional[str]:
        '''
        getcurrenttimecanobtaindatastart timedate

        Args:
                early_time: default '09:30'
                late_time: default '16:30'
                early_time-late_time betweenasusepreviouscounttrading daydatatime period，othertimeasusewhendaysdatatime period
        Returns:
                start_date: canobtaindatastartingdate
        '''
        china_now = self._get_china_now()
        china_date = china_now.strftime("%Y%m%d")
        china_clock = china_now.strftime("%H:%M")

        trade_dates = self._get_trade_dates(china_date)
        if not trade_dates:
            return None

        if china_date in trade_dates:
            if  early_time < china_clock < late_time: # usepreviouscounttrading daydatatime period
                use_today = False
            else:
                use_today = True
        else:
            use_today = False

        start_date = self._pick_trade_date(trade_dates, use_today=use_today)
        if start_date is None:
            return None

        if not use_today:
            logger.info(f"[Tushare] currenttime {china_clock} possiblycannot getwhendayschip distribution，trygetbeforeonecounttrading daydata {start_date}")

        return start_date
    
    def get_sector_rankings(self, n: int = 5) -> Optional[Tuple[list, list]]:
        """
        get industrysectorgain/loss rankings (Tushare Pro)
        
        datasource priority：
        1. TongHuaShunAPI/interface (ts.pro_api().moneyflow_ind_ths)
        2. EastmoneyAPI/interface (ts.pro_api().moneyflow_ind_dc)
        Note：eachAPI/interfaceindustryclassificationandsectordefinenotsame，willcauseresulttwoerinconsistent
        """
        def _get_rank_top_n(df: pd.DataFrame, change_col: str, industry_name: str, n: int) -> Tuple[list, list]:
            df[change_col] = pd.to_numeric(df[change_col], errors='coerce')
            df = df.dropna(subset=[change_col])

            # gain percentagebeforen
            top = df.nlargest(n, change_col)
            top_sectors = [
                {'name': row[industry_name], 'change_pct': row[change_col]}
                for _, row in top.iterrows()
            ]

            bottom = df.nsmallest(n, change_col)
            bottom_sectors = [
                {'name': row[industry_name], 'change_pct': row[change_col]}
                for _, row in bottom.iterrows()
            ]
            return top_sectors, bottom_sectors

        # 15:30ofafteronly thenhaswhendaysdata
        start_date = self.get_trade_time(early_time='00:00', late_time='15:30')
        if not start_date:
            return None

        # priorityTongHuaShunAPI/interface
        logger.info("[Tushare] ts.pro_api().moneyflow_ind_ths getsectorranking(TongHuaShun)...")
        try:
            df = self._call_api_with_rate_limit("moneyflow_ind_ths", trade_date=start_date)
            if df is not None and not df.empty:
                change_col = 'pct_change'
                name = 'industry'
                if change_col in df.columns:
                    return _get_rank_top_n(df, change_col, name, n)
        except Exception as e:
            logger.warning(f"[Tushare] getTongHuaShunindustrysectorgain/loss rankingsfailed: {e} tryEastmoneyAPI/interface")

        # TongHuaShunAPI/interfacefailed，fallbacktryEastmoneyAPI/interface
        logger.info("[Tushare] ts.pro_api().moneyflow_ind_dc getsectorranking(Eastmoney)...")
        try:
            df = self._call_api_with_rate_limit("moneyflow_ind_dc", trade_date=start_date)
            if df is not None and not df.empty:
                df = df[df['content_type'] == 'industry']  # filteringoutindustrysector
                change_col = 'pct_change'
                name = 'name'
                if change_col in df.columns:
                    return _get_rank_top_n(df, change_col, name, n)
        except Exception as e:
            logger.warning(f"[Tushare] getEastmoneyindustrysectorgain/loss rankingsfailed: {e}")
            return None
        
        # getis emptyorerAPI/interfacecallfailed，return None
        return None
    
    

    
    def get_chip_distribution(self, stock_code: str) -> Optional[ChipDistribution]:
        """
        get chip distributiondata
        
        Data sources:ts.pro_api().cyq_chips()
        packageinclude：profitproportion、average cost、chip concentration
        
        Note：ETF/indexno chip distributiondata，willreturn directly None
        5000pointswithbeloweachdaysaccess15times,eachhoursaccess5times
        
        Args:
            stock_code: stock code
            
        Returns:
            ChipDistribution object（latesttrading daydata），fetch failedreturn None

        """
        if _is_us_code(stock_code):
            logger.warning(f"[Tushare] TushareFetcher not supportedUS stock {stock_code} chip distribution")
            return None
        
        if _is_etf_code(stock_code):
            logger.warning(f"[Tushare] TushareFetcher not supported ETF {stock_code} chip distribution")
            return None
        
        try:
            # 19pointofafteronly thenhaswhendaysdata
            start_date = self.get_trade_time(early_time='00:00', late_time='19:00') 
            if not start_date:
                return None

            ts_code = self._convert_stock_code(stock_code)

            df = self._call_api_with_rate_limit(
                "cyq_chips",
                ts_code=ts_code,
                start_date=start_date,
                end_date=start_date,
            )
            if df is not None and not df.empty:
                daily_df = self._call_api_with_rate_limit(
                    "daily",
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=start_date,
                )
                if daily_df is None or daily_df.empty:
                    return None
                current_price = daily_df.iloc[0]['close']
                metrics = self.compute_cyq_metrics(df, current_price)

                chip = ChipDistribution(
                    code=stock_code,
                    date=datetime.strptime(start_date, '%Y%m%d').strftime('%Y-%m-%d'),
                    profit_ratio=metrics['profitproportion'],
                    avg_cost=metrics['average cost'],
                    cost_90_low=metrics['90cost-low'],
                    cost_90_high=metrics['90cost-high'],
                    concentration_90=metrics['90concentration'],
                    cost_70_low=metrics['70cost-low'],
                    cost_70_high=metrics['70cost-high'],
                    concentration_70=metrics['70concentration'],
                )
                
                logger.info(f"[chip distribution] {stock_code} date={chip.date}: profitproportion={chip.profit_ratio:.1%}, "
                        f"average cost={chip.avg_cost}, 90%concentration={chip.concentration_90:.2%}, "
                        f"70%concentration={chip.concentration_70:.2%}")
                return chip

        except Exception as e:
            logger.warning(f"[Tushare] get chip distributionfailed {stock_code}: {e}")
            return None

    def compute_cyq_metrics(self, df: pd.DataFrame, current_price: float) -> dict:
        """
        based on Tushare chip distributioncleardetailtable (cyq_chips) calculatingnormallyusechipindicator  
        :param df: packageinclude 'price' and 'percent' column DataFrame  
        :param current_price: stockwhendayscurrentprice/closing price (forcalculatingprofitproportion)  
        :return: packageincludeeachitemchipindicatordictionary  
        """
        import numpy as np
        # 1. ensurebypricefromsmalltolargesorting (Tushare returneddataoftenispurereverse order)
        df_sorted = df.sort_values(by='price', ascending=True).reset_index(drop=True)

        # 2. preventrawdata percent totalandproducegeneratefloating pointcounterror/tolerance，normalizeto 100%
        total_percent = df_sorted['percent'].sum()

        df_sorted['norm_percent'] = df_sorted['percent'] / total_percent * 100

        # 3. calculatingchipaccumulatepointsdistribute
        df_sorted['cumsum'] = df_sorted['norm_percent'].cumsum()

        # --- profitproportion ---
        # allprice <= currentpricechipofand
        winner_rate = df_sorted[df_sorted['price'] <= current_price]['norm_percent'].sum()

        # --- average cost ---
        # priceweightedaverage
        avg_cost = np.average(df_sorted['price'], weights=df_sorted['norm_percent'])

        # --- helper function：requestspecifiedcumulativeproportionatprice ---
        def get_percentile_price(target_pct):
            # find cumulativerequestandtheoncegreater thanetcattargetpercentagerowindex
            idx = df_sorted['cumsum'].searchsorted(target_pct)
            idx = min(idx, len(df_sorted) - 1) # preventmoreboundary
            return df_sorted.loc[idx, 'price']

        # --- 90% costzonewithconcentration ---
        # trimeach 5%
        cost_90_low = get_percentile_price(5)
        cost_90_high = get_percentile_price(95)
        if (cost_90_high + cost_90_low) != 0:
            concentration_90 = (cost_90_high - cost_90_low) / (cost_90_high + cost_90_low) * 100
        else:
            concentration_90 = 0.0
            
        # --- 70% costzonewithconcentration ---
        # trimeach 15%
        cost_70_low = get_percentile_price(15)
        cost_70_high = get_percentile_price(85)
        if (cost_70_high + cost_70_low) != 0:
            concentration_70 = (cost_70_high - cost_70_low) / (cost_70_high + cost_70_low) * 100
        else:
            concentration_70 = 0.0

        # returnformattingresult
        return {
            "profitproportion": round(winner_rate/100, 4), # /100 withaksharekeep consistent，returnsmallcountformat
            "average cost": round(avg_cost, 4),
            "90cost-low": round(cost_90_low, 4),
            "90cost-high": round(cost_90_high, 4),
            "90concentration": round(concentration_90/100, 4),
            "70cost-low": round(cost_70_low, 4),
            "70cost-high": round(cost_70_high, 4),
            "70concentration": round(concentration_70/100, 4)
        }



if __name__ == "__main__":
    # testingcode
    logging.basicConfig(level=logging.DEBUG)
    
    fetcher = TushareFetcher()
    
    try:
        # testinghistoricaldata
        df = fetcher.get_daily_data('600519')  # Maotai
        print(f"fetch successful，total {len(df)} itemsdata")
        print(df.tail())
        
        # testingstockname
        name = fetcher.get_stock_name('600519')
        print(f"stockname: {name}")
        
    except Exception as e:
        print(f"fetch failed: {e}")

    # testingmarket statistics
    print("\n" + "=" * 50)
    print("Testing get_market_stats (tushare)")
    print("=" * 50)
    try:
        stats = fetcher.get_market_stats()
        if stats:
            print(f"Market Stats successfully computed:")
            print(f"Up: {stats['up_count']} (Limit Up: {stats['limit_up_count']})")
            print(f"Down: {stats['down_count']} (Limit Down: {stats['limit_down_count']})")
            print(f"Flat: {stats['flat_count']}")
            print(f"Total Amount: {stats['total_amount']:.2f} hundred million (Yi)")
        else:
            print("Failed to compute market stats.")
    except Exception as e:
        print(f"Failed to compute market stats: {e}")


    # testingchip distributiondata
    print("\n" + "=" * 50)
    print("testingchip distributiondataget")
    print("=" * 50)
    try:
        chip = fetcher.get_chip_distribution('600519')  # Maotai
    except Exception as e:
        print(f"[chip distribution] fetch failed: {e}")

    # testingindustrysectorranking
    print("\n" + "=" * 50)
    print("testingindustrysectorrankingget")
    print("=" * 50)
    try:
        rankings = fetcher.get_sector_rankings(n=5)
        if rankings:
            top, bottom = rankings
            print("gain rankings Top 5:")
            for sector in top:
                print(f"{sector['name']}: {sector['change_pct']}%")
            print("\nloss rankings Top 5:")
            for sector in bottom:
                print(f"{sector['name']}: {sector['change_pct']}%")
        else:
            print("failed to getindustrysectorrankingdata")
    except Exception as e:
        print(f"[industrysectorranking] fetch failed: {e}")
