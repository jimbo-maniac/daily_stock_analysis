# -*- coding: utf-8 -*-
"""
===================================
BaostockFetcher - backup data source 2 (Priority 3)
===================================

Data sources:securitiestreasure（Baostock）
features：free、no need for Token、needloginmanage
advantages：stable、noquotaconstraint

key strategy：
1. manage bs.login() and bs.logout() lifecycleperiod
2. usecontextmanagerpreventconnectingleak
3. failedafterindexbackoffretry
"""

import logging
import re
from contextlib import contextmanager
from datetime import datetime
from typing import Optional, Generator

import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from .base import BaseFetcher, DataFetchError, STANDARD_COLUMNS, is_bse_code, _is_hk_market
import os

logger = logging.getLogger(__name__)


def _is_us_code(stock_code: str) -> bool:
    """
    check if code isUS stock
    
    US stockcode rules：
    - 1-5uppercase letters，e.g. 'AAPL', 'TSLA'
    - possiblypackageinclude '.'，e.g. 'BRK.B'
    """
    code = stock_code.strip().upper()
    return bool(re.match(r'^[A-Z]{1,5}(\.[A-Z])?$', code))


class BaostockFetcher(BaseFetcher):
    """
    Baostock datasource implementation
    
    priority：3
    Data sources:securitiestreasure Baostock API
    
    key strategy：
    - usecontextmanager managesconnectinglifecycleperiod
    - each timerequestallre-login/logout，preventconnectingleak
    - failedafterindexbackoffretry
    
    Baostock features：
    - free、no need forregister
    - needexplicitlogin/logout
    - dataupdatingstrategyhasdelay（T+1）
    """
    
    name = "BaostockFetcher"
    priority = int(os.getenv("BAOSTOCK_PRIORITY", "3"))
    
    def __init__(self):
        """initializing BaostockFetcher"""
        self._bs_module = None
    
    def _get_baostock(self):
        """
        delayloading baostock module
        
        onlyinfirsttimesusewhenimport，avoidnotsetupwhenerror report
        """
        if self._bs_module is None:
            import baostock as bs
            self._bs_module = bs
        return self._bs_module
    
    @contextmanager
    def _baostock_session(self) -> Generator:
        """
        Baostock connectingcontextmanager
        
        ensure：
        1. entercontextautomatically whenlogin
        2. logoutcontextautomatically whenlogout
        3. abnormalwhencan alsocorrectlogout
        
        useExample：
            with self._baostock_session():
                # inhereexecutedataquerying
        """
        bs = self._get_baostock()
        login_result = None
        
        try:
            # login Baostock
            login_result = bs.login()
            
            if login_result.error_code != '0':
                raise DataFetchError(f"Baostock loginfailed: {login_result.error_msg}")
            
            logger.debug("Baostock loginsuccessful")
            
            yield bs
            
        finally:
            # ensurelogout，preventconnectingleak
            try:
                logout_result = bs.logout()
                if logout_result.error_code == '0':
                    logger.debug("Baostock logoutsuccessful")
                else:
                    logger.warning(f"Baostock logoutabnormal: {logout_result.error_msg}")
            except Exception as e:
                logger.warning(f"Baostock logoutwhensendgenerateerror: {e}")
    
    def _convert_stock_code(self, stock_code: str) -> str:
        """
        convertingstock codeas Baostock format
        
        Baostock needrequestformat：
        - Shanghai market：sh.600519
        - Shenzhen market：sz.000001
        
        Args:
            stock_code: original code，e.g. '600519', '000001'
            
        Returns:
            Baostock formatcode，e.g. 'sh.600519', 'sz.000001'
        """
        code = stock_code.strip()

        # HK stocks are not supported by Baostock
        if _is_hk_market(code):
            raise DataFetchError(f"BaostockFetcher not supportedHK stock {code}，please use AkshareFetcher")

        # alreadythroughpackageincludeprefixsituation
        if code.startswith(('sh.', 'sz.')):
            return code.lower()
        
        # removepossiblysuffix
        code = code.replace('.SH', '').replace('.SZ', '').replace('.sh', '').replace('.sz', '')
        
        # ETF: Shanghai ETF (51xx, 52xx, 56xx, 58xx) -> sh; Shenzhen ETF (15xx, 16xx, 18xx) -> sz
        if len(code) == 6:
            if code.startswith(('51', '52', '56', '58')):
                return f"sh.{code}"
            if code.startswith(('15', '16', '18')):
                return f"sz.{code}"

        # determine market by code prefix
        if code.startswith(('600', '601', '603', '688')):
            return f"sh.{code}"
        elif code.startswith(('000', '002', '300')):
            return f"sz.{code}"
        else:
            logger.warning(f"cannot determinestock {code} market，defaultuse Shenzhen market")
            return f"sz.{code}"
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        from Baostock get rawdata
        
        use query_history_k_data_plus() get daily datadata
        
        process：
        1. checkwhether isUS stock（not supported）
        2. usecontextmanager managesconnecting
        3. convertingstock codeformat
        4. call API queryingdata
        5. willresultconvertingas DataFrame
        """
        # US stocknot supported，raiseabnormallet DataFetcherManager switch to otherdatasource
        if _is_us_code(stock_code):
            raise DataFetchError(f"BaostockFetcher not supportedUS stock {stock_code}，please use AkshareFetcher or YfinanceFetcher")

        # HK stocknot supported，raiseabnormallet DataFetcherManager switch to otherdatasource
        if _is_hk_market(stock_code):
            raise DataFetchError(f"BaostockFetcher not supportedHK stock {stock_code}，please use AkshareFetcher")

        # BSEnot supported，raiseabnormallet DataFetcherManager switch to otherdatasource
        if is_bse_code(stock_code):
            raise DataFetchError(
                f"BaostockFetcher not supportedBSE {stock_code}，will automatically switch to otherdatasource"
            )
        
        # convertingcodeformat
        bs_code = self._convert_stock_code(stock_code)
        
        logger.debug(f"call Baostock query_history_k_data_plus({bs_code}, {start_date}, {end_date})")
        
        with self._baostock_session() as bs:
            try:
                # queryingdaily linedata
                # adjustflag: 1-backward adjusted，2-forward adjusted，3-notadjusted
                rs = bs.query_history_k_data_plus(
                    code=bs_code,
                    fields="date,open,high,low,close,volume,amount,pctChg",
                    start_date=start_date,
                    end_date=end_date,
                    frequency="d",  # daily line
                    adjustflag="2"  # forward adjusted
                )
                
                if rs.error_code != '0':
                    raise DataFetchError(f"Baostock queryingfailed: {rs.error_msg}")
                
                # convertingas DataFrame
                data_list = []
                while rs.next():
                    data_list.append(rs.get_row_data())
                
                if not data_list:
                    raise DataFetchError(f"Baostock notqueryingto {stock_code} data")
                
                df = pd.DataFrame(data_list, columns=rs.fields)
                
                return df
                
            except Exception as e:
                if isinstance(e, DataFetchError):
                    raise
                raise DataFetchError(f"Baostock getdatafailed: {e}") from e
    
    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        standardize Baostock data
        
        Baostock returned column names：
        date, open, high, low, close, volume, amount, pctChg
        
        need to map to standard column names：
        date, open, high, low, close, volume, amount, pct_chg
        """
        df = df.copy()
        
        # column name mapping（onlyneedprocessing pctChg）
        column_mapping = {
            'pctChg': 'pct_chg',
        }
        
        df = df.rename(columns=column_mapping)
        
        # valuetypeconverting（Baostock returnedallisstring）
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'pct_chg']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
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
        
        use Baostock  query_stock_basic API/interfacegetstockbasicinfo
        
        Args:
            stock_code: stock code
            
        Returns:
            stockname，failedreturn None
        """
        # checkcache
        if hasattr(self, '_stock_name_cache') and stock_code in self._stock_name_cache:
            return self._stock_name_cache[stock_code]
        
        # initializingcache
        if not hasattr(self, '_stock_name_cache'):
            self._stock_name_cache = {}
        
        try:
            bs_code = self._convert_stock_code(stock_code)
            
            with self._baostock_session() as bs:
                # queryingstockbasicinfo
                rs = bs.query_stock_basic(code=bs_code)
                
                if rs.error_code == '0':
                    data_list = []
                    while rs.next():
                        data_list.append(rs.get_row_data())
                    
                    if data_list:
                        # Baostock returnedfield：code, code_name, ipoDate, outDate, type, status
                        fields = rs.fields
                        name_idx = fields.index('code_name') if 'code_name' in fields else None
                        if name_idx is not None and len(data_list[0]) > name_idx:
                            name = data_list[0][name_idx]
                            self._stock_name_cache[stock_code] = name
                            logger.debug(f"Baostock getstocknamesuccessful: {stock_code} -> {name}")
                            return name
                
        except Exception as e:
            logger.warning(f"Baostock getstocknamefailed {stock_code}: {e}")
        
        return None
    
    def get_stock_list(self) -> Optional[pd.DataFrame]:
        """
        getstocklist
        
        use Baostock  query_stock_basic API/interfacegetallstocklist
        
        Returns:
            packageinclude code, name column DataFrame，failedreturn None
        """
        try:
            with self._baostock_session() as bs:
                # queryingallstockbasicinfo
                rs = bs.query_stock_basic()
                
                if rs.error_code == '0':
                    data_list = []
                    while rs.next():
                        data_list.append(rs.get_row_data())
                    
                    if data_list:
                        df = pd.DataFrame(data_list, columns=rs.fields)
                        
                        # convertingcodeformat（remove sh. or sz. prefix）
                        df['code'] = df['code'].apply(lambda x: x.split('.')[1] if '.' in x else x)
                        df = df.rename(columns={'code_name': 'name'})
                        
                        # updatingcache
                        if not hasattr(self, '_stock_name_cache'):
                            self._stock_name_cache = {}
                        for _, row in df.iterrows():
                            self._stock_name_cache[row['code']] = row['name']
                        
                        logger.info(f"Baostock getstocklistsuccessful: {len(df)} items")
                        return df[['code', 'name']]
                
        except Exception as e:
            logger.warning(f"Baostock getstocklistfailed: {e}")
        
        return None


if __name__ == "__main__":
    # testingcode
    logging.basicConfig(level=logging.DEBUG)
    
    fetcher = BaostockFetcher()
    
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
