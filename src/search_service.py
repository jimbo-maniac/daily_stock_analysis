# -*- coding: utf-8 -*-
"""
===================================
A-share Stock Intelligent Analysis System - searchservicemodule
===================================

Responsibilities:
1. provide unifiednewssearchAPI/interface
2. support Bocha、Tavily、Brave、SerpAPI、SearXNG multipletypesearchengine
3. multiple Key load balancingandfailureconvertmove
4. searchresultcacheandformatting
"""

import logging
import re
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import List, Dict, Any, Optional, Tuple
from itertools import cycle
import requests
from newspaper import Article, Config
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from data_provider.us_index_mapping import is_us_index_code
from src.config import (
    NEWS_STRATEGY_WINDOWS,
    normalize_news_strategy_profile,
    resolve_news_window_days,
)

logger = logging.getLogger(__name__)

# Transient network errors (retryable)
_SEARCH_TRANSIENT_EXCEPTIONS = (
    requests.exceptions.SSLError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(_SEARCH_TRANSIENT_EXCEPTIONS),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
def _post_with_retry(url: str, *, headers: Dict[str, str], json: Dict[str, Any], timeout: int) -> requests.Response:
    """POST with retry on transient SSL/network errors."""
    return requests.post(url, headers=headers, json=json, timeout=timeout)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(_SEARCH_TRANSIENT_EXCEPTIONS),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _get_with_retry(
    url: str, *, headers: Dict[str, str], params: Dict[str, Any], timeout: int
) -> requests.Response:
    """GET with retry on transient SSL/network errors."""
    return requests.get(url, headers=headers, params=params, timeout=timeout)


def fetch_url_content(url: str, timeout: int = 5) -> str:
    """
    get URL webpagebodycontent (use newspaper3k)
    """
    try:
        # configuration newspaper3k
        config = Config()
        config.browser_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        config.request_timeout = timeout
        config.fetch_images = False  # notdownloadingimage
        config.memoize_articles = False # notcache

        article = Article(url, config=config, language='zh') # defaultChinese，butalsosupportother
        article.download()
        article.parse()

        # getbody
        text = article.text.strip()

        # simpleafterprocessing，removeemptyrow
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        text = '\n'.join(lines)

        return text[:1500]  # constraintreturnlength（compared to bs4 slightlymultipleonepoint，becauseas newspaper parsingmoredonet）
    except Exception as e:
        logger.debug(f"Fetch content failed for {url}: {e}")

    return ""


@dataclass
class SearchResult:
    """searchresultdataclass"""
    title: str
    snippet: str  # summary
    url: str
    source: str  # sourcewebsite
    published_date: Optional[str] = None
    
    def to_text(self) -> str:
        """convertingas textformat"""
        date_str = f" ({self.published_date})" if self.published_date else ""
        return f"【{self.source}】{self.title}{date_str}\n{self.snippet}"


@dataclass 
class SearchResponse:
    """searchresponse"""
    query: str
    results: List[SearchResult]
    provider: str  # usesearchengine
    success: bool = True
    error_message: Optional[str] = None
    search_time: float = 0.0  # searchelapsed（seconds）
    
    def to_context(self, max_results: int = 5) -> str:
        """willsearchresultconvertingascanfor AI analyzingcontext"""
        if not self.success or not self.results:
            return f"search '{self.query}' not foundrelatedresult。"
        
        lines = [f"【{self.query} searchresult】（source：{self.provider}）"]
        for i, result in enumerate(self.results[:max_results], 1):
            lines.append(f"\n{i}. {result.to_text()}")
        
        return "\n".join(lines)


class BaseSearchProvider(ABC):
    """searchenginebaseclass"""
    
    def __init__(self, api_keys: List[str], name: str):
        """
        initializingsearchengine
        
        Args:
            api_keys: API Key list（support multiple key load balancing）
            name: searchenginename
        """
        self._api_keys = api_keys
        self._name = name
        self._key_cycle = cycle(api_keys) if api_keys else None
        self._key_usage: Dict[str, int] = {key: 0 for key in api_keys}
        self._key_errors: Dict[str, int] = {key: 0 for key in api_keys}
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def is_available(self) -> bool:
        """checkwhetherhasavailable API Key"""
        return bool(self._api_keys)
    
    def _get_next_key(self) -> Optional[str]:
        """
        getbelowonecountavailable API Key（load balancing）
        
        strategy：polling + skiperrormultiple key
        """
        if not self._key_cycle:
            return None
        
        # at mosttryall key
        for _ in range(len(self._api_keys)):
            key = next(self._key_cycle)
            # skiperrorcountmultiple key（exceed 3 times）
            if self._key_errors.get(key, 0) < 3:
                return key
        
        # all key allhasissue，reseterrorcountandreturntheonecount
        logger.warning(f"[{self._name}] all API Key allhaserrorrecord，reseterrorcount")
        self._key_errors = {key: 0 for key in self._api_keys}
        return self._api_keys[0] if self._api_keys else None
    
    def _record_success(self, key: str) -> None:
        """recordsuccessfuluse"""
        self._key_usage[key] = self._key_usage.get(key, 0) + 1
        # successfulafterreduceerrorcount
        if key in self._key_errors and self._key_errors[key] > 0:
            self._key_errors[key] -= 1
    
    def _record_error(self, key: str) -> None:
        """recorderror"""
        self._key_errors[key] = self._key_errors.get(key, 0) + 1
        logger.warning(f"[{self._name}] API Key {key[:8]}... errorcount: {self._key_errors[key]}")
    
    @abstractmethod
    def _do_search(self, query: str, api_key: str, max_results: int, days: int = 7) -> SearchResponse:
        """executesearch（sub-classimplement）"""
        pass
    
    def search(self, query: str, max_results: int = 5, days: int = 7) -> SearchResponse:
        """
        executesearch
        
        Args:
            query: searchkeyword
            max_results: maxreturnresultcount
            days: searchrecentseveraldaystimerange（default7days）
            
        Returns:
            SearchResponse object
        """
        api_key = self._get_next_key()
        if not api_key:
            return SearchResponse(
                query=query,
                results=[],
                provider=self._name,
                success=False,
                error_message=f"{self._name} notconfiguration API Key"
            )
        
        start_time = time.time()
        try:
            response = self._do_search(query, api_key, max_results, days=days)
            response.search_time = time.time() - start_time
            
            if response.success:
                self._record_success(api_key)
                logger.info(f"[{self._name}] search '{query}' successful，return {len(response.results)} results，elapsed {response.search_time:.2f}s")
            else:
                self._record_error(api_key)
            
            return response
            
        except Exception as e:
            self._record_error(api_key)
            elapsed = time.time() - start_time
            logger.error(f"[{self._name}] search '{query}' failed: {e}")
            return SearchResponse(
                query=query,
                results=[],
                provider=self._name,
                success=False,
                error_message=str(e),
                search_time=elapsed
            )


class TavilySearchProvider(BaseSearchProvider):
    """
    Tavily searchengine
    
    features：
    - specializedas AI/LLM optimizesearch API
    - freeversioneachmonth 1000 timesrequest
    - returnstructure-izesearchresult
    
    document：https://docs.tavily.com/
    """
    
    def __init__(self, api_keys: List[str]):
        super().__init__(api_keys, "Tavily")
    
    def _do_search(
        self,
        query: str,
        api_key: str,
        max_results: int,
        days: int = 7,
        topic: Optional[str] = None,
    ) -> SearchResponse:
        """execute Tavily search"""
        try:
            from tavily import TavilyClient
        except ImportError:
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message="tavily-python notsetup，pleaserunning: pip install tavily-python"
            )
        
        try:
            client = TavilyClient(api_key=api_key)
            
            # executesearch（optimize：useadvanceddepth、constraintrecentseveraldays）
            search_kwargs: Dict[str, Any] = {
                "query": query,
                "search_depth": "advanced",  # advanced getmoremultipleresult
                "max_results": max_results,
                "include_answer": False,
                "include_raw_content": False,
                "days": days,  # searchrecentdayscountcontent
            }
            if topic is not None:
                search_kwargs["topic"] = topic

            response = client.search(
                **search_kwargs,
            )
            
            # recordrawresponsetolog
            logger.info(f"[Tavily] searchcompleted，query='{query}', return {len(response.get('results', []))} results")
            logger.debug(f"[Tavily] rawresponse: {response}")
            
            # parsingresult
            results = []
            for item in response.get('results', []):
                results.append(SearchResult(
                    title=item.get('title', ''),
                    snippet=item.get('content', '')[:500],  # truncatebefore500character
                    url=item.get('url', ''),
                    source=self._extract_domain(item.get('url', '')),
                    published_date=item.get('published_date') or item.get('publishedDate'),
                ))
            
            return SearchResponse(
                query=query,
                results=results,
                provider=self.name,
                success=True,
            )
            
        except Exception as e:
            error_msg = str(e)
            # checkwhether isquotaissue
            if 'rate limit' in error_msg.lower() or 'quota' in error_msg.lower():
                error_msg = f"API quotaalreadyuseexhaust: {error_msg}"
            
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message=error_msg
            )

    def search(
        self,
        query: str,
        max_results: int = 5,
        days: int = 7,
        topic: Optional[str] = None,
    ) -> SearchResponse:
        """execute Tavily search，canbycallmethodselectwhetherenablednews topic。"""
        if topic is None:
            return super().search(query, max_results=max_results, days=days)

        api_key = self._get_next_key()
        if not api_key:
            return SearchResponse(
                query=query,
                results=[],
                provider=self._name,
                success=False,
                error_message=f"{self._name} notconfiguration API Key"
            )

        start_time = time.time()
        try:
            response = self._do_search(query, api_key, max_results, days=days, topic=topic)
            response.search_time = time.time() - start_time

            if response.success:
                self._record_success(api_key)
                logger.info(f"[{self._name}] search '{query}' successful，return {len(response.results)} results，elapsed {response.search_time:.2f}s")
            else:
                self._record_error(api_key)

            return response

        except Exception as e:
            self._record_error(api_key)
            elapsed = time.time() - start_time
            logger.error(f"[{self._name}] search '{query}' failed: {e}")
            return SearchResponse(
                query=query,
                results=[],
                provider=self._name,
                success=False,
                error_message=str(e),
                search_time=elapsed
            )
    
    @staticmethod
    def _extract_domain(url: str) -> str:
        """from URL extract domain as source"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc.replace('www.', '')
            return domain or 'unknown source'
        except Exception:
            return 'unknown source'


class SerpAPISearchProvider(BaseSearchProvider):
    """
    SerpAPI searchengine
    
    features：
    - support Google、Bing、Baiduetcmultipletypesearchengine
    - freeversioneachmonth 100 timesrequest
    - returnrealsearchresult
    
    document：https://serpapi.com/baidu-search-api?utm_source=github_daily_stock_analysis
    """
    
    def __init__(self, api_keys: List[str]):
        super().__init__(api_keys, "SerpAPI")
    
    def _do_search(self, query: str, api_key: str, max_results: int, days: int = 7) -> SearchResponse:
        """execute SerpAPI search"""
        try:
            from serpapi import GoogleSearch
        except ImportError:
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message="google-search-results notsetup，pleaserunning: pip install google-search-results"
            )
        
        try:
            # determine timerangeparameter tbs
            tbs = "qdr:w"  # defaultoneweek
            if days <= 1:
                tbs = "qdr:d"  # remove24hours
            elif days <= 7:
                tbs = "qdr:w"  # previous oneweek
            elif days <= 30:
                tbs = "qdr:m"  # previous onemonth
            else:
                tbs = "qdr:y"  # previous oneyear

            # use Google search (get Knowledge Graph, Answer Box etc)
            params = {
                "engine": "google",
                "q": query,
                "api_key": api_key,
                "google_domain": "google.com.hk", # useHong KongAlphabet，Chinesesupportrelativelygood
                "hl": "zh-cn",  # Chineseboundaryaspect
                "gl": "cn",     # Chinaadverb markerzonebiasedgood
                "tbs": tbs,     # timerangeconstraint
                "num": max_results # requestresultquantity，Note：Google APIhaswhennotstrictcomply with
            }
            
            search = GoogleSearch(params)
            response = search.get_dict()
            
            # recordrawresponsetolog
            logger.debug(f"[SerpAPI] rawresponse keys: {response.keys()}")
            
            # parsingresult
            results = []
            
            # 1. parsing Knowledge Graph (knowledge graph)
            kg = response.get('knowledge_graph', {})
            if kg:
                title = kg.get('title', 'knowledge graph')
                desc = kg.get('description', '')
                
                # extractextraproperty
                details = []
                for key in ['type', 'founded', 'headquarters', 'employees', 'ceo']:
                    val = kg.get(key)
                    if val:
                        details.append(f"{key}: {val}")
                        
                snippet = f"{desc}\n" + " | ".join(details) if details else desc
                
                results.append(SearchResult(
                    title=f"[knowledge graph] {title}",
                    snippet=snippet,
                    url=kg.get('source', {}).get('link', ''),
                    source="Google Knowledge Graph"
                ))
                
            # 2. parsing Answer Box (curated answer/quote/market datacard)
            ab = response.get('answer_box', {})
            if ab:
                ab_title = ab.get('title', 'curated answer')
                ab_snippet = ""
                
                # financethroughclassanswer
                if ab.get('type') == 'finance_results':
                    stock = ab.get('stock', '')
                    price = ab.get('price', '')
                    currency = ab.get('currency', '')
                    movement = ab.get('price_movement', {})
                    mv_val = movement.get('percentage', 0)
                    mv_dir = movement.get('movement', '')
                    
                    ab_title = f"[quote/market datacard] {stock}"
                    ab_snippet = f"price: {price} {currency}\nprice change: {mv_dir} {mv_val}%"
                    
                    # extracttabledata
                    if 'table' in ab:
                        table_data = []
                        for row in ab['table']:
                            if 'name' in row and 'value' in row:
                                table_data.append(f"{row['name']}: {row['value']}")
                        if table_data:
                            ab_snippet += "\n" + "; ".join(table_data)
                            
                # normaltextanswer
                elif 'snippet' in ab:
                    ab_snippet = ab.get('snippet', '')
                    list_items = ab.get('list', [])
                    if list_items:
                        ab_snippet += "\n" + "\n".join([f"- {item}" for item in list_items])
                
                elif 'answer' in ab:
                    ab_snippet = ab.get('answer', '')
                    
                if ab_snippet:
                    results.append(SearchResult(
                        title=f"[curated answer] {ab_title}",
                        snippet=ab_snippet,
                        url=ab.get('link', '') or ab.get('displayed_link', ''),
                        source="Google Answer Box"
                    ))

            # 3. parsing Related Questions (relatedissue)
            rqs = response.get('related_questions', [])
            for rq in rqs[:3]: # getbefore3count
                question = rq.get('question', '')
                snippet = rq.get('snippet', '')
                link = rq.get('link', '')
                
                if question and snippet:
                     results.append(SearchResult(
                        title=f"[relatedissue] {question}",
                        snippet=snippet,
                        url=link,
                        source="Google Related Questions"
                     ))

            # 4. parsing Organic Results (naturalsearchresult)
            organic_results = response.get('organic_results', [])

            for item in organic_results[:max_results]:
                link = item.get('link', '')
                snippet = item.get('snippet', '')

                # enhanced：if needed，parsingwebpagebody
                # strategy：ifsummarytoo short，orerasgetmoremultipleinfo，canrequestwebpage
                # hereIpluraltoallresulttrygetbody，butasperformance，only getbefore1000character
                content = ""
                if link:
                   try:
                       fetched_content = fetch_url_content(link, timeout=5)
                       if fetched_content:
                           # ifgettobody，willitsconcatenateto snippet in，orerreplace snippet
                           # hereselectconcatenate，keeporiginalsummary
                           content = fetched_content
                           if len(content) > 500:
                               snippet = f"{snippet}\n\n【webpagedetails】\n{content[:500]}..."
                           else:
                               snippet = f"{snippet}\n\n【webpagedetails】\n{content}"
                   except Exception as e:
                       logger.debug(f"[SerpAPI] Fetch content failed: {e}")

                results.append(SearchResult(
                    title=item.get('title', ''),
                    snippet=snippet[:1000], # constrainttotallength
                    url=link,
                    source=item.get('source', self._extract_domain(link)),
                    published_date=item.get('date'),
                ))

            return SearchResponse(
                query=query,
                results=results,
                provider=self.name,
                success=True,
            )
            
        except Exception as e:
            error_msg = str(e)
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message=error_msg
            )
    
    @staticmethod
    def _extract_domain(url: str) -> str:
        """from URL extractdomainname"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return parsed.netloc.replace('www.', '') or 'unknown source'
        except Exception:
            return 'unknown source'


class BochaSearchProvider(BaseSearchProvider):
    """
    Bochasearchengine
    
    features：
    - specializedasAIoptimizeChinesesearchAPI
    - resultaccurate、summarycomplete
    - supporttimerangefilteringandAIsummary
    - compatibleBing Search APIformat
    
    document：https://bocha-ai.feishu.cn/wiki/RXEOw02rFiwzGSkd9mUcqoeAnNK
    """
    
    def __init__(self, api_keys: List[str]):
        super().__init__(api_keys, "Bocha")
    
    def _do_search(self, query: str, api_key: str, max_results: int, days: int = 7) -> SearchResponse:
        """executeBochasearch"""
        try:
            import requests
        except ImportError:
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message="requests notsetup，pleaserunning: pip install requests"
            )
        
        try:
            # API endpoint
            url = "https://api.bocha.cn/v1/web-search"
            
            # request headers
            headers = {
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            }
            
            # determine timerange
            freshness = "oneWeek"
            if days <= 1:
                freshness = "oneDay"
            elif days <= 7:
                freshness = "oneWeek"
            elif days <= 30:
                freshness = "oneMonth"
            else:
                freshness = "oneYear"

            # requestparameter（strictaccording toAPIdocument）
            payload = {
                "query": query,
                "freshness": freshness,  # dynamictimerange
                "summary": True,  # enabledAIsummary
                "count": min(max_results, 50)  # max50items
            }
            
            # executesearch（withinstantwhen SSL/network errorretry）
            response = _post_with_retry(url, headers=headers, json=payload, timeout=10)
            
            # checkHTTPstatus code
            if response.status_code != 200:
                # tryparsingerror message
                try:
                    if response.headers.get('content-type', '').startswith('application/json'):
                        error_data = response.json()
                        error_message = error_data.get('message', response.text)
                    else:
                        error_message = response.text
                except Exception:
                    error_message = response.text
                
                # based onerror codeprocessing
                if response.status_code == 403:
                    error_msg = f"balanceinsufficient: {error_message}"
                elif response.status_code == 401:
                    error_msg = f"API KEYinvalid: {error_message}"
                elif response.status_code == 400:
                    error_msg = f"requestparametererror: {error_message}"
                elif response.status_code == 429:
                    error_msg = f"requestfrequencyreachtoconstraint: {error_message}"
                else:
                    error_msg = f"HTTP {response.status_code}: {error_message}"
                
                logger.warning(f"[Bocha] searchfailed: {error_msg}")
                
                return SearchResponse(
                    query=query,
                    results=[],
                    provider=self.name,
                    success=False,
                    error_message=error_msg
                )
            
            # parsingresponse
            try:
                data = response.json()
            except ValueError as e:
                error_msg = f"responseJSONparse failed: {str(e)}"
                logger.error(f"[Bocha] {error_msg}")
                return SearchResponse(
                    query=query,
                    results=[],
                    provider=self.name,
                    success=False,
                    error_message=error_msg
                )
            
            # checkresponsecode
            if data.get('code') != 200:
                error_msg = data.get('msg') or f"APIreturnerror code: {data.get('code')}"
                return SearchResponse(
                    query=query,
                    results=[],
                    provider=self.name,
                    success=False,
                    error_message=error_msg
                )
            
            # recordrawresponsetolog
            logger.info(f"[Bocha] searchcompleted，query='{query}'")
            logger.debug(f"[Bocha] rawresponse: {data}")
            
            # parsingsearchresult
            results = []
            web_pages = data.get('data', {}).get('webPages', {})
            value_list = web_pages.get('value', [])
            
            for item in value_list[:max_results]:
                # prefer to usesummary（AIsummary），fallbacktosnippet
                snippet = item.get('summary') or item.get('snippet', '')
                
                # truncatesummarylength
                if snippet:
                    snippet = snippet[:500]
                
                results.append(SearchResult(
                    title=item.get('name', ''),
                    snippet=snippet,
                    url=item.get('url', ''),
                    source=item.get('siteName') or self._extract_domain(item.get('url', '')),
                    published_date=item.get('datePublished'),  # UTC+8format，no need forconverting
                ))
            
            logger.info(f"[Bocha] successfulparsing {len(results)} results")
            
            return SearchResponse(
                query=query,
                results=results,
                provider=self.name,
                success=True,
            )
            
        except requests.exceptions.Timeout:
            error_msg = "requesttimeout"
            logger.error(f"[Bocha] {error_msg}")
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message=error_msg
            )
        except requests.exceptions.RequestException as e:
            error_msg = f"networkrequest failed: {str(e)}"
            logger.error(f"[Bocha] {error_msg}")
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message=error_msg
            )
        except Exception as e:
            error_msg = f"unknownerror: {str(e)}"
            logger.error(f"[Bocha] {error_msg}")
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message=error_msg
            )
    
    @staticmethod
    def _extract_domain(url: str) -> str:
        """from URL extract domain as source"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc.replace('www.', '')
            return domain or 'unknown source'
        except Exception:
            return 'unknown source'


class MiniMaxSearchProvider(BaseSearchProvider):
    """
    MiniMax Web Search (Coding Plan API)

    Features:
    - Backed by MiniMax Coding Plan subscription
    - Returns structured organic results with title/link/snippet/date
    - No native time-range parameter; time filtering is done via query
      augmentation and client-side date filtering
    - Circuit-breaker protection: 3 consecutive failures -> 300s cooldown

    API endpoint: POST https://api.minimaxi.com/v1/coding_plan/search
    """

    API_ENDPOINT = "https://api.minimaxi.com/v1/coding_plan/search"

    # Circuit-breaker settings
    _CB_FAILURE_THRESHOLD = 3
    _CB_COOLDOWN_SECONDS = 300  # 5 minutes

    def __init__(self, api_keys: List[str]):
        super().__init__(api_keys, "MiniMax")
        # Circuit breaker state
        self._consecutive_failures = 0
        self._circuit_open_until: float = 0.0

    @property
    def is_available(self) -> bool:
        """Check availability considering circuit breaker state."""
        if not super().is_available:
            return False
        if self._consecutive_failures >= self._CB_FAILURE_THRESHOLD:
            if time.time() < self._circuit_open_until:
                return False
            # Cooldown expired -> half-open, allow one probe
        return True

    def _record_success(self, key: str) -> None:
        super()._record_success(key)
        # Reset circuit breaker on success
        self._consecutive_failures = 0
        self._circuit_open_until = 0.0

    def _record_error(self, key: str) -> None:
        super()._record_error(key)
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._CB_FAILURE_THRESHOLD:
            self._circuit_open_until = time.time() + self._CB_COOLDOWN_SECONDS
            logger.warning(
                f"[MiniMax] Circuit breaker OPEN – "
                f"{self._consecutive_failures} consecutive failures, "
                f"cooldown {self._CB_COOLDOWN_SECONDS}s"
            )

    # ------------------------------------------------------------------
    # Time-range helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _time_hint(days: int, is_chinese: bool = True) -> str:
        """Build a time-hint string to append to the search query."""
        if is_chinese:
            if days <= 1:
                return "today"
            elif days <= 3:
                return "recentthreedays"
            elif days <= 7:
                return "recentoneweek"
            else:
                return "recentonecountmonth"
        else:
            if days <= 1:
                return "today"
            elif days <= 3:
                return "past 3 days"
            elif days <= 7:
                return "past week"
            else:
                return "past month"

    @staticmethod
    def _is_within_days(date_str: Optional[str], days: int) -> bool:
        """Check whether *date_str* falls within the last *days* days.

        Accepts common formats: ``2025-06-01``, ``2025/06/01``,
        ``Jun 1, 2025``, ISO-8601 with timezone, etc.
        Returns True when date_str is None or unparseable (keep the result).
        """
        if not date_str:
            return True
        try:
            from dateutil import parser as dateutil_parser
            dt = dateutil_parser.parse(date_str, fuzzy=True)
            from datetime import timedelta, timezone
            now = datetime.now(timezone.utc) if dt.tzinfo else datetime.now()
            return (now - dt) <= timedelta(days=days + 1)  # +1 buffer
        except Exception:
            return True  # Keep result when date is unparseable

    # ------------------------------------------------------------------

    def _do_search(self, query: str, api_key: str, max_results: int, days: int = 7) -> SearchResponse:
        """Execute MiniMax web search."""
        try:
            # Detect language hint from query (simple heuristic)
            has_cjk = any('\u4e00' <= ch <= '\u9fff' for ch in query)
            time_hint = self._time_hint(days, is_chinese=has_cjk)
            augmented_query = f"{query} {time_hint}"

            headers = {
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json',
                'MM-API-Source': 'Minimax-MCP',
            }
            payload = {"q": augmented_query}

            response = _post_with_retry(
                self.API_ENDPOINT, headers=headers, json=payload, timeout=15
            )

            # HTTP error handling
            if response.status_code != 200:
                error_msg = self._parse_http_error(response)
                logger.warning(f"[MiniMax] Search failed: {error_msg}")
                return SearchResponse(
                    query=query,
                    results=[],
                    provider=self.name,
                    success=False,
                    error_message=error_msg,
                )

            data = response.json()

            # Check base_resp status
            base_resp = data.get('base_resp', {})
            if base_resp.get('status_code', 0) != 0:
                error_msg = base_resp.get('status_msg', 'Unknown API error')
                return SearchResponse(
                    query=query,
                    results=[],
                    provider=self.name,
                    success=False,
                    error_message=error_msg,
                )

            logger.info(f"[MiniMax] Search done, query='{query}'")
            logger.debug(f"[MiniMax] Raw response keys: {list(data.keys())}")

            # Parse organic results
            results: List[SearchResult] = []
            for item in data.get('organic', []):
                date_val = item.get('date')

                # Client-side time filtering
                if not self._is_within_days(date_val, days):
                    continue

                results.append(SearchResult(
                    title=item.get('title', ''),
                    snippet=(item.get('snippet', '') or '')[:500],
                    url=item.get('link', ''),
                    source=self._extract_domain(item.get('link', '')),
                    published_date=date_val,
                ))

                if len(results) >= max_results:
                    break

            logger.info(f"[MiniMax] Parsed {len(results)} results (after time filter)")

            return SearchResponse(
                query=query,
                results=results,
                provider=self.name,
                success=True,
            )

        except requests.exceptions.Timeout:
            error_msg = "Request timeout"
            logger.error(f"[MiniMax] {error_msg}")
            return SearchResponse(
                query=query, results=[], provider=self.name,
                success=False, error_message=error_msg,
            )
        except requests.exceptions.RequestException as e:
            error_msg = f"Network error: {e}"
            logger.error(f"[MiniMax] {error_msg}")
            return SearchResponse(
                query=query, results=[], provider=self.name,
                success=False, error_message=error_msg,
            )
        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            logger.error(f"[MiniMax] {error_msg}")
            return SearchResponse(
                query=query, results=[], provider=self.name,
                success=False, error_message=error_msg,
            )

    @staticmethod
    def _parse_http_error(response) -> str:
        """Parse HTTP error response from MiniMax API."""
        try:
            ct = response.headers.get('content-type', '')
            if 'json' in ct:
                err = response.json()
                base_resp = err.get('base_resp', {})
                msg = base_resp.get('status_msg') or err.get('message') or str(err)
                return msg
            return response.text[:200]
        except Exception:
            return f"HTTP {response.status_code}: {response.text[:200]}"

    @staticmethod
    def _extract_domain(url: str) -> str:
        """Extract domain from URL as source label."""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc.replace('www.', '')
            return domain or 'unknown source'
        except Exception:
            return 'unknown source'


class BraveSearchProvider(BaseSearchProvider):
    """
    Brave Search searchengine

    features：
    - privacypriorityindependentsearchengine
    - indexexceed300hundred millionpage
    - freelayeravailable
    - supporttimerangefiltering

    document：https://brave.com/search/api/
    """

    API_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"

    def __init__(self, api_keys: List[str]):
        super().__init__(api_keys, "Brave")

    def _do_search(self, query: str, api_key: str, max_results: int, days: int = 7) -> SearchResponse:
        """execute Brave search"""
        try:
            # request headers
            headers = {
                'X-Subscription-Token': api_key,
                'Accept': 'application/json'
            }

            # determine timerange（freshness parameter）
            if days <= 1:
                freshness = "pd"  # Past day (24hours)
            elif days <= 7:
                freshness = "pw"  # Past week
            elif days <= 30:
                freshness = "pm"  # Past month
            else:
                freshness = "py"  # Past year

            # requestparameter
            params = {
                "q": query,
                "count": min(max_results, 20),  # Brave maxsupport20items
                "freshness": freshness,
                "search_lang": "en",  # Englishcontent（USstockpriority）
                "country": "US",  # USChinazonedomainbiasedgood
                "safesearch": "moderate"
            }

            # executesearch（GET request）
            response = requests.get(
                self.API_ENDPOINT,
                headers=headers,
                params=params,
                timeout=10
            )

            # checkHTTPstatus code
            if response.status_code != 200:
                error_msg = self._parse_error(response)
                logger.warning(f"[Brave] searchfailed: {error_msg}")
                return SearchResponse(
                    query=query,
                    results=[],
                    provider=self.name,
                    success=False,
                    error_message=error_msg
                )

            # parsingresponse
            try:
                data = response.json()
            except ValueError as e:
                error_msg = f"responseJSONparse failed: {str(e)}"
                logger.error(f"[Brave] {error_msg}")
                return SearchResponse(
                    query=query,
                    results=[],
                    provider=self.name,
                    success=False,
                    error_message=error_msg
                )

            logger.info(f"[Brave] searchcompleted，query='{query}'")
            logger.debug(f"[Brave] rawresponse: {data}")

            # parsingsearchresult
            results = []
            web_data = data.get('web', {})
            web_results = web_data.get('results', [])

            for item in web_results[:max_results]:
                # parsingpublishdate（ISO 8601 format）
                published_date = None
                age = item.get('age') or item.get('page_age')
                if age:
                    try:
                        # converting ISO formatassimpledatestring
                        dt = datetime.fromisoformat(age.replace('Z', '+00:00'))
                        published_date = dt.strftime('%Y-%m-%d')
                    except (ValueError, AttributeError):
                        published_date = age  # parse failedwhenuserawvalue

                results.append(SearchResult(
                    title=item.get('title', ''),
                    snippet=item.get('description', '')[:500],  # truncateto500character
                    url=item.get('url', ''),
                    source=self._extract_domain(item.get('url', '')),
                    published_date=published_date
                ))

            logger.info(f"[Brave] successfulparsing {len(results)} results")

            return SearchResponse(
                query=query,
                results=results,
                provider=self.name,
                success=True
            )

        except requests.exceptions.Timeout:
            error_msg = "requesttimeout"
            logger.error(f"[Brave] {error_msg}")
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message=error_msg
            )
        except requests.exceptions.RequestException as e:
            error_msg = f"networkrequest failed: {str(e)}"
            logger.error(f"[Brave] {error_msg}")
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message=error_msg
            )
        except Exception as e:
            error_msg = f"unknownerror: {str(e)}"
            logger.error(f"[Brave] {error_msg}")
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message=error_msg
            )

    def _parse_error(self, response) -> str:
        """parsingerrorresponse"""
        try:
            if response.headers.get('content-type', '').startswith('application/json'):
                error_data = response.json()
                # Brave API returnederrorformat
                if 'message' in error_data:
                    return error_data['message']
                if 'error' in error_data:
                    return error_data['error']
                return str(error_data)
            return response.text[:200]
        except Exception:
            return f"HTTP {response.status_code}: {response.text[:200]}"

    @staticmethod
    def _extract_domain(url: str) -> str:
        """from URL extract domain as source"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc.replace('www.', '')
            return domain or 'unknown source'
        except Exception:
            return 'unknown source'


class SearXNGSearchProvider(BaseSearchProvider):
    """
    SearXNG search engine (self-hosted, no quota).

    Self-hosted instances are used when explicitly configured.
    Otherwise, the provider can lazily discover public instances from
    searx.space and rotate across them with per-request failover.
    """

    PUBLIC_INSTANCES_URL = "https://searx.space/data/instances.json"
    PUBLIC_INSTANCES_CACHE_TTL_SECONDS = 3600
    PUBLIC_INSTANCES_STALE_REFRESH_BACKOFF_SECONDS = 60
    PUBLIC_INSTANCES_POOL_LIMIT = 20
    PUBLIC_INSTANCES_MAX_ATTEMPTS = 3
    PUBLIC_INSTANCES_TIMEOUT_SECONDS = 5
    SELF_HOSTED_TIMEOUT_SECONDS = 10

    _public_instances_cache: Optional[Tuple[float, List[str]]] = None
    _public_instances_stale_retry_after: float = 0.0
    _public_instances_lock = threading.Lock()

    def __init__(self, base_urls: Optional[List[str]] = None, *, use_public_instances: bool = False):
        normalized_base_urls = [url.rstrip("/") for url in (base_urls or []) if url.strip()]
        super().__init__(normalized_base_urls, "SearXNG")
        self._base_urls = normalized_base_urls
        self._use_public_instances = bool(use_public_instances and not self._base_urls)
        self._cursor = 0
        self._cursor_lock = threading.Lock()

    @property
    def is_available(self) -> bool:
        return bool(self._base_urls) or self._use_public_instances

    @classmethod
    def reset_public_instance_cache(cls) -> None:
        """Reset the shared searx.space cache (used by tests)."""
        with cls._public_instances_lock:
            cls._public_instances_cache = None
            cls._public_instances_stale_retry_after = 0.0

    @staticmethod
    def _parse_http_error(response) -> str:
        """Parse HTTP error details for easier diagnostics."""
        try:
            raw_content_type = response.headers.get("content-type", "")
            content_type = raw_content_type if isinstance(raw_content_type, str) else ""
            if "json" in content_type:
                error_data = response.json()
                if isinstance(error_data, dict):
                    message = error_data.get("error") or error_data.get("message")
                    if message:
                        return str(message)
                return str(error_data)
            raw_text = getattr(response, "text", "")
            body = raw_text.strip() if isinstance(raw_text, str) else ""
            return body[:200] if body else f"HTTP {response.status_code}"
        except Exception:
            raw_text = getattr(response, "text", "")
            body = raw_text if isinstance(raw_text, str) else ""
            return f"HTTP {response.status_code}: {body[:200]}"

    @staticmethod
    def _time_range(days: int) -> str:
        if days <= 1:
            return "day"
        if days <= 7:
            return "week"
        if days <= 30:
            return "month"
        return "year"

    @classmethod
    def _search_latency_seconds(cls, instance_data: Dict[str, Any]) -> float:
        timing = (instance_data.get("timing") or {}).get("search") or {}
        all_timing = timing.get("all")
        if isinstance(all_timing, dict):
            for key in ("mean", "median"):
                value = all_timing.get(key)
                if isinstance(value, (int, float)):
                    return float(value)
        return float("inf")

    @classmethod
    def _extract_public_instances(cls, payload: Any) -> List[str]:
        if not isinstance(payload, dict):
            return []

        instances = payload.get("instances")
        if not isinstance(instances, dict):
            return []

        ranked: List[Tuple[float, float, str]] = []
        for raw_url, item in instances.items():
            if not isinstance(raw_url, str) or not isinstance(item, dict):
                continue
            if item.get("network_type") != "normal":
                continue
            http_status = (item.get("http") or {}).get("status_code")
            if http_status != 200:
                continue
            timing = (item.get("timing") or {}).get("search") or {}
            uptime = timing.get("success_percentage")
            if not isinstance(uptime, (int, float)) or float(uptime) <= 0:
                continue

            ranked.append(
                (
                    float(uptime),
                    cls._search_latency_seconds(item),
                    raw_url.rstrip("/"),
                )
            )

        ranked.sort(key=lambda row: (-row[0], row[1], row[2]))
        return [url for _, _, url in ranked[: cls.PUBLIC_INSTANCES_POOL_LIMIT]]

    @classmethod
    def _get_public_instances(cls) -> List[str]:
        now = time.time()
        with cls._public_instances_lock:
            stale_urls: List[str] = []
            if cls._public_instances_cache is None and cls._public_instances_stale_retry_after > now:
                logger.debug(
                    "[SearXNG] publicinstancecoldstartrefreshbackoffin，remaining %.0fs",
                    cls._public_instances_stale_retry_after - now,
                )
                return []
            if cls._public_instances_cache is not None:
                cached_at, cached_urls = cls._public_instances_cache
                if now - cached_at < cls.PUBLIC_INSTANCES_CACHE_TTL_SECONDS:
                    return list(cached_urls)
                stale_urls = list(cached_urls)
                if cls._public_instances_stale_retry_after > now:
                    logger.debug(
                        "[SearXNG] publicinstancerefreshbackoffin，continuinguseperiodcache，remaining %.0fs",
                        cls._public_instances_stale_retry_after - now,
                    )
                    return stale_urls

            try:
                response = requests.get(
                    cls.PUBLIC_INSTANCES_URL,
                    timeout=cls.PUBLIC_INSTANCES_TIMEOUT_SECONDS,
                )
                if response.status_code != 200:
                    logger.warning(
                        "[SearXNG] pullpublicinstancelistfailed: HTTP %s",
                        response.status_code,
                    )
                else:
                    urls = cls._extract_public_instances(response.json())
                    if urls:
                        cls._public_instances_cache = (now, list(urls))
                        cls._public_instances_stale_retry_after = 0.0
                        logger.info("[SearXNG] alreadyrefreshpublicinstancepool，total %s countcandidateinstance", len(urls))
                        return list(urls)
                    logger.warning("[SearXNG] searx.space notreturnavailablepublicinstance，keepexistingcache")
            except Exception as exc:
                logger.warning("[SearXNG] pullpublicinstancelistfailed: %s", exc)

            if stale_urls:
                cls._public_instances_stale_retry_after = (
                    now + cls.PUBLIC_INSTANCES_STALE_REFRESH_BACKOFF_SECONDS
                )
                logger.warning(
                    "[SearXNG] publicinstancerefreshfailed，continuinguseperiodcache，total %s countcandidateinstance；"
                    "%.0fs innotagainrefresh",
                    len(stale_urls),
                    cls.PUBLIC_INSTANCES_STALE_REFRESH_BACKOFF_SECONDS,
                )
                return stale_urls
            cls._public_instances_stale_retry_after = (
                now + cls.PUBLIC_INSTANCES_STALE_REFRESH_BACKOFF_SECONDS
            )
            logger.warning(
                "[SearXNG] publicinstancecoldstartrefreshfailed，%.0fs innotagainrefresh",
                cls.PUBLIC_INSTANCES_STALE_REFRESH_BACKOFF_SECONDS,
            )
            return []

    def _rotate_candidates(self, pool: List[str], *, max_attempts: int) -> List[str]:
        if not pool or max_attempts <= 0:
            return []
        with self._cursor_lock:
            start = self._cursor % len(pool)
            self._cursor = (self._cursor + 1) % len(pool)
        ordered = pool[start:] + pool[:start]
        return ordered[:max_attempts]

    def _do_search(  # type: ignore[override]
        self,
        query: str,
        base_url: str,
        max_results: int,
        days: int = 7,
        *,
        timeout: int,
        retry_enabled: bool,
    ) -> SearchResponse:
        """Execute one SearXNG search against a specific instance."""
        try:
            base = base_url.rstrip("/")
            search_url = base if base.endswith("/search") else base + "/search"

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            params = {
                "q": query,
                "format": "json",
                "time_range": self._time_range(days),
                "pageno": 1,
            }

            request_get = _get_with_retry if retry_enabled else requests.get
            response = request_get(search_url, headers=headers, params=params, timeout=timeout)

            if response.status_code != 200:
                error_msg = self._parse_http_error(response)
                if response.status_code == 403:
                    error_msg = (
                        f"{error_msg}；SearXNG instancepossiblynotenabled JSON output（pleasecheck settings.yml），"
                        "orinstance/proxyrejectthistimesaccess"
                    )
                return SearchResponse(
                    query=query,
                    results=[],
                    provider=self.name,
                    success=False,
                    error_message=error_msg,
                )

            try:
                data = response.json()
            except Exception:
                return SearchResponse(
                    query=query,
                    results=[],
                    provider=self.name,
                    success=False,
                    error_message="responseJSONparse failed",
                )

            if not isinstance(data, dict):
                return SearchResponse(
                    query=query,
                    results=[],
                    provider=self.name,
                    success=False,
                    error_message="responseformatinvalid",
                )

            raw = data.get("results", [])
            if not isinstance(raw, list):
                raw = []

            results = []
            for item in raw:
                if not isinstance(item, dict):
                    continue
                url_val = item.get("url")
                if not url_val:
                    continue
                raw_published_date = item.get("publishedDate")

                snippet = (item.get("content") or item.get("description") or "")[:500]
                published_date = None
                if raw_published_date:
                    try:
                        dt = datetime.fromisoformat(raw_published_date.replace("Z", "+00:00"))
                        published_date = dt.strftime("%Y-%m-%d")
                    except (ValueError, AttributeError):
                        published_date = raw_published_date

                results.append(
                    SearchResult(
                        title=item.get("title", ""),
                        snippet=snippet,
                        url=url_val,
                        source=self._extract_domain(url_val),
                        published_date=published_date,
                    )
                )
                if len(results) >= max_results:
                    break

            return SearchResponse(query=query, results=results, provider=self.name, success=True)

        except requests.exceptions.Timeout:
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message="requesttimeout",
            )
        except requests.exceptions.RequestException as e:
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message=f"networkrequest failed: {e}",
            )
        except Exception as e:
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message=f"unknownerror: {e}",
            )

    @staticmethod
    def _extract_domain(url: str) -> str:
        """Extract domain from URL as source label."""
        try:
            from urllib.parse import urlparse

            parsed = urlparse(url)
            domain = parsed.netloc.replace("www.", "")
            return domain or "unknown source"
        except Exception:
            return "unknown source"

    def search(self, query: str, max_results: int = 5, days: int = 7) -> SearchResponse:
        """Execute SearXNG search with instance rotation and per-request failover."""
        start_time = time.time()
        if self._base_urls:
            candidates = self._rotate_candidates(
                self._base_urls,
                max_attempts=len(self._base_urls),
            )
            retry_enabled = True
            timeout = self.SELF_HOSTED_TIMEOUT_SECONDS
            empty_error = "SearXNG notconfigurationavailableinstance"
        elif self._use_public_instances:
            public_instances = self._get_public_instances()
            candidates = self._rotate_candidates(
                public_instances,
                max_attempts=min(len(public_instances), self.PUBLIC_INSTANCES_MAX_ATTEMPTS),
            )
            retry_enabled = False
            timeout = self.PUBLIC_INSTANCES_TIMEOUT_SECONDS
            empty_error = "failed to getavailablepublic SearXNG instance"
        else:
            candidates = []
            retry_enabled = False
            timeout = self.PUBLIC_INSTANCES_TIMEOUT_SECONDS
            empty_error = "SearXNG notconfigurationavailableinstance"

        if not candidates:
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message=empty_error,
                search_time=time.time() - start_time,
            )

        errors: List[str] = []
        for base_url in candidates:
            response = self._do_search(
                query,
                base_url,
                max_results,
                days=days,
                timeout=timeout,
                retry_enabled=retry_enabled,
            )
            response.search_time = time.time() - start_time
            if response.success:
                logger.info(
                    "[%s] search '%s' successful，instance=%s，return %s results，elapsed %.2fs",
                    self.name,
                    query,
                    base_url,
                    len(response.results),
                    response.search_time,
                )
                return response

            errors.append(f"{base_url}: {response.error_message or 'unknownerror'}")
            logger.warning("[%s] instance %s searchfailed: %s", self.name, base_url, response.error_message)

        elapsed = time.time() - start_time
        return SearchResponse(
            query=query,
            results=[],
            provider=self.name,
            success=False,
            error_message="；".join(errors[:3]) if errors else empty_error,
            search_time=elapsed,
        )


class SearchService:
    """
    searchservice
    
    feature：
    1. managemultiplecountsearchengine
    2. automaticfailureconvertmove
    3. resultaggregationandformatting
    4. datasourcefailedwhenenhancedsearch（stock price、trendetc）
    5. HK stock/US stockautomaticuseEnglishsearchkeyword
    """
    
    # enhancedsearchkeywordTemplate（A-share Chinese）
    ENHANCED_SEARCH_KEYWORDS = [
        "{name} stock today stock price",
        "{name} {code} latest quote/market data trend",
        "{name} stock analyzing trendchart",
        "{name} candlestick technical analysis",
        "{name} {code} price change trading volume",
    ]

    # enhancedsearchkeywordTemplate（HK stock/US stock English）
    ENHANCED_SEARCH_KEYWORDS_EN = [
        "{name} stock price today",
        "{name} {code} latest quote trend",
        "{name} stock analysis chart",
        "{name} technical analysis",
        "{name} {code} performance volume",
    ]
    NEWS_OVERSAMPLE_FACTOR = 2
    NEWS_OVERSAMPLE_MAX = 10
    FUTURE_TOLERANCE_DAYS = 1
    
    def __init__(
        self,
        bocha_keys: Optional[List[str]] = None,
        tavily_keys: Optional[List[str]] = None,
        brave_keys: Optional[List[str]] = None,
        serpapi_keys: Optional[List[str]] = None,
        minimax_keys: Optional[List[str]] = None,
        searxng_base_urls: Optional[List[str]] = None,
        searxng_public_instances_enabled: bool = True,
        news_max_age_days: int = 3,
        news_strategy_profile: str = "short",
    ):
        """
        initializingsearchservice

        Args:
            bocha_keys: Bochasearch API Key list
            tavily_keys: Tavily API Key list
            brave_keys: Brave Search API Key list
            serpapi_keys: SerpAPI Key list
            minimax_keys: MiniMax API Key list
            searxng_base_urls: SearXNG instanceadverb markeraddresslist（self-builtnoquotafallback）
            searxng_public_instances_enabled: notconfigurationself-builtinstancewhen，whetherautomaticusepublic SearXNG instance
            news_max_age_days: newsmaxwheneffect（days）
            news_strategy_profile: newswindowstrategyleveldigit（ultra_short/short/medium/long）
        """
        self._providers: List[BaseSearchProvider] = []
        self.news_max_age_days = max(1, news_max_age_days)
        raw_profile = (news_strategy_profile or "short").strip().lower()
        self.news_strategy_profile = normalize_news_strategy_profile(news_strategy_profile)
        if raw_profile != self.news_strategy_profile:
            logger.warning(
                "NEWS_STRATEGY_PROFILE '%s' invalid，alreadyrollbackas 'short'",
                news_strategy_profile,
            )
        self.news_window_days = resolve_news_window_days(
            news_max_age_days=self.news_max_age_days,
            news_strategy_profile=self.news_strategy_profile,
        )
        self.news_profile_days = NEWS_STRATEGY_WINDOWS.get(
            self.news_strategy_profile,
            NEWS_STRATEGY_WINDOWS["short"],
        )

        # initializingsearchengine（by prioritysorting）
        # 1. Bocha priority（Chinesesearchoptimize，AIsummary）
        if bocha_keys:
            self._providers.append(BochaSearchProvider(bocha_keys))
            logger.info(f"alreadyconfiguration Bocha search，total {len(bocha_keys)} count API Key")

        # 2. Tavily（freequotamoremultiple，eachmonth 1000 times）
        if tavily_keys:
            self._providers.append(TavilySearchProvider(tavily_keys))
            logger.info(f"alreadyconfiguration Tavily search，total {len(tavily_keys)} count API Key")

        # 3. Brave Search（privacypriority，allglobaloverride）
        if brave_keys:
            self._providers.append(BraveSearchProvider(brave_keys))
            logger.info(f"alreadyconfiguration Brave search，total {len(brave_keys)} count API Key")

        # 4. SerpAPI act asasalternative（eachmonth 100 times）
        if serpapi_keys:
            self._providers.append(SerpAPISearchProvider(serpapi_keys))
            logger.info(f"alreadyconfiguration SerpAPI search，total {len(serpapi_keys)} count API Key")

        # 5. MiniMax（Coding Plan Web Search，structure-izeresult）
        if minimax_keys:
            self._providers.append(MiniMaxSearchProvider(minimax_keys))
            logger.info(f"alreadyconfiguration MiniMax search，total {len(minimax_keys)} count API Key")

        # 6. SearXNG（self-builtinstancepriority；notconfigurationwhencanautomaticsendcurrentpublicinstance）
        searxng_provider = SearXNGSearchProvider(
            searxng_base_urls,
            use_public_instances=bool(searxng_public_instances_enabled and not searxng_base_urls),
        )
        if searxng_provider.is_available:
            self._providers.append(searxng_provider)
            if searxng_base_urls:
                logger.info("alreadyconfiguration SearXNG search，total %s countself-builtinstance", len(searxng_base_urls))
            else:
                logger.info("alreadyenabled SearXNG publicinstanceautomaticsendcurrentmode")

        if not self._providers:
            logger.warning("notconfigurationanysearchcapability，newssearchfeature will be unavailable")

        # In-memory search result cache: {cache_key: (timestamp, SearchResponse)}
        self._cache: Dict[str, Tuple[float, 'SearchResponse']] = {}
        # Default cache TTL in seconds (10 minutes)
        self._cache_ttl: int = 600
        logger.info(
            "newswheneffectstrategyalreadyenabled: profile=%s, profile_days=%s, NEWS_MAX_AGE_DAYS=%s, effective_window=%s",
            self.news_strategy_profile,
            self.news_profile_days,
            self.news_max_age_days,
            self.news_window_days,
        )
    
    @staticmethod
    def _is_foreign_stock(stock_code: str) -> bool:
        """determinewhether isHK stockorUS stock"""
        import re
        code = stock_code.strip()
        # US stock：1-5uppercase letters，possiblypackageincludepoint（e.g. BRK.B）
        if re.match(r'^[A-Za-z]{1,5}(\.[A-Za-z])?$', code):
            return True
        # HK stock：with hk prefixor 5digitpurecountcharacter
        lower = code.lower()
        if lower.startswith('hk'):
            return True
        if code.isdigit() and len(code) == 5:
            return True
        return False

    # A-share ETF code prefixes (Shanghai 51/52/56/58, Shenzhen 15/16/18)
    _A_ETF_PREFIXES = ('51', '52', '56', '58', '15', '16', '18')
    _ETF_NAME_KEYWORDS = ('ETF', 'FUND', 'TRUST', 'INDEX', 'TRACKER', 'UNIT')  # US/HK ETF name hints

    @staticmethod
    def is_index_or_etf(stock_code: str, stock_name: str) -> bool:
        """
        Judge if symbol is index-tracking ETF or market index.
        For such symbols, analysis focuses on index movement only, not issuer company risks.
        """
        code = (stock_code or '').strip().split('.')[0]
        if not code:
            return False
        # A-share ETF
        if code.isdigit() and len(code) == 6 and code.startswith(SearchService._A_ETF_PREFIXES):
            return True
        # US index (SPX, DJI, IXIC etc.)
        if is_us_index_code(code):
            return True
        # US/HK ETF: foreign symbol + name contains fund-like keywords
        if SearchService._is_foreign_stock(code):
            name_upper = (stock_name or '').upper()
            return any(kw in name_upper for kw in SearchService._ETF_NAME_KEYWORDS)
        return False

    @property
    def is_available(self) -> bool:
        """checkwhetherhasavailablesearchengine"""
        return any(p.is_available for p in self._providers)

    def _cache_key(self, query: str, max_results: int, days: int) -> str:
        """Build a cache key from query parameters."""
        return f"{query}|{max_results}|{days}"

    def _get_cached(self, key: str) -> Optional['SearchResponse']:
        """Return cached SearchResponse if still valid, else None."""
        entry = self._cache.get(key)
        if entry is None:
            return None
        ts, response = entry
        if time.time() - ts > self._cache_ttl:
            del self._cache[key]
            return None
        logger.debug(f"Search cache hit: {key[:60]}...")
        return response

    def _put_cache(self, key: str, response: 'SearchResponse') -> None:
        """Store a successful SearchResponse in cache."""
        # Hard cap: evict oldest entries when cache exceeds limit
        _MAX_CACHE_SIZE = 500
        if len(self._cache) >= _MAX_CACHE_SIZE:
            now = time.time()
            # First pass: remove expired entries
            expired = [k for k, (ts, _) in self._cache.items() if now - ts > self._cache_ttl]
            for k in expired:
                del self._cache[k]
            # Second pass: if still over limit, evict oldest entries (FIFO)
            if len(self._cache) >= _MAX_CACHE_SIZE:
                excess = len(self._cache) - _MAX_CACHE_SIZE + 1
                oldest = sorted(self._cache.keys(), key=lambda k: self._cache[k][0])[:excess]
                for k in oldest:
                    del self._cache[k]
        self._cache[key] = (time.time(), response)

    def _effective_news_window_days(self) -> int:
        """Resolve effective news window from strategy profile and global max-age."""
        return resolve_news_window_days(
            news_max_age_days=self.news_max_age_days,
            news_strategy_profile=self.news_strategy_profile,
        )

    @classmethod
    def _provider_request_size(cls, max_results: int) -> int:
        """Apply light overfetch before time filtering to avoid sparse outputs."""
        target = max(1, int(max_results))
        return max(target, min(target * cls.NEWS_OVERSAMPLE_FACTOR, cls.NEWS_OVERSAMPLE_MAX))

    @staticmethod
    def _parse_relative_news_date(text: str, now: datetime) -> Optional[date]:
        """Parse common Chinese/English relative-time strings."""
        raw = (text or "").strip()
        if not raw:
            return None

        lower = raw.lower()
        if raw in {"today", "today", "just now"} or lower in {"today", "just now", "now"}:
            return now.date()
        if raw == "yesterdaydays" or lower == "yesterday":
            return (now - timedelta(days=1)).date()
        if raw == "beforedays":
            return (now - timedelta(days=2)).date()

        zh = re.match(r"^\s*(\d+)\s*(minutes|hours|days|week|countmonth|month|year)\s*before\s*$", raw)
        if zh:
            amount = int(zh.group(1))
            unit = zh.group(2)
            if unit == "minutes":
                return (now - timedelta(minutes=amount)).date()
            if unit == "hours":
                return (now - timedelta(hours=amount)).date()
            if unit == "days":
                return (now - timedelta(days=amount)).date()
            if unit == "week":
                return (now - timedelta(weeks=amount)).date()
            if unit in {"countmonth", "month"}:
                return (now - timedelta(days=amount * 30)).date()
            if unit == "year":
                return (now - timedelta(days=amount * 365)).date()

        en = re.match(
            r"^\s*(\d+)\s*(minute|minutes|min|mins|hour|hours|day|days|week|weeks|month|months|year|years)\s*ago\s*$",
            lower,
        )
        if en:
            amount = int(en.group(1))
            unit = en.group(2)
            if unit in {"minute", "minutes", "min", "mins"}:
                return (now - timedelta(minutes=amount)).date()
            if unit in {"hour", "hours"}:
                return (now - timedelta(hours=amount)).date()
            if unit in {"day", "days"}:
                return (now - timedelta(days=amount)).date()
            if unit in {"week", "weeks"}:
                return (now - timedelta(weeks=amount)).date()
            if unit in {"month", "months"}:
                return (now - timedelta(days=amount * 30)).date()
            if unit in {"year", "years"}:
                return (now - timedelta(days=amount * 365)).date()

        return None

    @classmethod
    def _normalize_news_publish_date(cls, value: Any) -> Optional[date]:
        """Normalize provider date value into a date object."""
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is not None:
                local_tz = datetime.now().astimezone().tzinfo or timezone.utc
                return value.astimezone(local_tz).date()
            return value.date()
        if isinstance(value, date):
            return value

        text = str(value).strip()
        if not text:
            return None
        now = datetime.now()
        local_tz = now.astimezone().tzinfo or timezone.utc

        relative_date = cls._parse_relative_news_date(text, now)
        if relative_date:
            return relative_date

        # Unix timestamp fallback
        if text.isdigit() and len(text) in (10, 13):
            try:
                ts = int(text[:10]) if len(text) == 13 else int(text)
                # Provider timestamps are typically UTC epoch seconds.
                # Normalize to local date to keep window checks aligned with local "today".
                return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(local_tz).date()
            except (OSError, OverflowError, ValueError):
                pass

        iso_candidate = text.replace("Z", "+00:00")
        try:
            parsed_iso = datetime.fromisoformat(iso_candidate)
            if parsed_iso.tzinfo is not None:
                return parsed_iso.astimezone(local_tz).date()
            return parsed_iso.date()
        except ValueError:
            pass

        normalized = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", text, flags=re.IGNORECASE)

        try:
            parsed_rfc = parsedate_to_datetime(normalized)
            if parsed_rfc:
                if parsed_rfc.tzinfo is not None:
                    return parsed_rfc.astimezone(local_tz).date()
                return parsed_rfc.date()
        except (TypeError, ValueError):
            pass

        zh_match = re.search(r"(\d{4})\s*[year/\-.]\s*(\d{1,2})\s*[month/\-.]\s*(\d{1,2})\s*day?", text)
        if zh_match:
            try:
                return date(int(zh_match.group(1)), int(zh_match.group(2)), int(zh_match.group(3)))
            except ValueError:
                pass

        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d",
            "%Y.%m.%d %H:%M:%S",
            "%Y.%m.%d %H:%M",
            "%Y.%m.%d",
            "%Y%m%d",
            "%b %d, %Y",
            "%B %d, %Y",
            "%d %b %Y",
            "%d %B %Y",
            "%a, %d %b %Y %H:%M:%S %z",
        ):
            try:
                parsed_dt = datetime.strptime(normalized, fmt)
                if parsed_dt.tzinfo is not None:
                    return parsed_dt.astimezone(local_tz).date()
                return parsed_dt.date()
            except ValueError:
                continue

        return None

    def _filter_news_response(
        self,
        response: SearchResponse,
        *,
        search_days: int,
        max_results: int,
        log_scope: str,
    ) -> SearchResponse:
        """Hard-filter results by published_date recency and normalize date strings."""
        if not response.success or not response.results:
            return response

        today = datetime.now().date()
        earliest = today - timedelta(days=max(0, int(search_days) - 1))
        latest = today + timedelta(days=self.FUTURE_TOLERANCE_DAYS)

        filtered: List[SearchResult] = []
        dropped_unknown = 0
        dropped_old = 0
        dropped_future = 0

        for item in response.results:
            published = self._normalize_news_publish_date(item.published_date)
            if published is None:
                dropped_unknown += 1
                continue
            if published < earliest:
                dropped_old += 1
                continue
            if published > latest:
                dropped_future += 1
                continue

            filtered.append(
                SearchResult(
                    title=item.title,
                    snippet=item.snippet,
                    url=item.url,
                    source=item.source,
                    published_date=published.isoformat(),
                )
            )
            if len(filtered) >= max_results:
                break

        if dropped_unknown or dropped_old or dropped_future:
            logger.info(
                "[newsfiltering] %s: provider=%s, total=%s, kept=%s, drop_unknown=%s, drop_old=%s, drop_future=%s, window=[%s,%s]",
                log_scope,
                response.provider,
                len(response.results),
                len(filtered),
                dropped_unknown,
                dropped_old,
                dropped_future,
                earliest.isoformat(),
                latest.isoformat(),
            )

        return SearchResponse(
            query=response.query,
            results=filtered,
            provider=response.provider,
            success=response.success,
            error_message=response.error_message,
            search_time=response.search_time,
        )

    def _normalize_and_limit_response(
        self,
        response: SearchResponse,
        *,
        max_results: int,
    ) -> SearchResponse:
        """Normalize parseable dates without enforcing freshness filtering."""
        if not response.success or not response.results:
            return response

        normalized_results: List[SearchResult] = []
        for item in response.results[:max_results]:
            normalized_date = self._normalize_news_publish_date(item.published_date)
            normalized_results.append(
                SearchResult(
                    title=item.title,
                    snippet=item.snippet,
                    url=item.url,
                    source=item.source,
                    published_date=(
                        normalized_date.isoformat() if normalized_date is not None else item.published_date
                    ),
                )
            )

        return SearchResponse(
            query=response.query,
            results=normalized_results,
            provider=response.provider,
            success=response.success,
            error_message=response.error_message,
            search_time=response.search_time,
        )
    
    def search_stock_news(
        self,
        stock_code: str,
        stock_name: str,
        max_results: int = 5,
        focus_keywords: Optional[List[str]] = None
    ) -> SearchResponse:
        """
        searchstockrelatednews
        
        Args:
            stock_code: stock code
            stock_name: stockname
            max_results: maxreturnresultcount
            focus_keywords: key pointmonitorkeywordlist
            
        Returns:
            SearchResponse object
        """
        # strategywindowpriority：ultra_short/short/medium/long = 1/3/7/30 days，
        # andunifiedreceive NEWS_MAX_AGE_DAYS upper limitapproximatelyconstrain。
        search_days = self._effective_news_window_days()
        provider_max_results = self._provider_request_size(max_results)

        # buildsearchquerying（optimizesearcheffect）
        CHINA_RELEVANT = {"ASML", "MP", "LNG", "NEM"}
        symbol = stock_code.strip().upper()
        if focus_keywords:
            queries = [" ".join(focus_keywords)]
        elif symbol in CHINA_RELEVANT:
            queries = [
                f"{symbol} stock latest news analysis 2026",
                f"{symbol} latestmessage analyzing",
            ]
        else:
            queries = [f"{symbol} stock latest news analysis 2026"]
        query = queries[0]

        logger.info(
            (
                "searchstocknews: %s(%s), query='%s', timerange: recent%sdays "
                "(profile=%s, NEWS_MAX_AGE_DAYS=%s), targetcount=%s, providerrequestcount=%s"
            ),
            stock_name,
            stock_code,
            query,
            search_days,
            self.news_strategy_profile,
            self.news_max_age_days,
            max_results,
            provider_max_results,
        )

        # Check cache first
        cache_key = self._cache_key(query, max_results, search_days)
        cached = self._get_cached(cache_key)
        if cached is not None:
            logger.info(f"usecachesearchresult: {stock_name}({stock_code})")
            return cached

        # depend ontimestoeachqueryingtryeachcountsearchengine，mergingresult
        had_provider_success = False
        all_results = []

        for q in queries:
            for provider in self._providers:
                if not provider.is_available:
                    continue

                search_kwargs: Dict[str, Any] = {}
                if isinstance(provider, TavilySearchProvider):
                    search_kwargs["topic"] = "news"

                response = provider.search(q, provider_max_results, days=search_days, **search_kwargs)
                filtered_response = self._filter_news_response(
                    response,
                    search_days=search_days,
                    max_results=max_results,
                    log_scope=f"{stock_code}:{provider.name}:stock_news",
                )
                had_provider_success = had_provider_success or bool(response.success)

                if filtered_response.success and filtered_response.results:
                    logger.info(f"use {provider.name} searchsuccessful (query='{q}')")
                    all_results.extend(filtered_response.results)
                    break
                else:
                    if response.success and not filtered_response.results:
                        logger.info(
                            "%s searchsuccessfulbutfilteringafternovalidnews，continuingtrybelowoneengine",
                            provider.name,
                        )
                    else:
                        logger.warning(
                            "%s searchfailed: %s，try nextengine",
                            provider.name,
                            response.error_message,
                        )

        if all_results:
            combined_response = SearchResponse(
                query=query,
                results=all_results,
                provider="Combined",
                success=True,
                error_message=None,
            )
            self._put_cache(cache_key, combined_response)
            return combined_response

        if had_provider_success:
            return SearchResponse(
                query=query,
                results=[],
                provider="Filtered",
                success=True,
                error_message=None,
            )

        # allengineallfailed
        return SearchResponse(
            query=query,
            results=[],
            provider="None",
            success=False,
            error_message="allsearchengineallunavailableorsearchfailed"
        )
    
    def search_stock_events(
        self,
        stock_code: str,
        stock_name: str,
        event_types: Optional[List[str]] = None
    ) -> SearchResponse:
        """
        searchstockspecificevent（annual report forecast、reduce holdingsetc）
        
        specializedspecifictotradedecisionrelatedImportanteventproceedsearch
        
        Args:
            stock_code: stock code
            stock_name: stockname
            event_types: eventtypelist
            
        Returns:
            SearchResponse object
        """
        if event_types is None:
            if self._is_foreign_stock(stock_code):
                event_types = ["earnings report", "insider selling", "quarterly results"]
            else:
                event_types = ["annual report forecast", "reduce holdingsannouncement", "performanceflash report"]
        
        # buildtargetto-nessquerying
        event_query = " OR ".join(event_types)
        query = f"{stock_name} ({event_query})"
        
        logger.info(f"searchstockevent: {stock_name}({stock_code}) - {event_types}")
        
        # try each in sequencesearchengine
        for provider in self._providers:
            if not provider.is_available:
                continue
            
            response = provider.search(query, max_results=5)
            
            if response.success:
                return response
        
        return SearchResponse(
            query=query,
            results=[],
            provider="None",
            success=False,
            error_message="eventsearchfailed"
        )
    
    def search_comprehensive_intel(
        self,
        stock_code: str,
        stock_name: str,
        max_searches: int = 3
    ) -> Dict[str, SearchResponse]:
        """
        multi-dimensional intelligencesearch（simultaneouslyusemultiplecountengine、multiplecountdimension）
        
        searchdimension：
        1. latestmessage - recentperiodnewsdynamic
        2. risktroubleshoot - reduce holdings、penalty、negative news
        3. performanceexpected - annual report forecast、performanceflash report
        
        Args:
            stock_code: stock code
            stock_name: stockname
            max_searches: maxsearchcount
            
        Returns:
            {dimensionname: SearchResponse} dictionary
        """
        results = {}
        search_count = 0

        is_foreign = self._is_foreign_stock(stock_code)
        is_index_etf = self.is_index_or_etf(stock_code, stock_name)

        if is_foreign:
            search_dimensions = [
                {
                    'name': 'latest_news',
                    'query': f"{stock_name} {stock_code} latest news events",
                    'desc': 'latestmessage',
                    'tavily_topic': 'news',
                    'strict_freshness': True,
                },
                {
                    'name': 'market_analysis',
                    'query': f"{stock_name} analyst rating target price report",
                    'desc': 'institutionanalyzing',
                    'tavily_topic': None,
                    'strict_freshness': False,
                },
                {
                    'name': 'risk_check',
                    'query': (
                        f"{stock_name} {stock_code} index performance outlook tracking error"
                        if is_index_etf else f"{stock_name} risk insider selling lawsuit litigation"
                    ),
                    'desc': 'risktroubleshoot',
                    'tavily_topic': None if is_index_etf else 'news',
                    'strict_freshness': not is_index_etf,
                },
                {
                    'name': 'earnings',
                    'query': (
                        f"{stock_name} {stock_code} index performance composition outlook"
                        if is_index_etf else f"{stock_name} earnings revenue profit growth forecast"
                    ),
                    'desc': 'performanceexpected',
                    'tavily_topic': None,
                    'strict_freshness': False,
                },
                {
                    'name': 'industry',
                    'query': (
                        f"{stock_name} {stock_code} index sector allocation holdings"
                        if is_index_etf else f"{stock_name} industry competitors market share outlook"
                    ),
                    'desc': 'industry analysis',
                    'tavily_topic': None,
                    'strict_freshness': False,
                },
            ]
        else:
            search_dimensions = [
                {
                    'name': 'latest_news',
                    'query': f"{stock_name} {stock_code} latest news major event",
                    'desc': 'latestmessage',
                    'tavily_topic': 'news',
                    'strict_freshness': True,
                },
                {
                    'name': 'market_analysis',
                    'query': f"{stock_name} research report target price evaluatelevel depthanalyzing",
                    'desc': 'institutionanalyzing',
                    'tavily_topic': None,
                    'strict_freshness': False,
                },
                {
                    'name': 'risk_check',
                    'query': (
                        f"{stock_name} indextrend trackerror/tolerance netvalue performance"
                        if is_index_etf else f"{stock_name} reduce holdings penalty violation litigation negative news risk"
                    ),
                    'desc': 'risktroubleshoot',
                    'tavily_topic': None if is_index_etf else 'news',
                    'strict_freshness': not is_index_etf,
                },
                {
                    'name': 'earnings',
                    'query': (
                        f"{stock_name} indexbecomeminute netvalue trackperformance"
                        if is_index_etf else f"{stock_name} performanceforecast financial report revenue net profit year-on-year growth"
                    ),
                    'desc': 'performanceexpected',
                    'tavily_topic': None,
                    'strict_freshness': False,
                },
                {
                    'name': 'industry',
                    'query': (
                        f"{stock_name} indexbecomeminutestocks industryconfiguration weight"
                        if is_index_etf else f"{stock_name} placeinindustry competitiontohand marketsharesamount industrybeforeoutlook"
                    ),
                    'desc': 'industry analysis',
                    'tavily_topic': None,
                    'strict_freshness': False,
                },
            ]
        
        search_days = self._effective_news_window_days()
        target_per_dimension = 3
        provider_max_results = self._provider_request_size(target_per_dimension)

        logger.info(
            (
                "startingmulti-dimensional intelligencesearch: %s(%s), timerange: recent%sdays "
                "(profile=%s, NEWS_MAX_AGE_DAYS=%s), targetcount=%s, providerrequestcount=%s"
            ),
            stock_name,
            stock_code,
            search_days,
            self.news_strategy_profile,
            self.news_max_age_days,
            target_per_dimension,
            provider_max_results,
        )
        
        # roundstreamusenotsamesearchengine
        provider_index = 0
        
        for dim in search_dimensions:
            if search_count >= max_searches:
                break
            
            # selectsearchengine（roundstreamuse）
            available_providers = [p for p in self._providers if p.is_available]
            if not available_providers:
                break
            
            provider = available_providers[provider_index % len(available_providers)]
            provider_index += 1
            
            logger.info(f"[intelligencesearch] {dim['desc']}: use {provider.name}")

            if isinstance(provider, TavilySearchProvider) and dim.get('tavily_topic'):
                response = provider.search(
                    dim['query'],
                    max_results=provider_max_results,
                    days=search_days,
                    topic=dim['tavily_topic'],
                )
            else:
                response = provider.search(
                    dim['query'],
                    max_results=provider_max_results,
                    days=search_days,
                )
            if dim['strict_freshness']:
                filtered_response = self._filter_news_response(
                    response,
                    search_days=search_days,
                    max_results=target_per_dimension,
                    log_scope=f"{stock_code}:{provider.name}:{dim['name']}",
                )
            else:
                filtered_response = self._normalize_and_limit_response(
                    response,
                    max_results=target_per_dimension,
                )
            results[dim['name']] = filtered_response
            search_count += 1
            
            if response.success:
                logger.info(
                    "[intelligencesearch] %s: raw=%sitems, filteringafter=%sitems",
                    dim['desc'],
                    len(response.results),
                    len(filtered_response.results),
                )
            else:
                logger.warning(f"[intelligencesearch] {dim['desc']}: searchfailed - {response.error_message}")
            
            # brief delayavoidrequesttoo fast
            time.sleep(0.5)
        
        return results
    
    def format_intel_report(self, intel_results: Dict[str, SearchResponse], stock_name: str) -> str:
        """
        formattingintelligencesearchresultasreport
        
        Args:
            intel_results: multipledimensionsearchresult
            stock_name: stockname
            
        Returns:
            formattingintelligencereporttext
        """
        lines = [f"【{stock_name} intelligencesearchresult】"]
        
        # dimensiondisplayorder
        display_order = ['latest_news', 'market_analysis', 'risk_check', 'earnings', 'industry']
        
        for dim_name in display_order:
            if dim_name not in intel_results:
                continue
                
            resp = intel_results[dim_name]
            
            # getdimensiondescription
            dim_desc = dim_name
            if dim_name == 'latest_news': dim_desc = '📰 latestmessage'
            elif dim_name == 'market_analysis': dim_desc = '📈 institutionanalyzing'
            elif dim_name == 'risk_check': dim_desc = '⚠️ risktroubleshoot'
            elif dim_name == 'earnings': dim_desc = '📊 performanceexpected'
            elif dim_name == 'industry': dim_desc = '🏭 industry analysis'
            
            lines.append(f"\n{dim_desc} (source: {resp.provider}):")
            if resp.success and resp.results:
                # increasedisplaycount
                for i, r in enumerate(resp.results[:4], 1):
                    date_str = f" [{r.published_date}]" if r.published_date else ""
                    lines.append(f"  {i}. {r.title}{date_str}")
                    # ifsummarytoo short，possiblyinfovolumeinsufficient
                    snippet = r.snippet[:150] if len(r.snippet) > 20 else r.snippet
                    lines.append(f"     {snippet}...")
            else:
                lines.append("  not foundrelated information")
        
        return "\n".join(lines)
    
    def batch_search(
        self,
        stocks: List[Dict[str, str]],
        max_results_per_stock: int = 3,
        delay_between: float = 1.0
    ) -> Dict[str, SearchResponse]:
        """
        Batch search news for multiple stocks.
        
        Args:
            stocks: List of stocks
            max_results_per_stock: Max results per stock
            delay_between: Delay between searches (seconds)
            
        Returns:
            Dict of results
        """
        results = {}
        
        for i, stock in enumerate(stocks):
            if i > 0:
                time.sleep(delay_between)
            
            code = stock.get('code', '')
            name = stock.get('name', '')
            
            response = self.search_stock_news(code, name, max_results_per_stock)
            results[code] = response
        
        return results

    def search_stock_price_fallback(
        self,
        stock_code: str,
        stock_name: str,
        max_attempts: int = 3,
        max_results: int = 5
    ) -> SearchResponse:
        """
        Enhance search when data sources fail.
        
        When all data sources (efinance, akshare, tushare, baostock, etc.) fail to get
        stock data, use search engines to find stock trends and price info as supplemental data for AI analysis.
        
        Strategy:
        1. Search using multiple keyword templates
        2. Try all available search engines for each keyword
        3. Aggregate and deduplicate results
        
        Args:
            stock_code: Stock Code
            stock_name: Stock Name
            max_attempts: Max search attempts (using different keywords)
            max_results: Max results to return
            
        Returns:
            SearchResponse object with aggregated results
        """

        if not self.is_available:
            return SearchResponse(
                query=f"{stock_name} stock pricetrend",
                results=[],
                provider="None",
                success=False,
                error_message="notconfigurationsearchcapability"
            )
        
        logger.info(f"[enhancedsearch] datasourcefailed，startenhancedsearch: {stock_name}({stock_code})")
        
        all_results = []
        seen_urls = set()
        successful_providers = []
        
        # usemultiplecountkeywordTemplatesearch
        is_foreign = self._is_foreign_stock(stock_code)
        keywords = self.ENHANCED_SEARCH_KEYWORDS_EN if is_foreign else self.ENHANCED_SEARCH_KEYWORDS
        for i, keyword_template in enumerate(keywords[:max_attempts]):
            query = keyword_template.format(name=stock_name, code=stock_code)
            
            logger.info(f"[enhancedsearch] the {i+1}/{max_attempts} timessearch: {query}")
            
            # try each in sequencesearchengine
            for provider in self._providers:
                if not provider.is_available:
                    continue
                
                try:
                    response = provider.search(query, max_results=3)
                    
                    if response.success and response.results:
                        # deduplicateandaddresult
                        for result in response.results:
                            if result.url not in seen_urls:
                                seen_urls.add(result.url)
                                all_results.append(result)
                                
                        if provider.name not in successful_providers:
                            successful_providers.append(provider.name)
                        
                        logger.info(f"[enhancedsearch] {provider.name} return {len(response.results)} results")
                        break  # successfulafterjumptobelowonecountkeyword
                    else:
                        logger.debug(f"[enhancedsearch] {provider.name} noresultorfailed")
                        
                except Exception as e:
                    logger.warning(f"[enhancedsearch] {provider.name} searchabnormal: {e}")
                    continue
            
            # brief delayavoidrequesttoo fast
            if i < max_attempts - 1:
                time.sleep(0.5)
        
        # summaryresult
        if all_results:
            # truncatebefore max_results items
            final_results = all_results[:max_results]
            provider_str = ", ".join(successful_providers) if successful_providers else "None"
            
            logger.info(f"[enhancedsearch] completed，totalget {len(final_results)} results（source: {provider_str}）")
            
            return SearchResponse(
                query=f"{stock_name}({stock_code}) stock pricetrend",
                results=final_results,
                provider=provider_str,
                success=True,
            )
        else:
            logger.warning(f"[enhancedsearch] allsearchaveragenotreturnresult")
            return SearchResponse(
                query=f"{stock_name}({stock_code}) stock pricetrend",
                results=[],
                provider="None",
                success=False,
                error_message="enhancedsearchnot foundrelated information"
            )

    def search_stock_with_enhanced_fallback(
        self,
        stock_code: str,
        stock_name: str,
        include_news: bool = True,
        include_price: bool = False,
        max_results: int = 5
    ) -> Dict[str, SearchResponse]:
        """
        compositesearchAPI/interface（supportnewsandstock priceinfo）
        
        when include_price=True when，willsimultaneouslysearchnewsandstock priceinfo。
        mainly fordatasourcecompletelyfailedwhenfallbackplan。
        
        Args:
            stock_code: stock code
            stock_name: stockname
            include_news: whethersearchnews
            include_price: whethersearchstock price/trendinfo
            max_results: eachclasssearchmaxresultcount
            
        Returns:
            {'news': SearchResponse, 'price': SearchResponse} dictionary
        """
        results = {}
        
        if include_news:
            results['news'] = self.search_stock_news(
                stock_code, 
                stock_name, 
                max_results=max_results
            )
        
        if include_price:
            results['price'] = self.search_stock_price_fallback(
                stock_code,
                stock_name,
                max_attempts=3,
                max_results=max_results
            )
        
        return results

    def format_price_search_context(self, response: SearchResponse) -> str:
        """
        willstock pricesearchresultformattingas AI analyzingcontext
        
        Args:
            response: searchresponseobject
            
        Returns:
            formattingtext，candirectlyfor AI analyzing
        """
        if not response.success or not response.results:
            return "【stock pricetrendsearch】not foundrelated information，pleasewithotherchanneldataasaccurate。"
        
        lines = [
            f"【stock pricetrendsearchresult】（source: {response.provider}）",
            "⚠️ Note：withbelowinfofromnetworksearch，onlyprovidereference，possiblyexistsdelayornotaccurate。",
            ""
        ]
        
        for i, result in enumerate(response.results, 1):
            date_str = f" [{result.published_date}]" if result.published_date else ""
            lines.append(f"{i}. 【{result.source}】{result.title}{date_str}")
            lines.append(f"   {result.snippet[:200]}...")
            lines.append("")
        
        return "\n".join(lines)


# === convenientfunction ===
_search_service: Optional[SearchService] = None


def get_search_service() -> SearchService:
    """getsearchservicesingleton"""
    global _search_service
    
    if _search_service is None:
        from src.config import get_config
        config = get_config()
        
        _search_service = SearchService(
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
    
    return _search_service


def reset_search_service() -> None:
    """resetsearchservice（fortesting）"""
    global _search_service
    _search_service = None


if __name__ == "__main__":
    # testingsearchservice
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s'
    )
    
    # manualtesting（needconfiguration API Key）
    service = get_search_service()
    
    if service.is_available:
        print("=== testingstocknewssearch ===")
        response = service.search_stock_news("300389", "Aicompared toSen")
        print(f"searchstatus: {'successful' if response.success else 'failed'}")
        print(f"searchengine: {response.provider}")
        print(f"resultquantity: {len(response.results)}")
        print(f"elapsed: {response.search_time:.2f}s")
        print("\n" + response.to_context())
    else:
        print("notconfigurationsearchcapability，skiptesting")
