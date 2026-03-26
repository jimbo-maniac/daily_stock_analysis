# -*- coding: utf-8 -*-
"""
===================================
realtimequote/market dataunifiedtypedefine & circuit breakmechanism
===================================

designtarget：
1. unifiedeachdatasourcerealtimequote/market datareturnstructure
2. implementcircuit break/cooling downmechanism，avoidconsecutivefailedwhenrepeatedrequest
3. support multipledatasourcefailureswitch

usage：
- all Fetcher  get_realtime_quote() unifiedreturn UnifiedRealtimeQuote
- CircuitBreaker manageeachdatasource'scircuit breakstatus
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Union
from enum import Enum

logger = logging.getLogger(__name__)


# ============================================
# generictypeconvertingutility function
# ============================================
# designDescription：
# eachdatasourcereturnedrawdatatypeinconsistent（str/float/int/NaN），
# usethesefunctionunifiedconverting，avoidineach Fetcher induplicatedefine。

def safe_float(val: Any, default: Optional[float] = None) -> Optional[float]:
    """
    safeconvertingasfloating pointcount
    
    processingscenario：
    - None / emptystring → default
    - pandas NaN / numpy NaN → default
    - valuestring → float
    - alreadyisvalue → float
    
    Args:
        val: pendingconvertingvalue
        default: convertingfailedwhendefault value
        
    Returns:
        convertingafterfloating pointcount，ordefault value
    """
    try:
        if val is None:
            return default
        
        # processingstring
        if isinstance(val, str):
            val = val.strip()
            if val == "" or val == "-" or val == "--":
                return default
        
        # processing pandas/numpy NaN
        # use math.isnan andis not pd.isna，avoidmandatorydependency pandas
        import math
        try:
            if math.isnan(float(val)):
                return default
        except (ValueError, TypeError):
            pass
        
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_int(val: Any, default: Optional[int] = None) -> Optional[int]:
    """
    safeconvertingasinteger
    
    firstconvertingas float，againgetfull，processing "123.0" thisclasssituation
    
    Args:
        val: pendingconvertingvalue
        default: convertingfailedwhendefault value
        
    Returns:
        convertingafterinteger，ordefault value
    """
    f_val = safe_float(val, default=None)
    if f_val is not None:
        return int(f_val)
    return default


class RealtimeSource(Enum):
    """realtimequote/market datadatasource"""
    EFINANCE = "efinance"           # Eastmoney（efinancelibrary）
    AKSHARE_EM = "akshare_em"       # Eastmoney（aksharelibrary）
    AKSHARE_SINA = "akshare_sina"   # Sina Finance
    AKSHARE_QQ = "akshare_qq"       # Tencent Finance
    TUSHARE = "tushare"             # Tushare Pro
    TENCENT = "tencent"             # Tencentdirect connect
    SINA = "sina"                   # Sinadirect connect
    STOOQ = "stooq"                 # Stooq US stockfallback
    FALLBACK = "fallback"           # fallbackfallback


@dataclass
class UnifiedRealtimeQuote:
    """
    unifiedrealtimequote/market datadatastructure
    
    designoriginalthen：
    - eachdatasourcereturnedfieldpossiblynotsame，missingfielduse None indicates
    - mainprocessuse getattr(quote, field, None) get，guaranteecompatible-ness
    - source fieldmarkdatasource，for conveniencedebug
    """
    code: str
    name: str = ""
    source: RealtimeSource = RealtimeSource.FALLBACK
    
    # === corepricedata（severalseemallall sourceshas）===
    price: Optional[float] = None           # latest price
    change_pct: Optional[float] = None      # price change percentage(%)
    change_amount: Optional[float] = None   # price change amount
    
    # === volume-priceindicator（partialsourcepossiblymissing）===
    volume: Optional[int] = None            # trading volume（hand）
    amount: Optional[float] = None          # trading amount（yuan）
    volume_ratio: Optional[float] = None    # volume ratio
    turnover_rate: Optional[float] = None   # turnover rate(%)
    amplitude: Optional[float] = None       # amplitude(%)
    
    # === priceinterval ===
    open_price: Optional[float] = None      # opening price
    high: Optional[float] = None            # highest price
    low: Optional[float] = None             # lowest price
    pre_close: Optional[float] = None       # yesterday closeprice
    
    # === estimatevalueindicator（onlyEastmoneyetcfullAPI/interfacehas）===
    pe_ratio: Optional[float] = None        # P/E ratio(dynamic)
    pb_ratio: Optional[float] = None        # P/B ratio
    total_mv: Optional[float] = None        # total market cap(yuan)
    circ_mv: Optional[float] = None         # circulating market cap(yuan)
    
    # === otherindicator ===
    change_60d: Optional[float] = None      # 60dayprice change percentage(%)
    high_52w: Optional[float] = None        # 52weekly high
    low_52w: Optional[float] = None         # 52weekly low
    
    def to_dict(self) -> Dict[str, Any]:
        """convertingasdictionary（filtering None value）"""
        result = {
            'code': self.code,
            'name': self.name,
            'source': self.source.value,
        }
        # onlyaddnon- None field
        optional_fields = [
            'price', 'change_pct', 'change_amount', 'volume', 'amount',
            'volume_ratio', 'turnover_rate', 'amplitude',
            'open_price', 'high', 'low', 'pre_close',
            'pe_ratio', 'pb_ratio', 'total_mv', 'circ_mv',
            'change_60d', 'high_52w', 'low_52w'
        ]
        for f in optional_fields:
            val = getattr(self, f, None)
            if val is not None:
                result[f] = val
        return result
    
    def has_basic_data(self) -> bool:
        """checkwhetherhasbasicpricedata"""
        return self.price is not None and self.price > 0
    
    def has_volume_data(self) -> bool:
        """checkwhetherhasvolume-pricedata"""
        return self.volume_ratio is not None or self.turnover_rate is not None


@dataclass
class ChipDistribution:
    """
    chip distributiondata
    
    reflectholdingcostminutedistributeandprofitsituation
    """
    code: str
    date: str = ""
    source: str = "akshare"
    
    # profitsituation
    profit_ratio: float = 0.0     # profitproportion(0-1)
    avg_cost: float = 0.0         # average cost
    
    # chip concentration
    cost_90_low: float = 0.0      # 90%chip costlower limit
    cost_90_high: float = 0.0     # 90%chip costupper limit
    concentration_90: float = 0.0  # 90%chip concentration（moresmallmoresetin）
    
    cost_70_low: float = 0.0      # 70%chip costlower limit
    cost_70_high: float = 0.0     # 70%chip costupper limit
    concentration_70: float = 0.0  # 70%chip concentration
    
    def to_dict(self) -> Dict[str, Any]:
        """convertingasdictionary"""
        return {
            'code': self.code,
            'date': self.date,
            'source': self.source,
            'profit_ratio': self.profit_ratio,
            'avg_cost': self.avg_cost,
            'cost_90_low': self.cost_90_low,
            'cost_90_high': self.cost_90_high,
            'concentration_90': self.concentration_90,
            'concentration_70': self.concentration_70,
        }
    
    def get_chip_status(self, current_price: float) -> str:
        """
        getchipstatusdescription
        
        Args:
            current_price: currentstock price
            
        Returns:
            chipstatusdescription
        """
        status_parts = []
        
        # profitproportionanalyzing
        if self.profit_ratio >= 0.9:
            status_parts.append("profitable positionsextremehigh(profitable positions>90%)")
        elif self.profit_ratio >= 0.7:
            status_parts.append("profitable positionsrelativelyhigh(profitable positions70-90%)")
        elif self.profit_ratio >= 0.5:
            status_parts.append("profitable positionsinetc(profitable positions50-70%)")
        elif self.profit_ratio >= 0.3:
            status_parts.append("trapped positionsinetc(trapped positions50-70%)")
        elif self.profit_ratio >= 0.1:
            status_parts.append("trapped positionsrelativelyhigh(trapped positions70-90%)")
        else:
            status_parts.append("trapped positionsextremehigh(trapped positions>90%)")
        
        # chip concentrationanalyzing (90%concentration < 10% indicatessetin)
        if self.concentration_90 < 0.08:
            status_parts.append("chiphighdegreesetin")
        elif self.concentration_90 < 0.15:
            status_parts.append("chiprelativelysetin")
        elif self.concentration_90 < 0.25:
            status_parts.append("chipminutescattereddegreeinetc")
        else:
            status_parts.append("chiprelativelyminutescattered")
        
        # costwithcurrent price relationship
        if current_price > 0 and self.avg_cost > 0:
            cost_diff = (current_price - self.avg_cost) / self.avg_cost * 100
            if cost_diff > 20:
                status_parts.append(f"current pricehighataverage cost{cost_diff:.1f}%")
            elif cost_diff > 5:
                status_parts.append(f"current pricestrategyhighatcost{cost_diff:.1f}%")
            elif cost_diff > -5:
                status_parts.append("current priceconnectrecentaverage cost")
            else:
                status_parts.append(f"current pricelowataverage cost{abs(cost_diff):.1f}%")
        
        return "，".join(status_parts)


class CircuitBreaker:
    """
    circuit breaker - managedatasource'scircuit break/cooling downstatus
    
    strategy：
    - consecutivefailed N timesafterentercircuit breakstatus
    - circuit breakperiodbetweenskipthisdatasource
    - cooldown timeafterautomaticrestorehalf-openstatus
    - half-openstatusbelowsinglesuccessfulthencompletelyrestore，failedthencontinuingcircuit break
    
    statusmachine：
    CLOSED（normal） --failedNtimes--> OPEN（circuit break）--cooldown timeto--> HALF_OPEN（half-open）
    HALF_OPEN --successful--> CLOSED
    HALF_OPEN --failed--> OPEN
    """
    
    # statusconstant
    CLOSED = "closed"      # normalstatus
    OPEN = "open"          # circuit breakstatus（unavailable）
    HALF_OPEN = "half_open"  # half-openstatus（probe-nessrequest）
    
    def __init__(
        self,
        failure_threshold: int = 3,       # consecutivefailedcountthreshold
        cooldown_seconds: float = 300.0,  # cooldown time（seconds），default5minutes
        half_open_max_calls: int = 1      # half-openstatusmaxtrycount
    ):
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self.half_open_max_calls = half_open_max_calls
        
        # eachdatasourcestatus {source_name: {state, failures, last_failure_time, half_open_calls}}
        self._states: Dict[str, Dict[str, Any]] = {}
    
    def _get_state(self, source: str) -> Dict[str, Any]:
        """getorinitializingdatasourcestatus"""
        if source not in self._states:
            self._states[source] = {
                'state': self.CLOSED,
                'failures': 0,
                'last_failure_time': 0.0,
                'half_open_calls': 0
            }
        return self._states[source]
    
    def is_available(self, source: str) -> bool:
        """
        checkdatasourcewhetheravailable
        
        return True indicatescantryrequest
        return False indicatesshouldskipthisdatasource
        """
        state = self._get_state(source)
        current_time = time.time()
        
        if state['state'] == self.CLOSED:
            return True
        
        if state['state'] == self.OPEN:
            # checkcooldown time
            time_since_failure = current_time - state['last_failure_time']
            if time_since_failure >= self.cooldown_seconds:
                # cooling downcompleted，enterhalf-openstatus
                state['state'] = self.HALF_OPEN
                state['half_open_calls'] = 0
                logger.info(f"[circuit breaker] {source} cooling downcompleted，enterhalf-openstatus")
                return True
            else:
                remaining = self.cooldown_seconds - time_since_failure
                logger.debug(f"[circuit breaker] {source} atcircuit breakstatus，remainingcooldown time: {remaining:.0f}s")
                return False
        
        if state['state'] == self.HALF_OPEN:
            # half-openstatusbelowconstraintrequestcount
            if state['half_open_calls'] < self.half_open_max_calls:
                return True
            return False
        
        return True
    
    def record_success(self, source: str) -> None:
        """recordsuccessfulrequest"""
        state = self._get_state(source)
        
        if state['state'] == self.HALF_OPEN:
            # half-openstatusbelowsuccessful，completelyrestore
            logger.info(f"[circuit breaker] {source} half-openstatusrequest successful，restorenormal")
        
        # resetstatus
        state['state'] = self.CLOSED
        state['failures'] = 0
        state['half_open_calls'] = 0
    
    def record_failure(self, source: str, error: Optional[str] = None) -> None:
        """recordfailedrequest"""
        state = self._get_state(source)
        current_time = time.time()
        
        state['failures'] += 1
        state['last_failure_time'] = current_time
        
        if state['state'] == self.HALF_OPEN:
            # half-openstatusbelowfailed，continuingcircuit break
            state['state'] = self.OPEN
            state['half_open_calls'] = 0
            logger.warning(f"[circuit breaker] {source} half-openstatusrequest failed，continuingcircuit break {self.cooldown_seconds}s")
        elif state['failures'] >= self.failure_threshold:
            # reachtothreshold，entercircuit break
            state['state'] = self.OPEN
            logger.warning(f"[circuit breaker] {source} consecutivefailed {state['failures']} times，entercircuit breakstatus "
                          f"(cooling down {self.cooldown_seconds}s)")
            if error:
                logger.warning(f"[circuit breaker] mostaftererror: {error}")
    
    def get_status(self) -> Dict[str, str]:
        """get alldatasourcestatus"""
        return {source: info['state'] for source, info in self._states.items()}
    
    def reset(self, source: Optional[str] = None) -> None:
        """resetcircuit breakerstatus"""
        if source:
            if source in self._states:
                del self._states[source]
        else:
            self._states.clear()


# globalcircuit breakerinstance（realtimequote/market dataspecializeduse）
_realtime_circuit_breaker = CircuitBreaker(
    failure_threshold=3,      # consecutivefailed3timescircuit break
    cooldown_seconds=300.0,   # cooling down5minutes
    half_open_max_calls=1
)

# chipAPI/interfacecircuit breaker（moreconservativestrategy，becauseasthisAPI/interfacemoreunstable）
_chip_circuit_breaker = CircuitBreaker(
    failure_threshold=2,      # consecutivefailed2timescircuit break
    cooldown_seconds=600.0,   # cooling down10minutes
    half_open_max_calls=1
)


def get_realtime_circuit_breaker() -> CircuitBreaker:
    """get realtimequote/market datacircuit breaker"""
    return _realtime_circuit_breaker


def get_chip_circuit_breaker() -> CircuitBreaker:
    """getchipAPI/interfacecircuit breaker"""
    return _chip_circuit_breaker
