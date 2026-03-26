# -*- coding: utf-8 -*-
"""
===================================
trendtradeanalyzinghandler - based onusertrading philosophy
===================================

trading philosophycoreoriginalthen：
1. strict entry strategy - don't chase highs，chaserequesteachtradetradesuccessfulrate
2. trendtrade - MA5>MA10>MA20 long positionarrange，following the trendas
3. efficiency first - monitorchip structuregoodstock
4. buy point preference - in MA5/MA10 nearpullbackbuy

technicalstandard：
- long positionarrange：MA5 > MA10 > MA20
- BIAS ratio：(Close - MA5) / MA5 < 5%（don't chase highs）
- volumepattern：volume contractionpullbackpriority
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, Any, List
from enum import Enum

import pandas as pd
import numpy as np

from src.config import get_config

logger = logging.getLogger(__name__)


class TrendStatus(Enum):
    """trendstatusenum"""
    STRONG_BULL = "stronglong position"      # MA5 > MA10 > MA20，andgap widening
    BULL = "long positionarrange"             # MA5 > MA10 > MA20
    WEAK_BULL = "weaklong position"        # MA5 > MA10，but MA10 < MA20
    CONSOLIDATION = "platefull"        # moving averageentangle
    WEAK_BEAR = "weakshort position"        # MA5 < MA10，but MA10 > MA20
    BEAR = "short positionarrange"             # MA5 < MA10 < MA20
    STRONG_BEAR = "strongshort position"      # MA5 < MA10 < MA20，andgap widening


class VolumeStatus(Enum):
    """volumestatusenum"""
    HEAVY_VOLUME_UP = "volume increaserising"       # volume-pricerise together
    HEAVY_VOLUME_DOWN = "volume increasefalling"     # volume increasesell-off
    SHRINK_VOLUME_UP = "volume contractionrising"      # novolumerising
    SHRINK_VOLUME_DOWN = "volume contractionpullback"    # volume contractionpullback（good）
    NORMAL = "volumenormal"


class BuySignal(Enum):
    """buy signalenum"""
    STRONG_BUY = "strong buy"       # multipleitemsconditions met
    BUY = "buy"                  # basicitemsconditions met
    HOLD = "hold"                 # alreadyholdcancontinuing
    WAIT = "wait and see"                 # waitingmoregoodwhenmachine
    SELL = "sell"                 # trendweakening
    STRONG_SELL = "strong sell"      # trenddestroy


class MACDStatus(Enum):
    """MACDstatusenum"""
    GOLDEN_CROSS_ZERO = "golden cross above zero axis"      # DIFcross aboveDEA，andinabove zero axismethod
    GOLDEN_CROSS = "golden cross"                # DIFcross aboveDEA
    BULLISH = "long position"                    # DIF>DEA>0
    CROSSING_UP = "cross above zero axis"             # DIFcross above zero axis
    CROSSING_DOWN = "cross below zero axis"           # DIFcross below zero axis
    BEARISH = "short position"                    # DIF<DEA<0
    DEATH_CROSS = "death cross"                # DIFcross belowDEA


class RSIStatus(Enum):
    """RSIstatusenum"""
    OVERBOUGHT = "overbought"        # RSI > 70
    STRONG_BUY = "strongbuy"    # 50 < RSI < 70
    NEUTRAL = "neutral"          # 40 <= RSI <= 60
    WEAK = "weak"             # 30 < RSI < 40
    OVERSOLD = "oversold"         # RSI < 30


@dataclass
class TrendAnalysisResult:
    """trendanalysis result"""
    code: str
    
    # trenddetermine
    trend_status: TrendStatus = TrendStatus.CONSOLIDATION
    ma_alignment: str = ""           # moving averagearrangedescription
    trend_strength: float = 0.0      # trendintensity 0-100
    
    # moving averagedata
    ma5: float = 0.0
    ma10: float = 0.0
    ma20: float = 0.0
    ma60: float = 0.0
    current_price: float = 0.0
    
    # BIAS ratio（with MA5 deviationdegree）
    bias_ma5: float = 0.0            # (Close - MA5) / MA5 * 100
    bias_ma10: float = 0.0
    bias_ma20: float = 0.0
    
    # volumeanalyzing
    volume_status: VolumeStatus = VolumeStatus.NORMAL
    volume_ratio_5d: float = 0.0     # todaytrading volume/5average daily volume
    volume_trend: str = ""           # volumetrenddescription
    
    # supportstress
    support_ma5: bool = False        # MA5 whetherconstitutesupport
    support_ma10: bool = False       # MA10 whetherconstitutesupport
    resistance_levels: List[float] = field(default_factory=list)
    support_levels: List[float] = field(default_factory=list)

    # MACD indicator
    macd_dif: float = 0.0          # DIF fastline
    macd_dea: float = 0.0          # DEA slowline
    macd_bar: float = 0.0           # MACD histogram
    macd_status: MACDStatus = MACDStatus.BULLISH
    macd_signal: str = ""            # MACD signaldescription

    # RSI indicator
    rsi_6: float = 0.0              # RSI(6) short-term
    rsi_12: float = 0.0             # RSI(12) medium-term
    rsi_24: float = 0.0             # RSI(24) longperiod
    rsi_status: RSIStatus = RSIStatus.NEUTRAL
    rsi_signal: str = ""              # RSI signaldescription

    # buy signal
    buy_signal: BuySignal = BuySignal.WAIT
    signal_score: int = 0            # compositescore 0-100
    signal_reasons: List[str] = field(default_factory=list)
    risk_factors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'code': self.code,
            'trend_status': self.trend_status.value,
            'ma_alignment': self.ma_alignment,
            'trend_strength': self.trend_strength,
            'ma5': self.ma5,
            'ma10': self.ma10,
            'ma20': self.ma20,
            'ma60': self.ma60,
            'current_price': self.current_price,
            'bias_ma5': self.bias_ma5,
            'bias_ma10': self.bias_ma10,
            'bias_ma20': self.bias_ma20,
            'volume_status': self.volume_status.value,
            'volume_ratio_5d': self.volume_ratio_5d,
            'volume_trend': self.volume_trend,
            'support_ma5': self.support_ma5,
            'support_ma10': self.support_ma10,
            'buy_signal': self.buy_signal.value,
            'signal_score': self.signal_score,
            'signal_reasons': self.signal_reasons,
            'risk_factors': self.risk_factors,
            'macd_dif': self.macd_dif,
            'macd_dea': self.macd_dea,
            'macd_bar': self.macd_bar,
            'macd_status': self.macd_status.value,
            'macd_signal': self.macd_signal,
            'rsi_6': self.rsi_6,
            'rsi_12': self.rsi_12,
            'rsi_24': self.rsi_24,
            'rsi_status': self.rsi_status.value,
            'rsi_signal': self.rsi_signal,
        }


class StockTrendAnalyzer:
    """
    stocktrend analysishandler

    based onusertrading philosophyimplement：
    1. trenddetermine - MA5>MA10>MA20 long positionarrange
    2. BIAS ratiodetect - don't chase highs，deviation MA5 exceed 5% notbuy
    3. volumeanalyzing - biasedgoodvolume contractionpullback
    4. buypointidentify - pullback MA5/MA10 support
    5. MACD indicator - trendconfirmandgolden crossdeath crosssignal
    6. RSI indicator - overboughtoversolddetermine
    """
    
    # tradeparameterconfiguration（BIAS_THRESHOLD from Config reading，see _generate_signal）
    VOLUME_SHRINK_RATIO = 0.7   # volume contractiondeterminethreshold（todayvolume/5average daily volume）
    VOLUME_HEAVY_RATIO = 1.5    # volume increasedeterminethreshold
    MA_SUPPORT_TOLERANCE = 0.02  # MA supportdeterminecapacitytolerance（2%）

    # MACD parameter（standard12/26/9）
    MACD_FAST = 12              # fastline period
    MACD_SLOW = 26             # slowline period
    MACD_SIGNAL = 9             # signalline period

    # RSI parameter
    RSI_SHORT = 6               # short-termRSIperiod
    RSI_MID = 12               # medium-termRSIperiod
    RSI_LONG = 24              # longperiodRSIperiod
    RSI_OVERBOUGHT = 70        # overboughtthreshold
    RSI_OVERSOLD = 30          # oversoldthreshold
    
    def __init__(self):
        """initializinganalyzinghandler"""
        pass
    
    def analyze(self, df: pd.DataFrame, code: str) -> TrendAnalysisResult:
        """
        analyzingstocktrend
        
        Args:
            df: packageinclude OHLCV data DataFrame
            code: stock code
            
        Returns:
            TrendAnalysisResult analysis result
        """
        result = TrendAnalysisResult(code=code)
        
        if df is None or df.empty or len(df) < 20:
            logger.warning(f"{code} datainsufficient，unable toproceedtrend analysis")
            result.risk_factors.append("datainsufficient，unable tocompletedanalyzing")
            return result
        
        # ensuredataby datesorting
        df = df.sort_values('date').reset_index(drop=True)
        
        # calculatingmoving average
        df = self._calculate_mas(df)

        # calculating MACD and RSI
        df = self._calculate_macd(df)
        df = self._calculate_rsi(df)

        # get latestdata
        latest = df.iloc[-1]
        result.current_price = float(latest['close'])
        result.ma5 = float(latest['MA5'])
        result.ma10 = float(latest['MA10'])
        result.ma20 = float(latest['MA20'])
        result.ma60 = float(latest.get('MA60', 0))

        # 1. trenddetermine
        self._analyze_trend(df, result)

        # 2. BIAS ratiocalculating
        self._calculate_bias(result)

        # 3. volumeanalyzing
        self._analyze_volume(df, result)

        # 4. supportstressanalyzing
        self._analyze_support_resistance(df, result)

        # 5. MACD analyzing
        self._analyze_macd(df, result)

        # 6. RSI analyzing
        self._analyze_rsi(df, result)

        # 7. generatingbuy signal
        self._generate_signal(result)

        return result
    
    def _calculate_mas(self, df: pd.DataFrame) -> pd.DataFrame:
        """calculatingmoving average"""
        df = df.copy()
        df['MA5'] = df['close'].rolling(window=5).mean()
        df['MA10'] = df['close'].rolling(window=10).mean()
        df['MA20'] = df['close'].rolling(window=20).mean()
        if len(df) >= 60:
            df['MA60'] = df['close'].rolling(window=60).mean()
        else:
            df['MA60'] = df['MA20']  # datainsufficientwhenuse MA20 replace
        return df

    def _calculate_macd(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        calculating MACD indicator

        formula：
        - EMA(12)：12dayindexmoveaverage
        - EMA(26)：26dayindexmoveaverage
        - DIF = EMA(12) - EMA(26)
        - DEA = EMA(DIF, 9)
        - MACD = (DIF - DEA) * 2
        """
        df = df.copy()

        # calculatingfastslowline EMA
        ema_fast = df['close'].ewm(span=self.MACD_FAST, adjust=False).mean()
        ema_slow = df['close'].ewm(span=self.MACD_SLOW, adjust=False).mean()

        # calculatingfastline DIF
        df['MACD_DIF'] = ema_fast - ema_slow

        # calculatingsignalline DEA
        df['MACD_DEA'] = df['MACD_DIF'].ewm(span=self.MACD_SIGNAL, adjust=False).mean()

        # calculatinghistogram
        df['MACD_BAR'] = (df['MACD_DIF'] - df['MACD_DEA']) * 2

        return df

    def _calculate_rsi(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        calculating RSI indicator

        formula：
        - RS = averagerisingamplitude / averagefallingamplitude
        - RSI = 100 - (100 / (1 + RS))
        """
        df = df.copy()

        for period in [self.RSI_SHORT, self.RSI_MID, self.RSI_LONG]:
            # calculatingpricechange
            delta = df['close'].diff()

            # minuteaway fromrisingandfalling
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)

            # calculatingaverageprice change percentage
            avg_gain = gain.rolling(window=period).mean()
            avg_loss = loss.rolling(window=period).mean()

            # calculating RS and RSI
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

            # fill NaN value
            rsi = rsi.fillna(50)  # defaultneutralvalue

            # addto DataFrame
            col_name = f'RSI_{period}'
            df[col_name] = rsi

        return df
    
    def _analyze_trend(self, df: pd.DataFrame, result: TrendAnalysisResult) -> None:
        """
        analyzingtrendstatus
        
        corelogic：determinemoving averagearrangeandtrendintensity
        """
        ma5, ma10, ma20 = result.ma5, result.ma10, result.ma20
        
        # determinemoving averagearrange
        if ma5 > ma10 > ma20:
            # checkbetweendistancewhether inexpandlarge（strong）
            prev = df.iloc[-5] if len(df) >= 5 else df.iloc[-1]
            prev_spread = (prev['MA5'] - prev['MA20']) / prev['MA20'] * 100 if prev['MA20'] > 0 else 0
            curr_spread = (ma5 - ma20) / ma20 * 100 if ma20 > 0 else 0
            
            if curr_spread > prev_spread and curr_spread > 5:
                result.trend_status = TrendStatus.STRONG_BULL
                result.ma_alignment = "stronglong positionarrange，moving averagesendscattered upwardrow"
                result.trend_strength = 90
            else:
                result.trend_status = TrendStatus.BULL
                result.ma_alignment = "long positionarrange MA5>MA10>MA20"
                result.trend_strength = 75
                
        elif ma5 > ma10 and ma10 <= ma20:
            result.trend_status = TrendStatus.WEAK_BULL
            result.ma_alignment = "weaklong position，MA5>MA10 but MA10≤MA20"
            result.trend_strength = 55
            
        elif ma5 < ma10 < ma20:
            prev = df.iloc[-5] if len(df) >= 5 else df.iloc[-1]
            prev_spread = (prev['MA20'] - prev['MA5']) / prev['MA5'] * 100 if prev['MA5'] > 0 else 0
            curr_spread = (ma20 - ma5) / ma5 * 100 if ma5 > 0 else 0
            
            if curr_spread > prev_spread and curr_spread > 5:
                result.trend_status = TrendStatus.STRONG_BEAR
                result.ma_alignment = "strongshort positionarrange，moving averagesendscatteredbelowrow"
                result.trend_strength = 10
            else:
                result.trend_status = TrendStatus.BEAR
                result.ma_alignment = "short positionarrange MA5<MA10<MA20"
                result.trend_strength = 25
                
        elif ma5 < ma10 and ma10 >= ma20:
            result.trend_status = TrendStatus.WEAK_BEAR
            result.ma_alignment = "weakshort position，MA5<MA10 but MA10≥MA20"
            result.trend_strength = 40
            
        else:
            result.trend_status = TrendStatus.CONSOLIDATION
            result.ma_alignment = "moving averageentangle，trendnotclear"
            result.trend_strength = 50
    
    def _calculate_bias(self, result: TrendAnalysisResult) -> None:
        """
        calculatingBIAS ratio
        
        BIAS ratio = (current price - moving average) / moving average * 100%
        
        strict entry strategy：BIAS ratioexceed 5% don't chase highs
        """
        price = result.current_price
        
        if result.ma5 > 0:
            result.bias_ma5 = (price - result.ma5) / result.ma5 * 100
        if result.ma10 > 0:
            result.bias_ma10 = (price - result.ma10) / result.ma10 * 100
        if result.ma20 > 0:
            result.bias_ma20 = (price - result.ma20) / result.ma20 * 100
    
    def _analyze_volume(self, df: pd.DataFrame, result: TrendAnalysisResult) -> None:
        """
        analyzingvolume
        
        biasedgood：volume contractionpullback > volume increaserising > volume contractionrising > volume increasefalling
        """
        if len(df) < 5:
            return
        
        latest = df.iloc[-1]
        vol_5d_avg = df['volume'].iloc[-6:-1].mean()
        
        if vol_5d_avg > 0:
            result.volume_ratio_5d = float(latest['volume']) / vol_5d_avg
        
        # determinepricechange
        prev_close = df.iloc[-2]['close']
        price_change = (latest['close'] - prev_close) / prev_close * 100
        
        # volumestatusdetermine
        if result.volume_ratio_5d >= self.VOLUME_HEAVY_RATIO:
            if price_change > 0:
                result.volume_status = VolumeStatus.HEAVY_VOLUME_UP
                result.volume_trend = "volume increaserising，long positionpowervolumestrongmomentum"
            else:
                result.volume_status = VolumeStatus.HEAVY_VOLUME_DOWN
                result.volume_trend = "volume increasefalling，Noterisk"
        elif result.volume_ratio_5d <= self.VOLUME_SHRINK_RATIO:
            if price_change > 0:
                result.volume_status = VolumeStatus.SHRINK_VOLUME_UP
                result.volume_trend = "volume contractionrising，upward assaultdynamiccaninsufficient"
            else:
                result.volume_status = VolumeStatus.SHRINK_VOLUME_DOWN
                result.volume_trend = "volume contractionpullback，shakeoutfeatureobvious（good）"
        else:
            result.volume_status = VolumeStatus.NORMAL
            result.volume_trend = "volumenormal"
    
    def _analyze_support_resistance(self, df: pd.DataFrame, result: TrendAnalysisResult) -> None:
        """
        analyzingsupportstressdigit
        
        buy point preference：pullback MA5/MA10 get support
        """
        price = result.current_price
        
        # checkwhether in MA5 nearget support
        if result.ma5 > 0:
            ma5_distance = abs(price - result.ma5) / result.ma5
            if ma5_distance <= self.MA_SUPPORT_TOLERANCE and price >= result.ma5:
                result.support_ma5 = True
                result.support_levels.append(result.ma5)
        
        # checkwhether in MA10 nearget support
        if result.ma10 > 0:
            ma10_distance = abs(price - result.ma10) / result.ma10
            if ma10_distance <= self.MA_SUPPORT_TOLERANCE and price >= result.ma10:
                result.support_ma10 = True
                if result.ma10 not in result.support_levels:
                    result.support_levels.append(result.ma10)
        
        # MA20 act asasImportantsupport
        if result.ma20 > 0 and price >= result.ma20:
            result.support_levels.append(result.ma20)
        
        # recentperiodhighpointact asasstress
        if len(df) >= 20:
            recent_high = df['high'].iloc[-20:].max()
            if recent_high > price:
                result.resistance_levels.append(recent_high)

    def _analyze_macd(self, df: pd.DataFrame, result: TrendAnalysisResult) -> None:
        """
        analyzing MACD indicator

        coresignal：
        - golden cross above zero axis：moststrongbuy signal
        - golden cross：DIF cross above DEA
        - death cross：DIF cross below DEA
        """
        if len(df) < self.MACD_SLOW:
            result.macd_signal = "datainsufficient"
            return

        latest = df.iloc[-1]
        prev = df.iloc[-2]

        # get MACD data
        result.macd_dif = float(latest['MACD_DIF'])
        result.macd_dea = float(latest['MACD_DEA'])
        result.macd_bar = float(latest['MACD_BAR'])

        # determinegolden crossdeath cross
        prev_dif_dea = prev['MACD_DIF'] - prev['MACD_DEA']
        curr_dif_dea = result.macd_dif - result.macd_dea

        # golden cross：DIF cross above DEA
        is_golden_cross = prev_dif_dea <= 0 and curr_dif_dea > 0

        # death cross：DIF cross below DEA
        is_death_cross = prev_dif_dea >= 0 and curr_dif_dea < 0

        # cross zero axismore
        prev_zero = prev['MACD_DIF']
        curr_zero = result.macd_dif
        is_crossing_up = prev_zero <= 0 and curr_zero > 0
        is_crossing_down = prev_zero >= 0 and curr_zero < 0

        # determine MACD status
        if is_golden_cross and curr_zero > 0:
            result.macd_status = MACDStatus.GOLDEN_CROSS_ZERO
            result.macd_signal = "⭐ golden cross above zero axis，strongstrongbuy signal！"
        elif is_crossing_up:
            result.macd_status = MACDStatus.CROSSING_UP
            result.macd_signal = "⚡ DIFcross above zero axis，trendconvertstrong"
        elif is_golden_cross:
            result.macd_status = MACDStatus.GOLDEN_CROSS
            result.macd_signal = "✅ golden cross，trendtoabove/upper"
        elif is_death_cross:
            result.macd_status = MACDStatus.DEATH_CROSS
            result.macd_signal = "❌ death cross，trendtobelow"
        elif is_crossing_down:
            result.macd_status = MACDStatus.CROSSING_DOWN
            result.macd_signal = "⚠️ DIFcross below zero axis，trendweakening"
        elif result.macd_dif > 0 and result.macd_dea > 0:
            result.macd_status = MACDStatus.BULLISH
            result.macd_signal = "✓ long positionarrange，continuousrising"
        elif result.macd_dif < 0 and result.macd_dea < 0:
            result.macd_status = MACDStatus.BEARISH
            result.macd_signal = "⚠ short positionarrange，continuousfalling"
        else:
            result.macd_status = MACDStatus.BULLISH
            result.macd_signal = " MACD neutralzonedomain"

    def _analyze_rsi(self, df: pd.DataFrame, result: TrendAnalysisResult) -> None:
        """
        analyzing RSI indicator

        coredetermine：
        - RSI > 70：overbought，cautiously chasehigh
        - RSI < 30：oversold，monitorrebound
        - 40-60：neutralzonedomain
        """
        if len(df) < self.RSI_LONG:
            result.rsi_signal = "datainsufficient"
            return

        latest = df.iloc[-1]

        # get RSI data
        result.rsi_6 = float(latest[f'RSI_{self.RSI_SHORT}'])
        result.rsi_12 = float(latest[f'RSI_{self.RSI_MID}'])
        result.rsi_24 = float(latest[f'RSI_{self.RSI_LONG}'])

        # withmedium-term RSI(12) asmainproceeddetermine
        rsi_mid = result.rsi_12

        # determine RSI status
        if rsi_mid > self.RSI_OVERBOUGHT:
            result.rsi_status = RSIStatus.OVERBOUGHT
            result.rsi_signal = f"⚠️ RSIoverbought({rsi_mid:.1f}>70)，short-termpullbackriskhigh"
        elif rsi_mid > 60:
            result.rsi_status = RSIStatus.STRONG_BUY
            result.rsi_signal = f"✅ RSIstrong({rsi_mid:.1f})，long positionpowervolumesufficient"
        elif rsi_mid >= 40:
            result.rsi_status = RSIStatus.NEUTRAL
            result.rsi_signal = f" RSIneutral({rsi_mid:.1f})，oscillation consolidationin"
        elif rsi_mid >= self.RSI_OVERSOLD:
            result.rsi_status = RSIStatus.WEAK
            result.rsi_signal = f"⚡ RSIweak({rsi_mid:.1f})，monitorrebound"
        else:
            result.rsi_status = RSIStatus.OVERSOLD
            result.rsi_signal = f"⭐ RSIoversold({rsi_mid:.1f}<30)，reboundmachinewilllarge"

    def _generate_signal(self, result: TrendAnalysisResult) -> None:
        """
        generatingbuy signal

        compositescoresystem：
        - trend（30minute）：long positionarrangehigh score
        - BIAS ratio（20minute）：connectrecent MA5 high score
        - volume（15minute）：volume contractionpullbackhigh score
        - support（10minute）：obtainmoving averagesupporthigh score
        - MACD（15minute）：golden crossandlong positionhigh score
        - RSI（10minute）：oversoldandstronghigh score
        """
        score = 0
        reasons = []
        risks = []

        # === trendscore（30minute）===
        trend_scores = {
            TrendStatus.STRONG_BULL: 30,
            TrendStatus.BULL: 26,
            TrendStatus.WEAK_BULL: 18,
            TrendStatus.CONSOLIDATION: 12,
            TrendStatus.WEAK_BEAR: 8,
            TrendStatus.BEAR: 4,
            TrendStatus.STRONG_BEAR: 0,
        }
        trend_score = trend_scores.get(result.trend_status, 12)
        score += trend_score

        if result.trend_status in [TrendStatus.STRONG_BULL, TrendStatus.BULL]:
            reasons.append(f"✅ {result.trend_status.value}，follow the trenddomultiple")
        elif result.trend_status in [TrendStatus.BEAR, TrendStatus.STRONG_BEAR]:
            risks.append(f"⚠️ {result.trend_status.value}，notappropriatedomultiple")

        # === BIAS ratioscore（20minute，strongtrendcompensation）===
        bias = result.bias_ma5
        if bias != bias or bias is None:  # NaN or None defense
            bias = 0.0
        base_threshold = get_config().bias_threshold

        # Strong trend compensation: relax threshold for STRONG_BULL with high strength
        trend_strength = result.trend_strength if result.trend_strength == result.trend_strength else 0.0
        if result.trend_status == TrendStatus.STRONG_BULL and (trend_strength or 0) >= 70:
            effective_threshold = base_threshold * 1.5
            is_strong_trend = True
        else:
            effective_threshold = base_threshold
            is_strong_trend = False

        if bias < 0:
            # Price below MA5 (pullback)
            if bias > -3:
                score += 20
                reasons.append(f"✅ pricestrategylowatMA5({bias:.1f}%)，pullback buy point")
            elif bias > -5:
                score += 16
                reasons.append(f"✅ pricepullbackMA5({bias:.1f}%)，observesupport")
            else:
                score += 8
                risks.append(f"⚠️ BIAS ratiolarge({bias:.1f}%)，possiblybreakdigit")
        elif bias < 2:
            score += 18
            reasons.append(f"✅ priceclose torecentMA5({bias:.1f}%)，good time to enter")
        elif bias < base_threshold:
            score += 14
            reasons.append(f"⚡ pricestrategyhighatMA5({bias:.1f}%)，can enter with small position")
        elif bias > effective_threshold:
            score += 4
            risks.append(
                f"❌ BIAS ratiohigh({bias:.1f}%>{effective_threshold:.1f}%)，strictly prohibit chasing highs！"
            )
        elif bias > base_threshold and is_strong_trend:
            score += 10
            reasons.append(
                f"⚡ strongtrendinBIAS ratiobiasedhigh({bias:.1f}%)，can track with small position"
            )
        else:
            score += 4
            risks.append(
                f"❌ BIAS ratiohigh({bias:.1f}%>{base_threshold:.1f}%)，strictly prohibit chasing highs！"
            )

        # === volumescore（15minute）===
        volume_scores = {
            VolumeStatus.SHRINK_VOLUME_DOWN: 15,  # volume contractionpullbackbest
            VolumeStatus.HEAVY_VOLUME_UP: 12,     # volume increaserisingtimesof
            VolumeStatus.NORMAL: 10,
            VolumeStatus.SHRINK_VOLUME_UP: 6,     # novolumerisingrelativelypoor
            VolumeStatus.HEAVY_VOLUME_DOWN: 0,    # volume increasefallingworst
        }
        vol_score = volume_scores.get(result.volume_status, 8)
        score += vol_score

        if result.volume_status == VolumeStatus.SHRINK_VOLUME_DOWN:
            reasons.append("✅ volume contractionpullback，main forceshakeout")
        elif result.volume_status == VolumeStatus.HEAVY_VOLUME_DOWN:
            risks.append("⚠️ volume increasefalling，Noterisk")

        # === supportscore（10minute）===
        if result.support_ma5:
            score += 5
            reasons.append("✅ MA5supportvalid")
        if result.support_ma10:
            score += 5
            reasons.append("✅ MA10supportvalid")

        # === MACD score（15minute）===
        macd_scores = {
            MACDStatus.GOLDEN_CROSS_ZERO: 15,  # golden cross above zero axismoststrong
            MACDStatus.GOLDEN_CROSS: 12,      # golden cross
            MACDStatus.CROSSING_UP: 10,       # cross above zero axis
            MACDStatus.BULLISH: 8,            # long position
            MACDStatus.BEARISH: 2,            # short position
            MACDStatus.CROSSING_DOWN: 0,       # cross below zero axis
            MACDStatus.DEATH_CROSS: 0,        # death cross
        }
        macd_score = macd_scores.get(result.macd_status, 5)
        score += macd_score

        if result.macd_status in [MACDStatus.GOLDEN_CROSS_ZERO, MACDStatus.GOLDEN_CROSS]:
            reasons.append(f"✅ {result.macd_signal}")
        elif result.macd_status in [MACDStatus.DEATH_CROSS, MACDStatus.CROSSING_DOWN]:
            risks.append(f"⚠️ {result.macd_signal}")
        else:
            reasons.append(result.macd_signal)

        # === RSI score（10minute）===
        rsi_scores = {
            RSIStatus.OVERSOLD: 10,       # oversoldbest
            RSIStatus.STRONG_BUY: 8,     # strong
            RSIStatus.NEUTRAL: 5,        # neutral
            RSIStatus.WEAK: 3,            # weak
            RSIStatus.OVERBOUGHT: 0,       # overboughtworst
        }
        rsi_score = rsi_scores.get(result.rsi_status, 5)
        score += rsi_score

        if result.rsi_status in [RSIStatus.OVERSOLD, RSIStatus.STRONG_BUY]:
            reasons.append(f"✅ {result.rsi_signal}")
        elif result.rsi_status == RSIStatus.OVERBOUGHT:
            risks.append(f"⚠️ {result.rsi_signal}")
        else:
            reasons.append(result.rsi_signal)

        # === compositedetermine ===
        result.signal_score = score
        result.signal_reasons = reasons
        result.risk_factors = risks

        # generatingbuy signal（adjustthresholdwithsuitableshouldnew100minutecontrol）
        if score >= 75 and result.trend_status in [TrendStatus.STRONG_BULL, TrendStatus.BULL]:
            result.buy_signal = BuySignal.STRONG_BUY
        elif score >= 60 and result.trend_status in [TrendStatus.STRONG_BULL, TrendStatus.BULL, TrendStatus.WEAK_BULL]:
            result.buy_signal = BuySignal.BUY
        elif score >= 45:
            result.buy_signal = BuySignal.HOLD
        elif score >= 30:
            result.buy_signal = BuySignal.WAIT
        elif result.trend_status in [TrendStatus.BEAR, TrendStatus.STRONG_BEAR]:
            result.buy_signal = BuySignal.STRONG_SELL
        else:
            result.buy_signal = BuySignal.SELL
    
    def format_analysis(self, result: TrendAnalysisResult) -> str:
        """
        formattinganalysis resultas text

        Args:
            result: analysis result

        Returns:
            formattinganalyzingtext
        """
        lines = [
            f"=== {result.code} trend analysis ===",
            f"",
            f"📊 trenddetermine: {result.trend_status.value}",
            f"   moving averagearrange: {result.ma_alignment}",
            f"   trendintensity: {result.trend_strength}/100",
            f"",
            f"📈 moving averagedata:",
            f"   current price: {result.current_price:.2f}",
            f"   MA5:  {result.ma5:.2f} (deviation {result.bias_ma5:+.2f}%)",
            f"   MA10: {result.ma10:.2f} (deviation {result.bias_ma10:+.2f}%)",
            f"   MA20: {result.ma20:.2f} (deviation {result.bias_ma20:+.2f}%)",
            f"",
            f"📊 volumeanalyzing: {result.volume_status.value}",
            f"   volume ratio(vs5day): {result.volume_ratio_5d:.2f}",
            f"   volumetrend: {result.volume_trend}",
            f"",
            f"📈 MACDindicator: {result.macd_status.value}",
            f"   DIF: {result.macd_dif:.4f}",
            f"   DEA: {result.macd_dea:.4f}",
            f"   MACD: {result.macd_bar:.4f}",
            f"   signal: {result.macd_signal}",
            f"",
            f"📊 RSIindicator: {result.rsi_status.value}",
            f"   RSI(6): {result.rsi_6:.1f}",
            f"   RSI(12): {result.rsi_12:.1f}",
            f"   RSI(24): {result.rsi_24:.1f}",
            f"   signal: {result.rsi_signal}",
            f"",
            f"🎯 operationrecommended: {result.buy_signal.value}",
            f"   compositescore: {result.signal_score}/100",
        ]

        if result.signal_reasons:
            lines.append(f"")
            lines.append(f"✅ buyreasonby:")
            for reason in result.signal_reasons:
                lines.append(f"   {reason}")

        if result.risk_factors:
            lines.append(f"")
            lines.append(f"⚠️ riskbecauseelement:")
            for risk in result.risk_factors:
                lines.append(f"   {risk}")

        return "\n".join(lines)


def analyze_stock(df: pd.DataFrame, code: str) -> TrendAnalysisResult:
    """
    convenientfunction：analyzingsinglestock
    
    Args:
        df: packageinclude OHLCV data DataFrame
        code: stock code
        
    Returns:
        TrendAnalysisResult analysis result
    """
    analyzer = StockTrendAnalyzer()
    return analyzer.analyze(df, code)


if __name__ == "__main__":
    # testingcode
    logging.basicConfig(level=logging.INFO)
    
    # mockdatatesting
    import numpy as np
    
    dates = pd.date_range(start='2025-01-01', periods=60, freq='D')
    np.random.seed(42)
    
    # mocklong positionarrangedata
    base_price = 10.0
    prices = [base_price]
    for i in range(59):
        change = np.random.randn() * 0.02 + 0.003  # slightrisingtrend
        prices.append(prices[-1] * (1 + change))
    
    df = pd.DataFrame({
        'date': dates,
        'open': prices,
        'high': [p * (1 + np.random.uniform(0, 0.02)) for p in prices],
        'low': [p * (1 - np.random.uniform(0, 0.02)) for p in prices],
        'close': prices,
        'volume': [np.random.randint(1000000, 5000000) for _ in prices],
    })
    
    analyzer = StockTrendAnalyzer()
    result = analyzer.analyze(df, '000001')
    print(analyzer.format_analysis(result))
