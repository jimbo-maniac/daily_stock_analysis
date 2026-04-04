# -*- coding: utf-8 -*-
"""
===================================
A-Share Watchlist Intelligent Analysis System - AI Analysis Layer
===================================

Responsibilities:
1. Encapsulate LLM call logic (unified calls to Gemini/Anthropic/OpenAI etc. via LiteLLM)
2. Generate analysis reports combining technical and news data
3. Parse LLM responses into structured AnalysisResult
"""

import json
import logging
import math
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple

import litellm
from json_repair import repair_json
from litellm import Router

from src.agent.llm_adapter import get_thinking_extra_body
from src.agent.skills.defaults import CORE_TRADING_SKILL_POLICY_ZH
from src.config import (
    Config,
    extra_litellm_params,
    get_api_keys_for_model,
    get_config,
    get_configured_llm_models,
    resolve_news_window_days,
)
from src.storage import persist_llm_usage
from src.data.stock_mapping import STOCK_NAME_MAP
from src.report_language import (
    get_signal_level,
    get_no_data_text,
    get_placeholder_text,
    get_unknown_text,
    infer_decision_type_from_advice,
    localize_chip_health,
    localize_confidence_level,
    normalize_report_language,
)
from src.schemas.report_schema import AnalysisReportSchema

logger = logging.getLogger(__name__)


# === Strategy Context Loader ===
_STRATEGY_CONTEXT_CACHE: Optional[str] = None


def _load_strategy_context() -> str:
    """
    Load STRATEGY.md as permanent context for the LLM.

    Returns the full content of STRATEGY.md, cached after first load.
    If file is missing or unreadable, returns empty string with warning.
    """
    global _STRATEGY_CONTEXT_CACHE
    if _STRATEGY_CONTEXT_CACHE is not None:
        return _STRATEGY_CONTEXT_CACHE

    import os
    strategy_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "STRATEGY.md"
    )

    try:
        with open(strategy_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
        _STRATEGY_CONTEXT_CACHE = content
        logger.info(f"Strategy context loaded: {len(content)} characters from STRATEGY.md")
        return content
    except FileNotFoundError:
        logger.warning(f"STRATEGY.md not found at {strategy_path}, proceeding without strategy context")
        _STRATEGY_CONTEXT_CACHE = ""
        return ""
    except Exception as e:
        logger.warning(f"Failed to load STRATEGY.md: {e}, proceeding without strategy context")
        _STRATEGY_CONTEXT_CACHE = ""
        return ""


def check_content_integrity(result: "AnalysisResult") -> Tuple[bool, List[str]]:
    """
    Check mandatory fields for report content integrity.
    Returns (pass, missing_fields). Module-level for use by pipeline (agent weak mode).
    """
    missing: List[str] = []
    if result.sentiment_score is None:
        missing.append("sentiment_score")
    advice = result.operation_advice
    if not advice or not isinstance(advice, str) or not advice.strip():
        missing.append("operation_advice")
    summary = result.analysis_summary
    if not summary or not isinstance(summary, str) or not summary.strip():
        missing.append("analysis_summary")
    dash = result.dashboard if isinstance(result.dashboard, dict) else {}
    core = dash.get("core_conclusion")
    core = core if isinstance(core, dict) else {}
    if not (core.get("one_sentence") or "").strip():
        missing.append("dashboard.core_conclusion.one_sentence")
    intel = dash.get("intelligence")
    intel = intel if isinstance(intel, dict) else None
    if intel is None or "risk_alerts" not in intel:
        missing.append("dashboard.intelligence.risk_alerts")
    if result.decision_type in ("buy", "hold"):
        battle = dash.get("battle_plan")
        battle = battle if isinstance(battle, dict) else {}
        sp = battle.get("sniper_points")
        sp = sp if isinstance(sp, dict) else {}
        stop_loss = sp.get("stop_loss")
        if stop_loss is None or (isinstance(stop_loss, str) and not stop_loss.strip()):
            missing.append("dashboard.battle_plan.sniper_points.stop_loss")
    return len(missing) == 0, missing


def apply_placeholder_fill(result: "AnalysisResult", missing_fields: List[str]) -> None:
    """Fill missing mandatory fields with placeholders (in-place). Module-level for pipeline."""
    placeholder = get_placeholder_text(getattr(result, "report_language", "zh"))
    for field in missing_fields:
        if field == "sentiment_score":
            result.sentiment_score = 50
        elif field == "operation_advice":
            result.operation_advice = result.operation_advice or placeholder
        elif field == "analysis_summary":
            result.analysis_summary = result.analysis_summary or placeholder
        elif field == "dashboard.core_conclusion.one_sentence":
            if not result.dashboard:
                result.dashboard = {}
            if "core_conclusion" not in result.dashboard:
                result.dashboard["core_conclusion"] = {}
            result.dashboard["core_conclusion"]["one_sentence"] = (
                result.dashboard["core_conclusion"].get("one_sentence") or placeholder
            )
        elif field == "dashboard.intelligence.risk_alerts":
            if not result.dashboard:
                result.dashboard = {}
            if "intelligence" not in result.dashboard:
                result.dashboard["intelligence"] = {}
            if "risk_alerts" not in result.dashboard["intelligence"]:
                result.dashboard["intelligence"]["risk_alerts"] = []
        elif field == "dashboard.battle_plan.sniper_points.stop_loss":
            if not result.dashboard:
                result.dashboard = {}
            if "battle_plan" not in result.dashboard:
                result.dashboard["battle_plan"] = {}
            if "sniper_points" not in result.dashboard["battle_plan"]:
                result.dashboard["battle_plan"]["sniper_points"] = {}
            result.dashboard["battle_plan"]["sniper_points"]["stop_loss"] = placeholder


# ---------- chip_structure fallback (Issue #589) ----------

_CHIP_KEYS: tuple = ("profit_ratio", "avg_cost", "concentration", "chip_health")


def _is_value_placeholder(v: Any) -> bool:
    """True if value is empty or placeholder (N/A, data missing, etc.)."""
    if v is None:
        return True
    if isinstance(v, (int, float)) and v == 0:
        return True
    s = str(v).strip().lower()
    return s in ("", "n/a", "na", "data missing", "data unavailable", "unknown", "tbd")


def _safe_float(v: Any, default: float = 0.0) -> float:
    """Safely convert to float; return default on failure. Private helper for chip fill."""
    if v is None:
        return default
    if isinstance(v, (int, float)):
        try:
            return default if math.isnan(float(v)) else float(v)
        except (ValueError, TypeError):
            return default
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return default


def _derive_chip_health(profit_ratio: float, concentration_90: float, language: str = "zh") -> str:
    """Derive chip_health from profit_ratio and concentration_90."""
    if profit_ratio >= 0.9:
        return localize_chip_health("caution", language)  # profit ratio extremely high
    if concentration_90 >= 0.25:
        return localize_chip_health("caution", language)  # chips dispersed
    if concentration_90 < 0.15 and 0.3 <= profit_ratio < 0.9:
        return localize_chip_health("healthy", language)  # concentrated and moderate profit ratio
    return localize_chip_health("average", language)


def _build_chip_structure_from_data(chip_data: Any, language: str = "zh") -> Dict[str, Any]:
    """Build chip_structure dict from ChipDistribution or dict."""
    if hasattr(chip_data, "profit_ratio"):
        pr = _safe_float(chip_data.profit_ratio)
        ac = chip_data.avg_cost
        c90 = _safe_float(chip_data.concentration_90)
    else:
        d = chip_data if isinstance(chip_data, dict) else {}
        pr = _safe_float(d.get("profit_ratio"))
        ac = d.get("avg_cost")
        c90 = _safe_float(d.get("concentration_90"))
    chip_health = _derive_chip_health(pr, c90, language=language)
    return {
        "profit_ratio": f"{pr:.1%}",
        "avg_cost": ac if (ac is not None and _safe_float(ac) != 0.0) else "N/A",
        "concentration": f"{c90:.2%}",
        "chip_health": chip_health,
    }


def fill_chip_structure_if_needed(result: "AnalysisResult", chip_data: Any) -> None:
    """When chip_data exists, fill chip_structure placeholder fields from chip_data (in-place)."""
    if not result or not chip_data:
        return
    try:
        if not result.dashboard:
            result.dashboard = {}
        dash = result.dashboard
        # Use `or {}` rather than setdefault so that an explicit `null` from LLM is also replaced
        dp = dash.get("data_perspective") or {}
        dash["data_perspective"] = dp
        cs = dp.get("chip_structure") or {}
        filled = _build_chip_structure_from_data(
            chip_data,
            language=getattr(result, "report_language", "zh"),
        )
        # Start from a copy of cs to preserve any extra keys the LLM may have added
        merged = dict(cs)
        for k in _CHIP_KEYS:
            if _is_value_placeholder(merged.get(k)):
                merged[k] = filled[k]
        if merged != cs:
            dp["chip_structure"] = merged
            logger.info("[chip_structure] Filled placeholder chip fields from data source (Issue #589)")
    except Exception as e:
        logger.warning("[chip_structure] Fill failed, skipping: %s", e)


_PRICE_POS_KEYS = ("ma5", "ma10", "ma20", "bias_ma5", "bias_status", "current_price", "support_level", "resistance_level")


def fill_price_position_if_needed(
    result: "AnalysisResult",
    trend_result: Any = None,
    realtime_quote: Any = None,
) -> None:
    """Fill missing price_position fields from trend_result / realtime data (in-place)."""
    if not result:
        return
    try:
        if not result.dashboard:
            result.dashboard = {}
        dash = result.dashboard
        dp = dash.get("data_perspective") or {}
        dash["data_perspective"] = dp
        pp = dp.get("price_position") or {}

        computed: Dict[str, Any] = {}
        if trend_result:
            tr = trend_result if isinstance(trend_result, dict) else (
                trend_result.__dict__ if hasattr(trend_result, "__dict__") else {}
            )
            computed["ma5"] = tr.get("ma5")
            computed["ma10"] = tr.get("ma10")
            computed["ma20"] = tr.get("ma20")
            computed["bias_ma5"] = tr.get("bias_ma5")
            computed["current_price"] = tr.get("current_price")
            support_levels = tr.get("support_levels") or []
            resistance_levels = tr.get("resistance_levels") or []
            if support_levels:
                computed["support_level"] = support_levels[0]
            if resistance_levels:
                computed["resistance_level"] = resistance_levels[0]
        if realtime_quote:
            rq = realtime_quote if isinstance(realtime_quote, dict) else (
                realtime_quote.to_dict() if hasattr(realtime_quote, "to_dict") else {}
            )
            if _is_value_placeholder(computed.get("current_price")):
                computed["current_price"] = rq.get("price")

        filled = False
        for k in _PRICE_POS_KEYS:
            if _is_value_placeholder(pp.get(k)) and not _is_value_placeholder(computed.get(k)):
                pp[k] = computed[k]
                filled = True
        if filled:
            dp["price_position"] = pp
            logger.info("[price_position] Filled placeholder fields from computed data")
    except Exception as e:
        logger.warning("[price_position] Fill failed, skipping: %s", e)


def get_stock_name_multi_source(
    stock_code: str,
    context: Optional[Dict] = None,
    data_manager = None
) -> str:
    """
    Retrieve stock name from multiple sources.

    Lookup strategy (by priority):
    1. From the passed context (realtime data)
    2. From static mapping table STOCK_NAME_MAP
    3. From DataFetcherManager (various data sources)
    4. Return default name (Stock + code)

    Args:
        stock_code: Stock code
        context: Analysis context (optional)
        data_manager: DataFetcherManager instance (optional)

    Returns:
        Stock name
    """
    # 1. Get from context (realtime quote data)
    if context:
        # Prefer stock_name field
        if context.get('stock_name'):
            name = context['stock_name']
            if name and not name.startswith('Stock '):
                return name

        # Then get from realtime data
        if 'realtime' in context and context['realtime'].get('name'):
            return context['realtime']['name']

    # 2. Get from static mapping table
    if stock_code in STOCK_NAME_MAP:
        return STOCK_NAME_MAP[stock_code]

    # 3. Get from data source
    if data_manager is None:
        try:
            from data_provider.base import DataFetcherManager
            data_manager = DataFetcherManager()
        except Exception as e:
            logger.debug(f"Failed to initialize DataFetcherManager: {e}")

    if data_manager:
        try:
            name = data_manager.get_stock_name(stock_code)
            if name:
                # Update cache
                STOCK_NAME_MAP[stock_code] = name
                return name
        except Exception as e:
            logger.debug(f"Failed to get stock name from data source: {e}")

    # 4. Return default name
    return f'Stock {stock_code}'


@dataclass
class AnalysisResult:
    """
    AI analysis result data class - Decision Dashboard edition

    Encapsulates Gemini's analysis results, including the decision dashboard and detailed analysis
    """
    code: str
    name: str

    # ========== Core metrics ==========
    sentiment_score: int  # Composite score 0-100 (>70 strong bullish, >60 bullish, 40-60 sideways, <40 bearish)
    trend_prediction: str  # Trend prediction: strong bullish/bullish/sideways/bearish/strong bearish
    operation_advice: str  # Action advice: buy/add/hold/reduce/sell/watch
    decision_type: str = "hold"  # Decision type: buy/hold/sell (for statistics)
    confidence_level: str = "medium"  # Confidence: high/medium/low
    report_language: str = "zh"  # Report output language: zh/en

    # ========== Decision Dashboard (added) ==========
    dashboard: Optional[Dict[str, Any]] = None  # Full decision dashboard data

    # ========== Trend analysis ==========
    trend_analysis: str = ""  # Trend shape analysis (support/resistance/trend lines etc.)
    short_term_outlook: str = ""  # Short-term outlook (1-3 days)
    medium_term_outlook: str = ""  # Medium-term outlook (1-2 weeks)

    # ========== Technical analysis ==========
    technical_analysis: str = ""  # Comprehensive technical indicator analysis
    ma_analysis: str = ""  # Moving average analysis (bullish/bearish alignment, golden/death cross etc.)
    volume_analysis: str = ""  # Volume analysis (expansion/contraction, smart money direction etc.)
    pattern_analysis: str = ""  # Candlestick pattern analysis

    # ========== Fundamental analysis ==========
    fundamental_analysis: str = ""  # Comprehensive fundamental analysis
    sector_position: str = ""  # Sector position and industry trends
    company_highlights: str = ""  # Company highlights/risk factors

    # ========== Sentiment/news analysis ==========
    news_summary: str = ""  # Recent important news/announcement summary
    market_sentiment: str = ""  # Market sentiment analysis
    hot_topics: str = ""  # Related hot topics

    # ========== Comprehensive analysis ==========
    analysis_summary: str = ""  # Comprehensive analysis summary
    key_points: str = ""  # Key highlights (3-5 points)
    risk_warning: str = ""  # Risk warnings
    buy_reason: str = ""  # Buy/sell rationale

    # ========== Metadata ==========
    market_snapshot: Optional[Dict[str, Any]] = None  # Daily market snapshot (display use)
    raw_response: Optional[str] = None  # Raw response (debug use)
    search_performed: bool = False  # Whether online search was performed
    data_sources: str = ""  # Data source description
    success: bool = True
    error_message: Optional[str] = None

    # ========== Price data (snapshot at analysis time) ==========
    current_price: Optional[float] = None  # Stock price at analysis time
    change_pct: Optional[float] = None     # Change percentage at analysis time (%)

    # ========== Model tag (Issue #528) ==========
    model_used: Optional[str] = None  # LLM model used for analysis (full name, e.g. gemini/gemini-2.0-flash)

    # ========== Historical comparison (Report Engine P0) ==========
    query_id: Optional[str] = None  # query_id for this analysis, used to exclude current record in historical comparisons

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'code': self.code,
            'name': self.name,
            'sentiment_score': self.sentiment_score,
            'trend_prediction': self.trend_prediction,
            'operation_advice': self.operation_advice,
            'decision_type': self.decision_type,
            'confidence_level': self.confidence_level,
            'report_language': self.report_language,
            'dashboard': self.dashboard,  # decision dashboard data
            'trend_analysis': self.trend_analysis,
            'short_term_outlook': self.short_term_outlook,
            'medium_term_outlook': self.medium_term_outlook,
            'technical_analysis': self.technical_analysis,
            'ma_analysis': self.ma_analysis,
            'volume_analysis': self.volume_analysis,
            'pattern_analysis': self.pattern_analysis,
            'fundamental_analysis': self.fundamental_analysis,
            'sector_position': self.sector_position,
            'company_highlights': self.company_highlights,
            'news_summary': self.news_summary,
            'market_sentiment': self.market_sentiment,
            'hot_topics': self.hot_topics,
            'analysis_summary': self.analysis_summary,
            'key_points': self.key_points,
            'risk_warning': self.risk_warning,
            'buy_reason': self.buy_reason,
            'market_snapshot': self.market_snapshot,
            'search_performed': self.search_performed,
            'success': self.success,
            'error_message': self.error_message,
            'current_price': self.current_price,
            'change_pct': self.change_pct,
            'model_used': self.model_used,
        }

    def get_core_conclusion(self) -> str:
        """Get core conclusion (one sentence)"""
        if self.dashboard and 'core_conclusion' in self.dashboard:
            return self.dashboard['core_conclusion'].get('one_sentence', self.analysis_summary)
        return self.analysis_summary

    def get_position_advice(self, has_position: bool = False) -> str:
        """Get position advice"""
        if self.dashboard and 'core_conclusion' in self.dashboard:
            pos_advice = self.dashboard['core_conclusion'].get('position_advice', {})
            if has_position:
                return pos_advice.get('has_position', self.operation_advice)
            return pos_advice.get('no_position', self.operation_advice)
        return self.operation_advice

    def get_sniper_points(self) -> Dict[str, str]:
        """Get sniper entry points"""
        if self.dashboard and 'battle_plan' in self.dashboard:
            return self.dashboard['battle_plan'].get('sniper_points', {})
        return {}

    def get_checklist(self) -> List[str]:
        """Get action checklist"""
        if self.dashboard and 'battle_plan' in self.dashboard:
            return self.dashboard['battle_plan'].get('action_checklist', [])
        return []

    def get_risk_alerts(self) -> List[str]:
        """Get risk alerts"""
        if self.dashboard and 'intelligence' in self.dashboard:
            return self.dashboard['intelligence'].get('risk_alerts', [])
        return []

    def get_emoji(self) -> str:
        """Return emoji corresponding to operation advice"""
        _, emoji, _ = get_signal_level(
            self.operation_advice,
            self.sentiment_score,
            self.report_language,
        )
        return emoji

    def get_confidence_stars(self) -> str:
        """Return confidence star rating"""
        star_map = {
            "high": "⭐⭐⭐",
            "medium": "⭐⭐",
            "low": "⭐",
        }
        return star_map.get(str(self.confidence_level or "").strip().lower(), "⭐⭐")


class GeminiAnalyzer:
    """
    Gemini AI Analyzer

    Responsibilities:
    1. Call Google Gemini API for stock analysis
    2. Generate analysis reports combining pre-searched news and technical data
    3. Parse AI-returned JSON format results

    Usage:
        analyzer = GeminiAnalyzer()
        result = analyzer.analyze(context, news_context)
    """

    # ========================================
    # System prompt - Decision Dashboard v2.0
    # ========================================
    # Output format upgrade: from simple signal to decision dashboard
    # Core modules: Core Conclusion + Data Perspective + Intelligence + Battle Plan
    # ========================================

    SYSTEM_PROMPT = """You must respond entirely in English. Do not use any Chinese characters anywhere in your response. All analysis, recommendations, labels, section headers, and explanations must be in English only.

You are an A-share investment analyst focused on trend trading, responsible for generating professional [Decision Dashboard] analysis reports.

""" + CORE_TRADING_SKILL_POLICY_ZH + """

## Output Format: Decision Dashboard JSON

Please output strictly in the following JSON format — this is a complete [Decision Dashboard]:

```json
{
    "stock_name": "stock name",
    "sentiment_score": integer 0-100,
    "trend_prediction": "strong bullish/bullish/sideways/bearish/strong bearish",
    "operation_advice": "buy/add/hold/reduce/sell/watch",
    "decision_type": "buy/hold/sell",
    "confidence_level": "high/medium/low",

    "dashboard": {
        "core_conclusion": {
            "one_sentence": "One-sentence core conclusion (under 30 words, tell user what to do directly)",
            "signal_type": "🟢 buy signal/🟡 hold & watch/🔴 sell signal/⚠️ risk warning",
            "time_sensitivity": "act now/today/this week/no rush",
            "position_advice": {
                "no_position": "Advice for no-position: specific action guidance",
                "has_position": "Advice for holding position: specific action guidance"
            }
        },

        "data_perspective": {
            "trend_status": {
                "ma_alignment": "MA alignment status description",
                "is_bullish": true/false,
                "trend_score": 0-100
            },
            "price_position": {
                "current_price": current price numeric value,
                "ma5": MA5 numeric value,
                "ma10": MA10 numeric value,
                "ma20": MA20 numeric value,
                "bias_ma5": bias rate percentage numeric value,
                "bias_status": "safe/warning/danger",
                "support_level": support level price,
                "resistance_level": resistance level price
            },
            "volume_analysis": {
                "volume_ratio": volume ratio numeric value,
                "volume_status": "expansion/contraction/flat",
                "turnover_rate": turnover rate percentage,
                "volume_meaning": "Volume meaning interpretation (e.g. contraction on pullback means selling pressure eased)"
            },
            "chip_structure": {
                "profit_ratio": profit ratio,
                "avg_cost": average cost,
                "concentration": chip concentration,
                "chip_health": "healthy/fair/caution"
            }
        },

        "intelligence": {
            "latest_news": "[Latest News] Summary of recent important news",
            "risk_alerts": ["Risk 1: specific description", "Risk 2: specific description"],
            "positive_catalysts": ["Catalyst 1: specific description", "Catalyst 2: specific description"],
            "earnings_outlook": "Earnings outlook analysis (based on annual preview, earnings flash reports etc.)",
            "sentiment_summary": "One-sentence sentiment summary"
        },

        "battle_plan": {
            "sniper_points": {
                "ideal_buy": "Ideal buy point: XX [currency] (near MA5)",
                "secondary_buy": "Secondary buy point: XX [currency] (near MA10)",
                "stop_loss": "Stop loss: XX [currency] (break below MA20 or X%)",
                "take_profit": "Target: XX [currency] (prior high/key level)"
            },
            "position_strategy": {
                "suggested_position": "Suggested position: X tenths",
                "entry_plan": "Scaled entry strategy description",
                "risk_control": "Risk control strategy description"
            },
            "action_checklist": [
                "✅/⚠️/❌ Check 1: Bullish MA alignment",
                "✅/⚠️/❌ Check 2: Bias rate reasonable (<5%, can relax for strong trend)",
                "✅/⚠️/❌ Check 3: Volume confirmation",
                "✅/⚠️/❌ Check 4: No major negative catalyst",
                "✅/⚠️/❌ Check 5: Chip structure healthy",
                "✅/⚠️/❌ Check 6: PE valuation reasonable"
            ]
        }
    },

    "analysis_summary": "100-word comprehensive analysis summary",
    "key_points": "3-5 key highlights, comma-separated",
    "risk_warning": "risk warnings",
    "buy_reason": "action rationale, citing trading principles",

    "trend_analysis": "trend shape analysis",
    "short_term_outlook": "short-term 1-3 day outlook",
    "medium_term_outlook": "medium-term 1-2 week outlook",
    "technical_analysis": "comprehensive technical analysis",
    "ma_analysis": "moving average system analysis",
    "volume_analysis": "volume analysis",
    "pattern_analysis": "candlestick pattern analysis",
    "fundamental_analysis": "fundamental analysis",
    "sector_position": "sector and industry analysis",
    "company_highlights": "company highlights/risks",
    "news_summary": "news summary",
    "market_sentiment": "market sentiment",
    "hot_topics": "related hot topics",

    "search_performed": true/false,
    "data_sources": "data source description"
}
```

## Scoring Criteria

### Strong Buy (80-100 points):
- ✅ Bullish alignment: MA5 > MA10 > MA20
- ✅ Low bias rate: <2%, best entry point
- ✅ Contraction on pullback or expansion on breakout
- ✅ Chip concentration healthy
- ✅ Positive catalysts in news

### Buy (60-79 points):
- ✅ Bullish alignment or weakly bullish
- ✅ Bias rate <5%
- ✅ Volume normal
- ⚪ One minor condition may be unsatisfied

### Watch (40-59 points):
- ⚠️ Bias rate >5% (chasing high risk)
- ⚠️ MA tangling, trend unclear
- ⚠️ Risk event present

### Sell/Reduce (0-39 points):
- ❌ Bearish alignment
- ❌ Break below MA20
- ❌ High-volume decline
- ❌ Major negative catalyst

## Decision Dashboard Core Principles

1. **Core conclusion first**: One sentence on buy/sell action
2. **Position-based advice**: Different advice for no-position vs holding
3. **Precise sniper points**: Must give specific prices, no vague language
4. **Checklist visualization**: Use ✅⚠️❌ to clearly show each check result
5. **Risk priority**: Risk factors in intelligence must be prominently highlighted"""

    def __init__(self, api_key: Optional[str] = None):
        """Initialize LLM Analyzer via LiteLLM.

        Args:
            api_key: Ignored (kept for backward compatibility). Keys are loaded from config.
        """
        self._router = None
        self._litellm_available = False
        self._init_litellm()
        if not self._litellm_available:
            logger.warning("No LLM configured (LITELLM_MODEL / API keys), AI analysis will be unavailable")

    # ========================================
    # Global macro/thematic system prompt
    # ========================================
    GLOBAL_SYSTEM_PROMPT = """You are a macro/thematic investment analyst advising a European-based portfolio investor.

The investor holds positions across 5 thematic buckets: Hard Assets, Energy/Nuclear, Defense Supply Chain, Consumer Stress, and Geopolitical hedges. They can go long and short via Interactive Brokers.

Your job is to generate a [Decision Dashboard] for a single position within this portfolio context.

## Output Format: Decision Dashboard JSON

Output strictly in the following JSON format:

```json
{
    "stock_name": "stock name",
    "sentiment_score": integer 0-100,
    "trend_prediction": "strong bullish/bullish/sideways/bearish/strong bearish",
    "operation_advice": "buy/add/hold/reduce/sell/watch",
    "decision_type": "buy/hold/sell",
    "confidence_level": "high/medium/low",

    "dashboard": {
        "core_conclusion": {
            "one_sentence": "One-sentence core conclusion (under 30 words, what to do and why)",
            "signal_type": "buy signal/hold & watch/sell signal/risk warning",
            "time_sensitivity": "act now/today/this week/no rush",
            "position_advice": {
                "no_position": "Advice if not yet in this name",
                "has_position": "Advice if already holding"
            }
        },

        "data_perspective": {
            "relative_strength": {
                "vs_index_5d": "5-day return vs benchmark (%)",
                "vs_index_20d": "20-day return vs benchmark (%)",
                "rs_status": "outperforming/inline/underperforming",
                "rs_score": 0-100
            },
            "price_position": {
                "current_price": "current price",
                "pct_from_52w_high": "% from 52-week high",
                "pct_from_52w_low": "% from 52-week low",
                "support_level": "nearest support price",
                "resistance_level": "nearest resistance price"
            },
            "volume_analysis": {
                "volume_trend": "expanding/contracting/flat",
                "volume_meaning": "Interpretation of recent volume pattern"
            },
            "valuation": {
                "pe_ttm": "P/E ratio or N/A",
                "pb": "P/B ratio or N/A",
                "dividend_yield": "yield % or N/A",
                "valuation_assessment": "cheap/fair/expensive vs history and sector"
            }
        },

        "intelligence": {
            "latest_news": "Summary of recent important news",
            "risk_alerts": ["Risk 1", "Risk 2"],
            "positive_catalysts": ["Catalyst 1", "Catalyst 2"],
            "thesis_alignment": "Which portfolio thesis does this name serve and is that thesis intact?",
            "sentiment_summary": "One-sentence sentiment"
        },

        "battle_plan": {
            "sniper_points": {
                "ideal_buy": "Ideal entry price and rationale",
                "stop_loss": "Stop loss price and rationale",
                "take_profit": "Target price and rationale"
            },
            "position_strategy": {
                "suggested_allocation": "Suggested % of bucket allocation",
                "entry_plan": "Scaling strategy",
                "risk_control": "Risk management approach"
            },
            "action_checklist": [
                "Check 1: Relative strength vs benchmark",
                "Check 2: Thesis alignment intact",
                "Check 3: Valuation reasonable",
                "Check 4: No major negative catalyst",
                "Check 5: Volume confirms direction",
                "Check 6: Cross-asset context supportive"
            ]
        }
    },

    "analysis_summary": "100-word comprehensive analysis",
    "key_points": "3-5 key highlights, comma-separated",
    "risk_warning": "risk warnings",
    "buy_reason": "action rationale citing thesis and relative value",

    "trend_analysis": "price trend and momentum analysis",
    "short_term_outlook": "1-3 day outlook",
    "medium_term_outlook": "1-2 week outlook",
    "technical_analysis": "technical analysis (support/resistance, momentum)",
    "fundamental_analysis": "fundamental and valuation analysis",
    "sector_position": "thematic bucket context",
    "company_highlights": "company-specific highlights/risks",
    "news_summary": "news summary",
    "market_sentiment": "broader market sentiment context",

    "search_performed": true/false,
    "data_sources": "data source description"
}
```

## Scoring Criteria (Global/Thematic)

### Strong Buy (80-100 points):
- Relative strength outperforming benchmark on 5d and 20d
- Thesis alignment strong and confirming
- Valuation attractive vs history
- Positive catalysts present
- Volume confirming

### Buy (60-79 points):
- Relative strength positive or neutral
- Thesis intact
- Valuation fair to attractive
- No major headwinds

### Watch (40-59 points):
- Relative strength flat or mixed signals
- Thesis intact but not confirming
- Valuation stretched or uncertain

### Sell/Reduce (0-39 points):
- Relative strength underperforming
- Thesis weakening or invalidated
- Valuation expensive
- Material negative catalyst

## Decision Dashboard Core Principles

1. **Thesis-first**: Every position exists to express a macro thesis. Score it accordingly.
2. **Relative, not absolute**: Outperformance vs benchmark matters more than absolute price moves.
3. **Cross-asset awareness**: Gold, oil, VIX, bonds context informs equity positioning.
4. **Precise levels**: Give specific support/resistance/entry/exit prices.
5. **Risk priority**: Thesis invalidation risks must be prominently flagged."""

    def _get_analysis_system_prompt(self, report_language: str, stock_code: str = "") -> str:
        """Build the analyzer system prompt based on stock region and language.

        For global stocks (US, EU, crypto, FX), prepends STRATEGY.md content
        as permanent investment framework context before the system prompt.
        """
        from data_provider.us_index_mapping import (
            is_us_stock_code, is_european_ticker, is_crypto_pair, is_fx_pair,
        )

        # Use global prompt for non-Chinese stocks
        code = (stock_code or "").strip().upper()
        is_global = (
            is_us_stock_code(code) or is_european_ticker(code)
            or is_crypto_pair(code) or is_fx_pair(code)
        )

        base_prompt = self.GLOBAL_SYSTEM_PROMPT if is_global else self.SYSTEM_PROMPT

        # Prepend STRATEGY.md context for global stocks
        if is_global:
            strategy_context = _load_strategy_context()
            if strategy_context:
                base_prompt = (
                    "# INVESTMENT STRATEGY CONTEXT\n\n"
                    "The following is the investor's complete investment framework. "
                    "You MUST reason against this framework for every analysis. "
                    "Reference specific theses, buckets, and kill switch conditions.\n\n"
                    f"{strategy_context}\n\n"
                    "---\n\n"
                    f"{base_prompt}"
                )

        if normalize_report_language(report_language) == "en":
            return base_prompt + """

## Output Language (highest priority)

- Keep all JSON keys unchanged.
- `decision_type` must remain `buy|hold|sell`.
- All human-readable JSON values must be written in English.
- Use the common English company name when you are confident; otherwise keep the original listed company name instead of inventing one.
- This includes `stock_name`, `trend_prediction`, `operation_advice`, `confidence_level`, nested dashboard text, checklist items, and all narrative summaries.
"""
        return base_prompt + """

## Output language (highest priority)

- Keep all JSON key names unchanged.
- `decision_type` must remain `buy|hold|sell`.
- All human-readable text values for end users must be in Chinese.
"""

    def _has_channel_config(self, config: Config) -> bool:
        """Check if multi-channel config (channels / YAML / legacy model_list) is active."""
        return bool(config.llm_model_list) and not all(
            e.get('model_name', '').startswith('__legacy_') for e in config.llm_model_list
        )

    def _init_litellm(self) -> None:
        """Initialize litellm Router from channels / YAML / legacy keys."""
        config = get_config()
        litellm_model = config.litellm_model
        if not litellm_model:
            logger.warning("Analyzer LLM: LITELLM_MODEL not configured")
            return

        self._litellm_available = True

        # --- Channel / YAML path: build Router from pre-built model_list ---
        if self._has_channel_config(config):
            model_list = config.llm_model_list
            self._router = Router(
                model_list=model_list,
                routing_strategy="simple-shuffle",
                num_retries=2,
            )
            unique_models = list(dict.fromkeys(
                e['litellm_params']['model'] for e in model_list
            ))
            logger.info(
                f"Analyzer LLM: Router initialized from channels/YAML — "
                f"{len(model_list)} deployment(s), models: {unique_models}"
            )
            return

        # --- Legacy path: build Router for multi-key, or use single key ---
        keys = get_api_keys_for_model(litellm_model, config)

        if len(keys) > 1:
            # Build legacy Router for primary model multi-key load-balancing
            extra_params = extra_litellm_params(litellm_model, config)
            legacy_model_list = [
                {
                    "model_name": litellm_model,
                    "litellm_params": {
                        "model": litellm_model,
                        "api_key": k,
                        **extra_params,
                    },
                }
                for k in keys
            ]
            self._router = Router(
                model_list=legacy_model_list,
                routing_strategy="simple-shuffle",
                num_retries=2,
            )
            logger.info(
                f"Analyzer LLM: Legacy Router initialized with {len(keys)} keys "
                f"for {litellm_model}"
            )
        elif keys:
            logger.info(f"Analyzer LLM: litellm initialized (model={litellm_model})")
        else:
            logger.info(
                f"Analyzer LLM: litellm initialized (model={litellm_model}, "
                f"API key from environment)"
            )

    def is_available(self) -> bool:
        """Check if LiteLLM is properly configured with at least one API key."""
        return self._router is not None or self._litellm_available

    def _call_litellm(
        self,
        prompt: str,
        generation_config: dict,
        *,
        system_prompt: Optional[str] = None,
    ) -> Tuple[str, str, Dict[str, Any]]:
        """Call LLM via litellm with fallback across configured models.

        When channels/YAML are configured, every model goes through the Router
        (which handles per-model key selection, load balancing, and retries).
        In legacy mode, the primary model may use the Router while fallback
        models fall back to direct litellm.completion().

        Args:
            prompt: User prompt text.
            generation_config: Dict with optional keys: temperature, max_output_tokens, max_tokens.

        Returns:
            Tuple of (response text, model_used, usage). On success model_used is the full model
            name and usage is a dict with prompt_tokens, completion_tokens, total_tokens.
        """
        config = get_config()
        max_tokens = (
            generation_config.get('max_output_tokens')
            or generation_config.get('max_tokens')
            or 8192
        )
        temperature = generation_config.get('temperature', 0.7)

        models_to_try = [config.litellm_model] + (config.litellm_fallback_models or [])
        models_to_try = [m for m in models_to_try if m]

        use_channel_router = self._has_channel_config(config)

        last_error = None
        effective_system_prompt = system_prompt or self.SYSTEM_PROMPT
        for model in models_to_try:
            try:
                model_short = model.split("/")[-1] if "/" in model else model
                call_kwargs: Dict[str, Any] = {
                    "model": model,
                    "messages": [
                        {"role": "system", "content": effective_system_prompt},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                }
                extra = get_thinking_extra_body(model_short)
                if extra:
                    call_kwargs["extra_body"] = extra

                _router_model_names = set(get_configured_llm_models(config.llm_model_list))
                if use_channel_router and self._router and model in _router_model_names:
                    # Channel / YAML path: Router manages key + base_url per model
                    response = self._router.completion(**call_kwargs)
                elif self._router and model == config.litellm_model and not use_channel_router:
                    # Legacy path: Router only for primary model multi-key
                    response = self._router.completion(**call_kwargs)
                else:
                    # Legacy/direct-env path: direct call (also handles direct-env
                    # providers like groq/ or bedrock/ that are not in the Router
                    # model_list even when channel mode is active)
                    keys = get_api_keys_for_model(model, config)
                    if keys:
                        call_kwargs["api_key"] = keys[0]
                    call_kwargs.update(extra_litellm_params(model, config))
                    response = litellm.completion(**call_kwargs)

                if response and response.choices and response.choices[0].message.content:
                    usage: Dict[str, Any] = {}
                    if response.usage:
                        usage = {
                            "prompt_tokens": response.usage.prompt_tokens or 0,
                            "completion_tokens": response.usage.completion_tokens or 0,
                            "total_tokens": response.usage.total_tokens or 0,
                        }
                    return (response.choices[0].message.content, model, usage)
                raise ValueError("LLM returned empty response")

            except Exception as e:
                logger.warning(f"[LiteLLM] {model} failed: {e}")
                last_error = e
                continue

        raise Exception(f"All LLM models failed (tried {len(models_to_try)} model(s)). Last error: {last_error}")

    def generate_text(
        self,
        prompt: str,
        max_tokens: int = 2048,
        temperature: float = 0.7,
    ) -> Optional[str]:
        """Public entry point for free-form text generation.

        External callers (e.g. MarketAnalyzer) must use this method instead of
        calling _call_litellm() directly or accessing private attributes such as
        _litellm_available, _router, _model, _use_openai, or _use_anthropic.

        Args:
            prompt:      Text prompt to send to the LLM.
            max_tokens:  Maximum tokens in the response (default 2048).
            temperature: Sampling temperature (default 0.7).

        Returns:
            Response text, or None if the LLM call fails (error is logged).
        """
        try:
            result = self._call_litellm(
                prompt,
                generation_config={"max_tokens": max_tokens, "temperature": temperature},
            )
            if isinstance(result, tuple):
                text, model_used, usage = result
                persist_llm_usage(usage, model_used, call_type="market_review")
                return text
            return result
        except Exception as exc:
            logger.error("[generate_text] LLM call failed: %s", exc)
            return None

    def analyze(
        self,
        context: Dict[str, Any],
        news_context: Optional[str] = None
    ) -> AnalysisResult:
        """
        Analyze a single stock.

        Process:
        1. Format input data (technical + news)
        2. Call Gemini API (with retry and model switching)
        3. Parse JSON response
        4. Return structured result

        Args:
            context: Context data obtained from storage.get_analysis_context()
            news_context: Pre-searched news content (optional)

        Returns:
            AnalysisResult object
        """
        code = context.get('code', 'Unknown')
        config = get_config()
        report_language = normalize_report_language(getattr(config, "report_language", "zh"))
        system_prompt = self._get_analysis_system_prompt(report_language, stock_code=code)
        
        # Add delay before request (prevent rate limiting from consecutive requests)
        request_delay = config.gemini_request_delay
        if request_delay > 0:
            logger.debug(f"[LLM] Waiting {request_delay:.1f} seconds before request...")
            time.sleep(request_delay)

        # Prefer stock name from context (passed in by main.py)
        name = context.get('stock_name')
        if not name or name.startswith('Stock '):
            # Fallback: get from realtime data
            if 'realtime' in context and context['realtime'].get('name'):
                name = context['realtime']['name']
            else:
                # Last resort: get from mapping table
                name = STOCK_NAME_MAP.get(code, f'Stock {code}')
        
        # If the model is unavailable, return default result
        if not self.is_available():
            return AnalysisResult(
                code=code,
                name=name,
                sentiment_score=50,
                trend_prediction='Sideways' if report_language == "en" else 'Sideways',
                operation_advice='Hold' if report_language == "en" else 'Hold',
                confidence_level='Low' if report_language == "en" else 'Low',
                analysis_summary='AI analysis is unavailable because no API key is configured.',
                risk_warning='Configure an LLM API key (GEMINI_API_KEY/ANTHROPIC_API_KEY/OPENAI_API_KEY) and retry.',
                success=False,
                error_message='LLM API key is not configured',
                model_used=None,
                report_language=report_language,
            )

        try:
            # Format input (includes technical data and news)
            prompt = self._format_prompt(context, name, news_context, report_language=report_language)

            config = get_config()
            model_name = config.litellm_model or "unknown"
            logger.info(f"========== AI Analysis {name}({code}) ==========")
            logger.info(f"[LLM config] Model: {model_name}")
            logger.info(f"[LLM config] Prompt length: {len(prompt)} chars")
            logger.info(f"[LLM config] Includes news: {'yes' if news_context else 'no'}")

            # Log full prompt (INFO level for preview, DEBUG for full)
            prompt_preview = prompt[:500] + "..." if len(prompt) > 500 else prompt
            logger.info(f"[LLM Prompt preview]\n{prompt_preview}")
            logger.debug(f"=== Full Prompt ({len(prompt)} chars) ===\n{prompt}\n=== End Prompt ===")

            # Set generation config
            generation_config = {
                "temperature": config.llm_temperature,
                "max_output_tokens": 8192,
            }

            logger.info(f"[LLM call] Starting {model_name}...")

            # Use litellm call (supports integrity check retry)
            current_prompt = prompt
            retry_count = 0
            max_retries = config.report_integrity_retry if config.report_integrity_enabled else 0

            while True:
                start_time = time.time()
                response_text, model_used, llm_usage = self._call_litellm(
                    current_prompt,
                    generation_config,
                    system_prompt=system_prompt,
                )
                elapsed = time.time() - start_time

                # Log response info
                logger.info(
                    f"[LLM response] {model_name} success, elapsed {elapsed:.2f}s, response length {len(response_text)} chars"
                )
                response_preview = response_text[:300] + "..." if len(response_text) > 300 else response_text
                logger.info(f"[LLM response preview]\n{response_preview}")
                logger.debug(
                    f"=== {model_name} full response ({len(response_text)} chars) ===\n{response_text}\n=== End Response ==="
                )

                # Parse response
                result = self._parse_response(response_text, code, name)
                result.raw_response = response_text
                result.search_performed = bool(news_context)
                result.market_snapshot = self._build_market_snapshot(context)
                result.model_used = model_used
                result.report_language = report_language

                # Content integrity check (optional)
                if not config.report_integrity_enabled:
                    break
                pass_integrity, missing_fields = self._check_content_integrity(result)
                if pass_integrity:
                    break
                if retry_count < max_retries:
                    current_prompt = self._build_integrity_retry_prompt(
                        prompt,
                        response_text,
                        missing_fields,
                        report_language=report_language,
                    )
                    retry_count += 1
                    logger.info(
                        "[LLM integrity] Missing mandatory fields %s, retry #%d to fill",
                        missing_fields,
                        retry_count,
                    )
                else:
                    self._apply_placeholder_fill(result, missing_fields)
                    logger.warning(
                        "[LLM integrity] Missing mandatory fields %s, filled with placeholders, not blocking flow",
                        missing_fields,
                    )
                    break

            persist_llm_usage(llm_usage, model_used, call_type="analysis", stock_code=code)

            logger.info(f"[LLM parsed] {name}({code}) analysis complete: {result.trend_prediction}, score {result.sentiment_score}")

            return result
            
        except Exception as e:
            logger.error(f"AI analysis {name}({code}) failed: {e}")
            return AnalysisResult(
                code=code,
                name=name,
                sentiment_score=50,
                trend_prediction='Sideways',
                operation_advice='Hold',
                confidence_level='Low',
                analysis_summary=f'Analysis failed: {str(e)[:100]}',
                risk_warning='Analysis failed. Please retry later or review manually.',
                success=False,
                error_message=str(e),
                model_used=None,
                report_language=report_language,
            )
    
    @staticmethod
    def _currency_for_code(code: str) -> str:
        """Return display currency for a stock code based on its market pattern."""
        from data_provider.us_index_mapping import (
            is_us_stock_code, is_european_ticker, is_crypto_pair, is_fx_pair,
        )
        c = (code or "").strip().upper()
        if is_crypto_pair(c) or is_fx_pair(c):
            return "USD"
        if is_european_ticker(c):
            suffix = c[c.rfind("."):]
            _eu_currency = {
                ".L": "GBP", ".PA": "EUR", ".DE": "EUR", ".AS": "EUR",
                ".MI": "EUR", ".MC": "EUR", ".BR": "EUR", ".OL": "NOK",
                ".ST": "SEK", ".CO": "DKK", ".HE": "EUR", ".VI": "EUR",
                ".SW": "CHF",
            }
            return _eu_currency.get(suffix, "EUR")
        if is_us_stock_code(c):
            return "USD"
        if c.startswith("HK") or c.startswith("hk"):
            return "HKD"
        return "CNY"

    def _format_prompt(
        self,
        context: Dict[str, Any],
        name: str,
        news_context: Optional[str] = None,
        report_language: str = "zh",
    ) -> str:
        """
        Format analysis prompt (Decision Dashboard v2.0)

        Includes: technical indicators, realtime quote (volume ratio/turnover rate), chip distribution, trend analysis, news

        Args:
            context: Technical data context (includes enhanced data)
            name: Stock name (default value, may be overridden by context)
            news_context: Pre-searched news content
        """
        code = context.get('code', 'Unknown')
        report_language = normalize_report_language(report_language)
        ccy = self._currency_for_code(code)

        # Prefer stock name from context (from realtime_quote)
        stock_name = context.get('stock_name', name)
        if not stock_name or stock_name == f'Stock {code}':
            stock_name = STOCK_NAME_MAP.get(code, f'Stock {code}')
            
        today = context.get('today', {})
        unknown_text = get_unknown_text(report_language)
        no_data_text = get_no_data_text(report_language)
        
        # ========== Build decision dashboard format input ==========
        prompt = f"""# Decision Dashboard Analysis Request

## 📊 Stock Basic Info
| Item | Data |
|------|------|
| Stock Code | **{code}** |
| Stock Name | **{stock_name}** |
| Analysis Date | {context.get('date', unknown_text)} |

---

## 📈 Technical Data

### Today's Quote
| Indicator | Value |
|-----------|-------|
| Close | {today.get('close', 'N/A')} {ccy} |
| Open | {today.get('open', 'N/A')} {ccy} |
| High | {today.get('high', 'N/A')} {ccy} |
| Low | {today.get('low', 'N/A')} {ccy} |
| Change % | {today.get('pct_chg', 'N/A')}% |
| Volume | {self._format_volume(today.get('volume'))} |
| Amount | {self._format_amount(today.get('amount'), ccy)} |

### Moving Average System (Key Judgment Indicators)
| MA | Value | Note |
|----|-------|------|
| MA5 | {today.get('ma5', 'N/A')} | Short-term trend line |
| MA10 | {today.get('ma10', 'N/A')} | Short-medium term trend line |
| MA20 | {today.get('ma20', 'N/A')} | Medium-term trend line |
| MA Pattern | {context.get('ma_status', unknown_text)} | Bullish/Bearish/Tangling |
"""
        
        # Add realtime quote data (volume ratio, turnover rate, etc.)
        if 'realtime' in context:
            rt = context['realtime']
            prompt += f"""
### Realtime Quote Enhanced Data
| Indicator | Value | Interpretation |
|-----------|-------|----------------|
| Current Price | {rt.get('price', 'N/A')} {ccy} | |
| **Volume Ratio** | **{rt.get('volume_ratio', 'N/A')}** | {rt.get('volume_ratio_desc', '')} |
| **Turnover Rate** | **{rt.get('turnover_rate', 'N/A')}%** | |
| P/E (dynamic) | {rt.get('pe_ratio', 'N/A')} | |
| P/B | {rt.get('pb_ratio', 'N/A')} | |
| Total Market Cap | {self._format_amount(rt.get('total_mv'), ccy)} | |
| Float Market Cap | {self._format_amount(rt.get('circ_mv'), ccy)} | |
| 60-day Change | {rt.get('change_60d', 'N/A')}% | Medium-term performance |
"""

        # Add financial report and dividends (value investing basis)
        fundamental_context = context.get("fundamental_context") if isinstance(context, dict) else None
        earnings_block = (
            fundamental_context.get("earnings", {})
            if isinstance(fundamental_context, dict)
            else {}
        )
        earnings_data = (
            earnings_block.get("data", {})
            if isinstance(earnings_block, dict)
            else {}
        )
        financial_report = (
            earnings_data.get("financial_report", {})
            if isinstance(earnings_data, dict)
            else {}
        )
        dividend_metrics = (
            earnings_data.get("dividend", {})
            if isinstance(earnings_data, dict)
            else {}
        )
        if isinstance(financial_report, dict) or isinstance(dividend_metrics, dict):
            financial_report = financial_report if isinstance(financial_report, dict) else {}
            dividend_metrics = dividend_metrics if isinstance(dividend_metrics, dict) else {}
            ttm_yield = dividend_metrics.get("ttm_dividend_yield_pct", "N/A")
            ttm_cash = dividend_metrics.get("ttm_cash_dividend_per_share", "N/A")
            ttm_count = dividend_metrics.get("ttm_event_count", "N/A")
            report_date = financial_report.get("report_date", "N/A")
            prompt += f"""
### Financial Report & Dividends (Value Investing Basis)
| Metric | Value | Note |
|--------|-------|------|
| Latest Report Period | {report_date} | From structured financial report fields |
| Revenue | {financial_report.get('revenue', 'N/A')} | |
| Net Profit (parent) | {financial_report.get('net_profit_parent', 'N/A')} | |
| Operating Cash Flow | {financial_report.get('operating_cash_flow', 'N/A')} | |
| ROE | {financial_report.get('roe', 'N/A')} | |
| TTM Cash Dividend Per Share | {ttm_cash} | Cash dividends only, pre-tax basis |
| TTM Dividend Yield | {ttm_yield} | Formula: TTM cash dividend per share / current price x 100% |
| TTM Dividend Event Count | {ttm_count} | |

> If any field above is N/A or missing, explicitly state "data missing, cannot assess" -- do not fabricate values.
"""

        # FMP fundamentals block (English, shown when FMP_API_KEY is configured)
        fmp = context.get("fmp_fundamentals")
        if isinstance(fmp, dict):
            def _fmt_fmp(val, pct: bool = False, suffix: str = "") -> str:
                if val is None:
                    return "N/A"
                try:
                    f = float(val)
                    if pct:
                        return f"{f * 100:.2f}%"
                    return f"{f:.2f}{suffix}"
                except (TypeError, ValueError):
                    return "N/A"

            prompt += f"""
### Fundamentals (FMP)
| Metric | Value |
|--------|-------|
| P/E (TTM) | {_fmt_fmp(fmp.get('pe_ttm'))} |
| P/B | {_fmt_fmp(fmp.get('pb'))} |
| Dividend Yield | {_fmt_fmp(fmp.get('dividend_yield'), pct=True)} |
| ROE | {_fmt_fmp(fmp.get('roe'), pct=True)} |
| Revenue YoY Growth | {_fmt_fmp(fmp.get('revenue_yoy_growth'), suffix='%')} |

> If any field shows N/A, explicitly state "data unavailable" — do not invent values.
"""

        # Add chip distribution data
        if 'chip' in context:
            chip = context['chip']
            profit_ratio = chip.get('profit_ratio', 0)
            prompt += f"""
### Chip Distribution Data (Efficiency Metrics)
| Metric | Value | Health Standard |
|--------|-------|----------------|
| **Profit Ratio** | **{profit_ratio:.1%}** | Caution when 70-90% |
| Average Cost | {chip.get('avg_cost', 'N/A')} {ccy} | Current price should be 5-15% above |
| 90% Chip Concentration | {chip.get('concentration_90', 0):.2%} | <15% is concentrated |
| 70% Chip Concentration | {chip.get('concentration_70', 0):.2%} | |
| Chip Status | {chip.get('chip_status', unknown_text)} | |
"""
        
        # Add trend analysis results (prediction based on trading principles)
        if 'trend_analysis' in context:
            trend = context['trend_analysis']
            bias_warning = "🚨 Over 5%, chasing high forbidden!" if trend.get('bias_ma5', 0) > 5 else "✅ Safe range"
            prompt += f"""
### Trend Analysis Forecast (Based on Trading Principles)
| Indicator | Value | Judgment |
|-----------|-------|----------|
| Trend Status | {trend.get('trend_status', unknown_text)} | |
| MA Alignment | {trend.get('ma_alignment', unknown_text)} | MA5>MA10>MA20 is bullish |
| Trend Strength | {trend.get('trend_strength', 0)}/100 | |
| **Bias Rate (MA5)** | **{trend.get('bias_ma5', 0):+.2f}%** | {bias_warning} |
| Bias Rate (MA10) | {trend.get('bias_ma10', 0):+.2f}% | |
| Volume Status | {trend.get('volume_status', unknown_text)} | {trend.get('volume_trend', '')} |
| System Signal | {trend.get('buy_signal', unknown_text)} | |
| System Score | {trend.get('signal_score', 0)}/100 | |

#### System Analysis Rationale
**Buy Reasons**:
{chr(10).join('- ' + r for r in trend.get('signal_reasons', ['None'])) if trend.get('signal_reasons') else '- None'}

**Risk Factors**:
{chr(10).join('- ' + r for r in trend.get('risk_factors', ['None'])) if trend.get('risk_factors') else '- None'}
"""
        
        # Add yesterday comparison data
        if 'yesterday' in context:
            volume_change = context.get('volume_change_ratio', 'N/A')
            prompt += f"""
### Volume/Price Change
- Volume change vs yesterday: {volume_change}x
- Price change vs yesterday: {context.get('price_change_ratio', 'N/A')}%
"""
        
        # Reddit Sentiment block (English, shown when Apify returned data)
        reddit = context.get("reddit_sentiment")
        if isinstance(reddit, dict):
            titles = reddit.get("top_3_titles") or []
            titles_md = "\n".join(f"  {i+1}. {t}" for i, t in enumerate(titles)) if titles else "  N/A"
            prompt += f"""
### Reddit Sentiment (last 7 days)
| Metric | Value |
|--------|-------|
| Total Mentions (posts) | {reddit.get('total_mentions', 'N/A')} |
| Sentiment | **{reddit.get('sentiment_label', 'N/A').upper()}** |

**Top posts by upvotes:**
{titles_md}

"""

        # Add news search results (key area)
        news_window_days: Optional[int] = None
        context_window = context.get("news_window_days")
        try:
            if context_window is not None:
                parsed_window = int(context_window)
                if parsed_window > 0:
                    news_window_days = parsed_window
        except (TypeError, ValueError):
            news_window_days = None

        if news_window_days is None:
            prompt_config = get_config()
            news_window_days = resolve_news_window_days(
                news_max_age_days=getattr(prompt_config, "news_max_age_days", 3),
                news_strategy_profile=getattr(prompt_config, "news_strategy_profile", "short"),
            )
        prompt += """
---

## 📰 Intelligence / News
"""
        if news_context:
            prompt += f"""
Below are news search results for **{stock_name}({code})** over the past {news_window_days} days. Please extract:
1. 🚨 **Risk Alerts**: stake reduction, penalties, negative news
2. 🎯 **Positive Catalysts**: earnings, contracts, policy
3. 📊 **Earnings Outlook**: annual preview, earnings flash reports
4. 🕒 **Time Rules (mandatory)**:
   - Every entry output to `risk_alerts` / `positive_catalysts` / `latest_news` must include a specific date (YYYY-MM-DD)
   - News outside the past {news_window_days} day window must be ignored
   - News with unknown or unverifiable publication date must be ignored

```
{news_context}
```
"""
        else:
            prompt += """
No recent news found for this stock. Please rely primarily on technical data for analysis.
"""

        # Inject missing data warning
        if context.get('data_missing'):
            prompt += """
⚠️ **Data Missing Warning**
Due to API limitations, complete realtime quote and technical indicator data is currently unavailable.
Please **ignore N/A data in the tables above** and focus on fundamental and sentiment analysis from **[📰 Intelligence/News]**.
When answering technical questions (e.g. MA, bias rate), directly state "data missing, cannot assess" — **do not fabricate data**.
"""

        # Explicit output requirements
        prompt += f"""
---

## ✅ Analysis Task

Please generate a [Decision Dashboard] for **{stock_name}({code})** strictly in JSON format.
"""
        if context.get('is_index_etf'):
            prompt += """
> ⚠️ **Index/ETF Analysis Constraints**: This security is an index-tracking ETF or market index.
> - Risk analysis should focus only on: **index trend, tracking error, market liquidity**
> - Do NOT include fund company lawsuits, reputation, or management changes in risk alerts
> - Earnings outlook is based on **overall index constituent performance**, not fund company financials
> - `risk_alerts` must not include company operational risks related to the fund manager

"""
        prompt += f"""
### ⚠️ Important: Output correct stock name format
The correct stock name format is "Stock Name (Stock Code)", e.g. "Kweichow Moutai (600519)".
If the stock name shown above is "Stock {code}" or incorrect, please **explicitly output the correct full stock name** at the beginning of analysis.

### Key Checks (must explicitly answer):
1. ❓ Is MA5>MA10>MA20 bullish alignment satisfied?
2. ❓ Is the current bias rate within the safe range (<5%)? — Must note "chasing high forbidden" if over 5%
3. ❓ Is volume confirming (contraction on pullback / expansion on breakout)?
4. ❓ Is chip structure healthy?
5. ❓ Are there major negative catalysts in news? (stake reduction, penalties, earnings miss etc.)

### Decision Dashboard Requirements:
- **Stock Name**: Must output correct full name (e.g. "Kweichow Moutai" not "Stock 600519")
- **Core Conclusion**: One sentence on buy/sell/wait
- **Position-based Advice**: What to do with no position vs holding position
- **Precise Sniper Points**: Entry price, stop loss, target price (to the cent)
- **Checklist**: Each item marked with ✅/⚠️/❌
- **News Time Compliance**: `latest_news`, `risk_alerts`, `positive_catalysts` must not include news outside past {news_window_days} days or with unknown date

Please output the complete JSON format Decision Dashboard."""

        if report_language == "en":
            prompt += """

### Output language requirements (highest priority)
- Keep every JSON key exactly as defined above; do not translate keys.
- `decision_type` must remain `buy`, `hold`, or `sell`.
- All human-readable JSON values must be in English.
- This includes `stock_name`, `trend_prediction`, `operation_advice`, `confidence_level`, all nested dashboard text, checklist items, and every summary field.
- Use the common English company name when you are confident. If not, keep the listed company name rather than inventing one.
- When data is missing, explain it in English instead of Chinese.
"""
        else:
            prompt += f"""

### Output language requirements (highest priority)
- All JSON key names must remain unchanged; do not translate keys.
- `decision_type` must remain `buy`, `hold`, or `sell`.
- All human-readable text values for end users must be in Chinese.
- When data is missing, state in Chinese: "{no_data_text}, cannot assess".
"""
        
        return prompt
    
    def _format_volume(self, volume: Optional[float]) -> str:
        """Format volume display"""
        if volume is None:
            return 'N/A'
        if volume >= 1e8:
            return f"{volume / 1e8:.2f} B shares"
        elif volume >= 1e4:
            return f"{volume / 1e4:.2f} K shares"
        else:
            return f"{volume:.0f} shares"

    def _format_amount(self, amount: Optional[float], currency: str = "CNY") -> str:
        """Format amount display with appropriate currency."""
        if amount is None:
            return 'N/A'
        if amount >= 1e8:
            return f"{amount / 1e8:.2f}B {currency}"
        elif amount >= 1e4:
            return f"{amount / 1e4:.2f}K {currency}"
        else:
            return f"{amount:.0f} {currency}"

    def _format_percent(self, value: Optional[float]) -> str:
        """Format percentage display"""
        if value is None:
            return 'N/A'
        try:
            return f"{float(value):.2f}%"
        except (TypeError, ValueError):
            return 'N/A'

    def _format_price(self, value: Optional[float]) -> str:
        """Format price display"""
        if value is None:
            return 'N/A'
        try:
            return f"{float(value):.2f}"
        except (TypeError, ValueError):
            return 'N/A'

    def _build_market_snapshot(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Build daily market snapshot (display use)"""
        today = context.get('today', {}) or {}
        realtime = context.get('realtime', {}) or {}
        yesterday = context.get('yesterday', {}) or {}

        prev_close = yesterday.get('close')
        close = today.get('close')
        high = today.get('high')
        low = today.get('low')

        amplitude = None
        change_amount = None
        if prev_close not in (None, 0) and high is not None and low is not None:
            try:
                amplitude = (float(high) - float(low)) / float(prev_close) * 100
            except (TypeError, ValueError, ZeroDivisionError):
                amplitude = None
        if prev_close is not None and close is not None:
            try:
                change_amount = float(close) - float(prev_close)
            except (TypeError, ValueError):
                change_amount = None

        snapshot = {
            "date": context.get('date', 'unknown'),
            "close": self._format_price(close),
            "open": self._format_price(today.get('open')),
            "high": self._format_price(high),
            "low": self._format_price(low),
            "prev_close": self._format_price(prev_close),
            "pct_chg": self._format_percent(today.get('pct_chg')),
            "change_amount": self._format_price(change_amount),
            "amplitude": self._format_percent(amplitude),
            "volume": self._format_volume(today.get('volume')),
            "amount": self._format_amount(today.get('amount'), self._currency_for_code(context.get('code', ''))),
        }

        if realtime:
            snapshot.update({
                "price": self._format_price(realtime.get('price')),
                "volume_ratio": realtime.get('volume_ratio', 'N/A'),
                "turnover_rate": self._format_percent(realtime.get('turnover_rate')),
                "source": getattr(realtime.get('source'), 'value', realtime.get('source', 'N/A')),
            })

        return snapshot

    def _check_content_integrity(self, result: AnalysisResult) -> Tuple[bool, List[str]]:
        """Delegate to module-level check_content_integrity."""
        return check_content_integrity(result)

    def _build_integrity_complement_prompt(self, missing_fields: List[str], report_language: str = "zh") -> str:
        """Build complement instruction for missing mandatory fields."""
        report_language = normalize_report_language(report_language)
        if report_language == "en":
            lines = ["### Completion requirements: fill the missing mandatory fields below and output the full JSON again:"]
            for f in missing_fields:
                if f == "sentiment_score":
                    lines.append("- sentiment_score: integer score from 0 to 100")
                elif f == "operation_advice":
                    lines.append("- operation_advice: localized action advice")
                elif f == "analysis_summary":
                    lines.append("- analysis_summary: concise analysis summary")
                elif f == "dashboard.core_conclusion.one_sentence":
                    lines.append("- dashboard.core_conclusion.one_sentence: one-line decision")
                elif f == "dashboard.intelligence.risk_alerts":
                    lines.append("- dashboard.intelligence.risk_alerts: risk alert list (can be empty)")
                elif f == "dashboard.battle_plan.sniper_points.stop_loss":
                    lines.append("- dashboard.battle_plan.sniper_points.stop_loss: stop-loss level")
            return "\n".join(lines)

        lines = ["### Completion requirements: please supplement the following mandatory fields based on the analysis above and output the complete JSON:"]
        for f in missing_fields:
            if f == "sentiment_score":
                lines.append("- sentiment_score: 0-100 composite score")
            elif f == "operation_advice":
                lines.append("- operation_advice: buy/add/hold/reduce/sell/watch")
            elif f == "analysis_summary":
                lines.append("- analysis_summary: comprehensive analysis summary")
            elif f == "dashboard.core_conclusion.one_sentence":
                lines.append("- dashboard.core_conclusion.one_sentence: one-sentence decision")
            elif f == "dashboard.intelligence.risk_alerts":
                lines.append("- dashboard.intelligence.risk_alerts: risk alert list (can be empty array)")
            elif f == "dashboard.battle_plan.sniper_points.stop_loss":
                lines.append("- dashboard.battle_plan.sniper_points.stop_loss: stop loss price")
        return "\n".join(lines)

    def _build_integrity_retry_prompt(
        self,
        base_prompt: str,
        previous_response: str,
        missing_fields: List[str],
        report_language: str = "zh",
    ) -> str:
        """Build retry prompt using the previous response as the complement baseline."""
        complement = self._build_integrity_complement_prompt(missing_fields, report_language=report_language)
        previous_output = previous_response.strip()
        if normalize_report_language(report_language) == "en":
            prefix = "### The previous output is below. Complete the missing fields based on that output and return the full JSON again. Do not omit existing fields:"
        else:
            prefix = "### The previous output is below. Please fill in the missing fields based on that output and re-output the complete JSON. Do not omit existing fields:"
        return "\n\n".join([
            base_prompt,
            prefix,
            previous_output,
            complement,
        ])

    def _apply_placeholder_fill(self, result: AnalysisResult, missing_fields: List[str]) -> None:
        """Delegate to module-level apply_placeholder_fill."""
        apply_placeholder_fill(result, missing_fields)

    def _parse_response(
        self,
        response_text: str,
        code: str,
        name: str
    ) -> AnalysisResult:
        """
        Parse Gemini response (Decision Dashboard edition)

        Attempt to extract JSON format analysis result from response, including dashboard field.
        If parsing fails, attempt intelligent extraction or return default result.
        """
        try:
            report_language = normalize_report_language(getattr(get_config(), "report_language", "zh"))
            # Clean response text: remove markdown code block markers
            cleaned_text = response_text
            if '```json' in cleaned_text:
                cleaned_text = cleaned_text.replace('```json', '').replace('```', '')
            elif '```' in cleaned_text:
                cleaned_text = cleaned_text.replace('```', '')

            # Try to find JSON content
            json_start = cleaned_text.find('{')
            json_end = cleaned_text.rfind('}') + 1

            if json_start >= 0 and json_end > json_start:
                json_str = cleaned_text[json_start:json_end]

                # Try to fix common JSON issues
                json_str = self._fix_json_string(json_str)
                
                data = json.loads(json_str)

                # Schema validation (lenient: on failure, continue with raw dict)
                try:
                    AnalysisReportSchema.model_validate(data)
                except Exception as e:
                    logger.warning(
                        "LLM report schema validation failed, continuing with raw dict: %s",
                        str(e)[:100],
                    )

                # Extract dashboard data
                dashboard = data.get('dashboard', None)

                # Prefer AI-returned stock name (if original name is invalid or contains code)
                ai_stock_name = data.get('stock_name')
                if ai_stock_name and (name.startswith('Stock ') or name == code or 'Unknown' in name):
                    name = ai_stock_name

                # Parse all fields, use defaults to prevent missing fields
                # Parse decision_type, infer from operation_advice if absent
                decision_type = data.get('decision_type', '')
                if not decision_type:
                    op = data.get('operation_advice', 'Hold')
                    decision_type = infer_decision_type_from_advice(op, default='hold')

                return AnalysisResult(
                    code=code,
                    name=name,
                    # Core metrics
                    sentiment_score=int(data.get('sentiment_score', 50)),
                    trend_prediction=data.get('trend_prediction', 'Sideways'),
                    operation_advice=data.get('operation_advice', 'Hold'),
                    decision_type=decision_type,
                    confidence_level=localize_confidence_level(
                        data.get('confidence_level', 'Medium'),
                        report_language,
                    ),
                    report_language=report_language,
                    # Decision dashboard
                    dashboard=dashboard,
                    # Trend analysis
                    trend_analysis=data.get('trend_analysis', ''),
                    short_term_outlook=data.get('short_term_outlook', ''),
                    medium_term_outlook=data.get('medium_term_outlook', ''),
                    # Technical
                    technical_analysis=data.get('technical_analysis', ''),
                    ma_analysis=data.get('ma_analysis', ''),
                    volume_analysis=data.get('volume_analysis', ''),
                    pattern_analysis=data.get('pattern_analysis', ''),
                    # Fundamental
                    fundamental_analysis=data.get('fundamental_analysis', ''),
                    sector_position=data.get('sector_position', ''),
                    company_highlights=data.get('company_highlights', ''),
                    # Sentiment/news
                    news_summary=data.get('news_summary', ''),
                    market_sentiment=data.get('market_sentiment', ''),
                    hot_topics=data.get('hot_topics', ''),
                    # Comprehensive
                    analysis_summary=data.get('analysis_summary', 'Analysis completed'),
                    key_points=data.get('key_points', ''),
                    risk_warning=data.get('risk_warning', ''),
                    buy_reason=data.get('buy_reason', ''),
                    # Metadata
                    search_performed=data.get('search_performed', False),
                    data_sources=data.get('data_sources', 'Technical data'),
                    success=True,
                )
            else:
                # No JSON found, try to extract info from plain text
                logger.warning(f"Failed to extract JSON from response, using raw text analysis")
                return self._parse_text_response(response_text, code, name)

        except json.JSONDecodeError as e:
            logger.warning(f"JSON parse failed: {e}, trying text extraction")
            return self._parse_text_response(response_text, code, name)
    
    def _fix_json_string(self, json_str: str) -> str:
        """Fix common JSON format issues"""
        import re

        # Remove comments
        json_str = re.sub(r'//.*?\n', '\n', json_str)
        json_str = re.sub(r'/\*.*?\*/', '', json_str, flags=re.DOTALL)

        # Fix trailing commas
        json_str = re.sub(r',\s*}', '}', json_str)
        json_str = re.sub(r',\s*]', ']', json_str)

        # Ensure booleans are lowercase
        json_str = json_str.replace('True', 'true').replace('False', 'false')

        # fix by json-repair
        json_str = repair_json(json_str)

        return json_str
    
    def _parse_text_response(
        self,
        response_text: str,
        code: str,
        name: str
    ) -> AnalysisResult:
        """Extract analysis information from plain text response as best effort"""
        report_language = normalize_report_language(getattr(get_config(), "report_language", "zh"))
        # Try to identify keywords to judge sentiment
        sentiment_score = 50
        trend = 'Sideways'
        advice = 'Hold'

        text_lower = response_text.lower()

        # Simple sentiment recognition (includes Chinese keywords for zh-mode LLM responses)
        positive_keywords = ['bullish', 'buy', 'rising', 'breakout', 'strong']
        negative_keywords = ['bearish', 'sell', 'falling', 'breakdown', 'weak']

        positive_count = sum(1 for kw in positive_keywords if kw in text_lower)
        negative_count = sum(1 for kw in negative_keywords if kw in text_lower)

        if positive_count > negative_count + 1:
            sentiment_score = 65
            trend = 'Bullish'
            advice = 'Buy'
            decision_type = 'buy'
        elif negative_count > positive_count + 1:
            sentiment_score = 35
            trend = 'Bearish'
            advice = 'Sell'
            decision_type = 'sell'
        else:
            decision_type = 'hold'

        # Take first 500 chars as summary
        summary = response_text[:500] if response_text else 'No analysis result'

        return AnalysisResult(
            code=code,
            name=name,
            sentiment_score=sentiment_score,
            trend_prediction=trend,
            operation_advice=advice,
            decision_type=decision_type,
            confidence_level='Low',
            analysis_summary=summary,
            key_points='JSON parsing failed; treat this as best-effort output.',
            risk_warning='The result may be inaccurate. Cross-check with other information.',
            raw_response=response_text,
            success=True,
            report_language=report_language,
        )
    
    def batch_analyze(
        self,
        contexts: List[Dict[str, Any]],
        delay_between: float = 2.0
    ) -> List[AnalysisResult]:
        """
        Batch analyze multiple stocks.

        Note: To avoid API rate limits, there is a delay between each analysis.

        Args:
            contexts: List of context data
            delay_between: Delay between each analysis (seconds)

        Returns:
            List of AnalysisResult
        """
        results = []

        for i, context in enumerate(contexts):
            if i > 0:
                logger.debug(f"Waiting {delay_between} seconds before continuing...")
                time.sleep(delay_between)

            result = self.analyze(context)
            results.append(result)

        return results


# Convenience function
def get_analyzer() -> GeminiAnalyzer:
    """Get LLM analyzer instance"""
    return GeminiAnalyzer()


if __name__ == "__main__":
    # Test code
    logging.basicConfig(level=logging.DEBUG)

    # Simulate context data
    test_context = {
        'code': '600519',
        'date': '2026-01-09',
        'today': {
            'open': 1800.0,
            'high': 1850.0,
            'low': 1780.0,
            'close': 1820.0,
            'volume': 10000000,
            'amount': 18200000000,
            'pct_chg': 1.5,
            'ma5': 1810.0,
            'ma10': 1800.0,
            'ma20': 1790.0,
            'volume_ratio': 1.2,
        },
        'ma_status': 'Bullish alignment 📈',
        'volume_change_ratio': 1.3,
        'price_change_ratio': 1.5,
    }

    analyzer = GeminiAnalyzer()

    if analyzer.is_available():
        print("=== AI Analysis Test ===")
        result = analyzer.analyze(test_context)
        print(f"Analysis result: {result.to_dict()}")
    else:
        print("Gemini API not configured, skipping test")
