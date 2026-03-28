# -*- coding: utf-8 -*-
"""Helpers for report output language selection and localization."""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

SUPPORTED_REPORT_LANGUAGES = ("zh", "en")

_REPORT_LANGUAGE_ALIASES = {
    "zh-cn": "zh",
    "zh_cn": "zh",
    "zh-hans": "zh",
    "zh_hans": "zh",
    "zh-tw": "zh",
    "zh_tw": "zh",
    "cn": "zh",
    "chinese": "zh",
    "english": "en",
    "en-us": "en",
    "en_us": "en",
    "en-gb": "en",
    "en_gb": "en",
}

_OPERATION_ADVICE_CANONICAL_MAP = {
    "strong buy": "strong_buy",
    "strong buy": "strong_buy",
    "strong_buy": "strong_buy",
    "buy": "buy",
    "buy": "buy",
    "add to position": "buy",
    "accumulate": "buy",
    "add position": "buy",
    "hold": "hold",
    "hold": "hold",
    "wait and see": "watch",
    "watch": "watch",
    "wait": "watch",
    "wait and see": "watch",
    "reduce position": "reduce",
    "reduce": "reduce",
    "trim": "reduce",
    "sell": "sell",
    "sell": "sell",
    "strong sell": "strong_sell",
    "strong sell": "strong_sell",
    "strong_sell": "strong_sell",
}

_OPERATION_ADVICE_TRANSLATIONS = {
    "strong_buy": {"zh": "strong buy", "en": "Strong Buy"},
    "buy": {"zh": "buy", "en": "Buy"},
    "hold": {"zh": "hold", "en": "Hold"},
    "watch": {"zh": "wait and see", "en": "Watch"},
    "reduce": {"zh": "reduce position", "en": "Reduce"},
    "sell": {"zh": "sell", "en": "Sell"},
    "strong_sell": {"zh": "strong sell", "en": "Strong Sell"},
}

_TREND_PREDICTION_CANONICAL_MAP = {
    "strongly bullish": "strong_bullish",
    "strong bullish": "strong_bullish",
    "very bullish": "strong_bullish",
    "bullish": "bullish",
    "bullish": "bullish",
    "uptrend": "bullish",
    "oscillation": "sideways",
    "neutral": "sideways",
    "sideways": "sideways",
    "range-bound": "sideways",
    "bearish": "bearish",
    "bearish": "bearish",
    "downtrend": "bearish",
    "strongly bearish": "strong_bearish",
    "strong bearish": "strong_bearish",
    "very bearish": "strong_bearish",
}

_TREND_PREDICTION_TRANSLATIONS = {
    "strong_bullish": {"zh": "strongly bullish", "en": "Strong Bullish"},
    "bullish": {"zh": "bullish", "en": "Bullish"},
    "sideways": {"zh": "oscillation", "en": "Sideways"},
    "bearish": {"zh": "bearish", "en": "Bearish"},
    "strong_bearish": {"zh": "strongly bearish", "en": "Strong Bearish"},
}

_CONFIDENCE_LEVEL_CANONICAL_MAP = {
    "high": "high",
    "high": "high",
    "in": "medium",
    "medium": "medium",
    "med": "medium",
    "low": "low",
    "low": "low",
}

_CONFIDENCE_LEVEL_TRANSLATIONS = {
    "high": {"zh": "high", "en": "High"},
    "medium": {"zh": "in", "en": "Medium"},
    "low": {"zh": "low", "en": "Low"},
}

_CHIP_HEALTH_CANONICAL_MAP = {
    "health": "healthy",
    "healthy": "healthy",
    "general": "average",
    "average": "average",
    "beware": "caution",
    "caution": "caution",
}

_CHIP_HEALTH_TRANSLATIONS = {
    "healthy": {"zh": "health", "en": "Healthy"},
    "average": {"zh": "general", "en": "Average"},
    "caution": {"zh": "beware", "en": "Caution"},
}

_BIAS_STATUS_CANONICAL_MAP = {
    "safe": "safe",
    "safe": "safe",
    "alert": "caution",
    "beware": "caution",
    "caution": "caution",
    "dangerous": "danger",
    "risk": "danger",
    "danger": "danger",
}

_BIAS_STATUS_TRANSLATIONS = {
    "safe": {"zh": "safe", "en": "Safe"},
    "caution": {"zh": "alert", "en": "Caution"},
    "danger": {"zh": "dangerous", "en": "Danger"},
}

_PLACEHOLDER_BY_LANGUAGE = {
    "zh": "to be filled",
    "en": "TBD",
}

_UNKNOWN_BY_LANGUAGE = {
    "zh": "unknown",
    "en": "Unknown",
}

_NO_DATA_BY_LANGUAGE = {
    "zh": "Data unavailable",
    "en": "Data unavailable",
}

_GENERIC_STOCK_NAME_BY_LANGUAGE = {
    "zh": "Unnamed Stock",
    "en": "Unnamed Stock",
}

_REPORT_LABELS: Dict[str, Dict[str, str]] = {
    "zh": {
        "dashboard_title": "decisiondashboard",
        "brief_title": "decisionbriefing",
        "analyzed_prefix": "totalanalyzing",
        "stock_unit": "onlystock",
        "stock_unit_compact": "only",
        "buy_label": "buy",
        "watch_label": "wait and see",
        "sell_label": "sell",
        "summary_heading": "analysis resultsummary",
        "info_heading": "Importantinfoquick overview",
        "sentiment_summary_label": "public sentimentsentiment",
        "earnings_outlook_label": "performanceexpected",
        "risk_alerts_label": "riskalert",
        "positive_catalysts_label": "positive catalyst",
        "latest_news_label": "latestdynamic",
        "core_conclusion_heading": "coreconclusion",
        "one_sentence_label": "one sentencedecision",
        "time_sensitivity_label": "wheneffect-ness",
        "default_time_sensitivity": "within this week",
        "position_status_label": "holdingsituation",
        "action_advice_label": "operationrecommended",
        "no_position_label": "no positioner",
        "has_position_label": "holdinger",
        "continue_holding": "continuinghold",
        "market_snapshot_heading": "todayquote/market data",
        "close_label": "close",
        "prev_close_label": "yesterday close",
        "open_label": "open",
        "high_label": "highest",
        "low_label": "lowest",
        "change_pct_label": "price change percentage",
        "change_amount_label": "price change amount",
        "amplitude_label": "amplitude",
        "volume_label": "trading volume",
        "amount_label": "trading amount",
        "current_price_label": "currentprice",
        "volume_ratio_label": "volume ratio",
        "turnover_rate_label": "turnover rate",
        "source_label": "quote/market datasource",
        "data_perspective_heading": "dataperspective",
        "ma_alignment_label": "moving averagearrange",
        "bullish_alignment_label": "long positionarrange",
        "yes_label": "is",
        "no_label": "no",
        "trend_strength_label": "trendintensity",
        "price_metrics_label": "priceindicator",
        "ma5_label": "MA5",
        "ma10_label": "MA10",
        "ma20_label": "MA20",
        "bias_ma5_label": "BIAS ratio(MA5)",
        "support_level_label": "support level",
        "resistance_level_label": "stressdigit",
        "chip_label": "chip",
        "battle_plan_heading": "action plan",
        "ideal_buy_label": "ideal buy point",
        "secondary_buy_label": "secondary buy point",
        "stop_loss_label": "stop lossdigit",
        "take_profit_label": "target level",
        "suggested_position_label": "positionrecommended",
        "entry_plan_label": "open positionstrategy",
        "risk_control_label": "risk controlstrategy",
        "checklist_heading": "checklist",
        "failed_checks_heading": "checknotviaitem",
        "history_compare_heading": "historicalsignaltocompared to",
        "time_label": "time",
        "score_label": "score",
        "advice_label": "recommended",
        "trend_label": "trend",
        "generated_at_label": "report generationtime",
        "report_time_label": "generatingtime",
        "no_results": "noanalysis result",
        "report_title": "stockanalysis report",
        "avg_score_label": "averageminute",
        "action_points_heading": "operationpointdigit",
        "position_advice_heading": "holdingrecommended",
        "analysis_model_label": "analyzingmodel",
        "not_investment_advice": "AIgenerating，onlyprovidereference，notconstituteinvestmentrecommended",
        "details_report_hint": "detailedreportsee",
    },
    "en": {
        "dashboard_title": "Decision Dashboard",
        "brief_title": "Decision Brief",
        "analyzed_prefix": "Analyzed",
        "stock_unit": "stocks",
        "stock_unit_compact": "stocks",
        "buy_label": "Buy",
        "watch_label": "Watch",
        "sell_label": "Sell",
        "summary_heading": "Summary",
        "info_heading": "Key Updates",
        "sentiment_summary_label": "Sentiment",
        "earnings_outlook_label": "Earnings Outlook",
        "risk_alerts_label": "Risk Alerts",
        "positive_catalysts_label": "Positive Catalysts",
        "latest_news_label": "Latest News",
        "core_conclusion_heading": "Core Conclusion",
        "one_sentence_label": "One-line Decision",
        "time_sensitivity_label": "Time Sensitivity",
        "default_time_sensitivity": "This week",
        "position_status_label": "Position",
        "action_advice_label": "Action",
        "no_position_label": "No Position",
        "has_position_label": "Holding",
        "continue_holding": "Continue holding",
        "market_snapshot_heading": "Market Snapshot",
        "close_label": "Close",
        "prev_close_label": "Prev Close",
        "open_label": "Open",
        "high_label": "High",
        "low_label": "Low",
        "change_pct_label": "Change %",
        "change_amount_label": "Change",
        "amplitude_label": "Amplitude",
        "volume_label": "Volume",
        "amount_label": "Turnover",
        "current_price_label": "Price",
        "volume_ratio_label": "Volume Ratio",
        "turnover_rate_label": "Turnover Rate",
        "source_label": "Source",
        "data_perspective_heading": "Data View",
        "ma_alignment_label": "MA Alignment",
        "bullish_alignment_label": "Bullish Alignment",
        "yes_label": "Yes",
        "no_label": "No",
        "trend_strength_label": "Trend Strength",
        "price_metrics_label": "Price Metrics",
        "ma5_label": "MA5",
        "ma10_label": "MA10",
        "ma20_label": "MA20",
        "bias_ma5_label": "Bias (MA5)",
        "support_level_label": "Support",
        "resistance_level_label": "Resistance",
        "chip_label": "Chip Structure",
        "battle_plan_heading": "Battle Plan",
        "ideal_buy_label": "Ideal Entry",
        "secondary_buy_label": "Secondary Entry",
        "stop_loss_label": "Stop Loss",
        "take_profit_label": "Target",
        "suggested_position_label": "Position Size",
        "entry_plan_label": "Entry Plan",
        "risk_control_label": "Risk Control",
        "checklist_heading": "Checklist",
        "failed_checks_heading": "Failed Checks",
        "history_compare_heading": "Historical Signal Comparison",
        "time_label": "Time",
        "score_label": "Score",
        "advice_label": "Advice",
        "trend_label": "Trend",
        "generated_at_label": "Generated At",
        "report_time_label": "Generated",
        "no_results": "No analysis results",
        "report_title": "Stock Analysis Report",
        "avg_score_label": "Avg Score",
        "action_points_heading": "Action Levels",
        "position_advice_heading": "Position Advice",
        "analysis_model_label": "Model",
        "not_investment_advice": "AI-generated content for reference only. Not investment advice.",
        "details_report_hint": "See detailed report:",
    },
}


def normalize_report_language(value: Optional[str], default: str = "zh") -> str:
    """Normalize report language to a supported short code."""
    candidate = (value or default).strip().lower().replace(" ", "_")
    candidate = _REPORT_LANGUAGE_ALIASES.get(candidate, candidate)
    if candidate in SUPPORTED_REPORT_LANGUAGES:
        return candidate
    return default


def is_supported_report_language_value(value: Optional[str]) -> bool:
    """Return whether the raw value is a supported language code or alias."""
    candidate = (value or "").strip().lower().replace(" ", "_")
    if not candidate:
        return False
    return candidate in SUPPORTED_REPORT_LANGUAGES or candidate in _REPORT_LANGUAGE_ALIASES


def get_report_labels(language: Optional[str]) -> Dict[str, str]:
    """Return UI copy for the selected report language."""
    normalized = normalize_report_language(language)
    return _REPORT_LABELS[normalized]


def get_placeholder_text(language: Optional[str]) -> str:
    """Return placeholder text for missing localized content."""
    return _PLACEHOLDER_BY_LANGUAGE[normalize_report_language(language)]


def get_unknown_text(language: Optional[str]) -> str:
    """Return localized unknown text."""
    return _UNKNOWN_BY_LANGUAGE[normalize_report_language(language)]


def get_no_data_text(language: Optional[str]) -> str:
    """Return localized data unavailable text."""
    return _NO_DATA_BY_LANGUAGE[normalize_report_language(language)]


def _normalize_lookup_key(value: Any) -> str:
    return str(value or "").strip().lower().replace("_", " ").replace("-", " ")


def _iter_lookup_candidates(value: Any) -> list[str]:
    raw_text = str(value or "").strip()
    if not raw_text:
        return []

    candidates = [raw_text]
    for part in re.split(r"[/|,，、]+", raw_text):
        normalized = part.strip()
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    return candidates


def _canonicalize_lookup_value(value: Any, canonical_map: Dict[str, str]) -> Optional[str]:
    for candidate in _iter_lookup_candidates(value):
        canonical = canonical_map.get(_normalize_lookup_key(candidate))
        if canonical:
            return canonical
    return None


def _is_placeholder_stock_name(value: Any, code: Any = None) -> bool:
    text = str(value or "").strip()
    if not text:
        return True

    lowered = text.lower()
    if lowered in {"n/a", "na", "none", "null", "unknown"}:
        return True
    if text in {"-", "—", "unknown", "to be filled"}:
        return True

    code_text = str(code or "").strip()
    if code_text and lowered == code_text.lower():
        return True

    return text.startswith("stock")


def _translate_from_map(
    value: Any,
    language: Optional[str],
    *,
    canonical_map: Dict[str, str],
    translations: Dict[str, Dict[str, str]],
) -> str:
    normalized_language = normalize_report_language(language)
    raw_text = str(value or "").strip()
    if not raw_text:
        return raw_text

    canonical = _canonicalize_lookup_value(raw_text, canonical_map)
    if canonical:
        return translations[canonical][normalized_language]
    return raw_text


def localize_operation_advice(value: Any, language: Optional[str]) -> str:
    """Translate operation advice between Chinese and English when recognized."""
    return _translate_from_map(
        value,
        language,
        canonical_map=_OPERATION_ADVICE_CANONICAL_MAP,
        translations=_OPERATION_ADVICE_TRANSLATIONS,
    )


def localize_trend_prediction(value: Any, language: Optional[str]) -> str:
    """Translate trend prediction between Chinese and English when recognized."""
    return _translate_from_map(
        value,
        language,
        canonical_map=_TREND_PREDICTION_CANONICAL_MAP,
        translations=_TREND_PREDICTION_TRANSLATIONS,
    )


def localize_confidence_level(value: Any, language: Optional[str]) -> str:
    """Translate confidence level between Chinese and English when recognized."""
    return _translate_from_map(
        value,
        language,
        canonical_map=_CONFIDENCE_LEVEL_CANONICAL_MAP,
        translations=_CONFIDENCE_LEVEL_TRANSLATIONS,
    )


def localize_chip_health(value: Any, language: Optional[str]) -> str:
    """Translate chip health labels between Chinese and English when recognized."""
    return _translate_from_map(
        value,
        language,
        canonical_map=_CHIP_HEALTH_CANONICAL_MAP,
        translations=_CHIP_HEALTH_TRANSLATIONS,
    )


def localize_bias_status(value: Any, language: Optional[str]) -> str:
    """Translate price bias status labels between Chinese and English when recognized."""
    return _translate_from_map(
        value,
        language,
        canonical_map=_BIAS_STATUS_CANONICAL_MAP,
        translations=_BIAS_STATUS_TRANSLATIONS,
    )


def get_bias_status_emoji(value: Any) -> str:
    """Return the stable alert emoji for a localized or canonical bias status."""
    canonical = _canonicalize_lookup_value(value, _BIAS_STATUS_CANONICAL_MAP)
    if canonical == "safe":
        return "✅"
    if canonical == "caution":
        return "⚠️"
    return "🚨"


def infer_decision_type_from_advice(value: Any, default: str = "hold") -> str:
    """Infer buy/hold/sell from human-readable operation advice."""
    canonical = _canonicalize_lookup_value(value, _OPERATION_ADVICE_CANONICAL_MAP)
    if canonical in {"strong_buy", "buy"}:
        return "buy"
    if canonical in {"reduce", "sell", "strong_sell"}:
        return "sell"
    if canonical in {"hold", "watch"}:
        return "hold"
    return default


def get_signal_level(advice: Any, score: Any, language: Optional[str]) -> tuple[str, str, str]:
    """Return localized signal text, emoji, and stable color tag."""
    normalized_language = normalize_report_language(language)
    canonical = _canonicalize_lookup_value(advice, _OPERATION_ADVICE_CANONICAL_MAP)
    if canonical == "strong_buy":
        return (_OPERATION_ADVICE_TRANSLATIONS["strong_buy"][normalized_language], "💚", "strong_buy")
    if canonical == "buy":
        return (_OPERATION_ADVICE_TRANSLATIONS["buy"][normalized_language], "🟢", "buy")
    if canonical == "hold":
        return (_OPERATION_ADVICE_TRANSLATIONS["hold"][normalized_language], "🟡", "hold")
    if canonical == "watch":
        return (_OPERATION_ADVICE_TRANSLATIONS["watch"][normalized_language], "⚪", "watch")
    if canonical == "reduce":
        return (_OPERATION_ADVICE_TRANSLATIONS["reduce"][normalized_language], "🟠", "reduce")
    if canonical in {"sell", "strong_sell"}:
        return (_OPERATION_ADVICE_TRANSLATIONS["sell"][normalized_language], "🔴", "sell")

    try:
        numeric_score = int(float(score))
    except (TypeError, ValueError):
        numeric_score = 50

    if numeric_score >= 80:
        return (_OPERATION_ADVICE_TRANSLATIONS["strong_buy"][normalized_language], "💚", "strong_buy")
    if numeric_score >= 65:
        return (_OPERATION_ADVICE_TRANSLATIONS["buy"][normalized_language], "🟢", "buy")
    if numeric_score >= 55:
        return (_OPERATION_ADVICE_TRANSLATIONS["hold"][normalized_language], "🟡", "hold")
    if numeric_score >= 45:
        return (_OPERATION_ADVICE_TRANSLATIONS["watch"][normalized_language], "⚪", "watch")
    if numeric_score >= 35:
        return (_OPERATION_ADVICE_TRANSLATIONS["reduce"][normalized_language], "🟠", "reduce")
    return (_OPERATION_ADVICE_TRANSLATIONS["sell"][normalized_language], "🔴", "sell")


def get_localized_stock_name(value: Any, code: Any, language: Optional[str]) -> str:
    """Return a localized stock name placeholder when the original name is missing."""
    raw_text = str(value or "").strip()
    if not _is_placeholder_stock_name(raw_text, code):
        return raw_text
    return _GENERIC_STOCK_NAME_BY_LANGUAGE[normalize_report_language(language)]


def get_sentiment_label(score: int, language: Optional[str]) -> str:
    """Return localized sentiment label by score band."""
    normalized = normalize_report_language(language)
    if normalized == "en":
        if score >= 80:
            return "Very Bullish"
        if score >= 60:
            return "Bullish"
        if score >= 40:
            return "Neutral"
        if score >= 20:
            return "Bearish"
        return "Very Bearish"

    if score >= 80:
        return "extremelyoptimistic"
    if score >= 60:
        return "optimistic"
    if score >= 40:
        return "neutral"
    if score >= 20:
        return "pessimistic"
    return "extremelypessimistic"
