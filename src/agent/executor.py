# -*- coding: utf-8 -*-
"""
Agent Executor — ReAct loop with tool calling.

Orchestrates the LLM + tools interaction loop:
1. Build system prompt (persona + tools + skills)
2. Send to LLM with tool declarations
3. If tool_call → execute tool → feed result back
4. If text → parse as final answer
5. Loop until final answer or max_steps

The core execution loop is delegated to :mod:`src.agent.runner` so that
both the legacy single-agent path and future multi-agent runners share the
same implementation.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from src.agent.llm_adapter import LLMToolAdapter
from src.agent.runner import run_agent_loop, parse_dashboard_json
from src.agent.tools.registry import ToolRegistry
from src.report_language import normalize_report_language

logger = logging.getLogger(__name__)


# ============================================================
# Agent result
# ============================================================

@dataclass
class AgentResult:
    """Result from an agent execution run."""
    success: bool = False
    content: str = ""                          # final text answer from agent
    dashboard: Optional[Dict[str, Any]] = None  # parsed dashboard JSON
    tool_calls_log: List[Dict[str, Any]] = field(default_factory=list)  # execution trace
    total_steps: int = 0
    total_tokens: int = 0
    provider: str = ""
    model: str = ""                            # comma-separated models used (supports fallback)
    error: Optional[str] = None


# ============================================================
# System prompt builder
# ============================================================

AGENT_SYSTEM_PROMPT = """You must respond entirely in English. Do not use any Chinese characters anywhere in your response. All analysis, recommendations, labels, section headers, and explanations must be in English only.

youisonedigitfocused ontrendtrade A stocksinvestmentanalyzing Agent，havehasdatatoolandtradeskill，responsible forgeneratingspecializedindustry【decisiondashboard】analysis report。

## workprocess（muststrictbystageorderexecute，eachstageetctoolresultreturnafteragainenterbelowfirst stage）

**thefirst stage · quote/market datawithcandlestick**（firstfirstexecute）
- `get_realtime_quote` get realtimequote/market data
- `get_daily_history` get historicalcandlestick

**thesecond stage · technicalwithchip**（etcthefirst stageresultreturnafterexecute）
- `analyze_trend` gettechnical indicator
- `get_chip_distribution` get chip distribution

**thethird stage · intelligencesearch**（etcbeforetwo stagescompletedafterexecute）
- `search_stock_news` searchlatestcapitalmessage、reduce holdings、performanceforecastetcrisksignal

**thefourth stage · generatingreport**（alldatareadyafter，outputcompletedecisiondashboard JSON）

> ⚠️ eachstagetoolcallmustcompletereturnresultafter，only thencanenterbelowfirst stage。prohibitwillnotsame stagetoolmergingin the same call。
{default_skill_policy_section}

## rule

1. **mustcalltoolget realdata** — absolutelynotfabricatecountcharacter，alldatamustfromtoolreturnresult。
2. **system-izeanalyzing** — strictbyworkprocessminutestageexecute，eachstagecompletereturnafteragainenterbelowfirst stage，**prohibit**willnotsame stagetoolmergingin the same call。
3. **applytradeskill** — evaluationeachactivatedskillitemsitems，inreportinreflectskilldetermineresult。
4. **output format** — finalresponsemustisvaliddecisiondashboard JSON。
5. **riskpriority** — musttroubleshootrisk（shareholder reduction、performanceearly warning、regulatory issues）。
6. **toolfailedprocessing** — recordfailedreason，useexistingdatacontinuinganalyzing，notduplicatecallfailedtool。

{skills_section}

## output format：decisiondashboard JSON

youfinalresponsemustiswithbelowstructurevalid JSON object：

```json
{{
    "stock_name": "stockChinesename",
    "sentiment_score": 0-100integer,
    "trend_prediction": "strongly bullish/bullish/oscillation/bearish/strongly bearish",
    "operation_advice": "buy/add to position/hold/reduce position/sell/wait and see",
    "decision_type": "buy/hold/sell",
    "confidence_level": "high/in/low",
    "dashboard": {{
        "core_conclusion": {{
            "one_sentence": "one sentencecoreconclusion（30characterwithin）",
            "signal_type": "🟢buy signal/🟡hold and observe/🔴sell signal/⚠️riskWarning",
            "time_sensitivity": "immediatelyrowdynamic/todayin/within this week/noturgent",
            "position_advice": {{
                "no_position": "no positionerrecommended",
                "has_position": "holdingerrecommended"
            }}
        }},
        "data_perspective": {{
            "trend_status": {{"ma_alignment": "", "is_bullish": true, "trend_score": 0}},
            "price_position": {{"current_price": 0, "ma5": 0, "ma10": 0, "ma20": 0, "bias_ma5": 0, "bias_status": "", "support_level": 0, "resistance_level": 0}},
            "volume_analysis": {{"volume_ratio": 0, "volume_status": "", "turnover_rate": 0, "volume_meaning": ""}},
            "chip_structure": {{"profit_ratio": 0, "avg_cost": 0, "concentration": 0, "chip_health": ""}}
        }},
        "intelligence": {{
            "latest_news": "",
            "risk_alerts": [],
            "positive_catalysts": [],
            "earnings_outlook": "",
            "sentiment_summary": ""
        }},
        "battle_plan": {{
            "sniper_points": {{"ideal_buy": "", "secondary_buy": "", "stop_loss": "", "take_profit": ""}},
            "position_strategy": {{"suggested_position": "", "entry_plan": "", "risk_control": ""}},
            "action_checklist": []
        }}
    }},
    "analysis_summary": "100charactercompositeanalyzingsummary",
    "key_points": "3-5countcorekey point，comma-separated",
    "risk_warning": "riskTip",
    "buy_reason": "operation reason，referencetrading philosophy",
    "trend_analysis": "trendpatternanalyzing",
    "short_term_outlook": "short-term1-3dayoutlook",
    "medium_term_outlook": "medium-term1-2weekoutlook",
    "technical_analysis": "technicalscompositeanalyzing",
    "ma_analysis": "moving averagesystemanalyzing",
    "volume_analysis": "volumeanalyzing",
    "pattern_analysis": "candlestickpatternanalyzing",
    "fundamental_analysis": "fundamental analysis",
    "sector_position": "sectorindustry analysis",
    "company_highlights": "companyhighlight/risk",
    "news_summary": "newssummary",
    "market_sentiment": "market sentiment",
    "hot_topics": "relatedhotspot"
}}
```

## scorestandard

### strong buy（80-100minute）：
- ✅ long positionarrange：MA5 > MA10 > MA20
- ✅ lowBIAS ratio：<2%，best buy point
- ✅ volume contractionpullbackorvolume increasebreakout
- ✅ chipsetinhealth
- ✅ messageaspecthaspositive catalyst

### buy（60-79minute）：
- ✅ long positionarrangeorweaklong position
- ✅ BIAS ratio <5%
- ✅ volumenormal
- ⚪ allowoneitemtimesneeditemsitemsnotsatisfy

### wait and see（40-59minute）：
- ⚠️ BIAS ratio >5%（chasehighrisk）
- ⚠️ moving averageentangletrendnotclear
- ⚠️ hasriskevent

### sell/reduce position（0-39minute）：
- ❌ short positionarrange
- ❌ break belowMA20
- ❌ volume increasefalling
- ❌ major negative

## decisiondashboardcoreoriginalthen

1. **coreconclusionfirstrow**：one sentenceclarifythisbuythissell
2. **minuteholdingrecommended**：no positionerandholdingergivenotsamerecommended
3. **exactsnipepoint**：mustgiveoutspecificprice，notsayfuzzywords
4. **checklistvisualization**：use ✅⚠️❌ cleardeterminedisplayeachitemcheckresult
5. **riskpriority**：public sentimentinriskpointneedwaketargetout

{language_section}
"""

CHAT_SYSTEM_PROMPT = """You must respond entirely in English. Do not use any Chinese characters anywhere in your response. All analysis, recommendations, labels, section headers, and explanations must be in English only.

youisonedigitfocused ontrendtrade A stocksinvestmentanalyzing Agent，havehasdatatoolandtradeskill，responsible foransweruserstockinvestmentissue。

## analyzingworkprocess（muststrictbystageexecute，prohibitskip stepormergingstage）

whenuserqueryquestioncertainsupportstockwhen，mustbywithbelowfourcountstageordercalltool，eachstageetctoolresultallreturnafteragainenterbelowfirst stage：

**thefirst stage · quote/market datawithcandlestick**（mustfirstexecute）
- call `get_realtime_quote` get realtimequote/market dataandcurrentprice
- call `get_daily_history` getrecentperiodhistoricalcandlestickdata

**thesecond stage · technicalwithchip**（etcthefirst stageresultreturnafteragainexecute）
- call `analyze_trend` get MA/MACD/RSI etctechnical indicator
- call `get_chip_distribution` get chip distributionstructure

**thethird stage · intelligencesearch**（etcbeforetwo stagescompletedafteragainexecute）
- call `search_stock_news` searchlatestnewsannouncement、reduce holdings、performanceforecastetcrisksignal

**thefourth stage · compositeanalyzing**（alltooldatareadyaftergeneratinganswer）
- based onabove-mentionedrealdata，combineactivatedskillproceedcompositeanalyze，outputinvestmentrecommended

> ⚠️ prohibitwillnotsame stagetoolmergingin the same call（for exampleprohibitintheoncecallinsimultaneouslyrequestquote/market data、technical indicatorandnews）。
{default_skill_policy_section}

## rule

1. **mustcalltoolget realdata** — absolutelynotfabricatecountcharacter，alldatamustfromtoolreturnresult。
2. **applytradeskill** — evaluationeachactivatedskillitemsitems，inanswerinreflectskilldetermineresult。
3. **selfbyconversation** — based onuserissue，selfbyorganizelanguageanswer，no needoutput JSON。
4. **riskpriority** — musttroubleshootrisk（shareholder reduction、performanceearly warning、regulatory issues）。
5. **toolfailedprocessing** — recordfailedreason，useexistingdatacontinuinganalyzing，notduplicatecallfailedtool。

{skills_section}
{language_section}
"""


def _build_language_section(report_language: str, *, chat_mode: bool = False) -> str:
    """Build output-language guidance for the agent prompt."""
    normalized = normalize_report_language(report_language)
    if chat_mode:
        if normalized == "en":
            return """
## Output Language

- Reply in English.
- If you output JSON, keep the keys unchanged and write every human-readable value in English.
"""
        return """
## outputlanguage

- defaultuseChineseanswer。
- ifoutput JSON，key names unchanged，all facingusertextvalueuseChinese。
"""

    if normalized == "en":
        return """
## Output Language

- Keep every JSON key unchanged.
- `decision_type` must remain `buy|hold|sell`.
- All human-readable JSON values must be written in English.
- This includes `stock_name`, `trend_prediction`, `operation_advice`, `confidence_level`, all dashboard text, checklist items, and summaries.
"""

    return """
## outputlanguage

- all JSON key names unchanged。
- `decision_type` mustmaintainas `buy|hold|sell`。
- all facinguserpersonclasscanreadtextvaluemustuseChinese。
"""


# ============================================================
# Agent Executor
# ============================================================

class AgentExecutor:
    """ReAct agent loop with tool calling.

    Usage::

        executor = AgentExecutor(tool_registry, llm_adapter)
        result = executor.run("Analyze stock 600519")
    """

    def __init__(
        self,
        tool_registry: ToolRegistry,
        llm_adapter: LLMToolAdapter,
        skill_instructions: str = "",
        default_skill_policy: str = "",
        max_steps: int = 10,
        timeout_seconds: Optional[float] = None,
    ):
        self.tool_registry = tool_registry
        self.llm_adapter = llm_adapter
        self.skill_instructions = skill_instructions
        self.default_skill_policy = default_skill_policy
        self.max_steps = max_steps
        self.timeout_seconds = timeout_seconds

    def run(self, task: str, context: Optional[Dict[str, Any]] = None) -> AgentResult:
        """Execute the agent loop for a given task.

        Args:
            task: The user task / analysis request.
            context: Optional context dict (e.g., {"stock_code": "600519"}).

        Returns:
            AgentResult with parsed dashboard or error.
        """
        # Build system prompt with skills
        skills_section = ""
        if self.skill_instructions:
            skills_section = f"## activatedtradeskill\n\n{self.skill_instructions}"
        default_skill_policy_section = ""
        if self.default_skill_policy:
            default_skill_policy_section = f"\n{self.default_skill_policy}\n"
        report_language = normalize_report_language((context or {}).get("report_language", "zh"))
        system_prompt = AGENT_SYSTEM_PROMPT.format(
            default_skill_policy_section=default_skill_policy_section,
            skills_section=skills_section,
            language_section=_build_language_section(report_language),
        )

        # Build tool declarations in OpenAI format (litellm handles all providers)
        tool_decls = self.tool_registry.to_openai_tools()

        # Initialize conversation
        messages: List[Dict[str, Any]] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": self._build_user_message(task, context)},
        ]

        return self._run_loop(messages, tool_decls, parse_dashboard=True)

    def chat(self, message: str, session_id: str, progress_callback: Optional[Callable] = None, context: Optional[Dict[str, Any]] = None) -> AgentResult:
        """Execute the agent loop for a free-form chat message.

        Args:
            message: The user's chat message.
            session_id: The conversation session ID.
            progress_callback: Optional callback for streaming progress events.
            context: Optional context dict from previous analysis for data reuse.

        Returns:
            AgentResult with the text response.
        """
        from src.agent.conversation import conversation_manager

        # Build system prompt with skills
        skills_section = ""
        if self.skill_instructions:
            skills_section = f"## activatedtradeskill\n\n{self.skill_instructions}"
        default_skill_policy_section = ""
        if self.default_skill_policy:
            default_skill_policy_section = f"\n{self.default_skill_policy}\n"
        report_language = normalize_report_language((context or {}).get("report_language", "zh"))
        system_prompt = CHAT_SYSTEM_PROMPT.format(
            default_skill_policy_section=default_skill_policy_section,
            skills_section=skills_section,
            language_section=_build_language_section(report_language, chat_mode=True),
        )

        # Build tool declarations in OpenAI format (litellm handles all providers)
        tool_decls = self.tool_registry.to_openai_tools()

        # Get conversation history
        session = conversation_manager.get_or_create(session_id)
        history = session.get_history()

        # Initialize conversation
        messages: List[Dict[str, Any]] = [
            {"role": "system", "content": system_prompt},
        ]
        messages.extend(history)

        # Inject previous analysis context if provided (data reuse from report follow-up)
        if context:
            context_parts = []
            if context.get("stock_code"):
                context_parts.append(f"stock code: {context['stock_code']}")
            if context.get("stock_name"):
                context_parts.append(f"stockname: {context['stock_name']}")
            if context.get("previous_price"):
                context_parts.append(f"last timeanalyzingprice: {context['previous_price']}")
            if context.get("previous_change_pct"):
                context_parts.append(f"last timeprice change percentage: {context['previous_change_pct']}%")
            if context.get("previous_analysis_summary"):
                summary = context["previous_analysis_summary"]
                summary_text = json.dumps(summary, ensure_ascii=False) if isinstance(summary, dict) else str(summary)
                context_parts.append(f"last timeanalyzingsummary:\n{summary_text}")
            if context.get("previous_strategy"):
                strategy = context["previous_strategy"]
                strategy_text = json.dumps(strategy, ensure_ascii=False) if isinstance(strategy, dict) else str(strategy)
                context_parts.append(f"last timestrategyanalyzing:\n{strategy_text}")
            if context_parts:
                context_msg = "[systemprovidehistoricalanalyzingcontext，canprovidereferencetocompared to]\n" + "\n".join(context_parts)
                messages.append({"role": "user", "content": context_msg})
                messages.append({"role": "assistant", "content": "good，Ialreadysolvethisstockhistoricalanalyzingdata。pleasetellIyouwhat to solve？"})

        messages.append({"role": "user", "content": message})

        # Persist the user turn immediately so the session appears in history during processing
        conversation_manager.add_message(session_id, "user", message)

        result = self._run_loop(messages, tool_decls, parse_dashboard=False, progress_callback=progress_callback)

        # Persist assistant reply (or error note) for context continuity
        if result.success:
            conversation_manager.add_message(session_id, "assistant", result.content)
        else:
            error_note = f"[analyzingfailed] {result.error or 'unknownerror'}"
            conversation_manager.add_message(session_id, "assistant", error_note)

        return result

    def _run_loop(self, messages: List[Dict[str, Any]], tool_decls: List[Dict[str, Any]], parse_dashboard: bool, progress_callback: Optional[Callable] = None) -> AgentResult:
        """Delegate to the shared runner and adapt the result.

        This preserves the exact same observable behaviour as the original
        inline implementation while sharing the single authoritative loop
        in :mod:`src.agent.runner`.
        """
        loop_result = run_agent_loop(
            messages=messages,
            tool_registry=self.tool_registry,
            llm_adapter=self.llm_adapter,
            max_steps=self.max_steps,
            progress_callback=progress_callback,
            max_wall_clock_seconds=self.timeout_seconds,
        )

        model_str = loop_result.model

        if parse_dashboard and loop_result.success:
            dashboard = parse_dashboard_json(loop_result.content)
            return AgentResult(
                success=dashboard is not None,
                content=loop_result.content,
                dashboard=dashboard,
                tool_calls_log=loop_result.tool_calls_log,
                total_steps=loop_result.total_steps,
                total_tokens=loop_result.total_tokens,
                provider=loop_result.provider,
                model=model_str,
                error=None if dashboard else "Failed to parse dashboard JSON from agent response",
            )

        return AgentResult(
            success=loop_result.success,
            content=loop_result.content,
            dashboard=None,
            tool_calls_log=loop_result.tool_calls_log,
            total_steps=loop_result.total_steps,
            total_tokens=loop_result.total_tokens,
            provider=loop_result.provider,
            model=model_str,
            error=loop_result.error,
        )

    def _build_user_message(self, task: str, context: Optional[Dict[str, Any]] = None) -> str:
        """Build the initial user message."""
        parts = [task]
        if context:
            report_language = normalize_report_language(context.get("report_language", "zh"))
            if context.get("stock_code"):
                parts.append(f"\nstock code: {context['stock_code']}")
            if context.get("report_type"):
                parts.append(f"report type: {context['report_type']}")
            if report_language == "en":
                parts.append("outputlanguage: English（all JSON key names unchanged，all facingusertextvalueuseEnglish）")
            else:
                parts.append("outputlanguage: Chinese（all JSON key names unchanged，all facingusertextvalueuseChinese）")

            # Inject pre-fetched context data to avoid redundant fetches
            if context.get("realtime_quote"):
                parts.append(f"\n[systemalreadygetrealtimequote/market data]\n{json.dumps(context['realtime_quote'], ensure_ascii=False)}")
            if context.get("chip_distribution"):
                parts.append(f"\n[systemalreadygetchip distribution]\n{json.dumps(context['chip_distribution'], ensure_ascii=False)}")
            if context.get("news_context"):
                parts.append(f"\n[systemalreadygetnewswithpublic sentimentintelligence]\n{context['news_context']}")

        parts.append("\nplease useavailabletoolgetmissingdata（e.g.historicalcandlestick、newsetc），thenafterwithdecisiondashboard JSON formatoutputanalysis result。")
        return "\n".join(parts)
