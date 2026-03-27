# -*- coding: utf-8 -*-
"""Market strategy blueprints for CN/US daily market recap."""

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class StrategyDimension:
    """Single strategy dimension used by market recap prompts."""

    name: str
    objective: str
    checkpoints: List[str]


@dataclass(frozen=True)
class MarketStrategyBlueprint:
    """Region specific market strategy blueprint."""

    region: str
    title: str
    positioning: str
    principles: List[str]
    dimensions: List[StrategyDimension]
    action_framework: List[str]

    def to_prompt_block(self) -> str:
        """Render blueprint as prompt instructions."""
        principles_text = "\n".join([f"- {item}" for item in self.principles])
        action_text = "\n".join([f"- {item}" for item in self.action_framework])

        dims = []
        for dim in self.dimensions:
            checkpoints = "\n".join([f"  - {cp}" for cp in dim.checkpoints])
            dims.append(f"- {dim.name}: {dim.objective}\n{checkpoints}")
        dimensions_text = "\n".join(dims)

        return (
            f"## Strategy Blueprint: {self.title}\n"
            f"{self.positioning}\n\n"
            f"### Strategy Principles\n{principles_text}\n\n"
            f"### Analysis Dimensions\n{dimensions_text}\n\n"
            f"### Action Framework\n{action_text}"
        )

    def to_markdown_block(self) -> str:
        """Render blueprint as markdown section for template fallback report."""
        dims = "\n".join([f"- **{dim.name}**: {dim.objective}" for dim in self.dimensions])
        section_title = "### six、strategyframework" if self.region == "cn" else "### VI. Strategy Framework"
        return f"{section_title}\n{dims}\n"


CN_BLUEPRINT = MarketStrategyBlueprint(
    region="cn",
    title="A-sharemarket three-stage review strategy",
    positioning="focusindextrend、capitalgame theorywithsectorrotation，formbecometimesdaytradeplan。",
    principles=[
        "firstviewindexdirection，againviewvolumestructure，mostafterviewsectorcontinuous-ness。",
        "conclusionmustmappingtoposition、rhythmwithriskcontroldynamicact as。",
        "determineusetodaydatawithrecent3daynews，notspeculatenotverificationinfo。",
    ],
    dimensions=[
        StrategyDimension(
            name="trendstructure",
            objective="determinemarketatrising、oscillationstillisdefensivestage。",
            checkpoints=["Shanghai Composite/Shenzhen/ChiNextwhethersameto", "volume increaserisingorvolume contractionfallingwhetherbecomeestablish", "keysupportresistancewhetherbybreakout"],
        ),
        StrategyDimension(
            name="capitalsentiment",
            objective="identifyshortlineriskbiasedgoodwithsentimenttemperature。",
            checkpoints=["price changecountcountwithprice changestopstructure", "trading amountwhetherexpansion", "highdigitstockswhetherappearminuteambiguous"],
        ),
        StrategyDimension(
            name="mainlinesector",
            objective="mentionrefinecantrademainlinewithavoiddirection。",
            checkpoints=["leading gainersectorwhetherpossesseventurge-ize", "sectorinternalwhetherhasleading stockwithdynamic", "leading losersectorwhetherexpandscattered"],
        ),
    ],
    action_framework=[
        "offensive：indextotaloscillate upwardrow + trading amountputlarge + mainlinestrong-ize。",
        "averagebalance：indexminute-izeorvolume contractionoscillation，controlpositionandwaitingconfirm。",
        "defensive：indexweakening + leading loserexpandscattered，priorityrisk controlwithreduce position。",
    ],
)

US_BLUEPRINT = MarketStrategyBlueprint(
    region="us",
    title="US Market Regime Strategy",
    positioning="Focus on index trend, macro narrative, and sector rotation to define next-session risk posture.",
    principles=[
        "Read market regime from S&P 500, Nasdaq, and Dow alignment first.",
        "Separate beta move from theme-driven alpha rotation.",
        "Translate recap into actionable risk-on/risk-off stance with clear invalidation points.",
    ],
    dimensions=[
        StrategyDimension(
            name="Trend Regime",
            objective="Classify the market as momentum, range, or risk-off.",
            checkpoints=[
                "Are SPX/NDX/DJI directionally aligned",
                "Did volume confirm the move",
                "Are key index levels reclaimed or lost",
            ],
        ),
        StrategyDimension(
            name="Macro & Flows",
            objective="Map policy/rates narrative into equity risk appetite.",
            checkpoints=[
                "Treasury yield and USD implications",
                "Breadth and leadership concentration",
                "Defensive vs growth factor rotation",
            ],
        ),
        StrategyDimension(
            name="Sector Themes",
            objective="Identify persistent leaders and vulnerable laggards.",
            checkpoints=[
                "AI/semiconductor/software trend persistence",
                "Energy/financials sensitivity to macro data",
                "Volatility signals from VIX and large-cap earnings",
            ],
        ),
    ],
    action_framework=[
        "Risk-on: broad index breakout with expanding participation.",
        "Neutral: mixed index signals; focus on selective relative strength.",
        "Risk-off: failed breakouts and rising volatility; prioritize capital preservation.",
    ],
)


GLOBAL_BLUEPRINT = MarketStrategyBlueprint(
    region="global",
    title="Global Macro Thematic Portfolio Strategy",
    positioning=(
        "European-based macro portfolio across 5 thematic buckets "
        "(Hard Assets, Energy/Nuclear, Defense Supply Chain, Consumer Stress, Geopolitical). "
        "Focus on cross-asset regime, thesis health, and relative-value pair signals."
    ),
    principles=[
        "Read the macro regime from cross-asset signals: equities, bonds, commodities, FX, VIX.",
        "Evaluate each thesis (Iran, Stagflation, Dalio Stage 6) for confirming or disconfirming evidence.",
        "Translate into bucket-level tilt: overweight, neutral, or underweight.",
        "Pair trade signals override single-name views when z-scores are extreme.",
    ],
    dimensions=[
        StrategyDimension(
            name="Cross-Asset Regime",
            objective="Classify macro environment as risk-on, risk-off, or transitional.",
            checkpoints=[
                "SPX/STOXX50/DAX directional alignment",
                "Gold vs TLT vs USD relative moves",
                "VIX level and term structure",
                "Brent crude direction and energy complex",
            ],
        ),
        StrategyDimension(
            name="Thesis Health",
            objective="Check whether each active thesis is strengthening, intact, or weakening.",
            checkpoints=[
                "Iran settlement proxies: Brent, KSA, UAE, defense names",
                "Stagflation proxies: Gold, TIPS, BTC, discount retail vs growth",
                "Dalio Stage 6 proxies: defense spending, gold, VIX, EM divergence",
            ],
        ),
        StrategyDimension(
            name="Pair Trade Signals",
            objective="Flag actionable spread dislocations in tracked pairs.",
            checkpoints=[
                "Spread z-scores exceeding +/- 2 standard deviations",
                "Momentum divergence between long and short legs",
                "Spread direction (widening vs narrowing) trend",
            ],
        ),
    ],
    action_framework=[
        "Risk-on: broad equity strength, VIX below 18, thesis-aligned momentum — full allocation.",
        "Neutral: mixed signals, thesis intact but not confirming — hold positions, tighten stops.",
        "Risk-off: VIX spike, thesis disconfirmation, spread reversals — reduce exposure, hedge.",
    ],
)


def get_market_strategy_blueprint(region: str) -> MarketStrategyBlueprint:
    """Return strategy blueprint by market region."""
    if region in ("global", "eu"):
        return GLOBAL_BLUEPRINT
    return US_BLUEPRINT if region == "us" else CN_BLUEPRINT
