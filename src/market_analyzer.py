# -*- coding: utf-8 -*-
"""
===================================
Market Review Analysis Module
===================================

Responsibilities:
1. Fetch market index data (Shanghai, Shenzhen, ChiNext)
2. Search market news for review intelligence
3. Use large model to generate daily market review report
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List

import pandas as pd

from src.config import get_config
from src.search_service import SearchService
from src.core.market_profile import get_profile, MarketProfile
from src.core.market_strategy import get_market_strategy_blueprint
from data_provider.base import DataFetcherManager

logger = logging.getLogger(__name__)


@dataclass
class MarketIndex:
    """Market index data"""
    code: str                    # Index code
    name: str                    # Index name
    current: float = 0.0         # Current level
    change: float = 0.0          # Change points
    change_pct: float = 0.0      # Change percentage (%)
    open: float = 0.0            # Open level
    high: float = 0.0            # High level
    low: float = 0.0             # Low level
    prev_close: float = 0.0      # Previous close level
    volume: float = 0.0          # Volume (lots)
    amount: float = 0.0          # Amount (CNY)
    amplitude: float = 0.0       # Amplitude (%)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'code': self.code,
            'name': self.name,
            'current': self.current,
            'change': self.change,
            'change_pct': self.change_pct,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'volume': self.volume,
            'amount': self.amount,
            'amplitude': self.amplitude,
        }


@dataclass
class MarketOverview:
    """Market overview data"""
    date: str                           # Date
    indices: List[MarketIndex] = field(default_factory=list)  # Major indices
    up_count: int = 0                   # Number of gainers
    down_count: int = 0                 # Number of losers
    flat_count: int = 0                 # Number flat
    limit_up_count: int = 0             # Number limit-up
    limit_down_count: int = 0           # Number limit-down
    total_amount: float = 0.0           # Total market amount (B CNY)
    # north_flow: float = 0.0           # Northbound net inflow (B CNY) - deprecated, API unavailable

    # Sector rankings
    top_sectors: List[Dict] = field(default_factory=list)     # Top 5 gaining sectors
    bottom_sectors: List[Dict] = field(default_factory=list)  # Top 5 losing sectors


class MarketAnalyzer:
    """
    Market Review Analyzer

    Features:
    1. Fetch realtime market index quotes
    2. Get market advance/decline statistics
    3. Get sector performance rankings
    4. Search market news
    5. Generate market review report
    """

    def __init__(
        self,
        search_service: Optional[SearchService] = None,
        analyzer=None,
        region: str = "cn",
    ):
        """
        Initialize market analyzer

        Args:
            search_service: Search service instance
            analyzer: AI analyzer instance (for calling LLM)
            region: Market region cn=A-share us=US stock
        """
        self.config = get_config()
        self.search_service = search_service
        self.analyzer = analyzer
        self.data_manager = DataFetcherManager()
        self.region = region if region in ("cn", "us", "global", "eu") else "cn"
        self.profile: MarketProfile = get_profile(self.region)
        self.strategy = get_market_strategy_blueprint(self.region)

    def get_market_overview(self) -> MarketOverview:
        """
        Get market overview data

        Returns:
            MarketOverview: Market overview data object
        """
        today = datetime.now().strftime('%Y-%m-%d')
        overview = MarketOverview(date=today)

        # 1. Get major index quotes (switch between A-share/US stock by region)
        overview.indices = self._get_main_indices()

        # 2. Get advance/decline statistics (available for A-share, no equivalent for US)
        if self.profile.has_market_stats:
            self._get_market_statistics(overview)

        # 3. Get sector rankings (available for A-share, not yet for US)
        if self.profile.has_sector_rankings:
            self._get_sector_rankings(overview)

        # 4. Get northbound flow (optional)
        # self._get_north_flow(overview)

        return overview

    
    def _get_main_indices(self) -> List[MarketIndex]:
        """Fetch realtime quotes for major indices"""
        indices = []

        try:
            logger.info("[Market] Fetching realtime quotes for major indices...")

            # Use DataFetcherManager to fetch index quotes (switch by region)
            data_list = self.data_manager.get_main_indices(region=self.region)

            if data_list:
                for item in data_list:
                    index = MarketIndex(
                        code=item['code'],
                        name=item['name'],
                        current=item['current'],
                        change=item['change'],
                        change_pct=item['change_pct'],
                        open=item['open'],
                        high=item['high'],
                        low=item['low'],
                        prev_close=item['prev_close'],
                        volume=item['volume'],
                        amount=item['amount'],
                        amplitude=item['amplitude']
                    )
                    indices.append(index)

            if not indices:
                logger.warning("[Market] All quote data sources failed, will rely on news search for analysis")
            else:
                logger.info(f"[Market] Fetched {len(indices)} index quotes")

        except Exception as e:
            logger.error(f"[Market] Failed to fetch index quotes: {e}")

        return indices

    def _get_market_statistics(self, overview: MarketOverview):
        """Fetch market advance/decline statistics"""
        try:
            logger.info("[Market] Fetching market advance/decline statistics...")

            stats = self.data_manager.get_market_stats()

            if stats:
                overview.up_count = stats.get('up_count', 0)
                overview.down_count = stats.get('down_count', 0)
                overview.flat_count = stats.get('flat_count', 0)
                overview.limit_up_count = stats.get('limit_up_count', 0)
                overview.limit_down_count = stats.get('limit_down_count', 0)
                overview.total_amount = stats.get('total_amount', 0.0)

                logger.info(f"[Market] Up:{overview.up_count} Down:{overview.down_count} Flat:{overview.flat_count} "
                          f"LimitUp:{overview.limit_up_count} LimitDown:{overview.limit_down_count} "
                          f"Amount:{overview.total_amount:.0f}B")

        except Exception as e:
            logger.error(f"[Market] Failed to fetch advance/decline statistics: {e}")

    def _get_sector_rankings(self, overview: MarketOverview):
        """Fetch sector performance rankings"""
        try:
            logger.info("[Market] Fetching sector performance rankings...")

            top_sectors, bottom_sectors = self.data_manager.get_sector_rankings(5)

            if top_sectors or bottom_sectors:
                overview.top_sectors = top_sectors
                overview.bottom_sectors = bottom_sectors

                logger.info(f"[Market] Top gaining sectors: {[s['name'] for s in overview.top_sectors]}")
                logger.info(f"[Market] Top losing sectors: {[s['name'] for s in overview.bottom_sectors]}")

        except Exception as e:
            logger.error(f"[Market] Failed to fetch sector rankings: {e}")
    
    # def _get_north_flow(self, overview: MarketOverview):
    #     """Fetch northbound fund inflow"""
    #     try:
    #         logger.info("[Market] Fetching northbound funds...")
    #
    #         # Fetch northbound fund data
    #         df = ak.stock_hsgt_north_net_flow_in_em(symbol="north")
    #
    #         if df is not None and not df.empty:
    #             # Get latest record
    #             latest = df.iloc[-1]
    #             if 'daily_net_inflow' in df.columns:
    #                 overview.north_flow = float(latest['daily_net_inflow']) / 1e8  # Convert to B CNY
    #             elif 'net_inflow' in df.columns:
    #                 overview.north_flow = float(latest['net_inflow']) / 1e8
    #
    #             logger.info(f"[Market] Northbound net inflow: {overview.north_flow:.2f}B")
    #
    #     except Exception as e:
    #         logger.warning(f"[Market] Failed to fetch northbound funds: {e}")
    
    def search_market_news(self) -> List[Dict]:
        """
        Search market news

        Returns:
            News list
        """
        if not self.search_service:
            logger.warning("[Market] Search service not configured, skipping news search")
            return []

        all_news = []

        # Use different news search queries by region
        search_queries = self.profile.news_queries

        try:
            logger.info("[Market] Starting market news search...")

            # Set search context name by region to avoid US stock searches being interpreted in A-share context
            market_name = "market" if self.region == "cn" else "US market"
            for query in search_queries:
                response = self.search_service.search_stock_news(
                    stock_code="market",
                    stock_name=market_name,
                    max_results=3,
                    focus_keywords=query.split()
                )
                if response and response.results:
                    all_news.extend(response.results)
                    logger.info(f"[Market] Query '{query}' returned {len(response.results)} results")

            logger.info(f"[Market] Total {len(all_news)} market news items fetched")

        except Exception as e:
            logger.error(f"[Market] Failed to search market news: {e}")

        return all_news
    
    def generate_market_review(self, overview: MarketOverview, news: List) -> str:
        """
        Use large model to generate market review report

        Args:
            overview: Market overview data
            news: Market news list (list of SearchResult objects)

        Returns:
            Market review report text
        """
        if not self.analyzer or not self.analyzer.is_available():
            logger.warning("[Market] AI analyzer not configured or unavailable, using template report")
            return self._generate_template_review(overview, news)

        # Build Prompt
        prompt = self._build_review_prompt(overview, news)

        logger.info("[Market] Calling large model to generate review report...")
        # Use the public generate_text() entry point — never access private analyzer attributes.
        review = self.analyzer.generate_text(prompt, max_tokens=2048, temperature=0.7)

        if review:
            logger.info("[Market] Review report generated successfully, length: %d chars", len(review))
            # Inject structured data tables into LLM prose sections
            return self._inject_data_into_review(review, overview)
        else:
            logger.warning("[Market] Large model returned empty, using template report")
            return self._generate_template_review(overview, news)
    
    def _inject_data_into_review(self, review: str, overview: MarketOverview) -> str:
        """Inject structured data tables into the corresponding LLM prose sections."""
        import re

        # Build data blocks
        stats_block = self._build_stats_block(overview)
        indices_block = self._build_indices_block(overview)
        sector_block = self._build_sector_block(overview)

        if self.region in ("global", "eu"):
            # Global template uses different section names. Prepend the indices
            # dashboard block right after the top-level ## heading so it appears
            # at the top of the report.
            if indices_block:
                first_section = re.search(r'\n###\s', review)
                if first_section:
                    pos = first_section.start()
                    review = review[:pos].rstrip() + '\n\n' + indices_block + '\n\n' + review[pos:].lstrip('\n')
                else:
                    review = indices_block + '\n\n' + review
            return review

        # Inject market stats after "### 1. Market Summary" section (before next ###)
        if stats_block:
            review = self._insert_after_section(review, r'###\s*1[.、]\s*Market Summary', stats_block)

        # Inject indices table after "### 2. Index Commentary" section
        if indices_block:
            review = self._insert_after_section(review, r'###\s*2[.、]\s*Index Commentary', indices_block)

        # Inject sector rankings after "### 4. Hot Sectors" section
        if sector_block:
            review = self._insert_after_section(review, r'###\s*4[.、]\s*(?:Hot Sectors|Sector)', sector_block)

        return review

    @staticmethod
    def _insert_after_section(text: str, heading_pattern: str, block: str) -> str:
        """Insert a data block at the end of a markdown section (before the next ### heading)."""
        import re
        # Find the heading
        match = re.search(heading_pattern, text)
        if not match:
            return text
        start = match.end()
        # Find the next ### heading after this one
        next_heading = re.search(r'\n###\s', text[start:])
        if next_heading:
            insert_pos = start + next_heading.start()
        else:
            # No next heading — append at end
            insert_pos = len(text)
        # Insert the block before the next heading, with spacing
        return text[:insert_pos].rstrip() + '\n\n' + block + '\n\n' + text[insert_pos:].lstrip('\n')

    @property
    def _amount_unit(self) -> str:
        """Return the amount unit label based on region."""
        if self.region in ("global", "eu", "us"):
            return "B"
        return "B CNY"

    def _build_stats_block(self, overview: MarketOverview) -> str:
        """Build market statistics block."""
        has_stats = overview.up_count or overview.down_count or overview.total_amount
        if not has_stats:
            return ""
        lines = [
            f"> 📈 Up **{overview.up_count}** / Down **{overview.down_count}** / "
            f"Flat **{overview.flat_count}** | "
            f"LimitUp **{overview.limit_up_count}** / LimitDown **{overview.limit_down_count}** | "
            f"Amount **{overview.total_amount:.0f}** {self._amount_unit}"
        ]
        return "\n".join(lines)

    def _build_indices_block(self, overview: MarketOverview) -> str:
        """Build index quote table (without amplitude)"""
        if not overview.indices:
            return ""
        lines = [
            f"| Index | Latest | Change | Amount ({self._amount_unit}) |",
            "|-------|--------|--------|----------------|"]
        for idx in overview.indices:
            arrow = "🔴" if idx.change_pct < 0 else "🟢" if idx.change_pct > 0 else "⚪"
            amount_raw = idx.amount or 0.0
            if amount_raw == 0.0:
                # Yahoo Finance does not provide amount data, display N/A to avoid misleading
                amount_str = "N/A"
            elif amount_raw > 1e6:
                amount_str = f"{amount_raw / 1e8:.0f}"
            else:
                amount_str = f"{amount_raw:.0f}"
            lines.append(f"| {idx.name} | {idx.current:.2f} | {arrow} {idx.change_pct:+.2f}% | {amount_str} |")
        return "\n".join(lines)

    def _build_sector_block(self, overview: MarketOverview) -> str:
        """Build sector ranking block."""
        if not overview.top_sectors and not overview.bottom_sectors:
            return ""
        lines = []
        if overview.top_sectors:
            top = " | ".join(
                [f"**{s['name']}**({s['change_pct']:+.2f}%)" for s in overview.top_sectors[:5]]
            )
            lines.append(f"> 🔥 Leading gainers: {top}")
        if overview.bottom_sectors:
            bot = " | ".join(
                [f"**{s['name']}**({s['change_pct']:+.2f}%)" for s in overview.bottom_sectors[:5]]
            )
            lines.append(f"> 💧 Leading losers: {bot}")
        return "\n".join(lines)

    def _build_review_prompt(self, overview: MarketOverview, news: List) -> str:
        """Build review report Prompt"""
        # Index quote info (concise format, no emoji)
        indices_text = ""
        for idx in overview.indices:
            direction = "↑" if idx.change_pct > 0 else "↓" if idx.change_pct < 0 else "-"
            indices_text += f"- {idx.name}: {idx.current:.2f} ({direction}{abs(idx.change_pct):.2f}%)\n"
        
        # Sector info
        top_sectors_text = ", ".join([f"{s['name']}({s['change_pct']:+.2f}%)" for s in overview.top_sectors[:3]])
        bottom_sectors_text = ", ".join([f"{s['name']}({s['change_pct']:+.2f}%)" for s in overview.bottom_sectors[:3]])

        # News info - supports SearchResult objects or dicts
        news_text = ""
        for i, n in enumerate(news[:6], 1):
            # Compatible with SearchResult objects and dicts
            if hasattr(n, 'title'):
                title = n.title[:50] if n.title else ''
                snippet = n.snippet[:100] if n.snippet else ''
            else:
                title = n.get('title', '')[:50]
                snippet = n.get('snippet', '')[:100]
            news_text += f"{i}. {title}\n   {snippet}\n"
        
        # Assemble market stats and sector blocks by region (US market has no advance/decline or sector data)
        stats_block = ""
        sector_block = ""
        if self.region == "us":
            if self.profile.has_market_stats:
                stats_block = f"""## Market Overview
- Up: {overview.up_count} | Down: {overview.down_count} | Flat: {overview.flat_count}
- Limit up: {overview.limit_up_count} | Limit down: {overview.limit_down_count}
- Total volume ({self._amount_unit}): {overview.total_amount:.0f}"""
            else:
                stats_block = "## Market Overview\n(US market has no equivalent advance/decline stats.)"

            if self.profile.has_sector_rankings:
                sector_block = f"""## Sector Performance
Leading: {top_sectors_text if top_sectors_text else "N/A"}
Lagging: {bottom_sectors_text if bottom_sectors_text else "N/A"}"""
            else:
                sector_block = "## Sector Performance\n(US sector data not available.)"
        else:
            if self.profile.has_market_stats:
                stats_block = f"""## Market Overview
- Up: {overview.up_count} | Down: {overview.down_count} | Flat: {overview.flat_count}
- Limit up: {overview.limit_up_count} | Limit down: {overview.limit_down_count}
- Total market amount: {overview.total_amount:.0f} {self._amount_unit}"""
            else:
                stats_block = "## Market Overview\n(No advance/decline stats available for US market)"

            if self.profile.has_sector_rankings:
                sector_block = f"""## Sector Performance
Leading gainers: {top_sectors_text if top_sectors_text else "No data"}
Leading losers: {bottom_sectors_text if bottom_sectors_text else "No data"}"""
            else:
                sector_block = "## Sector Performance\n(No sector data available for US market)"

        data_no_indices_hint = (
            "Note: Market data fetch failed. Please rely mainly on [Market News] for qualitative analysis. Do not invent specific index levels."
            if not indices_text
            else ""
        )
        indices_placeholder = indices_text if indices_text else ("No index data (API error)" if self.region == "us" else "No index data (API error)")
        news_placeholder = news_text if news_text else ("No relevant news" if self.region == "us" else "No relevant news")

        # Global macro portfolio prompt (European-based thematic investor)
        if self.region in ("global", "eu"):
            return f"""You are a senior macro strategist advising a European-based thematic portfolio investor.

The portfolio is structured in 5 buckets: Hard Assets, Energy/Nuclear, Defense Supply Chain, Consumer Stress, and Geopolitical hedges. The investor uses Interactive Brokers from Amsterdam and can go long and short.

Produce a concise global macro brief based on the data below.

[Requirements]
- Output pure Markdown only
- No JSON, no code blocks
- Focus on cross-asset narrative, not single-stock picks
- Every section must connect back to portfolio implications

---

# Today's Market Data

## Date
{overview.date}

## Global Indices & Cross-Asset Dashboard
{indices_placeholder}

{stats_block}

{sector_block}

## Market News
{news_placeholder}

{data_no_indices_hint}

{self.strategy.to_prompt_block()}

---

# Output Template (follow this structure)

## {overview.date} Global Macro Brief

### 1. Regime Check
(2-3 sentences: risk-on / risk-off / transitional? What is the cross-asset signal saying? SPX+STOXX+DAX alignment, VIX, Gold vs USD vs Bonds.)

### 2. Cross-Asset Dashboard Commentary
(Analyze indices, commodities, FX, crypto as interconnected signals. What is Brent telling us about Iran thesis? What is Gold/BTC saying about stagflation? What does VIX term structure imply?)

### 3. Thesis Status
(For each active thesis — Iran Settlement, Stagflation, Dalio Stage 6 — state whether today's data confirms, is neutral, or disconfirms. Be specific about which proxies moved.)

### 4. Bucket Tilts
(For each portfolio bucket — Hard Assets, Energy/Nuclear, Defense, Consumer Stress, Geopolitical — recommend overweight / neutral / underweight based on today's regime and thesis status.)

### 5. Pair Trade Alerts
(Flag any long/short pairs where spread has moved significantly. Mention direction and conviction.)

### 6. Risk Alerts
(Top 2-3 risks for next session. Be specific — not generic "markets are volatile".)

### 7. Action Plan
(Stance: offensive / balanced / defensive. One concrete action per bucket if warranted. One invalidation trigger that would flip the stance.)

---

Output the report content directly, no extra commentary.
"""

        # US market scenario uses English prompts to generate reports better suited to US market context
        if self.region == "us":
            data_no_indices_hint_en = (
                "Note: Market data fetch failed. Rely mainly on [Market News] for qualitative analysis. Do not invent index levels."
                if not indices_text
                else ""
            )
            return f"""You are a professional US/A/H market analyst. Please produce a concise US market recap report based on the data below.

[Requirements]
- Output pure Markdown only
- No JSON
- No code blocks
- Use emoji sparingly in headings (at most one per heading)

---

# Today's Market Data

## Date
{overview.date}

## Major Indices
{indices_placeholder}

{stats_block}

{sector_block}

## Market News
{news_placeholder}

{data_no_indices_hint_en}

{self.strategy.to_prompt_block()}

---

# Output Template (follow this structure)

## {overview.date} US Market Recap

### 1. Market Summary
(2-3 sentences on overall market performance, index moves, volume)

### 2. Index Commentary
(Analyse S&P 500, Nasdaq, Dow and other major index moves.)

### 3. Fund Flows
(Interpret volume and flow implications)

### 4. Sector/Theme Highlights
(Analyze drivers behind leading/lagging sectors)

### 5. Outlook
(Short-term view based on price action and news)

### 6. Risk Alerts
(Key risks to watch)

### 7. Strategy Plan
(Provide risk-on/neutral/risk-off stance, position sizing guideline, and one invalidation trigger.)

---

Output the report content directly, no extra commentary.
"""

        # A-share scenario prompt
        return f"""You must respond entirely in English. Do not use any Chinese characters anywhere in your response. All analysis, recommendations, labels, section headers, and explanations must be in English only.

You are a professional A/H/US market analyst. Please generate a concise market review report based on the data below.

[Important] Output requirements:
- Output pure Markdown text format
- No JSON output
- No code blocks
- Use emoji sparingly in headings (at most one per heading)

---

# Today's Market Data

## Date
{overview.date}

## Major Indices
{indices_placeholder}

{stats_block}

{sector_block}

## Market News
{news_placeholder}

{data_no_indices_hint}

{self.strategy.to_prompt_block()}

---

# Output Template (follow this structure strictly)

## {overview.date} Market Review

### 1. Market Summary
(2-3 sentences on overall market performance, including index moves and volume changes)

### 2. Index Commentary
({self.profile.prompt_index_hint})

### 3. Fund Flows
(Interpret the meaning of amount/volume direction)

### 4. Hot Sectors
(Analyze logic and drivers behind leading/lagging sectors)

### 5. Outlook
(Give next-day market forecast based on current trend and news)

### 6. Risk Alerts
(Key risks to watch)

### 7. Strategy Plan
(Give offensive/balanced/defensive conclusion, corresponding position guidance, and one invalidation trigger; conclude with "For reference only, not investment advice.")

---

Output the review report content directly, no extra commentary.
"""
    
    def _generate_template_review(self, overview: MarketOverview, news: List) -> str:
        """Generate review report from template (fallback when no large model available)"""
        mood_code = self.profile.mood_index_code
        # Find corresponding index by mood_index_code
        # cn: mood_code="000001", idx.code may be "sh000001" (ends with mood_code)
        # us: mood_code="SPX", idx.code is directly "SPX"
        mood_index = next(
            (
                idx
                for idx in overview.indices
                if idx.code == mood_code or idx.code.endswith(mood_code)
            ),
            None,
        )
        if mood_index:
            if mood_index.change_pct > 1:
                market_mood = "strong rally"
            elif mood_index.change_pct > 0:
                market_mood = "slight gain"
            elif mood_index.change_pct > -1:
                market_mood = "slight decline"
            else:
                market_mood = "notable decline"
        else:
            market_mood = "sideways consolidation"

        # Index quotes (concise format)
        indices_text = ""
        for idx in overview.indices[:4]:
            direction = "↑" if idx.change_pct > 0 else "↓" if idx.change_pct < 0 else "-"
            indices_text += f"- **{idx.name}**: {idx.current:.2f} ({direction}{abs(idx.change_pct):.2f}%)\n"
        
        # Sector info
        top_text = ", ".join([s['name'] for s in overview.top_sectors[:3]])
        bottom_text = ", ".join([s['name'] for s in overview.bottom_sectors[:3]])

        # Include advance/decline stats and sector by region (US market has none)
        stats_section = ""
        if self.profile.has_market_stats:
            stats_section = f"""
### 3. Advance/Decline Statistics
| Metric | Value |
|--------|-------|
| Gainers | {overview.up_count} |
| Losers | {overview.down_count} |
| Limit up | {overview.limit_up_count} |
| Limit down | {overview.limit_down_count} |
| Total amount | {overview.total_amount:.0f} {self._amount_unit} |
"""
        sector_section = ""
        if self.profile.has_sector_rankings and (top_text or bottom_text):
            sector_section = f"""
### 4. Sector Performance
- **Leading gainers**: {top_text}
- **Leading losers**: {bottom_text}
"""
        market_label = "A-share" if self.region == "cn" else "US stock"
        strategy_summary = self.strategy.to_markdown_block()
        report = f"""## {overview.date} Market Review

### 1. Market Summary
Today's {market_label} market showed an overall **{market_mood}** trend.

### 2. Major Indices
{indices_text}
{stats_section}
{sector_section}
### 5. Risk Alerts
Markets involve risk; invest with caution. The above data is for reference only and does not constitute investment advice.

{strategy_summary}

---
*Review time: {datetime.now().strftime('%H:%M')}*
"""
        return report
    
    def run_daily_review(self) -> str:
        """
        Execute daily market review process

        Returns:
            Review report text
        """
        logger.info("========== Starting market review analysis ==========")

        # 1. Get market overview
        overview = self.get_market_overview()

        # 2. Search market news
        news = self.search_market_news()

        # 3. Generate review report
        report = self.generate_market_review(overview, news)

        logger.info("========== Market review analysis complete ==========")

        return report


# Test entry point
if __name__ == "__main__":
    import sys
    sys.path.insert(0, '.')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
    )

    analyzer = MarketAnalyzer()

    # Test fetching market overview
    overview = analyzer.get_market_overview()
    print(f"\n=== Market Overview ===")
    print(f"Date: {overview.date}")
    print(f"Number of indices: {len(overview.indices)}")
    for idx in overview.indices:
        print(f"  {idx.name}: {idx.current:.2f} ({idx.change_pct:+.2f}%)")
    print(f"Up: {overview.up_count} | Down: {overview.down_count}")
    print(f"Total amount: {overview.total_amount:.0f}B")

    # Test generating template report
    report = analyzer._generate_template_review(overview, [])
    print(f"\n=== Review Report ===")
    print(report)
