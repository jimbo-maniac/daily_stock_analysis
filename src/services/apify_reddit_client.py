# -*- coding: utf-8 -*-
"""
Apify Reddit sentiment client using the trudax/reddit-scraper-lite actor.

Runs the actor via Apify's synchronous REST endpoint and derives a simple
bullish / neutral / bearish sentiment label from post scores and upvote ratios.

Only active for the tickers in REDDIT_TICKERS.
Results are cached in-process so each symbol is fetched at most once per run.
Each call runs inside the caller's thread (the existing ThreadPoolExecutor
worker), so MAX_WORKERS automatically limits concurrency — no extra pool needed.
All failures are non-fatal: a warning is logged and None is returned.
"""

import logging
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

# Tickers eligible for Reddit sentiment enrichment
REDDIT_TICKERS: frozenset = frozenset(
    {"CRWD", "ZS", "S", "RBRK", "GEO", "CXW", "MP", "BTC-USD", "FLNG", "LNG"}
)

_ACTOR_ID = "trudax~reddit-scraper-lite"
_BASE_URL = "https://api.apify.com/v2"
_TIMEOUT = 90       # seconds — Apify actor cold-start can take ~30–60 s
_MAX_ITEMS = 10


class ApifyRedditClient:
    """Runs the Apify Reddit scraper and computes a sentiment summary."""

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._cache: Dict[str, Optional[Dict[str, Any]]] = {}

    @property
    def is_available(self) -> bool:
        return bool(self._api_key)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def get_sentiment(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Return a Reddit sentiment dict for *symbol*, or None.

        Keys:
            total_mentions  (int)
            top_3_titles    (List[str])
            sentiment_label ("bullish" | "neutral" | "bearish")
        """
        if symbol.upper() not in REDDIT_TICKERS:
            return None
        if symbol in self._cache:
            return self._cache[symbol]

        result = self._fetch(symbol)
        self._cache[symbol] = result
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch(self, symbol: str) -> Optional[Dict[str, Any]]:
        url = f"{_BASE_URL}/acts/{_ACTOR_ID}/run-sync-get-dataset-items"
        payload = {
            "searches": [symbol],
            "maxItems": _MAX_ITEMS,
            "time": "week",
        }
        try:
            resp = requests.post(
                url,
                json=payload,
                params={"token": self._api_key},
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
            posts: List[Dict] = resp.json()
            if not isinstance(posts, list) or not posts:
                logger.warning("[Apify] No Reddit posts returned for %s", symbol)
                return None
            result = self._compute_sentiment(posts)
            logger.info(
                "[Apify] %s Reddit: %d posts, sentiment=%s",
                symbol,
                result["total_mentions"],
                result["sentiment_label"],
            )
            return result
        except requests.exceptions.Timeout:
            logger.warning("[Apify] Timeout fetching Reddit posts for %s", symbol)
            return None
        except Exception as exc:
            logger.warning("[Apify] Failed to fetch Reddit posts for %s: %s", symbol, exc)
            return None

    @staticmethod
    def _compute_sentiment(posts: List[Dict]) -> Dict[str, Any]:
        total = len(posts)

        def _score(p: Dict) -> float:
            return float(p.get("score") or p.get("ups") or 0)

        sorted_posts = sorted(posts, key=_score, reverse=True)
        top_3 = [p.get("title", "") for p in sorted_posts[:3] if p.get("title")]

        # Collect upvote ratios (field name varies by scraper version)
        ratios = [
            float(r)
            for p in posts
            for r in [p.get("upvote_ratio") or p.get("upvoteRatio")]
            if r is not None
        ]
        scores = [_score(p) for p in posts]
        avg_ratio = sum(ratios) / len(ratios) if ratios else None
        avg_score = sum(scores) / len(scores) if scores else 0.0

        # Sentiment label — prefer upvote_ratio when available
        if avg_ratio is not None:
            if avg_ratio >= 0.70:
                label = "bullish"
            elif avg_ratio <= 0.45:
                label = "bearish"
            else:
                label = "neutral"
        else:
            if avg_score >= 50:
                label = "bullish"
            elif avg_score < 5:
                label = "bearish"
            else:
                label = "neutral"

        return {
            "total_mentions": total,
            "top_3_titles": top_3,
            "sentiment_label": label,
        }
