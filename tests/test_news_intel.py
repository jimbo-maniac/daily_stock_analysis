# -*- coding: utf-8 -*-
"""
===================================
A-share Stock Intelligent Analysis System - newsintelligencestorageunittesting
===================================

Responsibilities:
1. verificationnewsintelligencesavingwithdeduplicatelogic
2. verificationno URL situationbelowfallbackdeduplicatekey
"""

import os
import tempfile
import unittest

from datetime import datetime

from src.config import Config
from src.storage import DatabaseManager, NewsIntel
from src.search_service import SearchResponse, SearchResult


class NewsIntelStorageTestCase(unittest.TestCase):
    """newsintelligencestoragetesting"""

    def setUp(self) -> None:
        """aseachuseexampleinitializingindependentdatabase"""
        self._temp_dir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._temp_dir.name, "test_news_intel.db")
        os.environ["DATABASE_PATH"] = self._db_path

        # resetconfigurationwithdatabasesingleton，ensureuseapproachingwhenlibrary
        Config._instance = None
        DatabaseManager.reset_instance()
        self.db = DatabaseManager.get_instance()

    def tearDown(self) -> None:
        """cleancapitalsource"""
        DatabaseManager.reset_instance()
        self._temp_dir.cleanup()

    def _build_response(self, results) -> SearchResponse:
        """construct SearchResponse fastquickfunction"""
        return SearchResponse(
            query="Kweichow Moutai latestmessage",
            results=results,
            provider="Bocha",
            success=True,
        )

    def test_save_news_intel_with_url_dedup(self) -> None:
        """same URL deduplicate，onlykeeponeitemsrecord"""
        result = SearchResult(
            title="Maotaipublishnewproduct",
            snippet="companypublishnewproduct...",
            url="https://news.example.com/a",
            source="example.com",
            published_date="2025-01-02"
        )
        response = self._build_response([result])

        query_context = {
            "query_id": "task_001",
            "query_source": "bot",
            "requester_platform": "feishu",
            "requester_user_id": "u_123",
            "requester_user_name": "testinguser",
            "requester_chat_id": "c_456",
            "requester_message_id": "m_789",
            "requester_query": "/analyze 600519",
        }

        saved_first = self.db.save_news_intel(
            code="600519",
            name="Kweichow Moutai",
            dimension="latest_news",
            query=response.query,
            response=response,
            query_context=query_context
        )
        saved_second = self.db.save_news_intel(
            code="600519",
            name="Kweichow Moutai",
            dimension="latest_news",
            query=response.query,
            response=response,
            query_context=query_context
        )

        self.assertEqual(saved_first, 1)
        self.assertEqual(saved_second, 0)

        with self.db.get_session() as session:
            total = session.query(NewsIntel).count()
            row = session.query(NewsIntel).first()
        self.assertEqual(total, 1)
        if row is None:
            self.fail("not foundsavingnewsrecord")
        self.assertEqual(row.query_id, "task_001")
        self.assertEqual(row.requester_user_name, "testinguser")

    def test_save_news_intel_without_url_fallback_key(self) -> None:
        """no URL whenusefallbackkeydeduplicate"""
        result = SearchResult(
            title="Maotaiperformanceforecast",
            snippet="performancelargeincrease ratelong...",
            url="",
            source="example.com",
            published_date="2025-01-03"
        )
        response = self._build_response([result])

        saved_first = self.db.save_news_intel(
            code="600519",
            name="Kweichow Moutai",
            dimension="earnings",
            query=response.query,
            response=response
        )
        saved_second = self.db.save_news_intel(
            code="600519",
            name="Kweichow Moutai",
            dimension="earnings",
            query=response.query,
            response=response
        )

        self.assertEqual(saved_first, 1)
        self.assertEqual(saved_second, 0)

        with self.db.get_session() as session:
            row = session.query(NewsIntel).first()
            if row is None:
                self.fail("not foundsavingnewsrecord")
            self.assertTrue(row.url.startswith("no-url:"))

    def test_get_recent_news(self) -> None:
        """canbytimerangequeryinglatestnews"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result = SearchResult(
            title="Maotaistock priceoscillation",
            snippet="plateinwavedynamicrelativelylarge...",
            url="https://news.example.com/b",
            source="example.com",
            published_date=now
        )
        response = self._build_response([result])

        self.db.save_news_intel(
            code="600519",
            name="Kweichow Moutai",
            dimension="market_analysis",
            query=response.query,
            response=response
        )

        recent_news = self.db.get_recent_news(code="600519", days=7, limit=10)
        self.assertEqual(len(recent_news), 1)
        self.assertEqual(recent_news[0].title, "Maotaistock priceoscillation")


if __name__ == "__main__":
    unittest.main()
