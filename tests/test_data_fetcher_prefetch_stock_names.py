# -*- coding: utf-8 -*-
"""
Regression tests for stock-name prefetch behavior.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, call, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data_provider.base import DataFetcherManager
from data_provider.pytdx_fetcher import PytdxFetcher


class _DummyFetcher:
    name = "DummyFetcher"

    @staticmethod
    def get_stock_name(_stock_code):
        return "testingstock"


class TestPrefetchStockNames(unittest.TestCase):
    def test_prefetch_stock_names_calls_get_stock_name_without_realtime(self):
        manager = DataFetcherManager.__new__(DataFetcherManager)
        manager.get_stock_name = MagicMock(return_value="")

        DataFetcherManager.prefetch_stock_names(manager, ["SH600519", "000001"], use_bulk=False)

        manager.get_stock_name.assert_has_calls(
            [
                call("600519", allow_realtime=False),
                call("000001", allow_realtime=False),
            ]
        )

    def test_get_stock_name_skips_realtime_when_allow_realtime_false(self):
        manager = DataFetcherManager.__new__(DataFetcherManager)
        manager._fetchers = [_DummyFetcher()]
        manager.get_realtime_quote = MagicMock(return_value=MagicMock(name="realtimename"))

        name = DataFetcherManager.get_stock_name(manager, "123456", allow_realtime=False)

        self.assertEqual(name, "testingstock")
        manager.get_realtime_quote.assert_not_called()

    def test_get_stock_name_prefers_static_mapping_before_remote_fetchers(self):
        manager = DataFetcherManager.__new__(DataFetcherManager)
        remote_fetcher = MagicMock()
        remote_fetcher.name = "RemoteFetcher"
        remote_fetcher.get_stock_name.return_value = "remoteprocessname"
        manager._fetchers = [remote_fetcher]
        manager.get_realtime_quote = MagicMock()

        name = DataFetcherManager.get_stock_name(manager, "600519", allow_realtime=False)

        self.assertEqual(name, "Kweichow Moutai")
        manager.get_realtime_quote.assert_not_called()
        remote_fetcher.get_stock_name.assert_not_called()
        self.assertEqual(manager._stock_name_cache["600519"], "Kweichow Moutai")

    def test_pytdx_get_stock_name_reads_all_security_list_pages(self):
        fetcher = PytdxFetcher(hosts=[])

        first_page = [
            {"code": f"{index:06d}", "name": f"stock{index:06d}"}
            for index in range(1000)
        ]
        second_page = [{"code": "300750", "name": "CATL"}]

        api = MagicMock()

        def fake_get_security_list(market, start):
            if market == 0 and start == 0:
                return first_page
            if market == 0 and start == 1000:
                return second_page
            return []

        api.get_security_list.side_effect = fake_get_security_list
        api.get_finance_info.return_value = None

        session = MagicMock()
        session.__enter__.return_value = api
        session.__exit__.return_value = False

        with patch.object(fetcher, "_pytdx_session", return_value=session):
            name = fetcher.get_stock_name("300750")

        self.assertEqual(name, "CATL")
        self.assertEqual(fetcher._stock_name_cache["300750"], "CATL")
        self.assertEqual(fetcher._stock_list_cache["300750"], "CATL")
        api.get_finance_info.assert_not_called()


if __name__ == "__main__":
    unittest.main()
