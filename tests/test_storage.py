# -*- coding: utf-8 -*-
import unittest
import sys
import os

# Ensure src module can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.storage import DatabaseManager

class TestStorage(unittest.TestCase):
    
    def test_parse_sniper_value(self):
        """testingparsingsniper entry pointvalue"""
        
        # 1. normalvalue
        self.assertEqual(DatabaseManager._parse_sniper_value(100), 100.0)
        self.assertEqual(DatabaseManager._parse_sniper_value(100.5), 100.5)
        self.assertEqual(DatabaseManager._parse_sniper_value("100"), 100.0)
        self.assertEqual(DatabaseManager._parse_sniper_value("100.5"), 100.5)
        
        # 2. packageincludeChinesedescriptionand"yuan"
        self.assertEqual(DatabaseManager._parse_sniper_value("recommendedin 100 yuannearbuy"), 100.0)
        self.assertEqual(DatabaseManager._parse_sniper_value("price：100.5yuan"), 100.5)
        
        # 3. packageincludeinterferencecountcharacter（fixBugscenario）
        # before "MA5" willbyerrorextractas 5.0，currentinshouldthisextract "yuan" beforeaspect 100
        text_bug = "unable togiveout。needwaitingMA5datarestore，instock pricepullbackMA5andBIAS ratio<2%whenconsider100yuan"
        self.assertEqual(DatabaseManager._parse_sniper_value(text_bug), 100.0)
        
        # 4. moremultipleinterferencescenario
        text_complex = "MA10as20.5，recommendedin30yuanbuy"
        self.assertEqual(DatabaseManager._parse_sniper_value(text_complex), 30.0)
        
        text_multiple = "support level10yuan，resistance level20yuan" # shouldthisextractmostafteronecount"yuan"beforeaspectcountcharacter，i.e.20，orermorecomplexlogic？
        # currentlogicisfindmostafteronecountrisknumber，thenafterfindofaftertheonecount"yuan"，extractinbetweencountcharacter。
        # testingnohasrisknumbersituation
        self.assertEqual(DatabaseManager._parse_sniper_value("30yuan"), 30.0)
        
        # testingmultiplecountcountcharacterin"yuan"before
        self.assertEqual(DatabaseManager._parse_sniper_value("MA5 10 20yuan"), 20.0)
        
        # 5. Fallback: no "yuan" character — extracts last non-MA number
        self.assertEqual(DatabaseManager._parse_sniper_value("102.10-103.00（MA5near）"), 103.0)
        self.assertEqual(DatabaseManager._parse_sniper_value("97.62-98.50（MA10near）"), 98.5)
        self.assertEqual(DatabaseManager._parse_sniper_value("93.40belowmethod（MA20support）"), 93.4)
        self.assertEqual(DatabaseManager._parse_sniper_value("108.00-110.00（beforeperiodhighpointresistance）"), 110.0)

        # 6. invalidinput
        self.assertIsNone(DatabaseManager._parse_sniper_value(None))
        self.assertIsNone(DatabaseManager._parse_sniper_value(""))
        self.assertIsNone(DatabaseManager._parse_sniper_value("nohascountcharacter"))
        self.assertIsNone(DatabaseManager._parse_sniper_value("MA5butnohasyuan"))

        # 7. regression：bracketwithin numbertechnical indicatorcountcharacternotshouldbyextract
        self.assertNotEqual(DatabaseManager._parse_sniper_value("1.52-1.53 (pullbackMA5/10near)"), 10.0)
        self.assertNotEqual(DatabaseManager._parse_sniper_value("1.55-1.56(MA5/M20support)"), 20.0)
        self.assertNotEqual(DatabaseManager._parse_sniper_value("1.49-1.50(MA60nearstabilize)"), 60.0)
        # verificationcorrectvalueinintervalin
        self.assertIn(DatabaseManager._parse_sniper_value("1.52-1.53 (pullbackMA5/10near)"), [1.52, 1.53])
        self.assertIn(DatabaseManager._parse_sniper_value("1.55-1.56(MA5/M20support)"), [1.55, 1.56])
        self.assertIn(DatabaseManager._parse_sniper_value("1.49-1.50(MA60nearstabilize)"), [1.49, 1.50])

    def test_get_chat_sessions_prefix_is_scoped_by_colon_boundary(self):
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url="sqlite:///:memory:")

        db.save_conversation_message("telegram_12345:chat", "user", "first user")
        db.save_conversation_message("telegram_123456:chat", "user", "second user")

        sessions = db.get_chat_sessions(session_prefix="telegram_12345")

        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0]["session_id"], "telegram_12345:chat")

        DatabaseManager.reset_instance()

    def test_get_chat_sessions_can_include_legacy_exact_session_id(self):
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url="sqlite:///:memory:")

        db.save_conversation_message("feishu_u1", "user", "legacy chat")
        db.save_conversation_message("feishu_u1:ask_600519", "user", "ask session")

        sessions = db.get_chat_sessions(
            session_prefix="feishu_u1:",
            extra_session_ids=["feishu_u1"],
        )

        self.assertEqual({item["session_id"] for item in sessions}, {"feishu_u1", "feishu_u1:ask_600519"})

        DatabaseManager.reset_instance()

if __name__ == '__main__':
    unittest.main()
