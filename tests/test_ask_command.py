# -*- coding: utf-8 -*-
"""Tests for metadata-driven AskCommand skill selection."""

import unittest
from unittest.mock import patch

from bot.commands.ask import AskCommand
from src.agent.skills.base import Skill


class AskCommandSkillSelectionTestCase(unittest.TestCase):
    """Verify /ask skill selection follows skill metadata instead of hardcoded ids."""

    def test_parse_skill_defaults_to_primary_metadata_skill(self) -> None:
        command = AskCommand()
        skills = [
            Skill(
                name="box_oscillation",
                display_name="box patternoscillation",
                description="box",
                instructions="box",
                default_priority=30,
            ),
            Skill(
                name="wave_theory",
                display_name="Elliott Wave Theory",
                description="wave",
                instructions="wave",
                default_active=True,
                default_priority=10,
            ),
        ]

        with patch.object(AskCommand, "_load_skills", return_value=skills):
            self.assertEqual(command._parse_skill(["600519"]), "wave_theory")

    def test_parse_skill_matches_alias_before_default(self) -> None:
        command = AskCommand()
        skills = [
            Skill(
                name="bull_trend",
                display_name="defaultlong positiontrend",
                description="trend",
                instructions="trend",
                aliases=["trend", "trend analysis"],
                default_active=True,
                default_priority=10,
            ),
            Skill(
                name="chan_theory",
                display_name="Chan theory",
                description="chan",
                instructions="chan",
                aliases=["Chan theory", "Chan theoryanalyzing"],
                default_priority=40,
            ),
        ]

        with patch.object(AskCommand, "_load_skills", return_value=skills):
            self.assertEqual(command._parse_skill(["600519", "please", "using Chan theoryanalyzing"]), "chan_theory")


if __name__ == "__main__":
    unittest.main()
