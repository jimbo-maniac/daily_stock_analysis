# -*- coding: utf-8 -*-
"""
===================================
Enum type definitions
===================================

Centrally manages enum types used in the system, providing type safety and code readability.
"""

from enum import Enum


class ReportType(str, Enum):
    """
    Report type enum

    Used to select the report format to push when API triggers analysis.
    Inherits str to allow direct comparison and serialization with strings.
    """
    SIMPLE = "simple"  # Simple report: uses generate_single_stock_report
    FULL = "full"      # Full report: uses generate_dashboard_report
    BRIEF = "brief"    # Brief mode: 3-5 sentence summary, suitable for mobile/push

    @classmethod
    def from_str(cls, value: str) -> "ReportType":
        """
        Safely converts a string to an enum value

        Args:
            value: String value

        Returns:
            Corresponding enum value, returns default SIMPLE for invalid input
        """
        try:
            normalized = value.lower().strip()
            if normalized == "detailed":
                normalized = cls.FULL.value
            return cls(normalized)
        except (ValueError, AttributeError):
            return cls.SIMPLE

    @property
    def display_name(self) -> str:
        """Get the display name"""
        return {
            ReportType.SIMPLE: "Simple Report",
            ReportType.FULL: "Full Report",
            ReportType.BRIEF: "Brief Report",
        }.get(self, "Simple Report")
