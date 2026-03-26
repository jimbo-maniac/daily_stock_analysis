# -*- coding: utf-8 -*-
"""
===================================
API v1 moduleinitializing
===================================

Responsibilities:
1. export v1 version API route
"""

from api.v1.router import router as api_v1_router

__all__ = ["api_v1_router"]
