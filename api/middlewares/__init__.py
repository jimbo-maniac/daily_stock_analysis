# -*- coding: utf-8 -*-
"""
===================================
API middlewaremoduleinitializing
===================================

Responsibilities:
1. export allmiddleware
"""

from api.middlewares.error_handler import ErrorHandlerMiddleware

__all__ = ["ErrorHandlerMiddleware"]
