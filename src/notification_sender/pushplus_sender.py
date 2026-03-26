# -*- coding: utf-8 -*-
"""
PushPlus sendingreminderservice

Responsibilities:
1. via PushPlus API sending PushPlus message
"""
import logging
import time
from typing import Optional
from datetime import datetime
import requests

from src.config import Config
from src.formatters import chunk_content_by_max_bytes


logger = logging.getLogger(__name__)


class PushplusSender:
    
    def __init__(self, config: Config):
        """
        initializing PushPlus configuration

        Args:
            config: configurationobject
        """
        self._pushplus_token = getattr(config, 'pushplus_token', None)
        self._pushplus_topic = getattr(config, 'pushplus_topic', None)
        self._pushplus_max_bytes = getattr(config, 'pushplus_max_bytes', 20000)
        
    def send_to_pushplus(self, content: str, title: Optional[str] = None) -> bool:
        """
        pushmessageto PushPlus

        PushPlus API format：
        POST http://www.pushplus.plus/send
        {
            "token": "usertoken",
            "title": "messagetitle",
            "content": "messagecontent",
            "template": "html/txt/json/markdown"
        }

        PushPlus features：
        - domesticpushservice，freesufficient quota
        - supportWeChatpublicnumberpush
        - support multipletypemessageformat

        Args:
            content: messagecontent（Markdown format）
            title: messagetitle（optional）

        Returns:
            whethersendingsuccessful
        """
        if not self._pushplus_token:
            logger.warning("PushPlus Token notconfiguration，skippush")
            return False

        api_url = "http://www.pushplus.plus/send"

        if title is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
            title = f"📈 stockanalysis report - {date_str}"

        try:
            content_bytes = len(content.encode('utf-8'))
            if content_bytes > self._pushplus_max_bytes:
                logger.info(
                    "PushPlus messagecontentextra long(%sbytes/%scharacter)，will batchsending",
                    content_bytes,
                    len(content),
                )
                return self._send_pushplus_chunked(
                    api_url,
                    content,
                    title,
                    self._pushplus_max_bytes,
                )

            return self._send_pushplus_message(api_url, content, title)
        except Exception as e:
            logger.error(f"sending PushPlus messagefailed: {e}")
            return False

    def _send_pushplus_message(self, api_url: str, content: str, title: str) -> bool:
        payload = {
            "token": self._pushplus_token,
            "title": title,
            "content": content,
            "template": "markdown",
        }

        if self._pushplus_topic:
            payload["topic"] = self._pushplus_topic

        response = requests.post(api_url, json=payload, timeout=10)

        if response.status_code == 200:
            result = response.json()
            if result.get('code') == 200:
                logger.info("PushPlus messagesendingsuccessful")
                return True

            error_msg = result.get('msg', 'unknownerror')
            logger.error(f"PushPlus returnerror: {error_msg}")
            return False

        logger.error(f"PushPlus request failed: HTTP {response.status_code}")
        return False

    def _send_pushplus_chunked(self, api_url: str, content: str, title: str, max_bytes: int) -> bool:
        """in batchessendinglong PushPlus message，give JSON payload reserveemptybetween。"""
        budget = max(1000, max_bytes - 1500)
        chunks = chunk_content_by_max_bytes(content, budget, add_page_marker=True)
        total_chunks = len(chunks)
        success_count = 0

        logger.info(f"PushPlus in batchessending：total {total_chunks} batch")

        for i, chunk in enumerate(chunks):
            chunk_title = f"{title} ({i+1}/{total_chunks})" if total_chunks > 1 else title
            if self._send_pushplus_message(api_url, chunk, chunk_title):
                success_count += 1
                logger.info(f"PushPlus the {i+1}/{total_chunks} batchsendingsuccessful")
            else:
                logger.error(f"PushPlus the {i+1}/{total_chunks} batchsendingfailed")

            if i < total_chunks - 1:
                time.sleep(1)

        return success_count == total_chunks
