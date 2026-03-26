# -*- coding: utf-8 -*-
"""
Telegram sendingreminderservice

Responsibilities:
1. via Telegram Bot API sending textmessage
2. via Telegram Bot API sending imagemessage
"""
import logging
from typing import Optional
import requests
import time
import re

from src.config import Config


logger = logging.getLogger(__name__)


class TelegramSender:
    
    def __init__(self, config: Config):
        """
        initializing Telegram configuration

        Args:
            config: configurationobject
        """
        self._telegram_config = {
            'bot_token': getattr(config, 'telegram_bot_token', None),
            'chat_id': getattr(config, 'telegram_chat_id', None),
            'message_thread_id': getattr(config, 'telegram_message_thread_id', None),
        }
    
    def _is_telegram_configured(self) -> bool:
        """check Telegram configurationis complete"""
        return bool(self._telegram_config['bot_token'] and self._telegram_config['chat_id'])
   
    def send_to_telegram(self, content: str) -> bool:
        """
        pushmessageto Telegram bot
        
        Telegram Bot API format：
        POST https://api.telegram.org/bot<token>/sendMessage
        {
            "chat_id": "xxx",
            "text": "messagecontent",
            "parse_mode": "Markdown"
        }
        
        Args:
            content: messagecontent（Markdown format）
            
        Returns:
            whethersendingsuccessful
        """
        if not self._is_telegram_configured():
            logger.warning("Telegram configurationincomplete，skippush")
            return False
        
        bot_token = self._telegram_config['bot_token']
        chat_id = self._telegram_config['chat_id']
        message_thread_id = self._telegram_config.get('message_thread_id')
        
        try:
            # Telegram API endpoint
            api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            
            # Telegram messagemaxlength 4096 character
            max_length = 4096
            
            if len(content) <= max_length:
                # single entrymessagesending
                return self._send_telegram_message(api_url, chat_id, content, message_thread_id)
            else:
                # segmentsendinglongmessage
                return self._send_telegram_chunked(api_url, chat_id, content, max_length, message_thread_id)
                
        except Exception as e:
            logger.error(f"sending Telegram messagefailed: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return False
    
    def _send_telegram_message(self, api_url: str, chat_id: str, text: str, message_thread_id: Optional[str] = None) -> bool:
        """Send a single Telegram message with exponential backoff retry (Fixes #287)"""
        # Convert Markdown to Telegram-compatible format
        telegram_text = self._convert_to_telegram_markdown(text)
        
        payload = {
            "chat_id": chat_id,
            "text": telegram_text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        }

        if message_thread_id:
            payload['message_thread_id'] = message_thread_id

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.post(api_url, json=payload, timeout=10)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                if attempt < max_retries:
                    delay = 2 ** attempt  # 2s, 4s
                    logger.warning(f"Telegram request failed (attempt {attempt}/{max_retries}): {e}, "
                                   f"retrying in {delay}s...")
                    time.sleep(delay)
                    continue
                else:
                    logger.error(f"Telegram request failed after {max_retries} attempts: {e}")
                    return False
        
            if response.status_code == 200:
                result = response.json()
                if result.get('ok'):
                    logger.info("Telegram messagesendingsuccessful")
                    return True
                else:
                    error_desc = result.get('description', 'unknownerror')
                    logger.error(f"Telegram returnerror: {error_desc}")
                    
                    # If Markdown parsing failed, fall back to plain text
                    if 'parse' in error_desc.lower() or 'markdown' in error_desc.lower():
                        logger.info("tryuseplain textformatre-sending...")
                        plain_payload = dict(payload)
                        plain_payload.pop('parse_mode', None)
                        plain_payload['text'] = text  # Use original text
                        
                        try:
                            response = requests.post(api_url, json=plain_payload, timeout=10)
                            if response.status_code == 200 and response.json().get('ok'):
                                logger.info("Telegram messagesendingsuccessful（plain text）")
                                return True
                        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                            logger.error(f"Telegram plain-text fallback failed: {e}")
                    
                    return False
            elif response.status_code == 429:
                # Rate limited — respect Retry-After header
                retry_after = int(response.headers.get('Retry-After', 2 ** attempt))
                if attempt < max_retries:
                    logger.warning(f"Telegram rate limited, retrying in {retry_after}s "
                                   f"(attempt {attempt}/{max_retries})...")
                    time.sleep(retry_after)
                    continue
                else:
                    logger.error(f"Telegram rate limited after {max_retries} attempts")
                    return False
            else:
                if attempt < max_retries and response.status_code >= 500:
                    delay = 2 ** attempt
                    logger.warning(f"Telegram server error HTTP {response.status_code} "
                                   f"(attempt {attempt}/{max_retries}), retrying in {delay}s...")
                    time.sleep(delay)
                    continue
                logger.error(f"Telegram request failed: HTTP {response.status_code}")
                logger.error(f"responsecontent: {response.text}")
                return False

        return False
    
    def _send_telegram_chunked(self, api_url: str, chat_id: str, content: str, max_length: int, message_thread_id: Optional[str] = None) -> bool:
        """segmentsendinglong Telegram message"""
        # by paragraphsplitting
        sections = content.split("\n---\n")
        
        current_chunk = []
        current_length = 0
        all_success = True
        chunk_index = 1
        
        for section in sections:
            section_length = len(section) + 5  # +5 for "\n---\n"
            
            if current_length + section_length > max_length:
                # sendingcurrent block
                if current_chunk:
                    chunk_content = "\n---\n".join(current_chunk)
                    logger.info(f"sending Telegram messageblock {chunk_index}...")
                    if not self._send_telegram_message(api_url, chat_id, chunk_content, message_thread_id):
                        all_success = False
                    chunk_index += 1
                
                # reset
                current_chunk = [section]
                current_length = section_length
            else:
                current_chunk.append(section)
                current_length += section_length
        
        # sendingmostafteroneblock
        if current_chunk:
            chunk_content = "\n---\n".join(current_chunk)
            logger.info(f"sending Telegram messageblock {chunk_index}...")
            if not self._send_telegram_message(api_url, chat_id, chunk_content, message_thread_id):
                all_success = False
                
        return all_success

    def _send_telegram_photo(self, image_bytes: bytes) -> bool:
        """Send image via Telegram sendPhoto API (Issue #289)."""
        if not self._is_telegram_configured():
            return False
        bot_token = self._telegram_config['bot_token']
        chat_id = self._telegram_config['chat_id']
        message_thread_id = self._telegram_config.get('message_thread_id')
        api_url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
        try:
            data = {"chat_id": chat_id}
            if message_thread_id:
                data['message_thread_id'] = message_thread_id
            files = {"photo": ("report.png", image_bytes, "image/png")}
            response = requests.post(api_url, data=data, files=files, timeout=30)
            if response.status_code == 200 and response.json().get('ok'):
                logger.info("Telegram imagesendingsuccessful")
                return True
            logger.error("Telegram imagesendingfailed: %s", response.text[:200])
            return False
        except Exception as e:
            logger.error("Telegram imagesendingabnormal: %s", e)
            return False

    def _convert_to_telegram_markdown(self, text: str) -> str:
        """
        willstandard Markdown convertingas Telegram supportedformat
        
        Telegram Markdown constraint：
        - not supported # title
        - use *bold* instead of **bold**
        - use _italic_ 
        """
        result = text
        
        # remove # titlemark（Telegram not supported）
        result = re.sub(r'^#{1,6}\s+', '', result, flags=re.MULTILINE)
        
        # converting **bold** as *bold*
        result = re.sub(r'\*\*(.+?)\*\*', r'*\1*', result)
        
        # Escape special characters for Telegram Markdown, but preserve link syntax [text](url)
        # Step 1: temporarily protect markdown links
        import uuid as _uuid
        _link_placeholder = f"__LINK_{_uuid.uuid4().hex[:8]}__"
        _links = []
        def _save_link(m):
            _links.append(m.group(0))
            return f"{_link_placeholder}{len(_links) - 1}"
        result = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', _save_link, result)

        # Step 2: escape remaining special chars
        for char in ['[', ']', '(', ')']:
            result = result.replace(char, f'\\{char}')

        # Step 3: restore links
        for i, link in enumerate(_links):
            result = result.replace(f"{_link_placeholder}{i}", link)

        return result
    