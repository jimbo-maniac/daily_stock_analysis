# -*- coding: utf-8 -*-
"""
Pushover sendingreminderservice

Responsibilities:
1. via Pushover API sending Pushover message
"""
import logging
from typing import Optional
from datetime import datetime
import requests

from src.config import Config
from src.formatters import markdown_to_plain_text


logger = logging.getLogger(__name__)


class PushoverSender:
    
    def __init__(self, config: Config):
        """
        initializing Pushover configuration

        Args:
            config: configurationobject
        """
        self._pushover_config = {
            'user_key': getattr(config, 'pushover_user_key', None),
            'api_token': getattr(config, 'pushover_api_token', None),
        }
        
    def _is_pushover_configured(self) -> bool:
        """check Pushover configurationis complete"""
        return bool(self._pushover_config['user_key'] and self._pushover_config['api_token'])

    def send_to_pushover(self, content: str, title: Optional[str] = None) -> bool:
        """
        pushmessageto Pushover
        
        Pushover API format：
        POST https://api.pushover.net/1/messages.json
        {
            "token": "apply API Token",
            "user": "user Key",
            "message": "messagecontent",
            "title": "title（optional）"
        }
        
        Pushover features：
        - support iOS/Android/desktopmultipleplatformpush
        - messageconstraint 1024 character
        - supportprioritysettings
        - support HTML format
        
        Args:
            content: messagecontent（Markdown format，willconvertasplain text）
            title: messagetitle（optional，defaultas"stockanalysis report"）
            
        Returns:
            whethersendingsuccessful
        """
        if not self._is_pushover_configured():
            logger.warning("Pushover configurationincomplete，skippush")
            return False
        
        user_key = self._pushover_config['user_key']
        api_token = self._pushover_config['api_token']
        
        # Pushover API endpoint
        api_url = "https://api.pushover.net/1/messages.json"
        
        # processingmessagetitle
        if title is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
            title = f"📈 stockanalysis report - {date_str}"
        
        # Pushover messageconstraint 1024 character
        max_length = 1024
        
        # converting Markdown asplain text（Pushover support HTML，butplain textmoregeneric）
        plain_content = markdown_to_plain_text(content)
        
        if len(plain_content) <= max_length:
            # single entrymessagesending
            return self._send_pushover_message(api_url, user_key, api_token, plain_content, title)
        else:
            # segmentsendinglongmessage
            return self._send_pushover_chunked(api_url, user_key, api_token, plain_content, title, max_length)
      
    def _send_pushover_message(
        self, 
        api_url: str, 
        user_key: str, 
        api_token: str, 
        message: str, 
        title: str,
        priority: int = 0
    ) -> bool:
        """
        sendingsingle entry Pushover message
        
        Args:
            api_url: Pushover API endpoint
            user_key: user Key
            api_token: apply API Token
            message: messagecontent
            title: messagetitle
            priority: priority (-2 ~ 2，default 0)
        """
        try:
            payload = {
                "token": api_token,
                "user": user_key,
                "message": message,
                "title": title,
                "priority": priority,
            }
            
            response = requests.post(api_url, data=payload, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('status') == 1:
                    logger.info("Pushover messagesendingsuccessful")
                    return True
                else:
                    errors = result.get('errors', ['unknownerror'])
                    logger.error(f"Pushover returnerror: {errors}")
                    return False
            else:
                logger.error(f"Pushover request failed: HTTP {response.status_code}")
                logger.debug(f"responsecontent: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"sending Pushover messagefailed: {e}")
            return False
    
    def _send_pushover_chunked(
        self, 
        api_url: str, 
        user_key: str, 
        api_token: str, 
        content: str, 
        title: str,
        max_length: int
    ) -> bool:
        """
        segmentsendinglong Pushover message
        
        by paragraphsplitting，ensureeachsegmentnotexceedmaxlength
        """
        import time
        
        # by paragraph（separatelineordual switchrow）splitting
        if "────────" in content:
            sections = content.split("────────")
            separator = "────────"
        else:
            sections = content.split("\n\n")
            separator = "\n\n"
        
        chunks = []
        current_chunk = []
        current_length = 0
        
        for section in sections:
            # calculatingaddthiscount section afteractuallength
            # join() onlyinyuanelementbetweenputsetseparatesymbol，is noteachyuanelementafteraspect
            # placewith：theonecountyuanelementno needseparatesymbol，aftercontinueyuanelementneedonecountseparatesymbolconnecting
            if current_chunk:
                # existingyuanelement，addnewyuanelementneed：currentlength + separatesymbol + new section
                new_length = current_length + len(separator) + len(section)
            else:
                # theonecountyuanelement，no needseparatesymbol
                new_length = len(section)
            
            if new_length > max_length:
                if current_chunk:
                    chunks.append(separator.join(current_chunk))
                current_chunk = [section]
                current_length = len(section)
            else:
                current_chunk.append(section)
                current_length = new_length
        
        if current_chunk:
            chunks.append(separator.join(current_chunk))
        
        total_chunks = len(chunks)
        success_count = 0
        
        logger.info(f"Pushover in batchessending：total {total_chunks} batch")
        
        for i, chunk in enumerate(chunks):
            # addpaginationmarktotitle
            chunk_title = f"{title} ({i+1}/{total_chunks})" if total_chunks > 1 else title
            
            if self._send_pushover_message(api_url, user_key, api_token, chunk, chunk_title):
                success_count += 1
                logger.info(f"Pushover the {i+1}/{total_chunks} batchsendingsuccessful")
            else:
                logger.error(f"Pushover the {i+1}/{total_chunks} batchsendingfailed")
            
            # batchinterval，avoid triggeringfrequencyconstraint
            if i < total_chunks - 1:
                time.sleep(1)
        
        return success_count == total_chunks
    