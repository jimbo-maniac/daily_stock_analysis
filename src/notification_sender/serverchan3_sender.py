# -*- coding: utf-8 -*-
"""
Serversauce3 sendingreminderservice

Responsibilities:
1. via Serversauce3 API sending Serversauce3 message
"""
import logging
from typing import Optional
import requests
from datetime import datetime
import re

from src.config import Config


logger = logging.getLogger(__name__)


class Serverchan3Sender:
    
    def __init__(self, config: Config):
        """
        initializing Serversauce3 configuration

        Args:
            config: configurationobject
        """
        self._serverchan3_sendkey = getattr(config, 'serverchan3_sendkey', None)
        
    def send_to_serverchan3(self, content: str, title: Optional[str] = None) -> bool:
        """
        pushmessageto Serversauce3

        Serversauce3 API format：
        POST https://sctapi.ftqq.com/{sendkey}.send
        or
        POST https://{num}.push.ft07.com/send/{sendkey}.send
        {
            "title": "messagetitle",
            "desp": "messagecontent",
            "options": {}
        }

        Serversauce3 features：
        - domesticpushservice，support multiplecountChinaproducesystempushchannel，cannoafterplatformpush
        - simpleeasyuse API API/interface

        Args:
            content: messagecontent（Markdown format）
            title: messagetitle（optional）

        Returns:
            whethersendingsuccessful
        """
        if not self._serverchan3_sendkey:
            logger.warning("Serversauce3 SendKey notconfiguration，skippush")
            return False

        # processingmessagetitle
        if title is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
            title = f"📈 stockanalysis report - {date_str}"

        try:
            # based on sendkey formatconstruct URL
            sendkey = self._serverchan3_sendkey
            if sendkey.startswith('sctp'):
                match = re.match(r'sctp(\d+)t', sendkey)
                if match:
                    num = match.group(1)
                    url = f"https://{num}.push.ft07.com/send/{sendkey}.send"
                else:
                    logger.error("Invalid sendkey format for sctp")
                    return False
            else:
                url = f"https://sctapi.ftqq.com/{sendkey}.send"

            # buildrequestparameter
            params = {
                'title': title,
                'desp': content,
                'options': {}
            }

            # sendingrequest
            headers = {
                'Content-Type': 'application/json;charset=utf-8'
            }
            response = requests.post(url, json=params, headers=headers, timeout=10)

            if response.status_code == 200:
                result = response.json()
                logger.info(f"Serversauce3 messagesendingsuccessful: {result}")
                return True
            else:
                logger.error(f"Serversauce3 request failed: HTTP {response.status_code}")
                logger.error(f"responsecontent: {response.text}")
                return False

        except Exception as e:
            logger.error(f"sending Serversauce3 messagefailed: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return False

