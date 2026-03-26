# -*- coding: utf-8 -*-
"""
AstrBot sendingreminderservice

Responsibilities:
1. via Astrbot API sending AstrBot message
"""
import logging
import json
import hmac
import hashlib
import requests

from src.config import Config
from src.formatters import markdown_to_html_document


logger = logging.getLogger(__name__)


class AstrbotSender:
    
    def __init__(self, config: Config):
        """
        initializing AstrBot configuration

        Args:
            config: configurationobject
        """
        self._astrbot_config = {
            'astrbot_url': getattr(config, 'astrbot_url', None),
            'astrbot_token': getattr(config, 'astrbot_token', None),
        }
        self._webhook_verify_ssl = getattr(config, 'webhook_verify_ssl', True)
        
    def _is_astrbot_configured(self) -> bool:
        """check AstrBot configurationis complete（support Bot or Webhook）"""
        # onlyneedconfiguration URL，i.e.view asasavailable
        url_ok = bool(self._astrbot_config['astrbot_url'])
        return url_ok

    def send_to_astrbot(self, content: str) -> bool:
        """
        pushmessageto AstrBot（viaadaptersupport）

        Args:
            content: Markdown formatmessagecontent

        Returns:
            whethersendingsuccessful
        """
        if self._astrbot_config['astrbot_url']:
            return self._send_astrbot(content)

        logger.warning("AstrBot configurationincomplete，skippush")
        return False


    def _send_astrbot(self, content: str) -> bool:
        import time
        """
        use Bot API sendingmessageto AstrBot

        Args:
            content: Markdown formatmessagecontent

        Returns:
            whethersendingsuccessful
        """

        html_content = markdown_to_html_document(content)

        try:
            payload = {
                'content': html_content
            }
            signature =  ""
            timestamp = str(int(time.time()))
            if self._astrbot_config['astrbot_token']:
                """calculatingrequestsignature"""
                payload_json = json.dumps(payload, sort_keys=True)
                sign_data = f"{timestamp}.{payload_json}".encode('utf-8')
                key = self._astrbot_config['astrbot_token']
                signature = hmac.new(
                    key.encode('utf-8'),
                    sign_data,
                    hashlib.sha256
                ).hexdigest()
            url = self._astrbot_config['astrbot_url']
            response = requests.post(
                url, json=payload, timeout=10,
                headers={
                    "Content-Type": "application/json",
                    "X-Signature": signature,
                    "X-Timestamp": timestamp
                },
                verify=self._webhook_verify_ssl
            )

            if response.status_code == 200:
                logger.info("AstrBot messagesendingsuccessful")
                return True
            else:
                logger.error(f"AstrBot sendingfailed: {response.status_code} {response.text}")
                return False
        except Exception as e:
            logger.error(f"AstrBot sendingabnormal: {e}")
            return False
