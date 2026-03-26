# -*- coding: utf-8 -*-
"""
custom Webhook sendingreminderservice

Responsibilities:
1. sendingcustom Webhook message
"""
import logging
import json
import requests

from src.config import Config
from src.formatters import chunk_content_by_max_bytes, slice_at_max_bytes


logger = logging.getLogger(__name__)


class CustomWebhookSender:

    def __init__(self, config: Config):
        """
        initializingcustom Webhook configuration

        Args:
            config: configurationobject
        """
        self._custom_webhook_urls = getattr(config, 'custom_webhook_urls', []) or []
        self._custom_webhook_bearer_token = getattr(config, 'custom_webhook_bearer_token', None)
        self._webhook_verify_ssl = getattr(config, 'webhook_verify_ssl', True)
 
    def send_to_custom(self, content: str) -> bool:
        """
        pushmessagetocustom Webhook
        
        supportarbitraryaccept POST JSON  Webhook endpoint
        defaultsendingformat：{"text": "messagecontent", "content": "messagecontent"}
        
        suitablefor：
        - DingTalkbot
        - Discord Webhook
        - Slack Incoming Webhook
        - self-builtnotificationservice
        - othersupport POST JSON service
        
        Args:
            content: messagecontent（Markdown format）
            
        Returns:
            whetherat leasthasonecount Webhook sendingsuccessful
        """
        if not self._custom_webhook_urls:
            logger.warning("notconfigurationcustom Webhook，skippush")
            return False
        
        success_count = 0
        
        for i, url in enumerate(self._custom_webhook_urls):
            try:
                # generic JSON format，compatiblelargemultiplecount Webhook
                # DingTalkformat: {"msgtype": "text", "text": {"content": "xxx"}}
                # Slack format: {"text": "xxx"}
                # Discord format: {"content": "xxx"}
                
                # DingTalkbotto body hasbytesupper limit（approximately 20000 bytes），extra longneedin batchessending
                if self._is_dingtalk_webhook(url):
                    if self._send_dingtalk_chunked(url, content, max_bytes=20000):
                        logger.info(f"custom Webhook {i+1}（DingTalk）pushsuccessful")
                        success_count += 1
                    else:
                        logger.error(f"custom Webhook {i+1}（DingTalk）pushfailed")
                    continue

                # other Webhook：singlesending
                payload = self._build_custom_webhook_payload(url, content)
                if self._post_custom_webhook(url, payload, timeout=30):
                    logger.info(f"custom Webhook {i+1} pushsuccessful")
                    success_count += 1
                else:
                    logger.error(f"custom Webhook {i+1} pushfailed")
                    
            except Exception as e:
                logger.error(f"custom Webhook {i+1} pushabnormal: {e}")
        
        logger.info(f"custom Webhook pushcompleted：successful {success_count}/{len(self._custom_webhook_urls)}")
        return success_count > 0

    
    def _send_custom_webhook_image(
        self, image_bytes: bytes, fallback_content: str = ""
    ) -> bool:
        """Send image to Custom Webhooks; Discord supports file attachment (Issue #289)."""
        if not self._custom_webhook_urls:
            return False
        success_count = 0
        for i, url in enumerate(self._custom_webhook_urls):
            try:
                if self._is_discord_webhook(url):
                    files = {"file": ("report.png", image_bytes, "image/png")}
                    data = {"content": "📈 stockintelligentanalysis report"}
                    headers = {"User-Agent": "StockAnalysis/1.0"}
                    if self._custom_webhook_bearer_token:
                        headers["Authorization"] = (
                            f"Bearer {self._custom_webhook_bearer_token}"
                        )
                    response = requests.post(
                        url, data=data, files=files, headers=headers, timeout=30,
                        verify=self._webhook_verify_ssl
                    )
                    if response.status_code in (200, 204):
                        logger.info("custom Webhook %d（Discord image）pushsuccessful", i + 1)
                        success_count += 1
                    else:
                        logger.error(
                            "custom Webhook %d（Discord image）pushfailed: HTTP %s",
                            i + 1, response.status_code,
                        )
                else:
                    if fallback_content:
                        payload = self._build_custom_webhook_payload(url, fallback_content)
                        if self._post_custom_webhook(url, payload, timeout=30):
                            logger.info(
                                "custom Webhook %d（imagenot supported，rollbacktext）pushsuccessful", i + 1
                            )
                            success_count += 1
                    else:
                        logger.warning(
                            "custom Webhook %d not supportedimage，andnorollbackcontent，skip", i + 1
                        )
            except Exception as e:
                logger.error("custom Webhook %d imagepushabnormal: %s", i + 1, e)
        return success_count > 0

    def _post_custom_webhook(self, url: str, payload: dict, timeout: int = 30) -> bool:
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'User-Agent': 'StockAnalysis/1.0',
        }
        # support Bearer Token authentication（#51）
        if self._custom_webhook_bearer_token:
            headers['Authorization'] = f'Bearer {self._custom_webhook_bearer_token}'
        body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        response = requests.post(url, data=body, headers=headers, timeout=timeout, verify=self._webhook_verify_ssl)
        if response.status_code == 200:
            return True
        logger.error(f"custom Webhook pushfailed: HTTP {response.status_code}")
        logger.debug(f"responsecontent: {response.text[:200]}")
        return False
    
    def _build_custom_webhook_payload(self, url: str, content: str) -> dict:
        """
        based on URL buildtoshould Webhook payload
        
        auto-detectnormallyseeserviceandusetoshouldformat
        """
        url_lower = url.lower()
        
        # DingTalkbot
        if 'dingtalk' in url_lower or 'oapi.dingtalk.com' in url_lower:
            return {
                "msgtype": "markdown",
                "markdown": {
                    "title": "stockanalysis report",
                    "text": content
                }
            }
        
        # Discord Webhook
        if 'discord.com/api/webhooks' in url_lower or 'discordapp.com/api/webhooks' in url_lower:
            # Discord constraint 2000 character
            truncated = content[:1900] + "..." if len(content) > 1900 else content
            return {
                "content": truncated
            }
        
        # Slack Incoming Webhook
        if 'hooks.slack.com' in url_lower:
            return {
                "text": content,
                "mrkdwn": True
            }
        
        # Bark (iOS push)
        if 'api.day.app' in url_lower:
            return {
                "title": "stockanalysis report",
                "body": content[:4000],  # Bark constraint
                "group": "stock"
            }
        
        # genericformat（compatiblelargemultiplecountservice）
        return {
            "text": content,
            "content": content,
            "message": content,
            "body": content
        }
    
    def _send_dingtalk_chunked(self, url: str, content: str, max_bytes: int = 20000) -> bool:
        import time as _time

        # as payload overheadreserveemptybetween，avoid body over limit
        budget = max(1000, max_bytes - 1500)
        chunks = chunk_content_by_max_bytes(content, budget)
        if not chunks:
            return False

        total = len(chunks)
        ok = 0

        for idx, chunk in enumerate(chunks):
            marker = f"\n\n📄 *({idx+1}/{total})*" if total > 1 else ""
            payload = {
                "msgtype": "markdown",
                "markdown": {
                    "title": "stockanalysis report",
                    "text": chunk + marker,
                },
            }

            # ifstillover limit（extremeendsituationbelow），againbybyteshardtruncateonce
            body_bytes = len(json.dumps(payload, ensure_ascii=False).encode('utf-8'))
            if body_bytes > max_bytes:
                hard_budget = max(200, budget - (body_bytes - max_bytes) - 200)
                payload["markdown"]["text"], _ = slice_at_max_bytes(payload["markdown"]["text"], hard_budget)

            if self._post_custom_webhook(url, payload, timeout=30):
                ok += 1
            else:
                logger.error(f"DingTalkin batchessendingfailed: the {idx+1}/{total} batch")

            if idx < total - 1:
                _time.sleep(1)

        return ok == total

    
    @staticmethod
    def _is_dingtalk_webhook(url: str) -> bool:
        url_lower = (url or "").lower()
        return 'dingtalk' in url_lower or 'oapi.dingtalk.com' in url_lower

    @staticmethod
    def _is_discord_webhook(url: str) -> bool:
        url_lower = (url or "").lower()
        return (
            'discord.com/api/webhooks' in url_lower
            or 'discordapp.com/api/webhooks' in url_lower
        )
