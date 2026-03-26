# -*- coding: utf-8 -*-
"""
Feishu sendingreminderservice

Responsibilities:
1. via webhook sendingFeishumessage
"""
import logging
from typing import Dict, Any
import requests
import time

from src.config import Config
from src.formatters import format_feishu_markdown, chunk_content_by_max_bytes


logger = logging.getLogger(__name__)


class FeishuSender:
    
    def __init__(self, config: Config):
        """
        initializingFeishuconfiguration

        Args:
            config: configurationobject
        """
        self._feishu_url = getattr(config, 'feishu_webhook_url', None)
        self._feishu_max_bytes = getattr(config, 'feishu_max_bytes', 20000)
        self._webhook_verify_ssl = getattr(config, 'webhook_verify_ssl', True)
    
          
    def send_to_feishu(self, content: str) -> bool:
        """
        pushmessagetoFeishubot
        
        Feishucustombot Webhook messageformat：
        {
            "msg_type": "text",
            "content": {
                "text": "textcontent"
            }
        }
        
        Description：Feishutextmessagenotwillrender Markdown，needuseinteractive card（lark_md）format
        
        Note：Feishutextmessageconstraintapproximately 20KB，extra longcontentwillautomaticin batchessending
        can viaenvironment variable FEISHU_MAX_BYTES adjustconstraintvalue
        
        Args:
            content: messagecontent（Markdown willconvertasplain text）
            
        Returns:
            whethersendingsuccessful
        """
        if not self._feishu_url:
            logger.warning("Feishu Webhook notconfiguration，skippush")
            return False
        
        # Feishu lark_md supportholdlimit，firstdoformatconverting
        formatted_content = format_feishu_markdown(content)

        max_bytes = self._feishu_max_bytes  # fromconfigurationreading，default 20000 bytes
        
        # checkbyteslength，extra longthenin batchessending
        content_bytes = len(formatted_content.encode('utf-8'))
        if content_bytes > max_bytes:
            logger.info(f"Feishumessagecontentextra long({content_bytes}bytes/{len(content)}character)，will batchsending")
            return self._send_feishu_chunked(formatted_content, max_bytes)
        
        try:
            return self._send_feishu_message(formatted_content)
        except Exception as e:
            logger.error(f"sendingFeishumessagefailed: {e}")
            return False
   
    def _send_feishu_chunked(self, content: str, max_bytes: int) -> bool:
        """
        in batchessendinglongmessagetoFeishu
        
        bystockanalyzingblock（with --- or ### separate）intelligentsplitting，ensureeachbatchnotexceedconstraint
        
        Args:
            content: completemessagecontent
            max_bytes: single entrymessagemax bytes
            
        Returns:
            whether allsendingsuccessful
        """
        chunks = chunk_content_by_max_bytes(content, max_bytes, add_page_marker=True)
        
        # in batchessending
        total_chunks = len(chunks)
        success_count = 0
        
        logger.info(f"Feishuin batchessending：total {total_chunks} batch")
        
        for i, chunk in enumerate(chunks):
            try:
                if self._send_feishu_message(chunk):
                    success_count += 1
                    logger.info(f"Feishuthe {i+1}/{total_chunks} batchsendingsuccessful")
                else:
                    logger.error(f"Feishuthe {i+1}/{total_chunks} batchsendingfailed")
            except Exception as e:
                logger.error(f"Feishuthe {i+1}/{total_chunks} batchsendingabnormal: {e}")
            
            # batchinterval，avoid triggeringfrequencyconstraint
            if i < total_chunks - 1:
                time.sleep(1)
        
        return success_count == total_chunks
    
    def _send_feishu_message(self, content: str) -> bool:
        """sendingsingle entryFeishumessage（prefer to use Markdown card）"""
        def _post_payload(payload: Dict[str, Any]) -> bool:
            logger.debug(f"Feishurequest URL: {self._feishu_url}")
            logger.debug(f"Feishurequest payload length: {len(content)} character")

            response = requests.post(
                self._feishu_url,
                json=payload,
                timeout=30,
                verify=self._webhook_verify_ssl
            )

            logger.debug(f"Feishuresponsestatus code: {response.status_code}")
            logger.debug(f"Feishuresponsecontent: {response.text}")

            if response.status_code == 200:
                result = response.json()
                code = result.get('code') if 'code' in result else result.get('StatusCode')
                if code == 0:
                    logger.info("Feishumessagesendingsuccessful")
                    return True
                else:
                    error_msg = result.get('msg') or result.get('StatusMessage', 'unknownerror')
                    error_code = result.get('code') or result.get('StatusCode', 'N/A')
                    logger.error(f"Feishureturnerror [code={error_code}]: {error_msg}")
                    logger.error(f"completeresponse: {result}")
                    return False
            else:
                logger.error(f"Feishurequest failed: HTTP {response.status_code}")
                logger.error(f"responsecontent: {response.text}")
                return False

        # 1) prefer to useinteractive card（support Markdown render）
        card_payload = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": "A-shareintelligentanalysis report"
                    }
                },
                "elements": [
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": content
                        }
                    }
                ]
            }
        }

        if _post_payload(card_payload):
            return True

        # 2) rollbackasnormaltextmessage
        text_payload = {
            "msg_type": "text",
            "content": {
                "text": content
            }
        }

        return _post_payload(text_payload)
