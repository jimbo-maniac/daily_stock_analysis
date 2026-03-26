# -*- coding: utf-8 -*-
"""
Wechat sendingreminderservice

Responsibilities:
1. viaEnterprise WeChat Webhook sendingtextmessage
2. viaEnterprise WeChat Webhook sendingimagemessage
"""
import logging
import base64
import hashlib
import requests
import time

from src.config import Config
from src.formatters import chunk_content_by_max_bytes


logger = logging.getLogger(__name__)


# WeChat Work image msgtype limit ~2MB (base64 payload)
WECHAT_IMAGE_MAX_BYTES = 2 * 1024 * 1024

class WechatSender:
    
    def __init__(self, config: Config):
        """
        initializingEnterprise WeChatconfiguration

        Args:
            config: configurationobject
        """
        self._wechat_url = config.wechat_webhook_url
        self._wechat_max_bytes = getattr(config, 'wechat_max_bytes', 4000)
        self._wechat_msg_type = getattr(config, 'wechat_msg_type', 'markdown')
        self._webhook_verify_ssl = getattr(config, 'webhook_verify_ssl', True)
        
    def send_to_wechat(self, content: str) -> bool:
        """
        pushmessagetoEnterprise WeChatbot
        
        Enterprise WeChat Webhook messageformat：
        support markdown typewithand text type, markdown typeinWeChatinunable todisplay，canuse text type,
        markdown typewillparsing markdown format,text typewilldirectlysendingplain text。

        markdown typeExample：
        {
            "msgtype": "markdown",
            "markdown": {
                "content": "## title\n\ncontent"
            }
        }
        
        text typeExample：
        {
            "msgtype": "text",
            "text": {
                "content": "content"
            }
        }

        Note：Enterprise WeChat Markdown constraint 4096 bytes（non-character）, Text typeconstraint 2048 bytes，extra longcontentwillautomaticin batchessending
        can viaenvironment variable WECHAT_MAX_BYTES adjustconstraintvalue
        
        Args:
            content: Markdown formatmessagecontent
            
        Returns:
            whethersendingsuccessful
        """
        if not self._wechat_url:
            logger.warning("Enterprise WeChat Webhook notconfiguration，skippush")
            return False
        
        # based onmessagetypedynamicconstraintupper limit，avoid text typeexceedEnterprise WeChat 2048 bytesconstraint
        if self._wechat_msg_type == 'text':
            max_bytes = min(self._wechat_max_bytes, 2000)  # reserveonefixedbytesgivesystem/paginationmark
        else:
            max_bytes = self._wechat_max_bytes  # markdown default 4000 bytes
        
        # checkbyteslength，extra longthenin batchessending
        content_bytes = len(content.encode('utf-8'))
        if content_bytes > max_bytes:
            logger.info(f"messagecontentextra long({content_bytes}bytes/{len(content)}character)，will batchsending")
            return self._send_wechat_chunked(content, max_bytes)
        
        try:
            return self._send_wechat_message(content)
        except Exception as e:
            logger.error(f"sendingEnterprise WeChatmessagefailed: {e}")
            return False

    def _send_wechat_image(self, image_bytes: bytes) -> bool:
        """Send image via WeChat Work webhook msgtype image (Issue #289)."""
        if not self._wechat_url:
            return False
        if len(image_bytes) > WECHAT_IMAGE_MAX_BYTES:
            logger.warning(
                "Enterprise WeChatimageover limit (%d > %d bytes)，rejectsending，callmethodshould fallback as text",
                len(image_bytes), WECHAT_IMAGE_MAX_BYTES,
            )
            return False
        try:
            b64 = base64.b64encode(image_bytes).decode("ascii")
            md5_hash = hashlib.md5(image_bytes).hexdigest()
            payload = {
                "msgtype": "image",
                "image": {"base64": b64, "md5": md5_hash},
            }
            response = requests.post(
                self._wechat_url, json=payload, timeout=30, verify=self._webhook_verify_ssl
            )
            if response.status_code == 200:
                result = response.json()
                if result.get("errcode") == 0:
                    logger.info("Enterprise WeChatimagesendingsuccessful")
                    return True
                logger.error("Enterprise WeChatimagesendingfailed: %s", result.get("errmsg", ""))
            else:
                logger.error("Enterprise WeChatrequest failed: HTTP %s", response.status_code)
            return False
        except Exception as e:
            logger.error("Enterprise WeChatimagesendingabnormal: %s", e)
            return False
    
    def _send_wechat_message(self, content: str) -> bool:
        """sendingEnterprise WeChatmessage"""
        payload = self._gen_wechat_payload(content)
        
        response = requests.post(
            self._wechat_url,
            json=payload,
            timeout=10,
            verify=self._webhook_verify_ssl
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('errcode') == 0:
                logger.info("Enterprise WeChatmessagesendingsuccessful")
                return True
            else:
                logger.error(f"Enterprise WeChatreturnerror: {result}")
                return False
        else:
            logger.error(f"Enterprise WeChatrequest failed: {response.status_code}")
            return False
        
    def _send_wechat_chunked(self, content: str, max_bytes: int) -> bool:
        """
        in batchessendinglongmessagetoEnterprise WeChat
        
        bystockanalyzingblock（with --- or ### separate）intelligentsplitting，ensureeachbatchnotexceedconstraint
        
        Args:
            content: completemessagecontent
            max_bytes: single entrymessagemax bytes
            
        Returns:
            whether allsendingsuccessful
        """
        chunks = chunk_content_by_max_bytes(content, max_bytes, add_page_marker=True)
        total_chunks = len(chunks)
        success_count = 0
        for i, chunk in enumerate(chunks):
            if self._send_wechat_message(chunk):
                success_count += 1
            else:
                logger.error(f"Enterprise WeChatthe {i+1}/{total_chunks} batchsendingfailed")
            if i < total_chunks - 1:
                time.sleep(1)
        return success_count == len(chunks)

    def _gen_wechat_payload(self, content: str) -> dict:
        """generatingEnterprise WeChatmessage payload"""
        if self._wechat_msg_type == 'text':
            return {
                "msgtype": "text",
                "text": {
                    "content": content
                }
            }
        else:
            return {
                "msgtype": "markdown",
                "markdown": {
                    "content": content
                }
            }
