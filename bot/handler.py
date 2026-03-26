# -*- coding: utf-8 -*-
"""
===================================
Bot Webhook handler
===================================

processingeachplatform Webhook pullback，dispatchtocommandhandler。
"""

import json
import logging
from typing import Dict, Any, Optional, TYPE_CHECKING

from bot.models import WebhookResponse
from bot.dispatcher import get_dispatcher
from bot.platforms import ALL_PLATFORMS

if TYPE_CHECKING:
    from bot.platforms.base import BotPlatform

logger = logging.getLogger(__name__)

# platforminstancecache
_platform_instances: Dict[str, 'BotPlatform'] = {}


def get_platform(platform_name: str) -> Optional['BotPlatform']:
    """
    getplatformadapterinstance
    
    usecacheavoid duplicatecreating。
    
    Args:
        platform_name: platformname
        
    Returns:
        platformadapterinstance，or None
    """
    if platform_name not in _platform_instances:
        platform_class = ALL_PLATFORMS.get(platform_name)
        if platform_class:
            _platform_instances[platform_name] = platform_class()
        else:
            logger.warning(f"[BotHandler] unknownplatform: {platform_name}")
            return None
    
    return _platform_instances[platform_name]


def handle_webhook(
    platform_name: str,
    headers: Dict[str, str],
    body: bytes,
    query_params: Optional[Dict[str, list]] = None
) -> WebhookResponse:
    """
    processing Webhook request
    
    thisisallplatform Webhook unifiedentry。
    
    Args:
        platform_name: platformname (feishu, dingtalk, wecom, telegram)
        headers: HTTP request headers
        body: request bodyraw bytes
        query_params: URL queryingparameter（forsomeplatformverification）
        
    Returns:
        WebhookResponse responseobject
    """
    logger.info(f"[BotHandler] received {platform_name} Webhook request")
    
    # checkbotfeaturewhetherenabled
    from src.config import get_config
    config = get_config()
    
    if not getattr(config, 'bot_enabled', True):
        logger.info("[BotHandler] botfeature notenabled")
        return WebhookResponse.success()
    
    # getplatformadapter
    platform = get_platform(platform_name)
    if not platform:
        return WebhookResponse.error(f"Unknown platform: {platform_name}", 400)
    
    # parsing JSON data
    try:
        data = json.loads(body.decode('utf-8')) if body else {}
    except json.JSONDecodeError as e:
        logger.error(f"[BotHandler] JSON parse failed: {e}")
        return WebhookResponse.error("Invalid JSON", 400)
    
    logger.debug(f"[BotHandler] request countdata: {json.dumps(data, ensure_ascii=False)[:500]}")
    
    # processing Webhook
    message, challenge_response = platform.handle_webhook(headers, body, data)
    
    # if it isverificationrequest，return directlyverificationresponse
    if challenge_response:
        logger.info(f"[BotHandler] returnverificationresponse")
        return challenge_response
    
    # if nomessageneedprocessing，return emptyresponse
    if not message:
        logger.debug("[BotHandler] no need forprocessingmessage")
        return WebhookResponse.success()
    
    logger.info(f"[BotHandler] parsingtomessage: user={message.user_name}, content={message.content[:50]}")
    
    # dispatchtocommandhandler
    dispatcher = get_dispatcher()
    response = dispatcher.dispatch(message)
    
    # formattingresponse
    if response.text:
        webhook_response = platform.format_response(response, message)
        return webhook_response
    
    return WebhookResponse.success()


def handle_feishu_webhook(headers: Dict[str, str], body: bytes) -> WebhookResponse:
    """processingFeishu Webhook"""
    return handle_webhook('feishu', headers, body)


def handle_dingtalk_webhook(headers: Dict[str, str], body: bytes) -> WebhookResponse:
    """processingDingTalk Webhook"""
    return handle_webhook('dingtalk', headers, body)


def handle_wecom_webhook(headers: Dict[str, str], body: bytes) -> WebhookResponse:
    """processingEnterprise WeChat Webhook"""
    return handle_webhook('wecom', headers, body)


def handle_telegram_webhook(headers: Dict[str, str], body: bytes) -> WebhookResponse:
    """processing Telegram Webhook"""
    return handle_webhook('telegram', headers, body)
