# -*- coding: utf-8 -*-
"""
===================================
botcommandtriggersystem
===================================

via @bot orsendingcommandtriggerstockanalyzingetcfeature。
supportFeishu、DingTalk、Enterprise WeChat、Telegram etcmultipleplatform。

modulestructure：
- models.py: unifiedmessage/responsemodel
- dispatcher.py: commanddispatcher
- commands/: commandhandler
- platforms/: platformadapter
- handler.py: Webhook handler

usage：
1. configurationenvironment variable（eachplatform Token etc）
2. start WebUI service
3. ineachplatformconfiguration Webhook URL：
   - Feishu: http://your-server/bot/feishu
   - DingTalk: http://your-server/bot/dingtalk
   - Enterprise WeChat: http://your-server/bot/wecom
   - Telegram: http://your-server/bot/telegram

supportedcommand：
- /analyze <stock code>  - analyzingspecifiedstock
- /market             - market review
- /batch              - batchanalyzingwatchlist stocks
- /help               - displayhelp
- /status             - systemstatus
"""

from bot.models import BotMessage, BotResponse, ChatType, WebhookResponse
from bot.dispatcher import CommandDispatcher, get_dispatcher

__all__ = [
    'BotMessage',
    'BotResponse',
    'ChatType',
    'WebhookResponse',
    'CommandDispatcher',
    'get_dispatcher',
]
