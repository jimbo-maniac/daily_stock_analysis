# -*- coding: utf-8 -*-
"""
===================================
statuscommand
===================================

display systemrunningstatusandconfiguration information。
"""

import platform
import sys
from datetime import datetime
from typing import List

from bot.commands.base import BotCommand
from bot.models import BotMessage, BotResponse


class StatusCommand(BotCommand):
    """
    statuscommand
    
    display systemrunningstatus，packagebracket：
    - servicestatus
    - configuration information
    - availablefeature
    """
    
    @property
    def name(self) -> str:
        return "status"
    
    @property
    def aliases(self) -> List[str]:
        return ["s", "status", "info"]
    
    @property
    def description(self) -> str:
        return "display systemstatus"
    
    @property
    def usage(self) -> str:
        return "/status"
    
    def execute(self, message: BotMessage, args: List[str]) -> BotResponse:
        """executestatuscommand"""
        from src.config import get_config
        
        config = get_config()
        
        # collectstatusinfo
        status_info = self._collect_status(config)
        
        # formattingoutput
        text = self._format_status(status_info, message.platform)
        
        return BotResponse.markdown_response(text)
    
    def _collect_status(self, config) -> dict:
        """collectsystemstatusinfo"""
        status = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "platform": platform.system(),
            "stock_count": len(config.stock_list),
            "stock_list": config.stock_list[:5],  # onlydisplaybefore5count
        }
        
        # AI configurationstatus
        status["ai_gemini"] = bool(config.gemini_api_key)
        status["ai_openai"] = bool(config.openai_api_key)
        
        # searchservicestatus
        status["search_bocha"] = len(config.bocha_api_keys) > 0
        status["search_tavily"] = len(config.tavily_api_keys) > 0
        status["search_brave"] = len(config.brave_api_keys) > 0
        status["search_serpapi"] = len(config.serpapi_keys) > 0
        status["search_minimax"] = len(config.minimax_api_keys) > 0
        status["search_searxng"] = config.has_searxng_enabled()
        
        # notification channelstatus
        status["notify_wechat"] = bool(config.wechat_webhook_url)
        status["notify_feishu"] = bool(config.feishu_webhook_url)
        status["notify_telegram"] = bool(config.telegram_bot_token and config.telegram_chat_id)
        status["notify_email"] = bool(config.email_sender and config.email_password)
        
        return status
    
    def _format_status(self, status: dict, platform: str) -> str:
        """formattingstatusinfo"""
        # statusicon
        def icon(enabled: bool) -> str:
            return "✅" if enabled else "❌"
        
        lines = [
            "📊 **stockanalyzingassistant - systemstatus**",
            "",
            f"🕐 time: {status['timestamp']}",
            f"🐍 Python: {status['python_version']}",
            f"💻 platform: {status['platform']}",
            "",
            "---",
            "",
            "**📈 watchlist stocksconfiguration**",
            f"• stockquantity: {status['stock_count']} only",
        ]
        
        if status['stock_list']:
            stocks_preview = ", ".join(status['stock_list'])
            if status['stock_count'] > 5:
                stocks_preview += f" ... etc {status['stock_count']} only"
            lines.append(f"• stocklist: {stocks_preview}")
        
        lines.extend([
            "",
            "**🤖 AI analyzingservice**",
            f"• Gemini API: {icon(status['ai_gemini'])}",
            f"• OpenAI API: {icon(status['ai_openai'])}",
            "",
            "**🔍 searchservice**",
            f"• Bocha: {icon(status['search_bocha'])}",
            f"• Tavily: {icon(status['search_tavily'])}",
            f"• Brave: {icon(status['search_brave'])}",
            f"• SerpAPI: {icon(status['search_serpapi'])}",
            f"• MiniMax: {icon(status['search_minimax'])}",
            f"• SearXNG: {icon(status['search_searxng'])}",
            "",
            "**📢 notification channel**",
            f"• Enterprise WeChat: {icon(status['notify_wechat'])}",
            f"• Feishu: {icon(status['notify_feishu'])}",
            f"• Telegram: {icon(status['notify_telegram'])}",
            f"• email: {icon(status['notify_email'])}",
        ])
        
        # AI servicetotalbodystatus
        ai_available = status['ai_gemini'] or status['ai_openai']
        if ai_available:
            lines.extend([
                "",
                "---",
                "✅ **systemready，canstartinganalyzing！**",
            ])
        else:
            lines.extend([
                "",
                "---",
                "⚠️ **AI servicenotconfiguration，analyzingfeatureunavailable**",
                "pleaseconfiguration Gemini or OpenAI API Key",
            ])
        
        return "\n".join(lines)
