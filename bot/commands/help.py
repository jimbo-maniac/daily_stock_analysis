# -*- coding: utf-8 -*-
"""
===================================
helpcommand
===================================

displayavailablecommandlistandusage instructions。
"""

from typing import List

from bot.commands.base import BotCommand
from bot.models import BotMessage, BotResponse


class HelpCommand(BotCommand):
    """
    helpcommand
    
    displayallavailablecommandlistandusage instructions。
    alsocanviewspecificcommanddetailedhelp。
    
    usage：
        /help         - displayallcommand
        /help analyze - display analyze commanddetailedhelp
    """
    
    @property
    def name(self) -> str:
        return "help"
    
    @property
    def aliases(self) -> List[str]:
        return ["h", "help", "?"]
    
    @property
    def description(self) -> str:
        return "displayhelp information"
    
    @property
    def usage(self) -> str:
        return "/help [commandname]"
    
    def execute(self, message: BotMessage, args: List[str]) -> BotResponse:
        """executehelpcommand"""
        # lazy import to avoid circulardependency
        from bot.dispatcher import get_dispatcher
        
        dispatcher = get_dispatcher()
        
        # ifspecifiedcommandname，displaythiscommanddetailedhelp
        if args:
            cmd_name = args[0]
            command = dispatcher.get_command(cmd_name)
            
            if command is None:
                return BotResponse.error_response(f"unknowncommand: {cmd_name}")
            
            # builddetailedhelp
            help_text = self._format_command_help(command, dispatcher.command_prefix)
            return BotResponse.markdown_response(help_text)
        
        # displayallcommandlist
        commands = dispatcher.list_commands(include_hidden=False)
        prefix = dispatcher.command_prefix
        
        help_text = self._format_help_list(commands, prefix)
        return BotResponse.markdown_response(help_text)
    
    def _format_help_list(self, commands: List[BotCommand], prefix: str) -> str:
        """formattingcommandlist"""
        lines = [
            "📚 **stockanalyzingassistant - commandhelp**",
            "",
            "availablecommand：",
            "",
        ]
        
        for cmd in commands:
            # commandnameandalias
            aliases_str = ""
            if cmd.aliases:
                # filteringdropChinesealias，onlydisplayEnglishalias
                en_aliases = [a for a in cmd.aliases if a.isascii()]
                if en_aliases:
                    aliases_str = f" ({', '.join(prefix + a for a in en_aliases[:2])})"
            
            lines.append(f"• {prefix}{cmd.name}{aliases_str} - {cmd.description}")
            lines.append("")

        lines.extend([
            "",
            "---",
            f"💡 input {prefix}help <commandname> viewdetailedusage",
            "",
            "**Example：**",
            "",
            f"• {prefix}analyze 301023 - Yifandynamic",
            "",
            f"• {prefix}market - viewmarket review",
            "",
            f"• {prefix}batch - batchanalyzingwatchlist stocks",
        ])
        
        return "\n".join(lines)
    
    def _format_command_help(self, command: BotCommand, prefix: str) -> str:
        """formattingsinglecommanddetailedhelp"""
        lines = [
            f"📖 **{prefix}{command.name}** - {command.description}",
            "",
            f"**usage：** `{command.usage}`",
            "",
        ]
        
        # alias
        if command.aliases:
            aliases = [f"`{prefix}{a}`" if a.isascii() else f"`{a}`" for a in command.aliases]
            lines.append(f"**alias：** {', '.join(aliases)}")
            lines.append("")
        
        # permission
        if command.admin_only:
            lines.append("⚠️ **requires adminpermission**")
            lines.append("")
        
        return "\n".join(lines)
