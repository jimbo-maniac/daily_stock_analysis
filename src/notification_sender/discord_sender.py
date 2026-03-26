# -*- coding: utf-8 -*-
"""
Discord sendingreminderservice

Responsibilities:
1. via webhook or Discord bot API sending Discord message
"""
import logging
import requests

from src.config import Config
from src.formatters import chunk_content_by_max_words


logger = logging.getLogger(__name__)


class DiscordSender:
    
    def __init__(self, config: Config):
        """
        initializing Discord configuration

        Args:
            config: configurationobject
        """
        self._discord_config = {
            'bot_token': getattr(config, 'discord_bot_token', None),
            'channel_id': getattr(config, 'discord_main_channel_id', None),
            'webhook_url': getattr(config, 'discord_webhook_url', None),
        }
        self._discord_max_words = getattr(config, 'discord_max_words', 2000)
        self._webhook_verify_ssl = getattr(config, 'webhook_verify_ssl', True)
    
    def _is_discord_configured(self) -> bool:
        """check Discord configurationis complete（support Bot or Webhook）"""
        # onlyneedconfiguration Webhook orcomplete Bot Token+Channel，i.e.view asasavailable
        bot_ok = bool(self._discord_config['bot_token'] and self._discord_config['channel_id'])
        webhook_ok = bool(self._discord_config['webhook_url'])
        return bot_ok or webhook_ok
    
    def send_to_discord(self, content: str) -> bool:
        """
        pushmessageto Discord（support Webhook and Bot API）
        
        Args:
            content: Markdown formatmessagecontent
            
        Returns:
            whethersendingsuccessful
        """
        # splittingcontent，avoidsingle entrymessageexceed Discord constraint
        try:
            chunks = chunk_content_by_max_words(content, self._discord_max_words)
        except ValueError as e:
            logger.error(f"splitting Discord messagefailed: {e}, tryentire segmentsending。")
            chunks = [content]

        # prefer to use Webhook（configurationsimple，permissionlow）
        if self._discord_config['webhook_url']:
            return all(self._send_discord_webhook(chunk) for chunk in chunks)

        # itstimesuse Bot API（permissionhigh，need channel_id）
        if self._discord_config['bot_token'] and self._discord_config['channel_id']:
            return all(self._send_discord_bot(chunk) for chunk in chunks)

        logger.warning("Discord configurationincomplete，skippush")
        return False

  
    def _send_discord_webhook(self, content: str) -> bool:
        """
        use Webhook sendingmessageto Discord
        
        Discord Webhook support Markdown format
        
        Args:
            content: Markdown formatmessagecontent
            
        Returns:
            whethersendingsuccessful
        """
        try:
            payload = {
                'content': content,
                'username': 'A-shareanalyzingbot',
                'avatar_url': 'https://picsum.photos/200'
            }
            
            response = requests.post(
                self._discord_config['webhook_url'],
                json=payload,
                timeout=10,
                verify=self._webhook_verify_ssl
            )
            
            if response.status_code in [200, 204]:
                logger.info("Discord Webhook messagesendingsuccessful")
                return True
            else:
                logger.error(f"Discord Webhook sendingfailed: {response.status_code} {response.text}")
                return False
        except Exception as e:
            logger.error(f"Discord Webhook sendingabnormal: {e}")
            return False
    
    def _send_discord_bot(self, content: str) -> bool:
        """
        use Bot API sendingmessageto Discord
        
        Args:
            content: Markdown formatmessagecontent
            
        Returns:
            whethersendingsuccessful
        """
        try:
            headers = {
                'Authorization': f'Bot {self._discord_config["bot_token"]}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                'content': content
            }
            
            url = f'https://discord.com/api/v10/channels/{self._discord_config["channel_id"]}/messages'
            response = requests.post(url, json=payload, headers=headers, timeout=10)
            
            if response.status_code == 200:
                logger.info("Discord Bot messagesendingsuccessful")
                return True
            else:
                logger.error(f"Discord Bot sendingfailed: {response.status_code} {response.text}")
                return False
        except Exception as e:
            logger.error(f"Discord Bot sendingabnormal: {e}")
            return False
