# -*- coding: utf-8 -*-
"""
Email sendingreminderservice

Responsibilities:
1. via SMTP sending Email message
"""
import logging
from typing import Optional, List
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.header import Header
from email.utils import formataddr
import smtplib

from src.config import Config
from src.formatters import markdown_to_html_document


logger = logging.getLogger(__name__)


# SMTP servicehandlerconfiguration（auto-detect）
SMTP_CONFIGS = {
    # QQemail
    "qq.com": {"server": "smtp.qq.com", "port": 465, "ssl": True},
    "foxmail.com": {"server": "smtp.qq.com", "port": 465, "ssl": True},
    # NetEaseemail
    "163.com": {"server": "smtp.163.com", "port": 465, "ssl": True},
    "126.com": {"server": "smtp.126.com", "port": 465, "ssl": True},
    # Gmail
    "gmail.com": {"server": "smtp.gmail.com", "port": 587, "ssl": False},
    # Outlook
    "outlook.com": {"server": "smtp-mail.outlook.com", "port": 587, "ssl": False},
    "hotmail.com": {"server": "smtp-mail.outlook.com", "port": 587, "ssl": False},
    "live.com": {"server": "smtp-mail.outlook.com", "port": 587, "ssl": False},
    # Sina
    "sina.com": {"server": "smtp.sina.com", "port": 465, "ssl": True},
    # Sohu
    "sohu.com": {"server": "smtp.sohu.com", "port": 465, "ssl": True},
    # Alibaba Cloud
    "aliyun.com": {"server": "smtp.aliyun.com", "port": 465, "ssl": True},
    # 139email
    "139.com": {"server": "smtp.139.com", "port": 465, "ssl": True},
}


class EmailSender:
    
    def __init__(self, config: Config):
        """
        initializing Email configuration

        Args:
            config: configurationobject
        """
        self._email_config = {
            'sender': config.email_sender,
            'sender_name': getattr(config, 'email_sender_name', 'daily_stock_analysisstockanalyzingassistant'),
            'password': config.email_password,
            'receivers': config.email_receivers or ([config.email_sender] if config.email_sender else []),
        }
        self._stock_email_groups = getattr(config, 'stock_email_groups', None) or []
        
    def _is_email_configured(self) -> bool:
        """checkemailconfigurationis complete（onlyneedemailandauthorizationcode）"""
        return bool(self._email_config['sender'] and self._email_config['password'])
    
    def get_receivers_for_stocks(self, stock_codes: List[str]) -> List[str]:
        """
        Look up email receivers for given stock codes based on stock_email_groups.
        Returns union of receivers for all matching groups; falls back to default if none match.
        """
        if not stock_codes or not self._stock_email_groups:
            return self._email_config['receivers']
        seen: set = set()
        result: List[str] = []
        for stocks, emails in self._stock_email_groups:
            for code in stock_codes:
                if code in stocks:
                    for e in emails:
                        if e not in seen:
                            seen.add(e)
                            result.append(e)
                    break
        return result if result else self._email_config['receivers']

    def get_all_email_receivers(self) -> List[str]:
        """
        Return union of all configured email receivers (all groups + default).
        Used for market review which should go to everyone.
        """
        seen: set = set()
        result: List[str] = []
        for _, emails in self._stock_email_groups:
            for e in emails:
                if e not in seen:
                    seen.add(e)
                    result.append(e)
        for e in self._email_config['receivers']:
            if e not in seen:
                seen.add(e)
                result.append(e)
        return result

    def _format_sender_address(self, sender: str) -> str:
        """Encode display name safely so non-ASCII sender names work across SMTP providers."""
        sender_name = self._email_config.get('sender_name') or 'stockanalyzingassistant'
        return formataddr((str(Header(str(sender_name), 'utf-8')), sender))

    @staticmethod
    def _close_server(server: Optional[smtplib.SMTP]) -> None:
        """Best-effort SMTP cleanup to avoid leaving sockets open on header/build errors.

        Exceptions from quit()/close() are intentionally silenced — connection may already
        be in a broken state, and there is nothing useful to do at this point.
        """
        if server is None:
            return
        try:
            server.quit()
        except Exception:
            try:
                server.close()
            except Exception:
                pass
    
    def send_to_email(
        self, content: str, subject: Optional[str] = None, receivers: Optional[List[str]] = None
    ) -> bool:
        """
        via SMTP sendingemail（auto-detect SMTP servicehandler）
        
        Args:
            content: emailcontent（support Markdown，willconvertingas HTML）
            subject: emailtopic（optional，defaultautomaticgenerating）
            receivers: recipientlist（optional，defaultuseconfiguration receivers）
            
        Returns:
            whethersendingsuccessful
        """
        if not self._is_email_configured():
            logger.warning("emailconfigurationincomplete，skippush")
            return False
        
        sender = self._email_config['sender']
        password = self._email_config['password']
        receivers = receivers or self._email_config['receivers']
        server: Optional[smtplib.SMTP] = None
        
        try:
            # generatingtopic
            if subject is None:
                date_str = datetime.now().strftime('%Y-%m-%d')
                subject = f"📈 stockintelligentanalysis report - {date_str}"
            
            # will Markdown convertingassimple HTML
            html_content = markdown_to_html_document(content)
            
            # buildemail
            msg = MIMEMultipart('alternative')
            msg['Subject'] = Header(subject, 'utf-8')
            msg['From'] = self._format_sender_address(sender)
            msg['To'] = ', '.join(receivers)
            
            # addplain textand HTML twocountversion
            text_part = MIMEText(content, 'plain', 'utf-8')
            html_part = MIMEText(html_content, 'html', 'utf-8')
            msg.attach(text_part)
            msg.attach(html_part)
            
            # auto-detect SMTP configuration
            domain = sender.split('@')[-1].lower()
            smtp_config = SMTP_CONFIGS.get(domain)
            
            if smtp_config:
                smtp_server = smtp_config['server']
                smtp_port = smtp_config['port']
                use_ssl = smtp_config['ssl']
                logger.info(f"auto-detectemailtype: {domain} -> {smtp_server}:{smtp_port}")
            else:
                # unknownemail，trygenericconfiguration
                smtp_server = f"smtp.{domain}"
                smtp_port = 465
                use_ssl = True
                logger.warning(f"unknownemailtype {domain}，trygenericconfiguration: {smtp_server}:{smtp_port}")
            
            # based onconfigurationselectconnectingmethod
            if use_ssl:
                # SSL connecting（port 465）
                server = smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=30)
            else:
                # TLS connecting（port 587）
                server = smtplib.SMTP(smtp_server, smtp_port, timeout=30)
                server.starttls()
            
            server.login(sender, password)
            server.send_message(msg)
            
            logger.info(f"emailsendingsuccessful，recipient: {receivers}")
            return True
            
        except smtplib.SMTPAuthenticationError:
            logger.error("emailsendingfailed：authenticationerror，pleasecheckemailandauthorizationcodewhethercorrect")
            return False
        except smtplib.SMTPConnectError as e:
            logger.error(f"emailsendingfailed：unable toconnecting SMTP servicehandler - {e}")
            return False
        except Exception as e:
            logger.error(f"sendingemailfailed: {e}")
            return False
        finally:
            self._close_server(server)

    def _send_email_with_inline_image(
        self, image_bytes: bytes, receivers: Optional[List[str]] = None
    ) -> bool:
        """Send email with inline image attachment (Issue #289)."""
        if not self._is_email_configured():
            return False
        sender = self._email_config['sender']
        password = self._email_config['password']
        receivers = receivers or self._email_config['receivers']
        server: Optional[smtplib.SMTP] = None
        try:
            date_str = datetime.now().strftime('%Y-%m-%d')
            subject = f"📈 stockintelligentanalysis report - {date_str}"
            msg = MIMEMultipart('related')
            msg['Subject'] = Header(subject, 'utf-8')
            msg['From'] = self._format_sender_address(sender)
            msg['To'] = ', '.join(receivers)

            alt = MIMEMultipart('alternative')
            alt.attach(MIMEText('reportalreadygenerating，detailedseebelowmethodimage。', 'plain', 'utf-8'))
            html_body = (
                '<p>reportalreadygenerating，detailedseebelowmethodimage（pointhitcanviewlargechart）：</p>'
                '<p><img src="cid:report-image" alt="stockanalysis report" style="max-width:100%%;" /></p>'
            )
            alt.attach(MIMEText(html_body, 'html', 'utf-8'))
            msg.attach(alt)

            img_part = MIMEImage(image_bytes, _subtype='png')
            img_part.add_header('Content-Disposition', 'inline', filename='report.png')
            img_part.add_header('Content-ID', '<report-image>')
            msg.attach(img_part)

            domain = sender.split('@')[-1].lower()
            smtp_config = SMTP_CONFIGS.get(domain)
            if smtp_config:
                smtp_server, smtp_port = smtp_config['server'], smtp_config['port']
                use_ssl = smtp_config['ssl']
            else:
                smtp_server, smtp_port = f"smtp.{domain}", 465
                use_ssl = True

            if use_ssl:
                server = smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=30)
            else:
                server = smtplib.SMTP(smtp_server, smtp_port, timeout=30)
                server.starttls()
            server.login(sender, password)
            server.send_message(msg)
            logger.info("email（inlineimage）sendingsuccessful，recipient: %s", receivers)
            return True
        except Exception as e:
            logger.error("email（inlineimage）sendingfailed: %s", e)
            return False
        finally:
            self._close_server(server)
