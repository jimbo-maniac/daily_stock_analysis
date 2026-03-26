# -*- coding: utf-8 -*-
"""
===================================
stockanalyzingcommand
===================================

analyzingspecifiedstock，call AI generatinganalysis report。
"""

import re
import logging
from typing import List, Optional

from bot.commands.base import BotCommand
from bot.models import BotMessage, BotResponse
from data_provider.base import canonical_stock_code

logger = logging.getLogger(__name__)


class AnalyzeCommand(BotCommand):
    """
    stockanalyzingcommand
    
    analyzingspecifiedstock code，generating AI analysis reportandpush。
    
    usage：
        /analyze 600519       - analyzingKweichow Moutai（Simple Report）
        /analyze 600519 full  - analyzingandgeneratingFull Report
    """
    
    @property
    def name(self) -> str:
        return "analyze"
    
    @property
    def aliases(self) -> List[str]:
        return ["a", "analyzing", "query"]
    
    @property
    def description(self) -> str:
        return "analyzingspecifiedstock"
    
    @property
    def usage(self) -> str:
        return "/analyze <stock code> [full]"
    
    def validate_args(self, args: List[str]) -> Optional[str]:
        """verificationparameter"""
        if not args:
            return "pleaseinputstock code"
        
        code = args[0].upper()

        # verificationstock codeformat
        # A-share：6digit number
        # HK stock：HK+5digit number
        # US stock：1-5uppercase letters+.+2countsuffixcharacterparent
        is_a_stock = re.match(r'^\d{6}$', code)
        is_hk_stock = re.match(r'^HK\d{5}$', code)
        is_us_stock = re.match(r'^[A-Z]{1,5}(\.[A-Z]{1,2})?$', code)

        if not (is_a_stock or is_hk_stock or is_us_stock):
            return f"invalid stock code: {code}（A-share6digit number / HK stockHK+5digit number / US stock1-5countcharacterparent）"
        
        return None
    
    def execute(self, message: BotMessage, args: List[str]) -> BotResponse:
        """executeanalyzingcommand"""
        code = canonical_stock_code(args[0])
        
        # checkwhetherneedFull Report（defaultsimplified，transmit full/complete/detailed switch）
        report_type = "simple"
        if len(args) > 1 and args[1].lower() in ["full", "complete", "detailed"]:
            report_type = "full"
        logger.info(f"[AnalyzeCommand] analyzingstock: {code}, report type: {report_type}")
        
        try:
            # callanalyzingservice
            from src.services.task_service import get_task_service
            from src.enums import ReportType
            
            service = get_task_service()
            
            # submitasynchronousanalyzingtask
            result = service.submit_analysis(
                code=code,
                report_type=ReportType.from_str(report_type),
                source_message=message
            )
            
            if result.get("success"):
                task_id = result.get("task_id", "")
                return BotResponse.markdown_response(
                    f"✅ **analyzingtaskalreadysubmit**\n\n"
                    f"• stock code: `{code}`\n"
                    f"• report type: {ReportType.from_str(report_type).display_name}\n"
                    f"• task ID: `{task_id[:20]}...`\n\n"
                    f"analyzingcompletedwill automatically afterpushresult。"
                )
            else:
                error = result.get("error", "unknownerror")
                return BotResponse.error_response(f"submitanalyzingtaskfailed: {error}")
                
        except Exception as e:
            logger.error(f"[AnalyzeCommand] executefailed: {e}")
            return BotResponse.error_response(f"analyzingfailed: {str(e)[:100]}")
