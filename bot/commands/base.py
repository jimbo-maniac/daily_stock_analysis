# -*- coding: utf-8 -*-
"""
===================================
commandbaseclass
===================================

definecommandhandlerabstractionbaseclass，allcommandallmustinheritancethisclass。
"""

from abc import ABC, abstractmethod
from typing import List, Optional

from bot.models import BotMessage, BotResponse


class BotCommand(ABC):
    """
    commandhandlerabstractionbaseclass
    
    allcommandallmustinheritancethisclassandimplementabstract method。
    
    useExample：
        class MyCommand(BotCommand):
            @property
            def name(self) -> str:
                return "mycommand"
            
            @property
            def aliases(self) -> List[str]:
                return ["mc", "Icommand"]
            
            @property
            def description(self) -> str:
                return "thisisIcommand"
            
            @property
            def usage(self) -> str:
                return "/mycommand [parameter]"
            
            def execute(self, message: BotMessage, args: List[str]) -> BotResponse:
                return BotResponse.text_response("commandexecutesuccessful")
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """
        commandname（notincludeprefix）
        
        for example "analyze"，userinput "/analyze" trigger
        """
        pass
    
    @property
    @abstractmethod
    def aliases(self) -> List[str]:
        """
        commandaliaslist
        
        for example ["a", "analyzing"]，userinput "/a" or "analyzing" can alsotrigger
        """
        pass
    
    @property
    @abstractmethod
    def description(self) -> str:
        """commanddescription（forhelp information）"""
        pass
    
    @property
    @abstractmethod
    def usage(self) -> str:
        """
        usage instructions（forhelp information）
        
        for example "/analyze <stock code>"
        """
        pass
    
    @property
    def hidden(self) -> bool:
        """
        whether inhelplistinhide
        
        default False，set to True thennotdisplayin /help listin
        """
        return False
    
    @property
    def admin_only(self) -> bool:
        """
        whetheronlyadminavailable
        
        default False，set to True thenrequires adminpermission
        """
        return False
    
    @abstractmethod
    def execute(self, message: BotMessage, args: List[str]) -> BotResponse:
        """
        executecommand
        
        Args:
            message: rawmessageobject
            args: commandparameterlist（alreadysplitting）
            
        Returns:
            BotResponse responseobject
        """
        pass
    
    def validate_args(self, args: List[str]) -> Optional[str]:
        """
        verificationparameter
        
        sub-classcanoverridethismethodproceedparametervalidate。
        
        Args:
            args: commandparameterlist
            
        Returns:
            ifparametervalidreturn None，nothen returnerror message
        """
        return None
    
    def get_help_text(self) -> str:
        """gethelptext"""
        return f"**{self.name}** - {self.description}\nusage: `{self.usage}`"
