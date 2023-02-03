"""class_base_class.py"""

from class_text_art import TextArt

class BaseClass():
    """BaseClass: all classes extends from this class"""
    __log_active__ = False
    art = None
    def __init__(self, __log_active__):
        self._log("BaseClass:__init__")
        self.__log_active__ = __log_active__
        self.art = TextArt()

    def _log(self,var):
        """function _log"""
        if self.__log_active__:
            print(f"LOG:{var}")

    def _art(self,var):
        """function _art"""
        return self.art.get_art(var)
