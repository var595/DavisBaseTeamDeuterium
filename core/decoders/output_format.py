import sys
import os
from core.config.config_manager import ConfigManager


class OutputFormat:
    @staticmethod
    def splash_screen():
        print(ConfigManager.line("-", 80))
        print(f"{ConfigManager.get_db_name()} CLI")
        print('\nFor a list of supported commands: "help;"')
        print(ConfigManager.line("-", 80))

    @staticmethod
    def disable_stdout():
        sys.stdout = open(os.devnull, "w")

    @staticmethod
    def enable_stdout():
        sys.stdout = sys.__stdout__
