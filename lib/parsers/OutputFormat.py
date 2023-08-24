
from string import Template
import sys
import os
from lib.settings.Settings import Settings


class OutputFormat:
    @staticmethod
    def splash_screen():
        print(Settings.line("-", 80))
        print(f"{Settings.get_db_name()} CLI")
        print("\nFor supported commands use: \"help;\".")
        print(Settings.line("-", 80)
)

    @staticmethod
    def disable_stdout():
        sys.stdout = open(os.devnull, 'w')

    @staticmethod
    def enable_stdout():
        sys.stdout = sys.__stdout__
