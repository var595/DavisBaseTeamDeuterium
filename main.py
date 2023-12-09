import os
import traceback
from core.decoders.output_format import OutputFormat
from core.config.config_manager import ConfigManager
from core.handlers.database_manager import database_manager

DEBUG = True


def main_loop():
    ConfigManager.set_exec_path(os.path.dirname(os.path.realpath(__file__)))

    OutputFormat.splash_screen()

    in_memory_tables = {}
    in_memory_indexes = {}

    database_manager.load_db(in_memory_tables, in_memory_indexes)
    while not ConfigManager.is_exit():
        print("\n")
        usr_in = [input(ConfigManager.get_prompt())]

        cont = not usr_in[0].endswith(";")

        while cont:
            temp = input()
            if temp.endswith(";") or temp == "":
                cont = False
            usr_in += [temp]

        usr_in = " ".join(usr_in)
        try:
            database_manager.command_parse(usr_in, in_memory_tables, in_memory_indexes)
        except Exception as e:
            if DEBUG:
                traceback.print_exc()
            else:
                print("\n")
                print(ConfigManager.line("-", 80))
                print(traceback.format_exception_only(e.__class__, e)[-1])
                print(ConfigManager.line("-", 80))
            continue


if __name__ == "__main__":
    main_loop()
