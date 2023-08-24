
import os
import traceback
from lib.parsers.OutputFormat import OutputFormat
from lib.settings.Settings import Settings
from lib.workers.DatabaseActions import DatabaseActions

DEBUG = True

def main_loop():

    Settings.set_exec_path(os.path.dirname(os.path.realpath(__file__)))
    
    OutputFormat.splash_screen()

    in_memory_tables = {}
    in_memory_indexes = {}
    
    DatabaseActions.load_db(in_memory_tables, in_memory_indexes)
    while not Settings.is_exit():
        print("\n")
        usr_in = [input(Settings.get_prompt())]

        cont = not usr_in[0].endswith(";")

        while cont:
            temp = input()
            if temp.endswith(";") or temp == "":
                cont = False
            usr_in += [temp]

        usr_in = " ".join(usr_in)
        try:
            DatabaseActions.command_parse(usr_in, in_memory_tables, in_memory_indexes)
        except Exception as e:
            if DEBUG:
                traceback.print_exc()
            else:
                print("\n")
                print(Settings.line("-", 80))
                print(traceback.format_exception_only(e.__class__, e)[-1])
                print(Settings.line("-", 80))
            continue

if __name__ == "__main__":
    main_loop()