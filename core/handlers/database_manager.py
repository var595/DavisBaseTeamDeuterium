import os
import shutil
import traceback
from pyparsing import Dict
from core.elements.Index import Index
from core.elements.system_table import SystemTable
from core.elements.Table import Table
from core.decoders.sql_command_handler import SQLCommandHandler
from core.decoders.output_format import OutputFormat

from core.decoders.sql_query_parser import SQLQueryParser
from core.config.config_manager import ConfigManager
from core.handlers.create_db import create_db


class database_manager:
    @staticmethod
    def command_parse(command: str, tables: Dict, indexes: Dict):
        if command.lower() == "exit;":
            ConfigManager.set_exit(True)
            return
        elif command.lower() == "clear;":
            os.system("cls" if os.name == "nt" else "clear")

        elif command.lower() == "init db;":
            database_manager.create_database(tables, indexes)
            database_manager.save_to_disk(tables, indexes)

        elif command.lower() == "drop db;":
            database_manager.drop_database(tables, indexes)

        elif command.lower() == "help;":
            database_manager.help()
        else:
            try:
                parsed_tokens = SQLQueryParser(command).parse()[0]
                SQLCommandHandler().router(parsed_tokens, tables, indexes)
                database_manager.save_to_disk(tables, indexes)
                database_manager.load_db(tables, indexes)

            except Exception as e:
                print("Invalid command! Check syntax")

    @staticmethod
    def create_database(tables: Dict, indexes: Dict):
        command_handler = SQLCommandHandler()
        command_handler.router(create_db.create_system_tables(), tables, indexes)
        command_handler.router(create_db.create_system_columns(), tables, indexes)

        for entry in create_db.fill_system_tables():
            try:
                command_handler.router(entry, tables, indexes)
            except:
                traceback.print_exc()

        for entry in create_db.fill_system_columns():
            try:
                command_handler.router(entry, tables, indexes)
            except:
                traceback.print_exc()

        print(f"Initial database created!")

        database_manager.save_to_disk(tables, indexes)

    def drop_database(tables, indexes):
        exec_path = ConfigManager.get_exec_path()
        if os.path.exists(os.path.join(exec_path, ConfigManager.get_data_dir())):
            tables.clear()
            indexes.clear()
            for root, dirs, files in os.walk(
                os.path.join(exec_path, ConfigManager.get_data_dir())
            ):
                for f in files:
                    os.unlink(os.path.join(root, f))
                for d in dirs:
                    shutil.rmtree(os.path.join(root, d))

        print(f"Initial database dropped!")

    @staticmethod
    def save_to_disk(tables, indexes):
        tbl_ext = ConfigManager.get_tbl_ext()
        ndx_ext = ConfigManager.get_ndx_ext()
        exec_path = ConfigManager.get_exec_path()

        if not os.path.exists(os.path.join(exec_path, ConfigManager.get_data_dir())):
            os.makedirs(os.path.join(exec_path, ConfigManager.get_data_dir()))

        for k, tab in tables.items():
            if k not in {"system_tables", "system_columns"}:
                rec_count = tab.id_row
                parsed_tokens = SystemTable.update_table_data(k, rec_count)

                SQLCommandHandler().router(parsed_tokens, tables, indexes)
                file_path = os.path.join(
                    exec_path, ConfigManager.get_data_dir(), f"{k}{tbl_ext}"
                )
                with open(file_path, "wb") as f:
                    f.write(tab.object_to_bytes())

        for k, tab in tables.items():
            if k in {"system_tables", "system_columns"}:
                rec_count = tab.id_row
                parsed_tokens = SystemTable.update_table_data(k, rec_count)
                SQLCommandHandler().router(parsed_tokens, tables, indexes)
                file_path = os.path.join(
                    exec_path, ConfigManager.get_data_dir(), f"{k}{tbl_ext}"
                )
                with open(file_path, "wb") as f:
                    f.write(tab.object_to_bytes())

        for k, ndx in indexes.items():
            file_path = os.path.join(
                exec_path, ConfigManager.get_data_dir(), f"{k}{ndx_ext}"
            )
            with open(file_path, "wb") as f:
                f.write(ndx.object_to_bytes())

    @staticmethod
    def help():
        help = [
            "\nclear;  # Clear screen",
            "SHOW TABLES; # List all tables currently in database",
            "INSERT INTO TABLE (<column list>) <table> VALUES (<value list>) # Insert data into a particular table",
            "CREATE TABLE <table> (column datatype constraint); # create a new table in the database",
            "CREATE INDEX <table> (column>); # Create an index on a column",
            "SELECT <column list | *> FROM <table> <WHERE condition>; # sql_query_parser data from the database",
            "UPDATE <table> SET <set directives> <WHERE condition>;  # Update data in the tables",
            "DELETE FROM TABLE <table> <WHERE condition> # Delete data from a table",
            "DROP TABLE <table>; # Delete table from database",
            "DROP INDEX <table> (column>); # Delete an index on a column",
            "exit; # Exit Program",
        ]
        print("\n".join(help))

    @staticmethod
    def load_db(tables: Dict, indexes: Dict):
        command_handler = SQLCommandHandler()
        page_size = ConfigManager.get_page_size()
        exec_path = ConfigManager.get_exec_path()
        tbl_ext = ConfigManager.get_tbl_ext()
        ndx_ext = ConfigManager.get_ndx_ext()
        data_dir = os.path.join(exec_path, ConfigManager.get_data_dir())
        system_tables = os.path.join(data_dir, f"system_tables{tbl_ext}")
        system_columns = os.path.join(data_dir, f"system_columns{tbl_ext}")

        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        if os.path.exists(system_tables) and os.path.exists(system_columns):
            with open(system_tables, "rb") as f:
                OutputFormat.disable_stdout()
                table = Table.bytes_to_object(
                    f.read(),
                    ConfigManager.get_page_size(),
                    2,
                    create_db.system_tables_column_data(),
                    "system_tables",
                )
                OutputFormat.enable_stdout()
                tables["system_tables"] = table
                f.close()

            with open(system_columns, "rb") as f:
                OutputFormat.disable_stdout()
                table = Table.bytes_to_object(
                    f.read(),
                    ConfigManager.get_page_size(),
                    11,
                    create_db.system_columns_column_data(),
                    "system_columns",
                )
                OutputFormat.enable_stdout()
                tables["system_columns"] = table
                f.close()

            pdict = SQLQueryParser(
                "select id_row from system_tables where table_name = 'system_tables';"
            ).parse()[0]
            pdict["ret_mode"] = True
            table_rec_count = command_handler.router(pdict, tables, indexes)

            pdict = SQLQueryParser(
                "select id_row from system_tables where table_name = 'system_columns';"
            ).parse()[0]
            pdict["ret_mode"] = True
            col_rec_count = command_handler.router(pdict, tables, indexes)

            tables["system_tables"].id_row = table_rec_count[-1][-1]
            tables["system_columns"].id_row = col_rec_count[-1][-1]

            pdict = SQLQueryParser("show tables;").parse()[0]
            pdict["ret_mode"] = True
            table_names = command_handler.router(pdict, tables, indexes)
            for t in table_names:
                table_name = t[-1]
                if table_name not in tables:
                    table_name = table_name.lower()
                    table_path = os.path.join(
                        exec_path,
                        ConfigManager.get_data_dir(),
                        f"{table_name}{tbl_ext}",
                    )

                    cdata = SystemTable.parse_column_data(table_name)
                    cdata["ret_mode"] = True
                    cdata_list = command_handler.router(cdata, tables, indexes)
                    cdata_list.sort(key=lambda x: x[2])

                    column_data = {
                        "column_names": [rec[0] for rec in cdata_list],
                        "data_types": [rec[1] for rec in cdata_list],
                        "is_nullable": [rec[3] for rec in cdata_list],
                        "column_key_types": [rec[4] for rec in cdata_list],
                    }

                    tdata = SystemTable.parse_table_data(table_name)
                    tdata["ret_mode"] = True
                    output = command_handler.router(tdata, tables, indexes)

                    rec_count = 0

                    if output:
                        page_size, rec_count = output[0]

                    with open(table_path, "rb") as f:
                        OutputFormat.disable_stdout()
                        new_table = Table.bytes_to_object(
                            f.read(), page_size, rec_count, column_data
                        )
                        OutputFormat.enable_stdout()
                        tables[table_name] = new_table
        for filename in os.listdir(data_dir):
            file_path = os.path.join(data_dir, filename)
            if len(filename.split(".")) == 3:
                table_name = filename.split(".")[0]
                column_name = filename.split(".")[1]
                ext = filename.split(".")[2]
                if ext == ndx_ext[-3:]:
                    with open(file_path, "rb") as f:
                        OutputFormat.disable_stdout()
                        new_index, count = Index.bytes_to_object(f.read(), page_size)
                        new_index.id_row = count
                        OutputFormat.enable_stdout()
                        indexes[table_name + "." + column_name] = new_index
                        tables[table_name].indexes[column_name] = new_index
