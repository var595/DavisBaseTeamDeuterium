import os
from lib.objects.SystemTable import SystemTable
from lib.parsers.handlers.base_handler import BaseHandler
from lib.parsers.handlers.drop_index_handler import DropIndexHandler
from lib.settings.Settings import Settings


class DropTableHandler(BaseHandler):
    def __init__(self, processor):
        super().__init__(processor)
        self.drop_index_handler = DropIndexHandler(processor)

    def handle_command(self, parsed_tokens):
        self.required_fields_check(
            parsed_tokens=parsed_tokens,
            required_field=["table_name", "cache"],
        )
        table_name = parsed_tokens.get("table_name", "").lower()
        cache_tables = parsed_tokens["cache"]["cache_tables"]
        cache_indexes = parsed_tokens["cache"]["cache_indexes"]

        tbl_ext = Settings.get_tbl_ext()
        exec_path = Settings.get_exec_path()
        if table_obj := self.get_table(table_name, cache_tables, cache_indexes):
            file_path = os.path.join(
                exec_path, Settings.get_data_dir(), f"{table_name}{tbl_ext}"
            )
            os.remove(file_path)
            del cache_tables[table_name]
            del table_obj

        index_copy = cache_indexes.copy()

        for index in index_copy:
            if index.startswith(table_name):
                table, column = index.split(".")[0:2]
                self.drop_index_handler.handle_command(
                    {"table_name": table, "column_name": column}
                )

        p1 = SystemTable.delete_table_data(table_name)
        self.processor.router(p1, cache_tables, cache_indexes)
        p2 = SystemTable.delete_column_data(table_name)
        self.processor.router(p2, cache_tables, cache_indexes)

        print(f"Table {table_name} dropped.")
