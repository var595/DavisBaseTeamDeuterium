import os
from lib.objects.Table import Table
from lib.parsers.handlers.base_handler import BaseHandler
from lib.settings.Settings import Settings


class DropIndexHandler(BaseHandler):
    def handle_command(self, parsed_tokens):
        self.required_fields_check(
            parsed_tokens=parsed_tokens,
        )
        table_name = parsed_tokens.get("table_name", "").lower()
        column_name = parsed_tokens.get("column_name", "")
        cache_tables = parsed_tokens["cache"]["cache_tables"]
        cache_indexes = parsed_tokens["cache"]["cache_indexes"]
        ndx_ext = Settings.get_ndx_ext()
        exec_path = Settings.get_exec_path()
        name = table_name + "." + column_name

        if name in cache_indexes:
            file_path = os.path.join(
                exec_path, Settings.get_data_dir(), f"{name}{ndx_ext}"
            )
            os.remove(file_path)
            del cache_indexes[name]
            table: Table = self.get_table(
                table_name, cache_tables, cache_indexes, creation_mode=False
            )
            del table.indexes[column_name]
        print(f"Index {name} dropped.")
