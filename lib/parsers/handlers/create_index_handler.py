from lib.objects.Index import Index
from lib.objects.Table import Table
from lib.parsers.handlers.base_handler import BaseHandler
from lib.settings.Settings import Settings
from lib.parsers.handlers.update_index_handler import UpdateIndexHandler


class CreateIndexHandler(BaseHandler):
    def __init__(self, processor):
        super().__init__(processor)
        self.update_index_handler = UpdateIndexHandler(processor)

    def handle_command(self, parsed_tokens):
        self.required_fields_check(parsed_tokens=parsed_tokens)
        table_name = parsed_tokens.get("table_name", "").lower()
        column_name = parsed_tokens.get("column_name", "")
        cache_tables = parsed_tokens["cache"]["cache_tables"]
        cache_indexes = parsed_tokens["cache"]["cache_indexes"]

        if not self.get_index(
            table_name, column_name, cache_tables, cache_indexes, creation_mode=True
        ):
            cache_indexes[table_name + "." + column_name] = Index.create_index(
                {"table_name": table_name, "column_name": column_name},
                page_size=Settings.get_page_size(),
            )
            self.update_index_handler.handle_command(
                {
                    "table_name": table_name,
                    "column_name": column_name,
                    "cache": parsed_tokens["cache"],
                }
            )
            table: Table = self.get_table(
                table_name, cache_tables, cache_indexes, creation_mode=False
            )
            table.indexes[column_name] = cache_indexes[table_name + "." + column_name]
        print(f"Index {table_name}.{column_name} created.")
