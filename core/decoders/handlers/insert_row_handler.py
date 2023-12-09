from core.decoders.handlers.base_handler import BaseHandler
from core.decoders.handlers.update_index_handler import UpdateIndexHandler
from core.elements.Table import Table


class InsertRowHandler(BaseHandler):
    def __init__(self, processor):
        super().__init__(processor)
        self.update_index_handler = UpdateIndexHandler(processor)

    def handle_command(self, parsed_tokens):
        self.required_fields_check(
            parsed_tokens=parsed_tokens,
            required_field=["table_name", "column_name_list", "cache", "value_list"],
        )
        table_name = parsed_tokens.get("table_name", "").lower()
        column_name_list = parsed_tokens.get("column_name_list", [])
        value_list = parsed_tokens.get("value_list", [])
        cache_tables = parsed_tokens["cache"]["cache_tables"]
        cache_indexes = parsed_tokens["cache"]["cache_indexes"]

        table_obj: Table = self.get_table(table_name, cache_tables, cache_indexes)

        table_obj.insert(
            {"column_name_list": column_name_list, "value_list": value_list}
        )

        for column_name in table_obj.column_data["column_names"]:
            if column_name in table_obj.indexes:
                self.update_index_handler.handle_command(
                    {
                        "table_name": table_name,
                        "column_name": column_name,
                        "cache": parsed_tokens["cache"],
                    }
                )
