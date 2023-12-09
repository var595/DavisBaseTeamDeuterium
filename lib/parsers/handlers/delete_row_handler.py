from lib.objects.Table import Table
from lib.parsers.handlers.base_handler import BaseHandler
from lib.parsers.handlers.update_index_handler import UpdateIndexHandler


class DeleteRowHandler(BaseHandler):
    def __init__(self, processor):
        super().__init__(processor)
        self.update_index_handler = UpdateIndexHandler(processor)

    def handle_command(self, parsed_tokens):
        self.required_fields_check(
            parsed_tokens=parsed_tokens,
            required_field=["table_name", "cache"],
        )
        table_name = parsed_tokens.get("table_name", "").lower()
        condition = parsed_tokens.get("condition", {})
        cache_tables = parsed_tokens["cache"]["cache_tables"]
        cache_indexes = parsed_tokens["cache"]["cache_indexes"]

        table_obj: Table = self.get_table(table_name, cache_tables, cache_indexes)

        if condition:
            table_obj.delete(condition)
        else:
            table_obj.delete()

        for column_name in table_obj.column_data["column_names"]:
            if column_name in table_obj.indexes:
                self.update_index_handler.handle_command(
                    {
                        "table_name": table_name,
                        "column_name": column_name,
                        "cache": parsed_tokens["cache"],
                    }
                )
