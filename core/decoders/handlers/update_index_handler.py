from core.decoders.handlers.base_handler import BaseHandler
from core.elements.Index import Index


class UpdateIndexHandler(BaseHandler):
    def handle_command(self, parsed_tokens):
        self.required_fields_check(
            parsed_tokens=parsed_tokens,
            required_field=["table_name", "column_name", "cache"],
        )
        table_name = parsed_tokens["table_name"].lower()
        column_name = parsed_tokens["column_name"]
        cache_tables = parsed_tokens["cache"]["cache_tables"]
        cache_indexes = parsed_tokens["cache"]["cache_indexes"]

        index_obj: Index = self.get_index(
            table_name, column_name, cache_tables, cache_indexes
        )

        index_obj.build_index(table_name, column_name, cache_tables)
