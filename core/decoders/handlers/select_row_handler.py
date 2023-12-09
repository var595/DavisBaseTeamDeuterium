import pandas as pd
from tabulate import tabulate
from core.decoders.handlers.base_handler import BaseHandler
from core.elements.Table import Table


class SelectRowsHandler(BaseHandler):
    def table_format_print(self, output_list, columns):
        df = pd.DataFrame(output_list, columns=columns)
        print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    def handle_command(self, parsed_tokens):
        self.required_fields_check(
            parsed_tokens=parsed_tokens,
            required_field=["table_name", "column_name_list", "cache"],
        )
        table_name = parsed_tokens.get("table_name", "").lower()
        column_name_list = parsed_tokens.get("column_name_list", [])
        condition = parsed_tokens.get("condition", {})
        cache_tables = parsed_tokens["cache"]["cache_tables"]
        cache_indexes = parsed_tokens["cache"]["cache_indexes"]

        table_obj: Table = self.get_table(table_name, cache_tables, cache_indexes)

        selections, cname_list = table_obj.select(
            {"column_name_list": column_name_list, "condition": condition}
        )
        if parsed_tokens.get("ret_mode", False):
            return selections

        self.table_format_print(selections, cname_list)
