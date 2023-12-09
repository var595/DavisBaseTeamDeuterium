from lib.parsers.Query import Query
from lib.parsers.handlers.base_handler import BaseHandler
from lib.objects.SystemTable import SystemTable
from lib.settings.Settings import Settings
from lib.objects.Table import Table


class CreateTableHandler(BaseHandler):
    def handle_command(self, parsed_tokens):
        self.required_fields_check(
            parsed_tokens=parsed_tokens,
            required_field=["table_name", "column_list", "cache"],
        )
        table_name = parsed_tokens.get("table_name", "").lower()
        column_list = parsed_tokens.get("column_list", [])
        cache_tables = parsed_tokens["cache"]["cache_tables"]
        cache_indexes = parsed_tokens["cache"]["cache_indexes"]

        if not self.get_table(
            table_name, cache_tables, cache_indexes, creation_mode=True
        ):
            if table_name not in {"system_tables", "system_columns"}:
                command = "select id_row from system_tables where table_name = 'system_columns';"
                parsed_tokens = Query(command).parse()[0]
                parsed_tokens["ret_mode"] = True
                col_rec_count = self.processor.router(
                    parsed_tokens, cache_tables, cache_indexes
                )

                parsed_tokens = SystemTable.update_table_data(
                    "system_columns", col_rec_count[-1][-1] + len(column_list)
                )
                self.processor.router(parsed_tokens, cache_tables, cache_indexes)

            cache_tables[table_name] = Table.create_table(
                {"table_name": table_name, "column_list": column_list},
                page_size=Settings.get_page_size(),
            )

            print(f"Table {table_name} created.")

        if table_name not in {"system_tables", "system_columns"}:
            table_rec_count = cache_tables["system_tables"].id_row
            pdict = SystemTable.insert_table_data(
                table_rec_count + 1, table_name, Settings.get_page_size(), 0
            )
            self.processor.router(pdict, cache_tables, cache_indexes)

            for column_data in column_list:
                col_rec_count = cache_tables["system_columns"].id_row
                column_data["row_id"] = col_rec_count + 1
                column_data["table_name"] = column_data["table_name"].lower()
                pdict2 = SystemTable.insert_column_data(**column_data)
                self.processor.router(pdict2, cache_tables, cache_indexes)
