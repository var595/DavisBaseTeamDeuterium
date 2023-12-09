from typing import Dict
from pyparsing import ParseSyntaxException

from lib.parsers.handlers.create_table_handler import CreateTableHandler
from lib.parsers.handlers.delete_row_handler import DeleteRowHandler
from lib.parsers.handlers.drop_index_handler import DropIndexHandler
from lib.parsers.handlers.drop_table_handler import DropTableHandler
from lib.parsers.handlers.insert_row_handler import InsertRowHandler
from lib.parsers.handlers.select_row_handler import SelectRowsHandler
from lib.parsers.handlers.update_row_handler import UpdateRowHandler
from lib.parsers.handlers.create_index_handler import CreateIndexHandler


class CommandProcessor:
    def __init__(self):
        self.handlers = {
            "CREATE TABLE": CreateTableHandler(self),
            "CREATE INDEX": CreateIndexHandler(self),
            "DROP TABLE": DropTableHandler(self),
            "DROP INDEX": DropIndexHandler(self),
            "INSERT INTO TABLE": InsertRowHandler(self),
            "DELETE": DeleteRowHandler(self),
            "UPDATE": UpdateRowHandler(self),
            "SELECT": SelectRowsHandler(self),
        }

    def router(self, parsed_tokens, in_memory_tables, in_memory_indexes: Dict):
        command = parsed_tokens.get("command", None)

        parsed_tokens["cache"] = {
            "cache_tables": in_memory_tables,
            "cache_indexes": in_memory_indexes,
        }

        if command.upper() not in self.handlers:
            raise ParseSyntaxException("", 0, msg="Clause not detected.")

        handler = self.handlers[command]
        print(handler)
        return handler.handle_command(parsed_tokens)
