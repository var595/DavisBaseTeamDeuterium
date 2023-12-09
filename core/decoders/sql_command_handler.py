from typing import Dict
from pyparsing import ParseSyntaxException

from core.decoders.handlers.create_table_handler import CreateTableHandler
from core.decoders.handlers.delete_row_handler import DeleteRowHandler
from core.decoders.handlers.drop_index_handler import DropIndexHandler
from core.decoders.handlers.drop_table_handler import DropTableHandler
from core.decoders.handlers.insert_row_handler import InsertRowHandler
from core.decoders.handlers.select_row_handler import SelectRowsHandler
from core.decoders.handlers.update_row_handler import UpdateRowHandler
from core.decoders.handlers.create_index_handler import CreateIndexHandler


class SQLCommandHandler:
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
        return handler.handle_command(parsed_tokens)
