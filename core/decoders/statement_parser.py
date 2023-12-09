import pyparsing as pp
from core.decoders.common import (
    COLUMN_LIST,
    CREATE,
    DELETE,
    DROP,
    INDEX,
    INSERT,
    INTO,
    LPAREN,
    MULTI_COLUMN_DEFINITIONS,
    NEW_TABLE_NAME,
    RPAREN,
    STATEMENT_TERMINATOR,
    FROM,
    TABLE,
    UPDATE,
    VALUE_LIST,
    IDENTIFIER,
    SHOW,
    TABLES,
)
from core.decoders.clauses import SELECT_CLAUSE, SET_CLAUSE, WHERE_CLAUSE


class StatementParser:
    def __init__(self):
        self.create_table_statement = (
            pp.Group(CREATE + TABLE).set_parse_action(lambda ls: " ".join(ls[0]))
            + NEW_TABLE_NAME
            + MULTI_COLUMN_DEFINITIONS
            + STATEMENT_TERMINATOR
        ).set_parse_action(self.table_creation_semantics)

        self.drop_table_statement = (
            pp.Group(DROP + TABLE).set_parse_action(lambda ls: " ".join(ls[0]))
            + IDENTIFIER
            + STATEMENT_TERMINATOR
        ).set_parse_action(
            lambda plist: {
                "command": plist[0],
                "table_name": plist[1],
            }
        )

        self.create_index_statement = (
            pp.Group(CREATE + INDEX).set_parse_action(lambda ls: " ".join(ls[0]))
            + IDENTIFIER
            + LPAREN
            + IDENTIFIER
            + RPAREN
            + STATEMENT_TERMINATOR
        ).set_parse_action(
            lambda plist: {
                "command": plist[0],
                "table_name": plist[1],
                "column_name": plist[2],
            }
        )

        self.drop_index_statement = (
            pp.Group(DROP + INDEX).set_parse_action(lambda ls: " ".join(ls[0]))
            + IDENTIFIER
            + LPAREN
            + IDENTIFIER
            + RPAREN
            + STATEMENT_TERMINATOR
        ).set_parse_action(
            lambda plist: {
                "command": plist[0],
                "table_name": plist[1],
                "column_name": plist[2],
            }
        )

        self.insert_row_statement = (
            pp.Group(INSERT + INTO + TABLE).set_parse_action(lambda ls: " ".join(ls[0]))
            + pp.Opt(COLUMN_LIST).set_parse_action(lambda x: [x])
            + IDENTIFIER
            + VALUE_LIST.set_parse_action(lambda x: [x])
            + STATEMENT_TERMINATOR
        ).set_parse_action(
            lambda plist: {
                "command": plist[0],
                "column_name_list": plist[1].as_list(),
                "table_name": plist[2],
                "value_list": plist[3].as_list(),
            }
        )

        self.delete_record_statement = (
            DELETE
            + pp.Suppress(FROM + TABLE)
            + IDENTIFIER
            + pp.Opt(WHERE_CLAUSE).set_parse_action(lambda plist: plist or {})
            + STATEMENT_TERMINATOR
        ).set_parse_action(
            lambda plist: {
                "command": plist[0],
                "table_name": plist[1],
                "condition": plist[2],
            }
        )

        self.update_record_statement = (
            UPDATE
            + IDENTIFIER
            + SET_CLAUSE
            + pp.Opt(WHERE_CLAUSE).set_parse_action(lambda plist: plist or {})
            + STATEMENT_TERMINATOR
        ).set_parse_action(
            lambda plist: {
                "command": plist[0],
                "table_name": plist[1],
                "operation": plist[2],
                "condition": plist[3],
            }
        )

        self.select_statement = (
            SELECT_CLAUSE
            + pp.Suppress(FROM)
            + IDENTIFIER
            + pp.Opt(WHERE_CLAUSE).set_parse_action(lambda plist: plist or {})
            + STATEMENT_TERMINATOR
        ).set_parse_action(
            lambda plist: {
                "command": plist[0],
                "column_name_list": [] if "*" in (x := plist[1].as_list()) else x,
                "table_name": plist[2],
                "condition": plist[3],
            }
        )

        self.show_table_statement = SHOW + TABLES + STATEMENT_TERMINATOR
        self.show_table_statement.set_parse_action(
            lambda: self.select_statement.parse_string(
                "SELECT table_name FROM system_tables;"
            )
        )

        self.statement = (
            self.create_table_statement
            | self.drop_table_statement
            | self.create_index_statement
            | self.drop_index_statement
            | self.insert_row_statement
            | self.delete_record_statement
            | self.update_record_statement
            | self.select_statement
            | self.show_table_statement
        )

    def parse(self, query):
        return self.statement.parse_string(query)

    @staticmethod
    def table_creation_semantics(parse_list):
        (
            cmd,
            t_name,
        ) = parse_list[0:2]
        col_json_list = parse_list[2:]
        columns_list = []
        for i, col_json in enumerate(col_json_list):
            col_json["column_position"] = i + 1
            col_json["table_name"] = t_name
            columns_list.append(col_json)
        return {"command": cmd, "table_name": t_name, "column_list": columns_list}
