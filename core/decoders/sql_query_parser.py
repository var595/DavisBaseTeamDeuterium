from core.decoders.statement_parser import StatementParser


class SQLQueryParser:
    def __init__(self, query) -> None:
        self.query = query
        self.parser = StatementParser()

    def parse(self):
        return self.parser.parse(self.query)
