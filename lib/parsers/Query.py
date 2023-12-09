from lib.parsers.statement_parser import StatementParser


class Query:
    def __init__(self, query) -> None:
        self.query = query
        self.parser = StatementParser()

    def parse(self):
        return self.parser.parse(self.query)
