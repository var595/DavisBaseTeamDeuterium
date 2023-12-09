import pyparsing as pp


def constraint_semantics(constraint_list):
    constraint_set = set(constraint_list)
    constraint_dict = {"column_key": "", "is_nullable": "YES"}
    if "PRIMARY_KEY" in constraint_set:
        constraint_dict["column_key"] = "PRI"
        constraint_dict["is_nullable"] = "NO"
    elif "UNIQUE" in constraint_set:
        constraint_dict["column_key"] = "UNI"
        constraint_dict["is_nullable"] = "NO"
    if "NOT_NULL" in constraint_set:
        constraint_dict["is_nullable"] = "NO"
    return constraint_dict


def column_definition_semantics(def_list):
    name, d_type, prop_dict = def_list
    prop_dict["column_name"] = name
    prop_dict["data_type"] = d_type
    return prop_dict


# Data Types
(
    NULL,
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    LONG,
    FLOAT,
    DOUBLE,
    YEAR,
    TIME,
    DATETIME,
    DATE,
    TEXT,
) = map(
    pp.CaselessKeyword,
    [
        "NULL",
        "TINYINT",
        "SMALLINT",
        "INT",
        "BIGINT",
        "LONG",
        "FLOAT",
        "DOUBLE",
        "YEAR",
        "TIME",
        "DATETIME",
        "DATE",
        "TEXT",
    ],
)
NULL.set_parse_action(lambda: "")

DATA_TYPES = (
    NULL
    | TINYINT
    | SMALLINT
    | INT
    | BIGINT
    | LONG
    | FLOAT
    | DOUBLE
    | YEAR
    | TIME
    | DATETIME
    | DATE
    | TEXT
).set_name("data type")

# Keywords
(
    SELECT,
    CREATE,
    DROP,
    SHOW,
    INSERT,
    DELETE,
    UPDATE,
    NOT,
    UNIQUE,
    PRIMARY,
    KEY,
    TABLES,
    TABLE,
    INDEX,
    VALUES,
    FROM,
    INTO,
    WHERE,
    SET,
) = map(
    pp.CaselessKeyword,
    [
        "SELECT",
        "CREATE",
        "DROP",
        "SHOW",
        "INSERT",
        "DELETE",
        "UPDATE",
        "NOT",
        "UNIQUE",
        "PRIMARY",
        "KEY",
        "TABLES",
        "TABLE",
        "INDEX",
        "VALUES",
        "FROM",
        "INTO",
        "WHERE",
        "SET",
    ],
)

KEYWORD = (
    SELECT
    | CREATE
    | DROP
    | SHOW
    | INSERT
    | DELETE
    | UPDATE
    | NOT
    | UNIQUE
    | PRIMARY
    | KEY
    | TABLES
    | TABLE
    | INDEX
    | VALUES
    | FROM
    | INTO
    | WHERE
    | SET
    | DATA_TYPES
)
KEYWORD.set_name("keyword")

# Common
COMPARATORS = pp.one_of([">", "<", ">=", "<=", "=", "<>"]).set_name("comparator")
LPAREN, RPAREN, EQUAL, STATEMENT_TERMINATOR = map(pp.Suppress, "()=;")

# Identifiers
DB_TABLES, DB_COLUMNS = map(
    pp.CaselessLiteral, ("davisbase_tables", "davisbase_columns")
)
RESERVED_IDENTIFIERS = DB_TABLES | DB_COLUMNS
RESERVED_IDENTIFIERS.set_name("reserved identifier")
IDENTIFIER = ~KEYWORD + pp.Word(pp.alphas, pp.alphanums + "_")
IDENTIFIER.set_name("identifier")

# Literal Values
INTEGER = pp.Regex(r"[+-]?\d+").set_name("integer")

NUMERIC_LITERAL = pp.Regex(r"[+-]?\d*\.?\d+([eE][+-]?\d+)?").set_name("real number")

STRING_LITERAL = pp.quoted_string.set_parse_action(
    lambda toks: str.replace(toks[0], "'", "").replace('"', "")
)
STRING_LITERAL.set_name("string")

LITERAL_VALUE = NUMERIC_LITERAL | STRING_LITERAL | NULL
LITERAL_VALUE.set_name("value literal")

VALUE_LIST = pp.Suppress(VALUES) + LPAREN + pp.delimited_list(LITERAL_VALUE) + RPAREN
VALUE_LIST.set_name("list of column values")

# Primary Key and Other Definitions
PRIMARY_KEY = pp.Group(PRIMARY + KEY).set_parse_action(lambda: "PRIMARY_KEY")
NOT_NULL = pp.Group(NOT + NULL).set_parse_action(lambda: "NOT_NULL")

COLUMN_CONSTRAINT_OPTS = UNIQUE | PRIMARY_KEY | NOT_NULL
COLUMN_CONSTRAINTS = COLUMN_CONSTRAINT_OPTS * (0, 3)
COLUMN_CONSTRAINTS.set_parse_action(constraint_semantics)

NEW_COL_NAME = ~RESERVED_IDENTIFIERS + IDENTIFIER
COLUMN_DEFINITION = NEW_COL_NAME + DATA_TYPES + COLUMN_CONSTRAINTS
COLUMN_DEFINITION.set_parse_action(column_definition_semantics)
MULTI_COLUMN_DEFINITIONS = LPAREN + pp.delimited_list(COLUMN_DEFINITION) + RPAREN
NEW_TABLE_NAME = NEW_COL_NAME.copy()

COLUMN_LIST = LPAREN + pp.delimited_list(IDENTIFIER) + RPAREN
COLUMN_LIST.set_name("paranthesized column name list")
