import pyparsing as pp
from lib.parsers.common import (
    IDENTIFIER,
    COMPARATORS,
    LITERAL_VALUE,
    NOT,
    EQUAL,
    SET,
    WHERE,
    SELECT,
)

PREDICATE = (
    pp.Opt(NOT).set_parse_action(lambda x: "TRUE" if x else "FALSE")
    + IDENTIFIER
    + COMPARATORS
    + LITERAL_VALUE
)
PREDICATE.set_parse_action(
    lambda plist: {
        "negated": plist[0],
        "column_name": plist[1],
        "comparator": plist[2],
        "value": plist[3],
    }
)

SET_CLAUSE = pp.Suppress(SET) + IDENTIFIER + EQUAL + LITERAL_VALUE
SET_CLAUSE.set_parse_action(
    lambda plist: {
        "column_name": plist[0],
        "comparator": "=",
        "value": plist[1],
    }
)

WHERE_CLAUSE = pp.Suppress(WHERE) + PREDICATE
SELECT_CLAUSE = SELECT + pp.Group(pp.Literal("*") | pp.delimited_list(IDENTIFIER))
