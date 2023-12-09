from typing import Dict

from lib.parsers.utils import check_keys_not_in_dict
from pyparsing import ParseSyntaxException


class BaseHandler:
    def __init__(self, processor):
        self.processor = processor

    def required_fields_check(self, required_field=None, parsed_tokens={}):
        if required_field is None:
            required_field = ["table_name", "column_name", "cache"]
        if check_keys_not_in_dict(required_field, parsed_tokens):
            raise ParseSyntaxException("", 0, msg="Malformed SQL Statement.")

    def handle_command(self, parsed_tokens):
        raise NotImplementedError("Subclasses must implement handle_command method.")

    def get_table(
        self, tname: str, in_mem_tables: Dict, in_mem_idx: Dict, creation_mode=False
    ):
        tname = tname.lower()

        if creation_mode:
            if tname in in_mem_tables:
                raise ValueError(f"Table named {tname} already exists!")
            else:
                return False
        else:
            if tname in in_mem_tables:
                return in_mem_tables[tname]
            else:
                raise FileNotFoundError(f"Table named {tname} not found!")

    def get_index(
        self,
        tname: str,
        cname: str,
        in_mem_tables: Dict,
        in_mem_idx: Dict,
        creation_mode=False,
    ):
        tname = tname.lower()
        name = tname + "." + cname

        if creation_mode:
            if name in in_mem_idx:
                raise ValueError(f"Index named {name} already exists!")
            else:
                return False
        else:
            if name in in_mem_idx:
                return in_mem_idx[name]
            else:
                raise FileNotFoundError(f"Index named {name} not found!")
