import inspect
import os
from re import L
from typing import Dict, List
import pandas as pd
from pyparsing import ParseSyntaxException
from tabulate import tabulate
from lib.objects.Index import Index
from lib.objects.SystemTable import SystemTable
from lib.objects.Table import Table
from lib.parsers.Query import Query
from lib.settings.Settings import Settings

class Command:
    def actions(cls):
        return {
            "CREATE TABLE": cls.create_table,
            "CREATE INDEX": cls.create_index,
            "DROP TABLE": cls.drop_table,
            "DROP INDEX": cls.drop_index,
            "INSERT INTO TABLE": cls.insert_row,
            "DELETE": cls.delete_row,
            "UPDATE": cls.update_row,
            "SELECT": cls.select_rows
            }

    @classmethod
    def router(cls,parsed_tokens,in_memory_tables, 
        in_memory_indexes: Dict):

        command = parsed_tokens.get("command", None)

        parsed_tokens["cache"] = {
            "cache_tables": in_memory_tables,
            "cache_indexes": in_memory_indexes
        }

        if command.upper() not in cls.actions(cls):
            raise ParseSyntaxException("", 0, msg="Clause not detected.")

        function_ptr = cls.actions(cls)[command]
        
        if command.upper() != "SELECT":
            sign = inspect.signature(function_ptr)
            for function_arg in sign.parameters.keys():
                if function_arg not in parsed_tokens:
                    raise ParseSyntaxException("", 0, msg="Malformed SQL Statement.")

        del parsed_tokens["command"]

        return function_ptr(**parsed_tokens)

    @classmethod
    def get_table(cls,tname: str, in_mem_tables: Dict, in_mem_idx: Dict, creation_mode=False):
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

    
    @classmethod
    def get_index(cls,tname: str, cname: str, in_mem_tables: Dict, in_mem_idx: Dict, creation_mode=False):
        tname = tname.lower()
        name = tname+"."+cname

        if creation_mode:

            if name  in in_mem_idx:
                raise ValueError(f"Index named {name} already exists!")
            else:
                return False
        else:
            if name in in_mem_idx:
                return in_mem_idx[name]
            else:
                raise FileNotFoundError(f"Index named {name} not found!")

    @classmethod
    def create_table(cls,table_name: str, column_list: List[Dict], cache: Dict):
        table_name = table_name.lower()
        cache_tables, cache_indexes = map(lambda x: cache[x], ["cache_tables", "cache_indexes"])
        
        if not cls.get_table(table_name, cache_tables, cache_indexes, creation_mode=True):

            if table_name not in {"system_tables", "system_columns"}:
                command = "select id_row from system_tables where table_name = 'system_columns';"
                parsed_tokens = Query(command).parse()[0]
                parsed_tokens["ret_mode"] = True
                col_rec_count = Command.router(parsed_tokens, cache_tables, cache_indexes)

                parsed_tokens = SystemTable.update_table_data('system_columns', col_rec_count[-1][-1] + len(column_list))     
                Command.router(parsed_tokens, cache_tables, cache_indexes)

            cache_tables[table_name] = Table.create_table(
                {
                    "table_name": table_name,
                    "column_list": column_list
                },
                page_size=Settings.get_page_size()
            )
          
            print(f"Table {table_name} created.")

        
            
        if table_name not in {"system_tables", "system_columns"}:
            table_rec_count = cache_tables["system_tables"].id_row
            pdict = SystemTable.insert_table_data(table_rec_count + 1, table_name, Settings.get_page_size(), 0)
            cls.router(pdict, cache_tables, cache_indexes)
            
            
            for column_data in column_list:
                col_rec_count = cache_tables["system_columns"].id_row
                column_data["row_id"] = col_rec_count + 1
                column_data["table_name"] = column_data["table_name"].lower()
                pdict2 = SystemTable.insert_column_data(**column_data)
                cls.router(pdict2, cache_tables, cache_indexes)

        return

    @classmethod
    def create_index(cls,table_name: str, column_name: str, cache: Dict):
        table_name = table_name.lower()
        name = table_name+"."+column_name
        cache_tables, cache_indexes = map(lambda x: cache[x], ["cache_tables", "cache_indexes"])
        
        
        if not cls.get_index(table_name,column_name,cache_tables, cache_indexes, creation_mode=True):
            cache_indexes[name] = Index.create_index(
                {
                    "table_name": table_name,
                    "column_name": column_name
                },
                page_size=Settings.get_page_size()
            )
            cls.update_index(table_name,column_name,cache)
            table : Table = cls.get_table(table_name, cache_tables, cache_indexes, creation_mode=False)
            table.indexes[column_name] = cache_indexes[name]
        print(f"Index {name} created.")

    @classmethod
    def drop_index(cls,table_name: str, column_name: str, cache: Dict):
        table_name = table_name.lower()
        name = table_name+"."+column_name
        cache_tables, cache_indexes = map(lambda x: cache[x], ["cache_tables", "cache_indexes"])
        ndx_ext = Settings.get_ndx_ext()
        exec_path = Settings.get_exec_path()

        if name in cache_indexes:
            file_path = os.path.join(exec_path,Settings.get_data_dir(),f"{name}{ndx_ext}")
            os.remove(file_path)
            del cache_indexes[name]
        print(f"Index {name} dropped.")
            

    @classmethod
    def drop_table(cls,table_name: str, cache: Dict):
        table_name = table_name.lower()
        cache_tables, cache_indexes = map(lambda x: cache[x], ["cache_tables", "cache_indexes"])

        tbl_ext = Settings.get_tbl_ext()
        exec_path = Settings.get_exec_path()
        if table_obj := cls.get_table(table_name, cache_tables, cache_indexes):
            file_path = os.path.join(exec_path,Settings.get_data_dir(),f"{table_name}{tbl_ext}")
            os.remove(file_path)
            del cache_tables[table_name]
            del table_obj

        index_copy = cache_indexes.copy()

        for index in index_copy:
            if index.startswith(table_name):
                table,column = index.split(".")[0:2]
                cls.drop_index(table,column,cache)
            
        p1 = SystemTable.delete_table_data(table_name)
        cls.router(p1, cache_tables, cache_indexes)
        p2 = SystemTable.delete_column_data(table_name)
        cls.router(p2, cache_tables, cache_indexes)

        print(f"Table {table_name} dropped.")

        return

    @classmethod
    def insert_row(cls,table_name: str, column_name_list: List[str], value_list: List[str], cache: Dict):
        table_name = table_name.lower()
        cache_tables, cache_indexes = map(lambda x: cache[x], ["cache_tables", "cache_indexes"])

        table_obj: Table = cls.get_table(table_name, cache_tables, cache_indexes)

        table_obj.insert(
            {
                "column_name_list": column_name_list,
                "value_list": value_list
            }
        )

        for i in table_obj.column_data["column_names"]:
            if i in table_obj.indexes:
                cls.update_index(table_name,i,cache)

        return
    
    @classmethod
    def delete_row(cls,table_name: str, condition: Dict, cache: Dict):
        table_name = table_name.lower()
        cache_tables, cache_indexes = map(lambda x: cache[x], ["cache_tables", "cache_indexes"])

        table_obj: Table = cls.get_table(table_name, cache_tables, cache_indexes)

        if condition:
            table_obj.delete(condition)

        else:
            table_obj.delete()

        for i in table_obj.column_data["column_names"]:
            if i in table_obj.indexes:
                cls.update_index(table_name,i,cache)

        return

    @classmethod
    def update_row(cls,table_name: str, operation: Dict, condition: Dict, cache: Dict):
        table_name = table_name.lower()
        cache_tables, cache_indexes = map(lambda x: cache[x], ["cache_tables", "cache_indexes"])

        table_obj: Table = cls.get_table(table_name, cache_tables, cache_indexes)

        table_obj.update(
            update_op=operation,
            condition=condition
        )

        for i in table_obj.column_data["column_names"]:
            if i in table_obj.indexes:
                cls.update_index(table_name,i,cache)
        return
    
    @classmethod
    def select_rows(cls,table_name: str, column_name_list: List[str], condition: Dict, cache: Dict, ret_mode: bool = False):
        table_name = table_name.lower()
        cache_tables, cache_indexes = map(lambda x: cache[x], ["cache_tables", "cache_indexes"])

        table_obj: Table = cls.get_table(table_name, cache_tables, cache_indexes)

        selections, cname_list = table_obj.select(
            {
                "column_name_list": column_name_list,
                "condition": condition
            }
        )
        
        if ret_mode:
            return selections

        cls.table_format_print(selections, cname_list)

    @classmethod
    def update_index(cls,table_name: str,column_name : str, cache: Dict):
        table_name = table_name.lower()
        cache_tables, cache_indexes = map(lambda x: cache[x], ["cache_tables", "cache_indexes"])
        index_obj: Index = cls.get_index(table_name,column_name, cache_tables, cache_indexes)
        index_obj.build_index(table_name,column_name,cache_tables)
        return
    
    @classmethod
    def table_format_print(cls,output_list, columns):
        df = pd.DataFrame(output_list, columns=columns)
        print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))
        

   