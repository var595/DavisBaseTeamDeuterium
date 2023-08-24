from __future__ import annotations
import datetime as dt
import io
import traceback
from typing import Any, Dict, List
import numpy as np
import math
from lib.objects.BPlusNode import BPlusNode
from lib.objects.BPlusTree import BPlusTree
from lib.objects.DataPointer import DataPointer
from lib.objects.DataType import DataType
from lib.objects.Index import Index
from lib.objects.PageHeader import PageHeader
from lib.objects.PageType import PageType
from lib.objects.DataCell import DataCell
from lib.objects.Record import Record
from lib.workers.PageWriter import PageWriter
from lib.workers.LeafWriter import LeafWriter

class Table:

    def __init__(self, page_size = 512) -> None:
        self.col_data_keys = {"column_names", "data_types", "is_nullable", "column_key_types"}
        self.indexes = {}
        page_space = page_size - 16
        self.max_rec_size = math.floor(page_space/6) - 2

        self.bptree = BPlusTree()
        self.column_data = {
            "column_names": [],
            "data_types": [],
            "is_nullable": [],
            "column_key_types": []
        }
        self.id_row = 0
        self.data_pointers = []
        self.page_size = page_size
        self.name = ""
        self.recently_deleted = set()

    @classmethod
    def create_table(cls, create_op: Dict, page_size=512) -> Table:

        new_table = cls(page_size)

        col_list: List = create_op["column_list"]
        col_list.sort(key=lambda x: x.get("column_position", 0))

        column_data = {
            "column_names": [],
            "data_types": [],
            "is_nullable": [],
            "column_key_types": []
        }

        for column in col_list:
            column_data["column_key_types"].append(column["column_key"])
            column_data["is_nullable"].append(column["is_nullable"])
            column_data["column_names"].append(column["column_name"])
            dtype_enum = DataType.from_type_name(column["data_type"])
            column_data["data_types"].append(dtype_enum)
        
        new_table.table_data(
            column_data=column_data,
            id_row=0,
            name=create_op["table_name"]
        )

        return new_table

    def table_data(self, column_data: Dict, id_row: int, name: str):
        # TODO: set up methods for retrieving info
        if self.col_data_keys.difference(set(column_data.keys())) == set():
            self.column_data = column_data
            if column_data["data_types"] and isinstance(column_data["data_types"][0], str):
                column_data["data_types"] = list(map(DataType.from_type_name, column_data["data_types"])) 
            self.id_row = id_row
            self.name = name

        else:
            print("Missing column data, update failed.")

    @classmethod
    def from_table_file(cls, file_path: str) -> Table:
        byte_stream = cls.read_table(file_path)
        return cls.bytes_to_object(byte_stream)

    @classmethod
    def bytes_to_object(self, byte_stream: bytes, 
                         page_size: int = 512, rec_count: int = 0, 
                         cdata: Dict = {}, name: str = "", update = True) -> Table:

        raw = io.BytesIO(byte_stream)
        read_buff = io.BufferedRandom(raw)


        page_n = read_buff.read(page_size)
        page_num = 0
        dps: List[DataPointer] = []

        while page_n:
            page_type = self.bytes_to_int(page_n[:1])

            if page_type == 13:
                node = LeafWriter.bytes_to_object(page_n, page_num)
                leaf = node.to_bpnode()
                dps.extend(leaf.keys)
            else:
                pass

            page_num+=1
            page_n = read_buff.read(page_size)

        new_table = self(page_size=page_size)
            
        dps.sort(key=DataPointer.get_id)
        for dp in dps:
            new_table.bptree.insert(dp)

        if update:
            new_table.table_data(
                cdata,
                rec_count,
                name
            )

        new_table.data_pointers = dps

        return new_table

    def object_to_bytes(self) -> bytes:

        page_list = []

        self.tree_to_binary(self.bptree.root, 0, 0xFFFFFFFF, page_list)

        return b"".join(reversed(page_list))

    @classmethod
    def int_object_to_bytes(cls,int_like_val: Any, size: int):

        try:
            int_like_val.tobytes(">")
        except Exception:
            pass
        finally:
            if int_like_val >= 0:
                return int(int_like_val).to_bytes(size, "big")
            else:
                return int(int_like_val).to_bytes(size, "big", signed=True)
                
    @classmethod
    def bytes_to_int(cls,byte_st: bytes):
        return int.from_bytes(byte_st, "big")

    def tree_to_binary(self, node: BPlusNode, page_num: int, parent_page_num: int, page_bytes_list: list, right_relative: int = 0):
        if node.is_leaf:
        
            head = PageHeader(
                PageType.table_leaf_page,
                num_cells=0,
                data_start=0,
                right_relatve=right_relative,
                parent=parent_page_num
            )

            writer = LeafWriter(
                page_number=page_num,
                header=head,
                records=[rec.data for rec in node.keys],
                page_size=self.page_size
            )

            page_bytes_list.append(writer.object_to_bytes())
            return page_num + 1

        else:

            n_children = len(node.pointers)
            this_page_num = page_num
            cc_page_num = this_page_num + 1
            this_cc_page_num = []

            for i, child in enumerate(node.pointers):
                this_cc_page_num.append(cc_page_num)
                if i == n_children-1:
                    cc_page_num = self.tree_to_binary(child, cc_page_num, this_page_num, page_bytes_list, 0)
                else:
                    cc_page_num = self.tree_to_binary(child, cc_page_num, this_page_num, page_bytes_list, cc_page_num+1)

            head = PageHeader(
                PageType.table_interior_page,
                0,
                0,
                this_cc_page_num[-1],
                parent_page_num
            )

            r_cells =[]
            for pno, rid in zip(this_cc_page_num[:-1], node.keys):
                r_cells.append(DataCell(rid, pno))

            writer = PageWriter(
                this_page_num,
                head,
                [],
                r_cells,
                this_cc_page_num[-1],
                self.page_size
            )

            page_bytes_list.append(writer.object_to_bytes(parent_page_num))
                
            return cc_page_num
          
    @staticmethod
    def read_table(tfile: str):
        fb = None
        with open(tfile, "rb") as inf:
            fb = inf.read()
        
        return fb

    def insert(self, insert_dict: Dict) -> int:

        insert_dict["col_ord_list"] = self.get_order_column_name_list(insert_dict["column_name_list"])
        insertion_values = []
        all_columns = self.get_order_column_name_list()

        for i in all_columns:
            if i in insert_dict["col_ord_list"]:
                insertion_values.append(insert_dict["value_list"][i])
            else:
                insertion_values.append(b"")

        try:
            self.validate_insert(insertion_values)
        except Exception as e:
            # traceback.print_exc()
            print('Constraint Violation has occurred: ', e)
            return
        
        insertion_record = Record(
            self.id_row,
            np.uint8(len(all_columns)),
            self.column_data["data_types"],
            insertion_values
        )

        if not self.validate_record(insertion_record):
            raise OverflowError(f"Record with values {insertion_values} exceeds maximum permissible record byte size of {self.max_rec_size}")

        ptr_to_record = DataPointer[Record](Record.get_id, insertion_record)
        self.bptree.insert(ptr_to_record)
        self.id_row += 1

        return self.id_row

    def update(self, update_op: Dict, condition: Dict) -> None:

        keys_to_update = []
        record_refs_all = []

        if condition and "column_order" not in condition:
            condition["column_order"] = self.get_order_column_name(condition["column_name"])
        
        if update_op and "column_order" not in update_op:
            update_op["column_order"] = self.get_order_column_name(update_op["column_name"])

        try:
            _ = self.validate_update(update_op) and self.validate_conditions(condition)
        except Exception as e:
            print(traceback.format_exception_only(e.__class__, e)[-1])
            return

        if condition and condition["column_name"] in self.indexes:
            index : Index  = self.indexes[condition["column_name"]]
            current_leaf = index.first_record()

            record_refs = []
            
            if current_leaf is not None:
                for key in current_leaf.keys:
                    for i in self.data_pointers:
                        dp : DataPointer = i
                        if key.id == dp.id:
                            record_refs.append(dp.data)
                
                updated_refs = Record.filter_update(record_refs, update_op, condition)

                for rec in updated_refs:
                    if not self.validate_record(rec):
                        raise OverflowError(f"Record with values {rec.data_values} exceeds maximum"
                                            f" permissible record byte size of {self.max_rec_size}")

                keys_to_update.extend(current_leaf.keys)
                record_refs_all.extend(updated_refs)

        else:
            current_leaf = self.first_record()

            while current_leaf:
                record_refs = [key.data for key in current_leaf.keys]

                updated_refs = Record.filter_update(record_refs, update_op, condition)

                for rec in updated_refs:
                    if not self.validate_record(rec):
                        raise OverflowError(f"Record with values {rec.data_values} exceeds maximum"
                                            f" permissible record byte size of {self.max_rec_size}")

                keys_to_update.extend(current_leaf.keys)
                record_refs_all.extend(updated_refs)

                current_leaf = current_leaf.next

        for key, rec in zip(keys_to_update, record_refs_all):
                key.data = rec
        
        return

    def delete(self, condition: Dict = None):

        ids_to_delete = set()

        if condition and "column_order" not in condition:
            condition["column_order"] = self.get_order_column_name(condition["column_name"])
        
        if condition is None:
            self.bptree = BPlusTree()
            return
        
        try:
            self.validate_conditions(condition)
        except Exception as e:
            print(traceback.format_exception_only(e.__class__, e)[-1])
            return

        if condition and condition["column_name"] in self.indexes:
            index : Index  = self.indexes[condition["column_name"]]

            current_leaf = index.first_record()

            record_refs = []
            record_id_set = set()

            if current_leaf is not None:
                for key in current_leaf.keys:
                    for i in self.data_pointers:
                        dp : DataPointer = i
                        if key.id == dp.id:
                            record_refs.append(dp.data) 
                            record_id_set.add(key.id)

                updated_refs = Record.filter_delete(record_refs, condition)

                retained_id_set = {ref.get_id():ref for ref in updated_refs}
            
            for id in record_id_set:
                if id not in retained_id_set:
                    ids_to_delete.add(id) 

            for id in ids_to_delete:
                self.bptree.delete(id)
                self.recently_deleted.add(id)

        else:
            current_leaf = self.first_record()

            while current_leaf:
                record_refs = [key.data for key in current_leaf.keys]
                record_id_set = set(key.id for key in current_leaf.keys)

                updated_refs = Record.filter_delete(record_refs, condition)

                retained_id_set = {ref.get_id():ref for ref in updated_refs}
                
                for id in record_id_set:
                    if id not in retained_id_set:
                        ids_to_delete.add(id) 

                current_leaf = current_leaf.next

            for id in ids_to_delete:
                self.bptree.delete(id)
                self.recently_deleted.add(id)

        return

    def select(self, selection_dict: Dict) -> List[List]:

        col_ord_list = self.get_order_column_name_list(selection_dict["column_name_list"])
        condition = selection_dict["condition"]
        
        if  condition and "column_order" not in condition:
            condition["column_order"] = self.get_order_column_name(condition["column_name"])
            
        if condition:
            self.validate_conditions(condition) 

        selections = set()

        if condition and condition["column_name"] in self.indexes:
            index : Index  = self.indexes[condition["column_name"]]

            current_leaf = index.first_record()

            if current_leaf is not None:
                record_refs = []

                for key in current_leaf.keys:
                    for i in self.data_pointers:
                        dp : DataPointer = i
                        if key.id == dp.id:
                            record_refs.append(dp.data) 

                sels = Record.filter_subset(record_refs, col_ord_list, condition)
                for sel in sels:
                    selections.add(tuple(sel))
                

        else:
            current_leaf = self.first_record()

            while current_leaf:
                record_refs = [key.data for key in current_leaf.keys if key.id not in self.recently_deleted]
                
                sels = Record.filter_subset(record_refs, col_ord_list, condition)
                
                for sel in sels:
                    selections.add(tuple(sel))
                
                current_leaf = current_leaf.next

        selections = [list(sel) for sel in selections]
        return selections, selection_dict["column_name_list"] or self.column_data["column_names"]

    def get_order_column_name_list(self, name_list: List[str] = []) -> List[int]:

        existing_cols = self.column_data["column_names"]
        if len(name_list) == 0:
            return list(range(len(existing_cols)))

        elif set(name_list).issubset(existing_cols):
            return list(map(self.get_order_column_name, name_list))

        else:
            raise NameError(f"One of the column names in {name_list} not found in table {self.name}")

    def get_order_column_name(self, col_name: str) -> int:
        existing_cols = self.column_data["column_names"]
        
        if col_name in existing_cols:
            return existing_cols.index(col_name)
        else:
            raise NameError(f"Column Name {col_name} not found in table {self.name}.")

    def validate_insert(self, value_list: List[Any]) -> bool:

        names = self.column_data["column_names"]
        types = self.column_data["data_types"]
        nullables = self.column_data["is_nullable"]
        col_role = self.column_data["column_key_types"]

        for i, (typ, is_null, role, val) in enumerate(zip(types, nullables, col_role, value_list)):
            nv = self.validate_property(i, typ, is_null, role, val, names)
            value_list[i] = nv
            
        return True

    def validate_property(self, i, typ, is_null, role, val, names, skip_uni=False):
        if val is None or val == b"" or val == "":
            if is_null == "YES":
                return 
            else:
                raise TypeError(f"Column {names[i]} of type {typ.value[0]} can't be NULL.")


        typ_string, type_class = typ.value[0], typ.value[3]

        new_val = None
        pred = False

        try:
            if typ_string in {"TINYINT", "SMALLINT", "INT", "BIGINT", "LONG"}:
                type_bounds = np.iinfo(type_class)
                min_, max_ = type_bounds.min, type_bounds.max
                pred = (min_ <= int(val) <= max_)
                new_val = type_class(val)
            
            elif typ_string in {"FLOAT", "DOUBLE"}:
                type_bounds = np.finfo(type_class)
                min_, max_ = type_bounds.min, type_bounds.max
                pred = (min_ <= float(val) <= max_)
                new_val = type_class(val)

            elif typ_string == "YEAR":
                if isinstance(val, str):
                    val = int(val)
                
                type_bounds = np.iinfo(type_class)
                min_, max_ = type_bounds.min, type_bounds.max
                pred = (min_ <= (2000 - val) <= max_)
                new_val = type_class(2000 - val)

            elif typ_string == "TIME":
                if isinstance(val, str):
                    new_val = dt.time.fromisoformat(val)
                elif isinstance(val, int):
                    min_, max_ = 0, 86400000
                    pred = (min_ <= int(val) <= max_)
                    if pred:
                        ms_since_midnight = int(val)
                        hours = ms_since_midnight // 3600 * 1000
                        rem_ms = ms_since_midnight % (3600 * 1000)
                        minutes = rem_ms // (60*1000)
                        rem_ms = rem_ms % (60 * 1000)
                        seconds = rem_ms // 1000
                        rem_ms = rem_ms % 1000
                        mu_s = rem_ms * 1000
                        new_val = dt.time(hours, minutes, seconds, mu_s)

                
            elif typ_string == "DATETIME":
                if isinstance(val, str):
                    new_val = dt.datetime.fromisoformat(val)
                    pred = True
                elif isinstance(val, int):
                    new_val = dt.datetime.fromtimestamp(val)
                    pred = True

            elif typ_string == "DATE":
                if isinstance(val, str):
                    new_val = dt.datetime.fromisoformat(val).date()
                    pred = True
                elif isinstance(val, int):
                    new_val = dt.datetime.fromtimestamp(val).date()
                    pred = True

            elif typ_string == "TEXT":
                pred = (isinstance(val, str)
                        and len(val) <= 115)
                new_val = val

        except ValueError:
            raise TypeError(f"A value for column {names[i]} of type {typ_string} can't be attained from {val}.")

        if not pred:
            raise ValueError(f"Column {names[i]} of type {typ_string} can't have the value {val}.")

        if (role == "UNI" or role == "PRI") and not skip_uni:
            found, _ = self.select({
                "condition": {
                    "negated": "FALSE",
                    "column_name": names[i],
                    "column_order": i,
                    "comparator": "=",
                    "value": new_val
                },
                "column_name_list": [names[i]],
            })

            if found:
                raise ValueError(f"Column {names[i]} has a uniqueness constraint, and value {val} already exists.")

        return new_val

    def validate_conditions(self, condition: Dict) -> bool:

        names = self.column_data["column_names"]
        types = self.column_data["data_types"]
        nullables = self.column_data["is_nullable"]
        col_role = self.column_data["column_key_types"]

        c_ord = condition.get("column_order", self.get_order_column_name(condition["column_name"]))

        typ = types[c_ord]
        is_null = nullables[c_ord]
        role = col_role[c_ord]
        val = condition["value"]

        new_val = self.validate_property(c_ord, typ, is_null, role, val, names, True)

        condition["value"] = new_val

        return True

    def validate_update(self, update_op: Dict) -> bool:
        names = self.column_data["column_names"]
        types = self.column_data["data_types"]
        nullables = self.column_data["is_nullable"]
        col_role = self.column_data["column_key_types"]

        c_ord = update_op.get("column_order", self.get_order_column_name(update_op["column_name"]))

        typ = types[c_ord]
        is_null = nullables[c_ord]
        role = col_role[c_ord]
        val = update_op["value"]

        new_val = self.validate_property(c_ord, typ, is_null, role, val, names)

        update_op["value"] = new_val
        
        return True

    def validate_record(self, rec: Record) -> bool:
        return len(rec.object_to_bytes()) <= self.max_rec_size

    def first_record(self) -> BPlusNode:
        for uid in range(self.id_row):
            if uid not in self.recently_deleted:
                node, idx = self.bptree.search(self.bptree.root, uid)
                if node and idx is not None:
                    return node

        return None

