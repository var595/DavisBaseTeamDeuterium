from __future__ import annotations
import io
from typing import Any, Dict, List
import numpy as np
import math

from core.elements.b_tree_node import b_tree_node
from core.elements.b_tree import b_tree
from core.elements.data_pointer import data_pointer
from core.elements.page_header import page_header
from core.elements.page_type import page_type
from core.elements.record import record
from core.elements.data_cell import data_cell
from core.handlers.page_writer import page_writer
from core.handlers.leaf_writer import leaf_writer

class Index:
    def __init__(self, page_size = 512) -> None:
        self.col_data_keys = {"keys", "pointers"}
        page_space = page_size - 16
        self.max_rec_size = math.floor(page_space/6) - 2
        self.btree = b_tree()
        self.column_data = {
            "keys": [],
            "pointers": []
        }
        self.id_row = 0
        self.page_size = page_size
        self.name = ""
        self.recently_deleted = set()

    @classmethod
    def create_index(cls, create_op: Dict, page_size=512) -> Index:
        new_index = cls(page_size)
        return new_index


    def build_index(self, table_name: str, column_name: str, data : Dict)  -> Index:
        table = data[table_name]

        col_idx = table.column_data["column_names"].index(column_name)
        datatype = table.column_data["data_types"][col_idx]
        selection_dict = {
            "column_name_list": [column_name],
            "condition" : ""
        }
        current_leaf = table.first_record()
        col_ord_list = table.get_order_column_name_list(selection_dict["column_name_list"])
        condition = selection_dict["condition"]
        
        if  condition and "column_order" not in condition:
            condition["column_order"] = table.get_order_column_name(condition["column_name"])
            
        if condition:
            table.validate_conditions(condition)


        self.id_row = 0
        while current_leaf:
            record_refs = [key.data for key in current_leaf.keys if key.id not in table.recently_deleted]
             
            keys =  [key.id for key in current_leaf.keys if key.id not in table.recently_deleted]
            sels = sum(record.filter_subset(record_refs, col_ord_list, condition),[])

            for (i,k) in zip(sels,keys):
                index_record = record(
                    k,
                    np.uint8(1),
                    [datatype],
                    [i]
                    )
                

                dp = data_pointer[record](record.get_id, index_record)
                self.btree.insert(dp)
                self.id_row +=1


            current_leaf = current_leaf.next


    @classmethod
    def from_index_file(cls, file_path: str) -> Index:
        byte_stream = cls.read_table(file_path)
        return cls.bytes_to_object(byte_stream)

    @classmethod
    def bytes_to_object(cls, byte_stream: bytes, 
                         page_size: int = 512) -> Index:

        raw = io.BytesIO(byte_stream)
        read_buff = io.BufferedRandom(raw)

        page_n = read_buff.read(page_size)
        page_num = 0
        dps: List[data_pointer] = []

        while page_n:
            pagetype = cls.bytes_to_int(page_n[:1])

            if pagetype == 13:
                node = leaf_writer.bytes_to_object(page_n, page_num)
                leaf = node.to_bnode()
                dps.extend(leaf.keys)
            else:
                pass

            page_num+=1
            page_n = read_buff.read(page_size)

        new_index = cls(page_size=page_size)
            
        dps.sort(key=data_pointer.get_id)
        for dp in dps:
            new_index.btree.insert(dp)
     

        return new_index,len(dps)

    def object_to_bytes(self) -> bytes:

        page_list = []

        self.tree_to_binary(self.btree.root, 0, 0xFFFFFFFF, page_list)

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

    def tree_to_binary(self, node: b_tree_node, page_num: int, parent_page_num: int, page_bytes_list: list, right_relative: int = 0):
        if node.is_leaf:
        
            head = page_header(
                page_type.table_leaf_page,
                num_cells=0,
                data_start=0,
                right_relatve=right_relative,
                parent=parent_page_num
            )

            writer = leaf_writer(
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

            head = page_header(
                page_type.table_interior_page,
                0,
                0,
                this_cc_page_num[-1],
                parent_page_num
            )

            r_cells =[]
            for pno, dip in zip(this_cc_page_num[:-1], node.keys):
                rid = dip.get_id()
                r_cells.append(data_cell(rid, pno))

            writer = page_writer(
                this_page_num,
                head,
                [],
                r_cells,
                this_cc_page_num[-1],
                self.page_size
            )

            page_bytes_list.append(writer.object_to_bytes(parent_page_num))
                
            return cc_page_num

    def first_record(self) -> b_tree_node:
        for uid in range(self.id_row):
            if uid not in self.recently_deleted:
                node, idx = self.btree.search(self.btree.root, uid)
                if node and idx is not None:
                    return node

        return None

  
    
if __name__ == '__main__':
    pass
