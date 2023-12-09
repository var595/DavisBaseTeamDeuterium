from collections import deque
from dataclasses import dataclass, field
import io
from typing import Any, List
import numpy as np
from core.elements.b_plus_node import b_plus_node
from core.elements.b_tree_node import b_tree_node
from core.elements.data_pointer import data_pointer
from core.elements.page_header import page_header
from core.elements.record import record
from core.config.config_manager import config_manager

@dataclass
class leaf_writer:
    page_number: int
    header: page_header = field(default_factory=page_header.default_header)
    offsets: List[bytes] = field(default_factory=list)
    records: List[record] = field(default_factory=list)
    page_size: int = config_manager.get_page_size()

    def object_to_bytes(self):
        self.offsets = deque()
        cell_bytes_ll = deque()
        num_cells = np.uint16(0)

        data_size = np.uint16(0)

        for record in self.records:
            num_cells += np.uint16(1)
            cell = record.object_to_bytes()
            cell_size = len(cell)

            data_size = data_size + np.uint16(cell_size)
            self.header.data_start = self.page_size - data_size
            offset = self.int_object_to_bytes(self.header.data_start, 2)
            self.offsets.append(offset)
            cell_bytes_ll.appendleft(cell)

        self.header.num_cells = num_cells
        header_bytes = self.header.object_to_bytes()
        offset_bytes = b''.join(self.offsets)
        cell_bytes = b''.join(cell_bytes_ll)
        padding_len = self.page_size - len(header_bytes) - len(offset_bytes) - len(cell_bytes)

        padding = self.int_object_to_bytes(0, 1) * padding_len
        self.offsets = []

        byte_stream = b''.join([
            header_bytes,
            offset_bytes,
            padding,
            cell_bytes
        ])

        return byte_stream

    def to_bpnode(self) -> b_plus_node:
        x = b_plus_node(True, None)
        x.keys = []
        
        for rec in self.records:
            dp = data_pointer(
                record.get_id,
                rec
            )
            x.keys.append(dp)

        x.keys.sort(key=data_pointer.get_id)
        return x

    def to_bnode(self) -> b_tree_node:
        x = b_tree_node(True)
        x.keys = []
        
        for rec in self.records:
            dp = data_pointer(
                record.get_id,
                rec
            )
            x.keys.append(dp)

        x.keys.sort(key=data_pointer.get_id)
        return x


    @classmethod
    def bytes_to_object(cls, byte_stream: bytes, pg_num: int):
        raw = io.BytesIO(byte_stream)
        read_buff = io.BufferedRandom(raw)

        header_bytes = read_buff.read(16)
        header = page_header.bytes_to_object(header_bytes)
        num_cells = header.num_cells

        read_buff.seek(16)

        offsets_paired = list()

        for _ in range(num_cells):
            cell_offset_bytes = read_buff.read(2)
            cell_offset_int = cls.bytes_to_int(cell_offset_bytes)
            offsets_paired.append((cell_offset_bytes, cell_offset_int))


        records = list()
        for _, ci in offsets_paired:
            read_buff.seek(ci)
            rec_len = cls.bytes_to_int(read_buff.read(2))
            read_buff.seek(ci)
            record_bytes = read_buff.read(6 + rec_len)
            rec = record.bytes_to_object(record_bytes)
            records.append(rec)

        return cls(
            pg_num,
            header,
            list(),
            records
        )

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
    def bytes_to_int(cls, byte_st: bytes):
        return int.from_bytes(byte_st, byteorder="big")
