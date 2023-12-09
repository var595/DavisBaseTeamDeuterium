
from collections import deque
from dataclasses import dataclass, field
import io
from typing import Any, List

import numpy as np
from core.elements.page_header import page_header
from core.elements.page_type import page_type
from core.elements.data_cell import data_cell
from core.config.config_manager import config_manager


@dataclass
class page_writer:
    """
    Class to handle writing pages.

    Attributes:
    - page_number (int): The page number.
    - header (page_header): The page header.
    - offsets (List[bytes]): Offsets list.
    - keys (List[data_cell]): List of data cells.
    - last_child_pg (int): Last child page number.
    - page_size (int): Page size obtained from config manager.
    """

    page_number: int
    header: page_header = field(default_factory=page_header.default_header)
    offsets: List[bytes] = field(default_factory=list)
    keys: List[data_cell] = field(default_factory=list)
    last_child_pg: int = field(default_factory=int)
    page_size: int = config_manager.get_page_size()


    def object_to_bytes(self, parent_page_num: int):
        """
        Convert object attributes to bytes.

        Parameters:
        - parent_page_num (int): Parent page number.

        Returns:
        - Byte stream representing object attributes.
        """
        self.offsets = deque()
        cell_bytes_ll = deque()
        num_cells = np.uint16(0)

        data_size = np.uint16(0)

        for key in self.keys:
            num_cells += np.uint16(1)
            cell = key.object_to_bytes()
            cell_size = len(cell)

            data_size = data_size + np.uint16(cell_size)
            self.header.data_start = self.page_size - data_size
            offset = self.int_object_to_bytes(self.header.data_start, 2)
            self.offsets.append(offset)
            cell_bytes_ll.appendleft(cell)

        self.header.num_cells = num_cells
        self.header.pagetype = page_type.table_interior_page
        self.header.parent = np.uint32(parent_page_num)
        self.header.right_relatve = np.uint32(self.last_child_pg)

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
    
    @classmethod
    def bytes_to_object(cls, byte_stream: bytes, pg_num: int):
        """
        Convert bytes to object attributes.

        Parameters:
        - byte_stream (bytes): Byte stream to convert.
        - pg_num (int): Page number.

        Returns:
        - Page Writer object with parsed attributes.
        """
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


        router_cells = list()
        for _, ci in offsets_paired:
            read_buff.seek(ci)
            cell_bytes = read_buff.read(8)
            rec = data_cell.bytes_to_object(cell_bytes)
            router_cells.append(rec)

        return InternalPageWriter(
            pg_num,
            header,
            list(),
            router_cells,
            header.right_relatve
        )
    @classmethod
    def int_object_to_bytes(cls,int_like_val: Any, size: int):
        """
        Convert integer-like objects to bytes.

        Parameters:
        - int_like_val (Any): Integer-like value to convert.
        - size (int): Size of the resulting bytes.

        Returns:
        - Bytes representation of the input integer-like value.
        """
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
        """
        Convert bytes to integers.

        Parameters:
        - byte_st (bytes): Byte stream to convert.

        Returns:
        - Integer value obtained from the byte stream.
        """
        return int.from_bytes(byte_st, "big")
