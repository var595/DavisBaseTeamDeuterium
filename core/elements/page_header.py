from dataclasses import dataclass
import io
from itertools import starmap
from typing import Any
import numpy as np
from core.elements.page_type import page_type


@dataclass
class page_header:
    """
    Represents the header of a page.

    Attributes:
    - pagetype: The type of the page.
    - num_cells: Number of cells in the page.
    - data_start: Start position of data in the page.
    - right_relative: Right relative pointer.
    - parent: Pointer to the parent page.
    """
    pagetype: page_type
    num_cells: np.uint16
    data_start: np.uint16
    right_relatve: np.uint32
    parent: np.uint32

    @classmethod
    def bytes_to_int(cls,byte_st: bytes):
        # Converts bytes to an integer
        return int.from_bytes(byte_st, "big")

    @classmethod
    def int_object_to_bytes(cls,int_like_val: Any, size: int):
        # Converts an integer-like value to bytes
        try:
            int_like_val.tobytes(">")
        except Exception:
            pass
        finally:
            if int_like_val >= 0:
                return int(int_like_val).to_bytes(size, "big")
            else:
                return int(int_like_val).to_bytes(size, "big", signed=True)

    def object_to_bytes(self):
        # Converts the object's attributes to bytes
        return b''.join(
            starmap(
                self.int_object_to_bytes,
                [
                    (self.pagetype, 1),
                    (0, 1),
                    (self.num_cells, 2),
                    (self.data_start, 2),
                    (self.right_relatve, 4),
                    (self.parent, 4),
                    (0, 2)
                ]
            )
        )

    @classmethod
    def default_header(cls, is_root=False):
        # Generates a default header
        return cls(
            page_type.table_leaf_page,
            np.uint16(0),
            np.uint16(0),
            np.uint32(0),
            ~np.uint32(0) if is_root else np.uint32(0)
        )

    @classmethod
    def bytes_to_object(cls, byte_stream: bytes):
        # Converts bytes to a page_header object
        
        base_obj = cls.default_header()
        raw = io.BytesIO(byte_stream)
        read_buff = io.BufferedRandom(raw)
        
        pagetype = cls.bytes_to_int(read_buff.read(1))
        base_obj.pagetype = page_type.from_int(pagetype)

        read_buff.read(1)
        n_cells = cls.bytes_to_int(read_buff.read(2))
        base_obj.num_cells = np.uint16(n_cells)

        pg_data_start = cls.bytes_to_int(read_buff.read(2))
        base_obj.data_start = np.uint16(pg_data_start)

        right_relative = cls.bytes_to_int(read_buff.read(4))
        base_obj.right_relatve = np.uint32(right_relative)

        parent = cls.bytes_to_int(read_buff.read(4))
        base_obj.parent = np.uint32(parent)

        return base_obj

