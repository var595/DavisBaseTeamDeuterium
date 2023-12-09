from dataclasses import dataclass
import io
from typing import Any
import numpy as np

@dataclass
class data_cell:
    def __init__(self, row_id, lc_page_no) -> None:
        # Initialize data cell attributes
        self.row_id_val: np.uint32 = row_id
        self.lc_page_number: np.uint32 = lc_page_no

    def object_to_bytes(self):
        # Convert data cell attributes to byte streams
        row_id_byte_stream = self.int_object_to_bytes(self.row_id_val, 4)
        cp_byte_stream = self.int_object_to_bytes(self.lc_page_number, 4)
        return b"".join((cp_byte_stream, row_id_byte_stream))

    @classmethod
    def bytes_to_object(cls, byte_stream: bytes):
        # Convert byte streams back to DataCell object
        raw = io.BytesIO(byte_stream)
        read_buff = io.BufferedRandom(raw)

        cp_byte_stream = read_buff.read(4)
        lc_page_num = cls.bytes_to_int(cp_byte_stream)

        row_id_b = read_buff.read(4)
        row_id = cls.bytes_to_int(row_id_b)
        return cls(row_id, lc_page_num)

    @classmethod
    def int_object_to_bytes(cls, int_like_val: Any, size: int):
        # Convert integer-like values to byte streams
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
        # Convert byte streams to integers
        return int.from_bytes(byte_st, "big")