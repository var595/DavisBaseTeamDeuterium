from dataclasses import dataclass
import io
from typing import Any
import numpy as np


@dataclass
class DataCell:
    def __init__(self, rid, pno) -> None:
        self.row_id: np.uint32 = rid
        self.lc_page_no: np.uint32 = pno

    def object_to_bytes(self):
        row_id_byte_stream = self.int_object_to_bytes(self.row_id, 4)
        cp_byte_stream = self.int_object_to_bytes(self.lc_page_no, 4)
        return b"".join((cp_byte_stream, row_id_byte_stream))

    @classmethod
    def bytes_to_object(cls, byte_stream: bytes):
        raw = io.BytesIO(byte_stream)
        read_buff = io.BufferedRandom(raw)

        cp_byte_stream = read_buff.read(4)
        lc_page_num = cls.bytes_to_int(cp_byte_stream)

        row_id_b = read_buff.read(4)
        row_id = cls.bytes_to_int(row_id_b)
        return cls(row_id, lc_page_num)

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