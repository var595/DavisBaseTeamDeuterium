from __future__ import annotations
from functools import total_ordering
from typing import Callable, Generic, TypeVar, Union

T = TypeVar("T")

@total_ordering
class DataPointer(Generic[T]):
    
    def __init__(self, type_id_extractor: Callable[[T], int], keyed_instance: T) -> None:
        self.id: int = type_id_extractor(keyed_instance)
        self.id_extractor: Callable[[T], int] = type_id_extractor
        self.data: T = keyed_instance

    def get_id(self):
        return self.id

    
    def __eq__(self, other: Union[DataPointer[T], int]) -> bool:
        if isinstance(other, DataPointer):
            return self.id == other.id
        return self.id == other

    def __lt__(self, other: Union[DataPointer[T], int]) -> bool:
        if isinstance(other, DataPointer):
            return self.id < other.id
        return self.id < other

    def __repr__(self) -> str:
        try:
            data_repr = str(self.data)
        except:
            data_repr = "..."
        finally:
            if len(data_repr) > 300:
                data_repr = "..."

        return (
            f"[KEY ID: {self.id} DATA CELL: {data_repr} ]"
        )