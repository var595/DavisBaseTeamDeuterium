from __future__ import annotations
from functools import total_ordering
from typing import Callable, Generic, TypeVar, Union

T = TypeVar("T")

@total_ordering
class data_pointer(Generic[T]):
    
    def __init__(self, type_id_extractor: Callable[[T], int], keyed_object: T) -> None:
        """
        Initializes a KeyedEntity object encapsulating an entity and its associated key.

        Args:
        - key_extractor: A function that extracts the key from the provided entity.
        - entity: An instance of some type T that has an associated key.

        Attributes:
        - id: The extracted key associated with the entity.
        - id_extractor: The function used to extract the key from the entity.
        - data: The original entity being encapsulated.
        """
        self.id: int = type_id_extractor(keyed_object)
        self.id_extractor: Callable[[T], int] = type_id_extractor
        self.data: T = keyed_object

    def get_id(self):
        return self.id

    
    def __eq__(self, other: Union[data_pointer[T], int]) -> bool:
        if isinstance(other, data_pointer):
            return self.id == other.id
        return self.id == other

    def __lt__(self, other: Union[data_pointer[T], int]) -> bool:
        if isinstance(other, data_pointer):
            return self.id < other.id
        return self.id < other

    def __repr__(self) -> str:
        try:
            var = str(self.data)
        except:
            var = "..."
        finally:
            if len(var) > 300:
                var = "..."

        return (
            f"[KEY ID: {self.id} DATA CELL: {var} ]"
        )