
from typing import List, Union
from DataPointer import DataPointer

class BTreeNode:
    def __init__(self, leaf: bool) -> None:
        self.keys: List[Union[DataPointer, int]] = []
        self.pointers: List[BTreeNode] = []
        self.is_leaf = leaf
