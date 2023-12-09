
from typing import List, Union
from data_pointer import data_pointer

class b_tree_node:
    def __init__(self, leaf: bool) -> None:
        self.keys: List[Union[data_pointer, int]] = []
        self.pointers: List[b_tree_node] = []
        self.is_leaf = leaf
