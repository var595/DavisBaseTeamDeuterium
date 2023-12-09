
from typing import List, Union
from data_pointer import data_pointer

class b_tree_node:
    def __init__(self, leaf: bool) -> None:
        """
        Represents a node in a B-tree.

        Attributes:
        - keys: List to hold keys (values) stored in this node.
        - pointers: List to hold pointers to child nodes or data in leaf nodes.
        - is_leaf: Flag indicating whether the node is a leaf or internal node.
        """
        self.keys: List[Union[data_pointer, int]] = []
        self.pointers: List[b_tree_node] = []
        self.is_leaf = leaf
