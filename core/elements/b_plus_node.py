from typing import List
from core.elements.b_tree_node import b_tree_node

class b_plus_node(b_tree_node):
    def __init__(self, leaf: bool, parent: b_tree_node) -> None:
        super().__init__(leaf)
        self.pointers: List[b_plus_node] = []
        self.parent: b_plus_node = parent
        self.next: b_plus_node = None
        self.prev: b_plus_node = None