from typing import List
from lib.objects.BTreeNode import BTreeNode

class BPlusNode(BTreeNode):
    def __init__(self, leaf: bool, parent: BTreeNode) -> None:
        super().__init__(leaf)
        self.pointers: List[BPlusNode] = []
        self.parent: BPlusNode = parent
        self.next: BPlusNode = None
        self.prev: BPlusNode = None