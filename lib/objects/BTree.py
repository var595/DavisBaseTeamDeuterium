from __future__ import annotations
from typing import Tuple, Union
from lib.objects.BTreeNode import BTreeNode
from lib.objects.DataPointer import DataPointer


class BTree:

    def __init__(self, min_deg: int = 3) -> None:
        self.root = BTreeNode(True)
        self.min_degree = max(min_deg, 3)

    def min_ptr_degree(self):
        return self.min_degree

    def max_ptr_degree(self):
        return 2 * self.min_degree
        
    def search(self, node: BTreeNode, key: Union[DataPointer, int]) -> Tuple[BTreeNode, int]:
        i = 0
        n = len(node.keys)

        while i < n and key > node.keys[i]:
            i += 1

        if i < n and key == node.keys[i]:
            return (node, i)
        elif node.is_leaf:
            return (None, None)
        else:
            return self.search(node.pointers[i], key)

    def insert(self, key: Union[DataPointer, int]):
        current_root = self.root

        if len(current_root.keys) == self.max_ptr_degree() - 1:
            new_node = BTreeNode(False)
            self.root = new_node
            new_node.pointers.append(current_root)
            self.split_child(new_node, 0)
            self.insert_available(new_node, key)

        else:
            self.insert_available(current_root, key)

    def split_child(self, node: BTreeNode, ptr_idx: int):
        
        node_to_split = node.pointers[ptr_idx]
        new_node = BTreeNode(node_to_split.is_leaf)

        t = self.min_ptr_degree()

        new_node.keys = node_to_split.keys[t:]        
        new_node.pointers = node_to_split.pointers[t:]

        median_key = node_to_split.keys[t-1]

        node_to_split.keys = node_to_split.keys[:t-1]
        node_to_split.pointers = node_to_split.pointers[:t]

        node.keys = node.keys[:ptr_idx] + [median_key] + node.keys[ptr_idx:]
        node.pointers = node.pointers[:ptr_idx+1] + [new_node] + node.pointers[ptr_idx+1:]


    def insert_available(self, node: BTreeNode, key: Union[DataPointer, int]):
        i = 0
        n = len(node.keys)

        while i < n and key > node.keys[i]:
                i += 1

        if node.is_leaf:
            node.keys = node.keys[:i] + [key] + node.keys[i:]

        else:
            insertion_ptr = node.pointers[i]

            if len(insertion_ptr.keys) == self.max_ptr_degree() - 1:
                self.split_child(node, i)
                if key > node.keys[i]:
                    i += 1
            
            self.insert_available(node.pointers[i], key)

    def delete(self, node: BTreeNode, key: Union[DataPointer, int]):

        i = 0
        n = len(node.keys)
        while i < n and key > node.keys[i]:
            i += 1

        if node.is_leaf:

            if i < n and key == node.keys[i]:
                node.keys.pop(i)
                return True

            else:
                print(node.keys)
                raise KeyError(f"Key {key} not found")
        else:
            if i < n and key == node.keys[i]:

                left_child = node.pointers[i]
                right_child = node.pointers[i+1]

                if self.check_node_fill(left_child):
                    replacement_key = self.find_predecessor(left_child)
                    node.keys[i] = replacement_key
                    return self._delete(left_child, replacement_key)
                elif self.check_node_fill(right_child):
                    replacement_key = self.find_successor(right_child)
                    node.keys[i] = replacement_key
                    return self._delete(right_child, replacement_key)
                else:
                    ptr_tomerge = node.pointers.pop(i+1)
                    matched_key = node.keys.pop(i)
                    self.merge(left_child, matched_key, ptr_tomerge)
                    return self._delete(left_child, key)
            elif self.check_node_fill(recurse_node := node.pointers[i]):
                return self._delete(recurse_node, key)
            else:
                left_sibling = node.pointers[i-1] if i-1 >= 0 else None
                right_sibling = node.pointers[i+1] if i+1 < len(node.pointers) else None
                if left_sibling and self.check_node_fill(left_sibling):
                    x_replacement = left_sibling.keys.pop()
                    x_donation = node.keys[i-1]
                    node.keys[i-1] = x_replacement
                    recurse_node.keys.insert(0, x_donation)
                    if len(left_sibling.pointers) > 0:
                        recurse_node.pointers.insert(0, left_sibling.pointers.pop())
                    return self._delete(recurse_node, key)
                elif right_sibling and self.check_node_fill(right_sibling):
                    x_replacement = right_sibling.keys.pop(0)
                    x_donation = node.keys[i]
                    node.keys[i] = x_replacement
                    recurse_node.keys.append(x_donation)
                    if not right_sibling.is_leaf:
                        recurse_node.pointers.append(right_sibling.pointers.pop(0))
                    return self._delete(recurse_node, key)
                else:
                    if right_sibling:
                        x_donation_final = node.keys.pop(i)
                        self.merge(recurse_node, x_donation_final, right_sibling)
                        node.pointers.pop(i+1)
                        return self._delete(recurse_node, key)
                    else:
                        x_donation_final = node.keys.pop(i-1)
                        self.merge(left_sibling, x_donation_final, recurse_node)
                        node.pointers.pop(i)
                        return self._delete(left_sibling, key)


    @staticmethod
    def merge(left: BTreeNode, median_key: Union[DataPointer, int], right: BTreeNode):
        left.keys = left.keys + [median_key] + right.keys
        left.pointers.extend(right.pointers)

    def check_node_fill(self, node: BTreeNode):
        return len(node.keys) >= self.min_ptr_degree()


    @staticmethod
    def find_predecessor(node: BTreeNode):
        if not node.is_leaf:
            return BTree.find_predecessor(node.pointers[-1])
        else:
            return node.keys[-1]

    @staticmethod
    def find_successor(node: BTreeNode):
        if not node.is_leaf:
            return BTree.find_successor(node.pointers[0])
        else:
            return node.keys[0]
        