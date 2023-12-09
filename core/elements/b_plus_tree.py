
from __future__ import annotations
import bisect
from typing import Tuple, Union
from core.elements.b_plus_node import b_plus_node
from core.elements.data_pointer import data_pointer

class b_plus_tree:
    def __init__(self, min_ptr_degree: int = 3) -> None:
        self.root = b_plus_node(True, None)
        self.min_degree = max(min_ptr_degree, 3)

    def min_ptr_degree(self):
        return self.min_degree

    def max_ptr_degree(self):
        return 2 * self.min_degree

    def search(self, node: b_plus_node, key: Union[data_pointer, int]) -> Tuple[b_plus_node, int]:
        i = 0
        n = len(node.keys)

        if node.is_leaf:
            while i < n and key > node.keys[i]:
                i += 1
            if i < n and key == node.keys[i]:
                return (node, i)
            else:
                return (node, None)
        else:
            while i < n and key >= node.keys[i]:
                i += 1
            return self.search(node.pointers[i], key)

    def insert(self, entry: Union[data_pointer, int]) -> None:
        insertion_leaf, _ = self.search(self.root, entry)
        max_key_fill = self.max_ptr_degree() - 1

        if len(insertion_leaf.keys) >= max_key_fill:
            (mkey, lc, rc) = self.split_insert_leaf(insertion_leaf, entry)
            if insertion_leaf.parent:
                self.up_insert(insertion_leaf.parent, mkey, lc)
            else:
                new_root = b_plus_node(False, None)
                new_root.keys = [mkey]
                new_root.pointers = [lc, rc]
                lc.parent = new_root
                rc.parent = new_root
                self.root = new_root

        else:
            bisect.insort_left(insertion_leaf.keys, entry)

    def up_insert(self, parent: b_plus_node, router: int, lc: b_plus_node):
        max_key_fill = self.max_ptr_degree() - 1

        if len(parent.keys) >= max_key_fill:
            (mkey, lci, rci) = self.split_insert_internal(parent, router, lc)
            if parent.parent:
                self.up_insert(parent.parent, mkey, lci)
            else:
                new_root = b_plus_node(False, None)
                new_root.keys = [mkey]
                new_root.pointers = [lci, rci]
                lci.parent = new_root
                rci.parent = new_root
                self.root = new_root
        else:
            i = bisect.bisect_left(parent.keys, router)
            parent.pointers.insert(i, lc)
            parent.keys.insert(i, router)

    def split_insert_internal(self, internal_node: b_plus_node, router: int, lc: b_plus_node):
        index = bisect.bisect_left(internal_node.keys, router)
        internal_node.pointers.insert(index, lc)
        internal_node.keys.insert(index, router)

        split_node = b_plus_node(False, internal_node.parent)
        t = self.min_ptr_degree()

        median_key = internal_node.keys[t]
        split_node.keys = internal_node.keys[:t]
        split_node.pointers = internal_node.pointers[:t+1]

        for ptr in split_node.pointers:
            ptr.parent = split_node

        internal_node.keys = internal_node.keys[t+1:]
        internal_node.pointers = internal_node.pointers[t+1:]

        return (median_key, split_node, internal_node)


    def split_insert_leaf(self, leaf_node: b_plus_node, entry: Union[data_pointer, int]) -> Tuple[int, b_plus_node, b_plus_node]:
        bisect.insort_left(leaf_node.keys, entry)

        split_node = b_plus_node(True, leaf_node.parent)
        t = self.min_ptr_degree()

        median_key = leaf_node.keys[t]
        median_key = median_key.id if isinstance(median_key, data_pointer) else median_key
        split_node.keys = leaf_node.keys[:t]
        leaf_node.keys = leaf_node.keys[t:]

        if leaf_node.prev:
            leaf_node.prev.next = split_node

        split_node.prev = leaf_node.prev
        split_node.next = leaf_node
        leaf_node.prev = split_node

        return (median_key, split_node, leaf_node)
        
    def delete(self, key: int):
        val_loc, idx = self.search(self.root, key)

        if idx is None:
            return

        val_loc.keys.remove(key)

        if self.is_underflow(val_loc) and val_loc.parent:
            vparent = val_loc.parent
            ptr_idx = vparent.pointers.index(val_loc)
            left_sib = vparent.pointers[ptr_idx - 1] if ptr_idx - 1 >= 0 else None
            right_sib = vparent.pointers[ptr_idx + 1] if ptr_idx + 1 < len(vparent.pointers) else None
            transfer_max = self.min_ptr_degree() + 1

            if right_sib and len(right_sib.keys) <= transfer_max:
                val_loc.keys += right_sib.keys
                vparent.pointers.pop(ptr_idx + 1)
                vparent.keys.pop(ptr_idx)

            elif left_sib and len(left_sib.keys) <= transfer_max:
                left_sib.keys += val_loc.keys
                vparent.pointers.pop(ptr_idx)
                vparent.keys.pop(ptr_idx - 1)

            elif right_sib and len(right_sib.keys) > transfer_max:

                while self.is_underflow(val_loc) and not self.is_underflow(right_sib):
                    pull_key = right_sib.keys.pop(0)
                    val_loc.keys.append(pull_key)
                    vparent.keys[ptr_idx] = right_sib.keys[0].id if isinstance(right_sib.keys[0], data_pointer) else right_sib.keys[0]

            elif left_sib and len(left_sib.keys) > transfer_max:

                while self.is_underflow(val_loc) and not self.is_underflow(left_sib):
                    pull_key = left_sib.keys.pop()
                    val_loc.keys.insert(0, pull_key)
                    vparent.keys[ptr_idx - 1] = pull_key.id if isinstance(pull_key, data_pointer) else pull_key

            if self.is_underflow(vparent):
                self.fuse(vparent)
            else:
                return True
        else:
            return True


    def fuse(self, node: b_plus_node):

        if gp := node.parent:
            idx = gp.pointers.index(node)
            ls = gp.pointers[idx-1] if idx - 1 >= 0 else None
            rs = gp.pointers[idx+1] if idx + 1 < len(gp.pointers) else None
            transfer_max = self.min_ptr_degree() + 1

            if rs and len(rs.keys) <= transfer_max:
                median_key = gp.keys.pop(idx)
                gp.pointers.pop(idx+1)
                node.keys = node.keys + [median_key] + rs.keys
                for ptr in rs.pointers:
                    ptr.parent = node
                node.pointers.extend(rs.pointers)

            elif ls and len(ls.keys) <= transfer_max:
                median_key = gp.keys.pop(idx-1)
                gp.pointers.pop(idx)
                ls.keys = ls.keys + [median_key] + node.keys
                for ptr in node.pointers:
                    ptr.parent = ls
                ls.pointers.extend(node.pointers)

            elif rs and len(rs.keys) > transfer_max:

                while self.is_underflow(node) and not self.is_underflow(rs):
                    pull_ptr = rs.pointers.pop(0)
                    pull_key = rs.keys.pop(0)
                    node.keys.append(gp.keys[idx])
                    pull_ptr.parent = node
                    node.pointers.append(pull_ptr)
                    gp.keys[idx] = pull_key

            elif ls and len(ls.keys) > transfer_max:

                while self.is_underflow(node) and not self.is_underflow(ls):
                    pull_ptr = ls.pointers.pop()
                    pull_key = ls.keys.pop()
                    node.keys.insert(0, gp.keys[idx-1])
                    pull_ptr.parent = node
                    node.pointers.insert(0, pull_ptr)
                    gp.keys[idx-1] = pull_key

            if self.is_underflow(gp):
                self.fuse(gp)

        elif len(node.keys) == 0:
            new_root = node.pointers.pop()
            new_root.parent = None
            self.root = new_root

    @staticmethod
    def borrow_left(node: b_plus_node, left_sib: b_plus_node):
        if left_sib and b_plus_tree.is_over_half(left_sib) and left_sib.is_leaf:
            new_key = left_sib.keys.pop()
            node.keys.insert(0, new_key)
            return True
        
        return False


    @staticmethod
    def borrow_right(node: b_plus_node, right_sib: b_plus_node):
        if right_sib and b_plus_tree.is_over_half(right_sib) and right_sib.is_leaf:
            new_key = right_sib.keys.pop(0)
            node.keys.append(new_key)
            return True
        
        return False

    @staticmethod
    def merge(left: b_plus_node, right: b_plus_node):
        
        left.next = right.next
        if right.next:
            right.next.prev = left
        
        left.keys += right.keys
        return 

    def is_underflow(self, node: b_plus_node):
        return len(node.keys) < self.min_ptr_degree()

    def is_over_half(self, node: b_plus_node):
        return len(node.keys) > self.min_ptr_degree()
