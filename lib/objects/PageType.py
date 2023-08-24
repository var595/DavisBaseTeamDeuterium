from enum import IntEnum


class PageType(IntEnum):
    
    index_interior_page = 2
    index_leaf_page = 10
    table_interior_page = 5
    table_leaf_page = 13

    @classmethod
    def from_int(cls, int_val: int):
        if int_val == 2:
            return cls.index_interior_page
        elif int_val == 5:
            return cls.table_interior_page
        elif int_val == 10:
            return cls.index_leaf_page
        elif int_val == 13:
            return cls.table_leaf_page
        else:
            raise ValueError