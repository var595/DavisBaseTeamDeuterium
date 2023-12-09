from enum import IntEnum


class page_type(IntEnum):
    
    """
    The one-byte flag at page offset 0 indicates the b-tree page type.
        • A value of 2 (0x02) means the page is an index b-tree interior page.
        • A value of 5 (0x05) means the page is an table b-tree interior page.
        • A value of 10 (0x0a) means the page is an index b-tree leaf page.
        • A value of 13 (0x0d) means the page is a table b-tree leaf page.
        Any other value for the b-tree page type is an error.
    """
    index_interior_page = 2
    table_interior_page = 5
    index_leaf_page = 10
    table_leaf_page = 13

    @classmethod
    def from_int(cls, int_val: int):
        """
        Maps integer values to enum types.
        
        Parameters:
        - int_val (int): Integer value to be mapped to an enum type.

        Returns:
        - Corresponding enum type based on the input integer value.
        
        Raises:
        - ValueError if the integer value doesn't match any enum value.
        """
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