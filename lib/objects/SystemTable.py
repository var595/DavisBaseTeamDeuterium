
class SystemTable:
    def parse_column_data(table_name):
        parsed_tokens = {
            "command": "SELECT",
            "column_name_list": [
                "column_name",
                "data_type",
                "column_position", 
                "is_nullable",
                "column_key"
                ],
            "table_name": "system_columns",
            "condition": {
            "negated": "FALSE",
            "column_name": "table_name",
            "comparator": "=",
            "value": table_name
            }
        }
        return parsed_tokens

    def parse_table_data(table_name):
        parsed_tokens = {
            "command": "SELECT",
            "column_name_list": [
                "page_size",
                "id_row"
            ],
            "table_name": "system_tables",
            "condition": {
            "negated": "FALSE",
            "column_name": "table_name",
            "comparator": "=",
            "value": table_name
            }
        }
        return parsed_tokens

    def insert_column_data(row_id: int, table_name: str, column_name: str, 
                                data_type: str, column_position: int, is_nullable: str, column_key: str):
        insert_parsed_tokens = {
            "command": "INSERT INTO TABLE",
            "column_name_list": [
                "rowid",
                "table_name",
                "column_name",
                "data_type",
                "column_position", 
                "is_nullable",
                "column_key"
            ],
            "table_name": "system_columns",
            "value_list": [
                row_id,
                table_name, 
                column_name,
                data_type, 
                column_position, 
                is_nullable, 
                column_key
            ]
        }
        return insert_parsed_tokens

    def insert_table_data(row_id, table_name, page_size, id_row):
        insert_parsed_tokens = {
            "command": "INSERT INTO TABLE",
            "column_name_list": [
                "rowid",
                "table_name",
                "page_size",
                "id_row"
            ],
            "table_name": "system_tables",
            "value_list": [
                row_id,
                table_name,
                page_size,
                id_row
            ]
        }
        return insert_parsed_tokens

    def update_table_data(table_name, id_row):
        
        utdict = {
            "command": "UPDATE",
            "table_name": "system_tables",
            "operation": {
                "operation_type": "SET",
                "column_name": "id_row",
                "comparator": "=",
                "value": id_row
                },
            "condition": {
                "negated": "FALSE",
                "column_name": "table_name",
                "comparator": "=",
                "value": table_name
            }
        }
            
        return utdict

    

    def delete_column_data(table_name):
        delete_dict =  {
            "command": "DELETE",
            "table_name": "system_columns",
            "condition": {
                "negated": "FALSE",
                "column_name": "table_name",
                "comparator": "=",
                "value": table_name
            }
        }
        return delete_dict

    def delete_table_data(table_name):
        delete_dict =  {
            "command": "DELETE",
            "table_name": "system_tables",
            "condition": {
                "negated": "FALSE",
                "column_name": "table_name",
                "comparator": "=",
                "value": table_name
            }
        }
        return delete_dict