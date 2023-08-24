from lib.settings.Settings import Settings


class CreateDatabase:
    @staticmethod
    def system_columns_column_data():
        return {
                "column_names": ["rowid", "table_name", "column_name", "data_type", "column_position", "is_nullable", "column_key"],
                "data_types": ["INT", "TEXT", "TEXT", "TEXT", "TINYINT", "TEXT", "TEXT"],
                "is_nullable": ["NO", "NO", "NO", "NO", "NO", "NO", "YES"],
                "column_key_types": ["UNI", "", "", "", "", "", "", ""]
            }

    @staticmethod
    def system_tables_column_data():
        return {
                "column_names": ["rowid", "table_name", "page_size", "id_row"],
                "data_types": ["INT", "TEXT", "SMALLINT", "INT"],
                "is_nullable": ["NO", "NO", "NO", "NO"],
                "column_key_types": ["UNI", "UNI", "", ""]
            }

    @staticmethod
    def create_system_tables(): 
        return {
            "command": "CREATE TABLE",
            "table_name": "system_tables",
            "column_list": [
                {
                    "column_name": "rowid",
                    "column_key": "UNI",
                    "is_nullable": "NO",
                    "data_type": "INT",
                    "column_position": 1,
                    "table_name": "system_tables"
                },
                {
                    "column_name": "table_name",
                    "column_key": "UNI",
                    "is_nullable": "NO",
                    "data_type": "TEXT",
                    "column_position": 2,
                    "table_name": "system_tables"
                },
                {
                    "column_name": "page_size",
                    "column_key": "",
                    "is_nullable": "NO",
                    "data_type": "SMALLINT",
                    "column_position": 3,
                    "table_name": "system_tables"
                },
                {
                    "column_name": "id_row",
                    "column_key": "",
                    "is_nullable": "NO",
                    "data_type": "INT",
                    "column_position": 4,
                    "table_name": "system_tables"
                },
            ]
            }

    @staticmethod
    def create_system_columns():
        return {
            "command": "CREATE TABLE",
            "table_name": "system_columns",
            "column_list": [
                {
                    "column_name": "rowid",
                    "column_key": "UNI",
                    "is_nullable": "NO",
                    "data_type": "INT",
                    "column_position": 1,
                    "table_name": "system_columns"
                },
                {
                    "column_name": "table_name",
                    "column_key": "",
                    "is_nullable": "NO",
                    "data_type": "TEXT",
                    "column_position": 2,
                    "table_name": "system_columns"
                },
                {
                    "column_name": "column_name",
                    "column_key": "",
                    "is_nullable": "NO",
                    "data_type": "TEXT",
                    "column_position": 3,
                    "table_name": "system_columns"
                },
                {
                    "column_name": "data_type",
                    "column_key": "",
                    "is_nullable": "NO",
                    "data_type": "TEXT",
                    "column_position": 4,
                    "table_name": "system_columns"
                },
                {
                    "column_name": "column_position",
                    "column_key": "",
                    "is_nullable": "NO",
                    "data_type": "TINYINT",
                    "column_position": 5,
                    "table_name": "system_columns"
                },
                {
                    "column_name": "is_nullable",
                    "column_key": "",
                    "is_nullable": "NO",
                    "data_type": "TEXT",
                    "column_position": 6,
                    "table_name": "system_columns"
                },
                {
                    "column_name": "column_key",
                    "column_key": "",
                    "is_nullable": "YES",
                    "data_type": "TEXT",
                    "column_position": 7,
                    "table_name": "system_columns"
                },
            ]
        }
    
    @staticmethod
    def fill_system_tables():
        return [
            {
                "command": "INSERT INTO TABLE",
                "column_name_list": [
                    "rowid",
                    "table_name",
                    "page_size",
                    "id_row"
                    ],
                "table_name": "system_tables",
                "value_list": [
                    1,
                    "system_tables",
                    Settings.get_page_size(),
                    2
                    ]
            },
            {
                "command": "INSERT INTO TABLE",
                "column_name_list": [
                    "rowid",
                    "table_name",
                    "page_size",
                    "id_row"
                    ],
                "table_name": "system_tables",
                "value_list": [
                    2,
                    "system_columns",
                    Settings.get_page_size(),
                    11
                    ]
            },
        ]

    @staticmethod
    def fill_system_columns():
        return [
                {
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
                        1,
                        "system_tables",
                        "rowid",
                        "INT",
                        1,
                        "NO",
                        "UNI"
                        ]
                },
                {
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
                        2,
                        "system_tables",
                        "table_name",
                        "TEXT",
                        2,
                        "NO",
                        "UNI"
                        ]
                },
                {
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
                        3,
                        "system_tables",
                        "page_size",
                        "SMALLINT",
                        3,
                        "NO",
                        ""
                        ]
                },
                {
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
                        4,
                        "system_tables",
                        "id_row",
                        "INT",
                        4,
                        "NO",
                        ""
                        ]
                },
                {
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
                        5,
                        "system_columns",
                        "rowid",
                        "INT",
                        1,
                        "NO",
                        "UNI"
                        ]
                },
                {
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
                        6,
                        "system_columns",
                        "table_name",
                        "TEXT",
                        2,
                        "NO",
                        ""
                        ]
                },
                {
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
                        7,
                        "system_columns",
                        "column_name",
                        "TEXT",
                        3,
                        "NO",
                        ""
                        ]
                },
                {
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
                        8,
                        "system_columns",
                        "data_type",
                        "TEXT",
                        4,
                        "NO",
                        ""
                        ]
                },
                {
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
                        9,
                        "system_columns",
                        "column_position",
                        "TINYINT",
                        5,
                        "NO",
                        ""
                        ]
                },
                {
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
                        10,
                        "system_columns",
                        "is_nullable",
                        "TEXT",
                        6,
                        "NO",
                        ""
                        ]
                },
                {
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
                        11,
                        "system_columns",
                        "column_key",
                        "TEXT",
                        7,
                        "YES",
                        ""
                        ]
                },
            ]

