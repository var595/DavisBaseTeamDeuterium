import os
import shutil
import traceback
from pyparsing import Dict
from lib.objects.Index import Index
from lib.objects.SystemTable import SystemTable
from lib.objects.Table import Table
from lib.parsers.Command import Command
from lib.parsers.OutputFormat import OutputFormat

from lib.parsers.Query import Query
from lib.settings.Settings import Settings
from lib.workers.CreateDatabase import CreateDatabase


class DatabaseActions:
    @staticmethod
    def command_parse(command: str, tables: Dict, indexes: Dict):
        if command.lower() == "exit;":
            Settings.set_exit(True)
            return
        elif command.lower() == "clear;":
            os.system('cls' if os.name == 'nt' else 'clear')

        elif command.lower() == "init db;":
            DatabaseActions.create_database(tables, indexes)
            DatabaseActions.save_to_disk(tables, indexes)

        elif command.lower() == "drop db;":
            DatabaseActions.drop_database(tables, indexes)
            
        elif command.lower() == "help;":
            DatabaseActions.help()
        else:
            try:
                parsed_tokens = Query(command).parse()[0]
                Command.router(parsed_tokens, tables, indexes)
                DatabaseActions.save_to_disk(tables, indexes)
                DatabaseActions.load_db(tables,indexes)
            except Exception as e:
                print("Error: Invalid Command - Please check your syntax and try again")
            
    
    @staticmethod
    def create_database(tables: Dict, indexes: Dict):
        Command.router(CreateDatabase.create_system_tables(), tables, indexes)
        Command.router(CreateDatabase.create_system_columns(), tables, indexes)
        
        for entry in CreateDatabase.fill_system_tables():
            try:
                Command.router(entry, tables, indexes)
            except:
                traceback.print_exc()
            
        for entry in CreateDatabase.fill_system_columns():
            try:
                Command.router(entry, tables, indexes)
            except:
                traceback.print_exc()

        print(f"Database created.")

        DatabaseActions.save_to_disk(tables,indexes)

    def drop_database(tables,indexes):
        exec_path = Settings.get_exec_path()
        if os.path.exists(os.path.join(exec_path,Settings.get_data_dir())):
            tables.clear()
            indexes.clear()
            for root, dirs, files in os.walk(os.path.join(exec_path,Settings.get_data_dir())):
                for f in files:
                    os.unlink(os.path.join(root, f))
                for d in dirs:
                    shutil.rmtree(os.path.join(root, d))

        print(f"Database dropped.")

    @staticmethod    
    def save_to_disk(tables,indexes):

        tbl_ext = Settings.get_tbl_ext()
        ndx_ext = Settings.get_ndx_ext()
        exec_path = Settings.get_exec_path()

        if not os.path.exists(os.path.join(exec_path,Settings.get_data_dir())):
                os.makedirs(os.path.join(exec_path,Settings.get_data_dir()))

        for k, tab in tables.items():
            if k not in {"system_tables", "system_columns"}:
                rec_count = tab.id_row
                parsed_tokens = SystemTable.update_table_data(k, rec_count)
                
                Command.router(parsed_tokens, tables, indexes)
                file_path = os.path.join(exec_path,Settings.get_data_dir(),f"{k}{tbl_ext}")
                with open(file_path, 'wb') as f:
                    f.write(tab.object_to_bytes())

                
        for k, tab in tables.items():
            if k in {"system_tables", "system_columns"}:
                rec_count = tab.id_row
                parsed_tokens = SystemTable.update_table_data(k, rec_count)
                Command.router(parsed_tokens, tables, indexes)
                file_path = os.path.join(exec_path,Settings.get_data_dir(),f"{k}{tbl_ext}")
                with open(file_path, 'wb') as f:
                    f.write(tab.object_to_bytes())

        for k,ndx in indexes.items():
            file_path = os.path.join(exec_path,Settings.get_data_dir(),f"{k}{ndx_ext}")
            with open(file_path, 'wb') as f:
                      f.write(ndx.object_to_bytes())
    
    @staticmethod
    def help():
        help = ["\nclear;  # Clear screen",
                "SHOW TABLES; # List all tables currently in database",
                "INSERT INTO TABLE (<column list>) <table> VALUES (<value list>) # Insert data into a particular table",
                "CREATE TABLE <table> (column datatype constraint); # create a new table in the database",
                "CREATE INDEX <table> (column>); # Create an index on a column",
                "SELECT <column list | *> FROM <table> <WHERE condition>; # Query data from the database",
                "UPDATE <table> SET <set directives> <WHERE condition>;  # Update data in the tables",
                "DELETE FROM TABLE <table> <WHERE condition> # Delete data from a table",
                "DROP TABLE <table>; # Delete table from database",
                "DROP INDEX <table> (column>); # Delete an index on a column",
                "exit; # Exit Program"  
        ]
        print("\n".join(help))


    @staticmethod 
    def load_db(tables: Dict, indexes: Dict):
        page_size = Settings.get_page_size()
        exec_path = Settings.get_exec_path()
        tbl_ext = Settings.get_tbl_ext()
        ndx_ext = Settings.get_ndx_ext()
        data_dir = os.path.join(exec_path,Settings.get_data_dir())
        system_tables  = os.path.join(data_dir,f"system_tables{tbl_ext}")
        system_columns = os.path.join(data_dir,f"system_columns{tbl_ext}")

        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        if os.path.exists(system_tables) and os.path.exists(system_columns):
            with open(system_tables, 'rb') as f:
                OutputFormat.disable_stdout()
                table = Table.bytes_to_object(f.read(), Settings.get_page_size(),2, CreateDatabase.system_tables_column_data(), "system_tables")
                OutputFormat.enable_stdout()
                tables["system_tables"] = table
                f.close()

            with open(system_columns, 'rb') as f:
                OutputFormat.disable_stdout()
                table= Table.bytes_to_object(f.read(), Settings.get_page_size(), 11, CreateDatabase.system_columns_column_data(), "system_columns")
                OutputFormat.enable_stdout()
                tables["system_columns"] = table
                f.close()

            pdict = Query("select id_row from system_tables where table_name = 'system_tables';").parse()[0]
            pdict["ret_mode"] = True
            table_rec_count = Command.router(pdict, tables, indexes)
            
            pdict = Query("select id_row from system_tables where table_name = 'system_columns';").parse()[0]
            pdict["ret_mode"] = True
            col_rec_count = Command.router(pdict, tables, indexes)
            
            tables["system_tables"].id_row = table_rec_count[-1][-1]
            tables["system_columns"].id_row = col_rec_count[-1][-1]

            pdict = Query("show tables;").parse()[0]
            pdict["ret_mode"] = True
            table_names = Command.router(pdict, tables, indexes)
            for t in table_names:
                    table_name = t[-1]
                    if table_name not in tables:
                        table_name = table_name.lower()
                        table_path = os.path.join(exec_path,Settings.get_data_dir(),f"{table_name}{tbl_ext}")
            
                        cdata = SystemTable.parse_column_data(table_name)
                        cdata["ret_mode"] = True
                        cdata_list = Command.router(cdata, tables, indexes)            
                        cdata_list.sort(key=lambda x: x[2])
                        
                        column_data = {
                            "column_names": [rec[0] for rec in cdata_list],
                            "data_types": [rec[1] for rec in cdata_list],
                            "is_nullable": [rec[3] for rec in cdata_list],
                            "column_key_types": [rec[4] for rec in cdata_list]
                        }
                        
                        tdata = SystemTable.parse_table_data(table_name)
                        tdata["ret_mode"] = True
                        output = Command.router(tdata, tables, indexes)
                        
                        
                        rec_count = 0
                        
                        if output:
                            page_size, rec_count = output[0]
                        
                        with open(table_path, 'rb') as f:
                            OutputFormat.disable_stdout()
                            new_table = Table.bytes_to_object(f.read(), page_size, rec_count, column_data)
                            OutputFormat.enable_stdout()      
                            tables[table_name] = new_table
        for filename in os.listdir(data_dir):     
            file_path = os.path.join(data_dir, filename)
            if len(filename.split(".")) == 3:
                table_name = filename.split(".")[0]
                column_name = filename.split(".")[1]
                ext = filename.split(".")[2]
                if ext == ndx_ext[-3:]:  
                    with open(file_path, 'rb') as f:
                        OutputFormat.disable_stdout()
                        new_index, count = Index.bytes_to_object(f.read(), page_size)
                        new_index.id_row = count
                        OutputFormat.enable_stdout()      
                        indexes[table_name+"."+column_name] = new_index
                        tables[table_name].indexes[column_name] = new_index
                        

   
                

                

