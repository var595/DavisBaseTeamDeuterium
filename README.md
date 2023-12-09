# 6360-TeamProject Database Engine
## DavisBase Database 

<br />

#### (1) Initial Set up in Local Environment

```bash
  git clone https://github.com/danielwscott23/6360-TeamProject.git
  
  
  cd 6360-TeamProject
  
  # install dependencies
  pip3 install -r requirements.txt
  ```
  

### (2) Launching application

  ```bash
  python3 main.py
  ```
  The output will be as follows :-
  
  <img width="572" alt="image" src="https://user-images.githubusercontent.com/56747530/205740343-bf4bfbc9-3107-4b23-80d5-804fabb4e892.png">
  
  run init db command to intilaize the database. A data folder will be created in your current directory which will store information about the tables and   indexes. Note that the database supports only one instance per directory
  
  ```bash
  oppenheimer-db> init db;
  ```
  
  
### (3) Supported Commands

  The database supports below commands 
  
  ```bash
    clear                                                           : Clear screen
    
    SHOW TABLES                                                     : List all tables currently in database
    
    INSERT INTO TABLE (<column list>) <table> VALUES (<value list>) : Insert data into a particular table
    
    CREATE TABLE <table> (column datatype constraint)               : create a new table in the database
    
    CREATE INDEX <table> (<column>)                                  : Create an index on a column
    
    SELECT <column list | *> FROM <table> <WHERE condition>         : sql_query_parser data from the database
    
    UPDATE <table> SET <set directives> <WHERE condition>           : Update data in the tables
    
    DELETE FROM TABLE <table> <WHERE condition>                     : Delete data from a table
    
    DROP TABLE <table>                                              : Delete table from database
    
    DROP INDEX <table> (<column>)                                    : Delete an index on a column
    
    exit                                                            : Exit Program
  ```
