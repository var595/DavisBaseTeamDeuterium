# CS6360 TeamDeuterium DavisBase
<br />

### (1) Project Setup

```bash
  gh repo clone var595/DavisBaseTeamDeuterium
  
  
  cd DavisBaseTeamDeuterium
  
  # install dependencies
  pip3 install -r requirements.txt
  ```
  
### (2) Launching application

  ```bash
  python3 main.py
  ```
  
  Initialize Database
  
  ```bash
  davisql> init db;
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
