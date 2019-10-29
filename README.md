# Define and Check Foreign Key Hints in Snowflake

Snowflake does not enforce foreign keys. This is meant for a situation where you have some tables without foreign keys, but would like to add some for documentation. At the same time, you want to check them.

This will be implemented with sqlalchemy.

### Planned steps
1. Create sample tables
2. Have a list of foreign key suspects
3. Create code to create (and drop) the foreign keys
4. Create code to check if there are foreign key violations
5. write this information back into snowflake

#### Table for Foreign Keys

Table for basic information:
child_table_schema, child_table, child_column, parent_table_schema, parent_table, parent_column, foreign_key_name (uniq)

Table for lifecycle information:
foreign_key_name, 

#### Table for Primary Keys

Snowflake does not present primary key information in their information schema in a view. It is accessible through "describe tables" or "show primary keys". To detect foreign key candidates, i would still prefer to have this information in a table. This is what `create_primary_key_table.py` is for.





Checks:
We only use primary keys in the parent table, so we do not have to check the condition that it is a candidate key (unique and minimal). (Does Snowflake allow non minimal primary keys? How would she know?) 
We only check if an entry in the parent table column exists for every non null value in the child column.
We don't do self links for now.

MATCH (for multi column foreign keys): Use MATCH SIMPLE. `MATCH SIMPLE allows any of the foreign key columns to be null; if any of them are null, the row is not required to have a match in the referenced table.` [postgres documentation](https://www.postgresql.org/docs/current/sql-createtable.html)
How expensive will this calculation be?
We go by columns, which is good, but through both entire tables, which is bad.



Consider:

add info if primary key (as view with join on information schema?)

### Issues

no exception handling or tests so far

The first step already has issues due to autoincrement inconsistencies in the sqlalchemy / snowflake implementation.

https://docs.snowflake.net/manuals/user-guide/sqlalchemy.html

### Prereqs - will go into setup.py

python 3
pip3 install sqlalchemy
pip install --upgrade snowflake-sqlalchemy

