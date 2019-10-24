# Define and Check Foreign Key Hints in Snowflake

Snowflake does not enforce foreign keys. This is meant for a situation where you have some tables without foreign keys, but would like to add some for documentation. At the same time, you want to check them.

This will be implemented with sqlalchemy.

### Planned steps
1. Create sample tables
2. Have a list of foreign key suspects
3. Create code to create (and drop) the foreign keys
4. Create code to check if there are foreign key violations
5. write this information back into snowflake

### Issues

The first step already has issues due to autoincrement inconsistencies in the sqlalchemy / snowflake implementation.

https://docs.snowflake.net/manuals/user-guide/sqlalchemy.html

### Prereqs - will go into setup.py

python 3
pip3 install sqlalchemy
pip install --upgrade snowflake-sqlalchemy

