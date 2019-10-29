import datetime
import configparser
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Sequence, Table
from sqlalchemy import create_engine, exc, insert, sql, text
from snowflake.sqlalchemy import MergeInto

# connect
config = configparser.ConfigParser()
config.read('foreign_key_snow/config.ini')

engine = create_engine(
    'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'.format(
        user=config['db_credentials']['user'],
        password=config['db_credentials']['password'],
        account=config['db_connection']['account'],
        database=config['db_connection']['database'],
        schema=config['db_connection']['schema'],
        warehouse=config['db_connection']['warehouse']
    )
)

def create_primary_key_table(eng):
    # Some sample tables, without foreign keys
    metadata = MetaData()
    users = Table('primary_keys', metadata,
        Column('created_on', DateTime, nullable=False),
        Column('database_name', String(16777216), nullable=False),
        Column('schema_name', String(16777216), nullable=False),
        Column('table_name', String(16777216), nullable=False),
        Column('column_name', String(16777216), nullable=False),
        Column('key_sequence', Integer, nullable=False),
        Column('constraint_name', String(16777216), nullable=False),
        Column('comment', String(16777216), nullable=True),
        Column('filled_on', DateTime, nullable=False)
    )
    connection = eng.connect()
    metadata.create_all(eng)

def drop_primary_key_table(eng):
    metadata2 = MetaData()
    try:
        table_to_drop = Table('primary_keys', metadata2, autoload=True, autoload_with=eng)
        table_to_drop.drop(eng)
    except exc.NoSuchTableError or exc.ProgrammingError:
        pass

def truncate_primary_key_table(eng):
    metadata2 = MetaData()
    try:
        table_to_delete = Table('primary_keys', metadata2, autoload=True, autoload_with=eng)
        table_to_delete.delete()
    except exc.NoSuchTableError or exc.ProgrammingError:
        pass

def fill_primary_key_table(eng):
    connection = eng.connect()
    metadata2 = MetaData()
    primary_keys = Table('primary_keys', metadata2, autoload=True, autoload_with=eng)    
    show_pk_results = connection.execute('SHOW PRIMARY KEYS').fetchall()
    for result in show_pk_results:
        result_dict = dict(result)
        result_dict['filled_on'] = datetime.datetime.now()
        ins = primary_keys.insert(result)
        connection.execute(ins)

# define update table function?
# define remove duplicates function?
# or simply truncate & fill
 
if __name__ == "__main__":
    #drop_primary_key_table(engine)
    #create_primary_key_table(engine)
    #truncate_primary_key_table(engine)
    fill_primary_key_table(engine)

 
