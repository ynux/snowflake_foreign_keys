from sqlalchemy import create_engine, MetaData, Table, Integer, String, Column, insert, sql, Sequence, exc
import configparser

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

# results = connection.execute('select current_version()').fetchone()

def create_sample_tables(eng):
    # Some sample tables, without foreign keys
    metadata = MetaData()
    users = Table('users', metadata,
        Column('user_id', Integer, Sequence('user_id_seq'), primary_key=True),
        Column('user_name', String(16), nullable=False),
        Column('nickname', String(50), nullable=False)
    )
    usergroups = Table('usergroups', metadata,
        Column('group_id', Integer, Sequence('usergroup_id_seq'), primary_key=True),
        Column('group_name', String(60)),
        Column('user_id', Integer)
    )
    pets = Table('pets', metadata,
        Column('pet_id', Integer, Sequence('pet_id_seq'), primary_key=True),
        Column('user_id', Integer, nullable=False),
        Column('pet_name', String(60)),
        Column('pet_kind', String(50))
    )
    connection = eng.connect()
    metadata.create_all(eng)

def drop_sample_tables(eng):
    metadata2 = MetaData()
    for table_to_drop in ['users', 'usergroups', 'pets']:
        try:
            table_to_drop = Table(table_to_drop, metadata2, autoload=True, autoload_with=eng)
            table_to_drop.drop(eng)
        except exc.NoSuchTableError:
            continue

def truncate_sample_tables(eng):
    metadata2 = MetaData()
    for table_to_delete in ['users', 'usergroups', 'pets']:
        try:
            table_to_delete = Table(table_to_delete, metadata2, autoload=True, autoload_with=eng)
            table_to_delete.delete()
        except exc.NoSuchTableError or exc.ProgrammingError:
            continue

def insert_sample_data(eng):
    connection = eng.connect()
    metadata2 = MetaData()
    users = Table('users', metadata2, autoload=True, autoload_with=eng)
    seq = Sequence('user_id_seq')
    values_list = [
        {'user_name': 'lucy', 'nickname': 'in the sky'},
        {'user_name': 'mary', 'nickname': 'jane'},
        {'user_name': 'johnny', 'nickname': 'jim and jack'}
    ]
    for val in values_list:
        nextid = connection.execute(seq)
        val[user_id] = nextid
        connection.execute(users.insert().values(val))
 
    usergroups = Table('usergroups', metadata2, autoload=True, autoload_with=eng)

    connection.execute(usergroups.insert(), [
        {'group_id': 1, 'group_name': 'catlovers', 'user_id': 1},
        {'group_id': 2, 'group_name': 'catlovers', 'user_id': 2},
        {'group_id': 3, 'group_name': 'fishlovers', 'user_id': 1},
        {'group_id': 4, 'group_name': 'doglovers', 'user_id': 1},
        {'group_id': 5, 'group_name': 'doglovers', 'user_id': 3} 
    ])
    pets = Table('pets', metadata2, autoload=True, autoload_with=eng)

    connection.execute(pets.insert(), [
        {'pet_id': 1, 'user_id': 1, 'pet_name': 'doggie', 'pet_kind': 'cat'},
        {'pet_id': 2, 'user_id': 1, 'pet_name': 'kitty', 'pet_kind': 'fish'},
        {'pet_id': 3, 'user_id': 2, 'pet_name': 'nemo', 'pet_kind': 'dog'},
        {'pet_id': 4, 'user_id': 2, 'pet_name': 'wolf', 'pet_kind': 'cat'},
        {'pet_id': 5, 'user_id': 3, 'pet_name': 'whale', 'pet_kind': 'cat'}
    ])


if __name__ == "__main__":
    #create_sample_tables(engine)
    #drop_sample_tables(engine)
    #truncate_sample_tables(engine)
    insert_sample_data(engine)
