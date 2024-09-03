import pandas as pd
import numpy as np
import psycopg2


LAST_ID_FILE_PATH = 'test_task/last_added_objectid.json'
DB_CONFIG_FILE_PATH = ''
db_config = {
    "ip": 'localhost',
    "port": 5432,
    "user": 'postgres',
    "pass": 'postgres',
    "dbname": 'dataforest' 
}
TARGET_TABLE_NAME = 'onsqpd_centroids'
FILE_PATH = 'data/ONSPD_Online_Latest_Centroids.csv'


def get_db_config():
    pass

def connect_psycopg_db(db_config):
    try:
        connection = psycopg2.connect(
            host=db_config["ip"],
            user=db_config["user"],
            password=db_config["pass"],
            dbname=db_config["dbname"],
            port=db_config["port"],
            connect_timeout=10
        )
        cursor = connection.cursor()
        return connection, cursor
    except Exception as e:
        print(f"Error - {e}")
        return None, None

def execute_query(query, results=False):
    with connection.cursor() as cursor:
        try:
            cursor.execute(query)
            connection.commit()
            return cursor.fetchall() if results else None
        except Exception as e:
            connection.rollback()
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {e}")
            return None
   
def create_target_table(table_name=TARGET_TABLE_NAME):
    create_table_query = """
        CREATE TABLE IF NOT EXISTS {table_name} (
            OBJECTID INTEGER PRIMARY KEY,
            PCD2 TEXT,
            DOINTR DATE,
            DOTERM DATE,
            OSCTY TEXT,
            LAT DECIMAL(9, 6),
            LONG DECIMAL(9, 6),
            MAX_DOINTR DATE,
            MIN_DOINTR DATE
        );
    """.format(table_name=table_name)
    execute_query(create_table_query)

def process_data(dataset):
    dataset['DOINTR'] = pd.to_datetime(dataset['DOINTR'], format='%Y%m', errors='coerce')
    dataset['DOTERM'] = pd.to_datetime(dataset['DOTERM'], format='%Y%m', errors='coerce')
    grouper_dataset = dataset.groupby('OSCTY')
    dataset['MAX_DOINTR'] = grouper_dataset['DOINTR'].transform('max')
    dataset['MIN_DOINTR'] = grouper_dataset['DOINTR'].transform('min')
    dataset.replace({np.NaN: None}, inplace=True)

def write_to_db(to_db_list, table_name, cursor, connection, id_tag=None, update_string=None, on_conflict=False):
    """
    :param to_db_list: list of lists
    :param table_name: str name
    :param connection: connection to db
    :param cursor: cursor of connection to db
    :param id_tag: primary key
    :param update_string: list ogf columns
    :param on_conflict: False by default
    
    :return: None
    """
    print('Formulating query...')
    signs = '(' + ('%s,' * len(to_db_list[0]))[:-1] + ')'
    try:
        args_str = b','.join(cursor.mogrify(signs, x) for x in to_db_list)
        args_str = args_str.decode()
        insert_statement = """INSERT INTO %s VALUES """ % table_name
        conflict_statement = """ ON CONFLICT DO NOTHING"""
        if on_conflict:
            conflict_statement = """ ON CONFLICT ("{0}") DO UPDATE SET {1};""".format(id_tag, update_string)
        print('Start execution...')
        cursor.execute(insert_statement + args_str + conflict_statement)
        connection.commit()
    except Exception as e:
        print(e)
        connection.rollback()

def add_geography_objects():
    add_geography_with_index_query = """
        ALTER TABLE onsqpd_centroids_prepare
        ADD COLUMN IF NOT EXISTS geog geography(POINT, 4326);

        UPDATE onsqpd_centroids_prepare
        SET geog = ST_SetSRID(ST_MakePoint(long, lat), 4326);

        CREATE INDEX geog_index_prepare ON onsqpd_centroids_prepare USING GIST (geog);
    """
    execute_query(add_geography_with_index_query)

def add_native_distance_calculations(n_closest=5):
    find_closest_function_query = """
        CREATE OR REPLACE FUNCTION find_closest_places(current_long DOUBLE PRECISION, current_lat DOUBLE PRECISION)
        RETURNS TABLE(objectid INT, pcd2 TEXT, distance DOUBLE PRECISION) AS $$
        BEGIN
            RETURN QUERY
            SELECT onsqpd_centroids_prepare.objectid, onsqpd_centroids_prepare.pcd2, SQRT(POWER((current_long - long), 2) + POWER((current_lat - lat), 2)) AS distance
            FROM onsqpd_centroids_prepare
            ORDER BY distance ASC
            LIMIT {n};
        END;
        $$ LANGUAGE plpgsql;
    """.format(n=n_closest)
    execute_query(find_closest_function_query)

def add_postgis_distance_calculations(n_closest=5):
    find_closest_function_query = """
        CREATE OR REPLACE FUNCTION find_closest_places_geo(
            current_long DOUBLE PRECISION, 
            current_lat DOUBLE PRECISION
        )
        RETURNS TABLE(objectid INT, pcd2 TEXT, distance DOUBLE PRECISION) AS $$
        BEGIN
            RETURN QUERY
            SELECT onsqpd_centroids_ready.objectid, onsqpd_centroids_ready.pcd2, ST_Distance(geog, ST_SetSRID(ST_MakePoint(current_long, current_lat), 4326)) / 1000 AS distance_in_km
            FROM onsqpd_centroids_ready
            ORDER BY geog <-> ST_SetSRID(ST_MakePoint(current_long, current_lat), 4326)
            LIMIT {n};
        END;
        $$ LANGUAGE plpgsql;
    """.format(n=n_closest)
    execute_query(find_closest_function_query) 

def switch_tables(table_preparation, table_ready):
    switch_table_names_query = f"""
        BEGIN;
        DROP INDEX IF EXISTS geog_index_ready;
        ALTER TABLE {table_preparation} RENAME TO table_temp;
        ALTER TABLE {table_ready} RENAME TO {table_preparation};
        ALTER TABLE table_temp RENAME TO {table_ready};
        ALTER INDEX geog_index_prepare RENAME TO geog_index_ready;
        TRUNCATE TABLE {table_preparation};
        COMMIT;
    """
    execute_query(switch_table_names_query)


if __name__ == '__main__':
    connection, cursor = connect_psycopg_db(db_config)
    create_target_table(table_name=f'{TARGET_TABLE_NAME}_prepare')
    create_target_table(table_name=f'{TARGET_TABLE_NAME}_ready')
    dataset = pd.read_csv(
        FILE_PATH, 
        usecols=['OBJECTID', 'PCD2', 'DOINTR', 'DOTERM', 'OSCTY', 'LAT', 'LONG']
    )
    process_data(dataset)
    write_to_db(
        to_db_list=dataset.values.tolist(),
        table_name=f'{TARGET_TABLE_NAME}_prepare',
        cursor=cursor,
        connection=connection
    )
    add_geography_objects()
    add_postgis_distance_calculations(n_closest=10)
    switch_tables(f'{TARGET_TABLE_NAME}_prepare', f'{TARGET_TABLE_NAME}_ready')   
