import argparse

import snowflake.connector as sf
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import sys


'''

Tasks

1. insert available data
2. template to create table if doesnt't exists
3. if exists, does column match
4. if column doesn't match, alter schema
5. merge statement /overwrite statement 


'''

'''
create database if not exists product_insights;
use database product_insights;
create schema if not exists product_schema;
use schema product_schema;
create table if not exists products (
 id integer autoincrement,
product_id	number(10,0),
product_name	varchar(256),
vol_2021	number(10,0),
vol_2022	number(10,0),
sum_revenue	number(10,4),
coefficient	number(10,4),
create_ts	timestamp,
update_ts	timestamp
);
'''


def read_data():
    file_path = './products_data_2022.csv'
    df = pd.read_csv(file_path, header=0)
    return df


def get_cred():
    parser = argparse.ArgumentParser()
    parser.add_argument('user', nargs=1, type=str)
    parser.add_argument('pwd', nargs=1, type=str)
    parser.add_argument('acc', nargs=1, type=str)
    parser.add_argument('db', nargs=1, type=str)
    parser.add_argument('sch', nargs=1, type=str)

    args = parser.parse_args()
    return args


def establish_sf_conn():
    cred = get_cred()
    conn = sf.connect(
        user=cred.user[0],
        password=cred.pwd[0],
        account=cred.acc[0],
        database=cred.db[0],
        schema=cred.sch[0]
    )

    print('------------ Connection established successfully----------------')
    cursor = conn.cursor()
    return conn, cursor


def check_sf_connection(cursor):
    print('---------- Connection Established--------------')
    query = 'select * from products limit 10;'
    res = cursor.execute(query).fetchone()
    print(res)
    print('---------- Executed Query----------------------')


def create_table(conn, cursor, table_name):
    try:
        # Define the SQL statement to create the table
        create_table_sql = f"""
            CREATE TABLE {table_name} if not exists(
                -- Add more columns here as needed
                product_id	number(10,0),
                product_name	varchar(256),
                vol_2021	number(10,0),
                vol_2022	number(10,0),
                sum_revenue	number(10,4),
                coefficient	number(10,4),
                create_ts	timestamp_ntz(9),
                update_ts	timestamp_ntz(9)
            )
        """

        # Execute the create table SQL statement
        cursor.execute(create_table_sql)

        print(f"------------ Table {table_name} created successfully in Snowflake------------ ")

    except conn.errors.ProgrammingError as pe:
        print(f"Snowflake Error: {pe}")
    except Exception as e:
        print(f"Error: {str(e)}")


def write_data(conn, cursor, table_name):
    try:
        df = read_data()
        missing_column = None

        # Check if the table exists
        result = cursor.execute(f"SHOW TABLES LIKE '{table_name}'").fetchone()

        # if table exists in snowflake 
        if result:
            print(f"------------ Table {table_name} exists in Snowflake------------ ")

            # fetch table columns
            cursor.execute(f"DESCRIBE TABLE {table_name}")
            existing_columns = [row[0].lower() for row in cursor.fetchall()]

            # print('------ existing columns are: ', end='')
            # print(existing_columns)
            if set(df.columns) - set(existing_columns):
                missing_column = (set(df.columns) - set(existing_columns)).pop()
                print(f'missing column is {missing_column}')

            # Columns do not match, alter the Snowflake table to add the extra column
            # if missing_column:
                print(f"------------ Altering Table {table_name} to add {missing_column}------------ ")
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {missing_column} STRING")
                conn.commit()
                print(f"------------ Altered Table {table_name} to add {missing_column}------------ ")

            else:
                print(f"------------ No column missing------------ ")

        # If table doesn't exist
        else:
            print(f"------------ Table '{table_name}' does not exist in Snowflake------------ ")
            create_table(conn, cursor, table_name)

        # write data to snowflake
        write_pandas(conn, df, table_name, quote_identifiers=False)
        # df.to_sql('products', conn, index=False, if_exists='append', method=pdwriter())
        print(f"------------ Data loaded successfully into table {table_name} ------------ ")

    except Exception as e:
        print(f"------------ Data load failed with error: {str(e)}------------ ")


def __main__():
    table_name = 'PRODUCTS'
    conn, cursor = establish_sf_conn()
    # check_sf_connection(conn,cursor)
    write_data(conn, cursor, table_name)
    conn.close()


__main__()


'''
errors faced : 
1. if number of columns don't match
Number of columns in file (8) does not match that of the corresponding table (9),
 use file format option error_on_column_count_mismatch=false to ignore this error
 
 2. Error: Execution failed on sql 'SELECT name FROM sqlite_master WHERE type='table' AND name=?;': not all arguments converted during string formatting
'''