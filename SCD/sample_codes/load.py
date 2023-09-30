import snowflake.connector as sf
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas


'''

Tasks

1. insert available data
2. template to create table if doesnt't exists
3. if exists, does column match
4. if column doesn't match, alter schema
5. merge statement

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
    df = pd.read_csv(file_path)
    return df


def establish_conn():
    conn = sf.connect(
        user='SHANA',
        password='Fruit@789',
        account='vdrasvo-kl08478',
        database='PRODUCT_INSIGHTS',
        schema='PRODUCT_SCHEMA'
        # session_parameters={
        #     'QUERY_TAG': 'EndOfMonthFinancials',
        # }
    )
    cursor = conn.cursor()
    return conn, cursor


def connect_check(cursor):
    print('---------- Connection Established--------------')
    query = 'select * from products limit 10;'
    res = cursor.execute(query).fetchone()
    print(res)
    print('---------- Executed Query----------------------')


def write_data(conn, cursor, table_name):
    try:

        # Check if the table exists
        result = cursor.execute(f"SHOW TABLES LIKE '{table_name}'").fetchone()
        if result:
            print(f"Table '{table_name}' exists in Snowflake.")

        else:
            print(f"Table '{table_name}' does not exist in Snowflake.")
            create_table(conn, cursor, table_name)

        df = read_data()
        print(df.head())
        # sf.pandas_tools.write_pandas(conn, df, table_name)
        # df.to_sql(table_name, engine, index=False, method= 'pd_writer')
        print(f"Data loaded successfully into table '{table_name}'")

    except Exception as e:
        print(f"Error: {str(e)}")


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

        print(f"Table '{table_name}' created successfully in Snowflake.")

    except conn.errors.ProgrammingError as pe:
        print(f"Snowflake Error: {pe}")
    except Exception as e:
        print(f"Error: {str(e)}")


def __main__():
    table_name = 'PRODUCTS'
    conn, cursor = establish_conn()
    # connect_check(conn,cursor)
    write_data(conn, cursor, table_name)


__main__()


'''
errors faced : 
1. if number of columns don't match
Number of columns in file (8) does not match that of the corresponding table (9),
 use file format option error_on_column_count_mismatch=false to ignore this error
'''