import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        This function iterates over the list of load queries that load the data from
         files in s3 bucket to the stage tables using COPY command.
        INPUTS:
        * cur the cursor variable of the database
        * conn the connection variable of the database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        This function iterates over the list of insert queries that insert the data from stage table to final table.
        INPUTS:
        * cur the cursor variable of the database
        * conn the connection variable of the database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        This function is the entry point of our program.
        It reads the db credentials from the config file, connects to the database, loads the s3 files into sage tables, loads the final tables from stage tables
        and eventually closes the database connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
