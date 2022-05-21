import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from time import time

def load_staging_tables(cur, conn):
    """
    Runs queries that loads the JSON data from S3 into the staging tables in Redshift
    """
    for table, query in copy_table_queries.items():
        print("======= LOADING SATGING TABLE: ** {} ** =======".format(table))
        start = time()
        cur.execute(query)
        conn.commit()
        load_time = time() - start
        print("=== DONE IN: {0:.2f} sec\n".format(load_time))


def insert_tables(cur, conn):
    """
    Runs queries that populate the data warehouse tables
    """
    for table, query in insert_table_queries.items():
        print("======= POPULATING TABLE: ** {} ** =======".format(table))
        start = time()
        cur.execute(query)
        conn.commit()
        insert_time = time() - start
        print("=== DONE IN: {0:.2f} sec\n".format(insert_time))


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()