import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Runs queries that drop all the tables
    """
    for table, query in drop_table_queries.items():
        print("------- DROPPING TABLE: ** {} ** -------".format(table))
        cur.execute(query)
        conn.commit()
        print("------- DONE\n")
def create_tables(cur, conn):
    """
    Runs queries that create all the tables
    """
    for table, query in create_table_queries.items():
        print("+++++++ CREATING TABLE: ** {} ** +++++++".format(table))
        cur.execute(query)
        conn.commit()
        print("+++++++ DONE\n")

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()