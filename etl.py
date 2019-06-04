import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load json files to staging_tables.
    
    Args:
        cur: a cursor object. Allows Python code to execute PostgreSQL command
            in a database session.
        conn: a connection object of a database session. Handles the connection
            to a PostgreSQL database instance.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert staging data to fact and dimension tables.
    
    Args:
        cur: a cursor object. Allows Python code to execute PostgreSQL command
            in a database session.
        conn: a connection object of a database session. Handles the connection
            to a PostgreSQL database instance.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


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