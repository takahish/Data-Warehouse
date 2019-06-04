import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop tables.
    
    Args:
        cur: a cursor object. Allows Python code to execute PostgreSQL command
            in a database session.
        conn: a connection object of a database session. Handles the connection
            to a PostgreSQL database instance.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create tables.
    
    Args:
        cur: a cursor object. Allows Python code to execute PostgreSQL command
            in a database session.
        conn: a connection object of a database session. Handles the connection
            to a PostgreSQL database instance.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


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