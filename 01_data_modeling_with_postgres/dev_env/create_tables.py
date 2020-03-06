import psycopg2
import logging
import re
import os
from sql_queries import create_table_queries, drop_table_queries

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
    )

DBUSER = os.getenv('POSTGRES_USER')
DBNAME = os.getenv('DATA_DB')
DEFAULT_DB = os.getenv('POSTGRES_DB')
DBPASS = os.getenv('POSTGRES_PASSWORD')

def _get_table_name(s):
    """
    Gets tables names from query to help track logging progress

    :param s: query to parse
    :return: the matching table name
    """
    m = re.search(r'(IF.*EXISTS) ([a-z]+)', s)
    return m.group(2)


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """

    # connect to default database
    logging.info('Connecting to default database')
    conn = psycopg2.connect(
        f"host=db dbname={DEFAULT_DB} user={DBUSER} password={DBPASS}"
        )
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    logging.info('Droping sparkifydb if exists')
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    logging.info('Creating sparkifydb')
    cur.execute(
        "CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0"
        )

    # close connection to default database
    conn.close()

    # connect to sparkify database
    logging.info('Connecting to sparkifydb')
    conn = psycopg2.connect(
        f"host=db dbname={DBNAME} user={DBUSER} password={DBUSER}"
        )
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        logging.info(f'Droping table {_get_table_name(query)}')
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        logging.info(f'Creating table {_get_table_name(query)}')
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database.

    - Establishes connection with the sparkify database and gets
    cursor to it.

    - Drops all the tables.

    - Creates all tables needed.

    - Finally, closes the connection.
    """
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
