import psycopg2
from utils import get_cluster_connection_str
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    print('---- Loading stage table ----')
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(f'---- Inserting into table ----')
        cur.execute(query)
        conn.commit()


def main():

    conn = psycopg2.connect(get_cluster_connection_str())
    print('---- Successfully connected to Redshift Cluster ----')
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    print('---- Process Finished ----')
    conn.close()


if __name__ == "__main__":
    main()
