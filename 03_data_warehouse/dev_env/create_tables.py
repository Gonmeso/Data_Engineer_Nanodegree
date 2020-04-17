import psycopg2
from utils import get_table_name, get_cluster_connection_str
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        print(f'---- Droping table {get_table_name(query)} ----')
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        print(f'---- Creating table {get_table_name(query)} ----')
        cur.execute(query)
        conn.commit()


def main():

    conn = psycopg2.connect(get_cluster_connection_str())
    print('---- Successfully connected to Redshift Cluster ----')
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
