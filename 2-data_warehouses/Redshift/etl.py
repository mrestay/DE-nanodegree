import configparser

import psycopg2

from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Function to copy data from the raw staging S3 bucket to the staging tables.
    Args:
        cur: Database connection cursor.
        conn: Database connection
    """
    for idx, query in enumerate(copy_table_queries):
        print(f"Copying into table {idx + 1}")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Function to data from the staging tables to the production tables defined in the star schema.
    Args:
        cur: Database connection cursor.
        conn: Database connection
    """
    for idx, query in enumerate(insert_table_queries):
        print(f"Inserting into table {idx + 1}")
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
