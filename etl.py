import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from AWS S3 to staging tables using
    SQL statements from sql_queries.py
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data from staging to analytics tables using
    SQL statements from sql_queries.py
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to AWS Redshift Cluster using credentials inside dwh.cfg
    Loads data from S3 to staging tables
    Inserts data from staging to analytics tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        host=config["CLUSTER"]["host"],
        dbname=config["CLUSTER"]["db_name"],
        user=config["CLUSTER"]["db_user"],
        password=config["CLUSTER"]["db_password"],
        port=config["CLUSTER"]["db_port"],
    )
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
