import configparser

import psycopg2

from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops tables if already created using SQL commands
    from sql_queries.py
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates tables for analytics tables and staging tables using SQL commands
    from sql_queries.py
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to AWS Redshift Cluster using credentials inside dwh.cfg
    Drops tables (if necessary) and creates tables for analytics tables
    and staging tables
    """
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        host=config["CLUSTER"]["host"],
        dbname=config["CLUSTER"]["db_name"],
        user=config["CLUSTER"]["db_user"],
        password=config["CLUSTER"]["db_password"],
        port=config["CLUSTER"]["db_port"],
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
