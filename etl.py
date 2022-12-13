# import libraries
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads staging tables by copy the data from s3 to redshift tables
    :param cur: database cursor to execute the queries
    :param conn: conn to be used to commit and close the transaction
    :return: None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert into fives dimension and fact tables from staging tables
    :param cur: database cursor to execute the queries
    :param conn: conn to be used to commit and close the transaction
    :return: None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main strating point of the file
    """
    # read the config files
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # create the connection to redshift database using the config files
    conn = psycopg2.connect("host={} dbname={} user={} password={} port=\
    {}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    # copy the files from S3 to staging tables in redshift
    print("Loading data into staging tables")
    load_staging_tables(cur, conn)
    print("Loading data into tables")
    insert_tables(cur, conn)
    print("Done")
    conn.close()


if __name__ == "__main__":
    main()
