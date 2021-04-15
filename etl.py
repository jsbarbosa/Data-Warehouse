import configparser
import psycopg2
from psycopg2.extras import DictCursor, DictConnection
from sql_queries import COPY_TABLE_QUERIES, INSERT_TABLE_QUERIES


def load_staging_tables(cur: DictCursor, conn: DictConnection):
    """
    Function that loads all data from the source S3 bucket into the staging
    Redshift tables by using the `COPY_TABLE_QUERIES` variable

    Parameters
    ----------
    cur
    conn
    """
    for query in COPY_TABLE_QUERIES:
        cur.execute(query)
        conn.commit()


def insert_tables(cur: DictCursor, conn: DictConnection):
    """
    Function that inserts data from the staging Redshift tables into the
    Redshift analytical tables by using the `INSERT_TABLE_QUERIES` variable

    Parameters
    ----------
    cur
    conn
    """
    for query in INSERT_TABLE_QUERIES:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()
        )
    )
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
