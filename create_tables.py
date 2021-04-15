import configparser

import psycopg2
from psycopg2.extras import DictCursor, DictConnection

from sql_queries import CREATE_TABLE_QUERIES, DROP_TABLES, DROP_TABLE_FORMAT


def drop_tables(cur: DictCursor, conn: DictConnection):
    """
    Function that drops all tables from `DROP_TABLES` by using the
    `DROP_TABLE_FORMAT` string format

    Parameters
    ----------
    cur
    conn
    """
    for table in DROP_TABLES:
        cur.execute(
            DROP_TABLE_FORMAT.format(
                table=table
            )
        )
        conn.commit()


def create_tables(cur: DictCursor, conn: DictConnection):
    """
    Function that creates all tables from `CREATE_TABLE_QUERIES`

    Parameters
    ----------
    cur
    conn
    """
    for query in CREATE_TABLE_QUERIES:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
