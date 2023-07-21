# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 11:26:58 2023

@author: Yan
"""

import configparser
import psycopg2
from sql_queries import select_rows_count_queries


def get_results(cur, conn):
    """
    Get the number of rows stored into each table.
    """
    for query in select_rows_count_queries:
        cur.execute(query)
        results = cur.fetchone()

        for row in results:
            print("Number of rows", row)


def main():
    """
    Run queries on the staging and dimensional tables to validate that the project has been created successfully.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    get_results(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()