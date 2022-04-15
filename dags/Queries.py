from aifc import Error
import csv
from pathlib import Path
from airflow import DAG
import os
import glob
import ntpath
from datetime import timedelta
from datetime import datetime
from datetime import date
from pathlib import Path
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.utils.dates import days_ago
# import pandas and numpy
import pandas as pd
import numpy as np
import sqlite3 as lite

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    db_file = r'/opt/airflow/Pipe.db'
    try:
        conn = psycopg2.connect(
            host="localhost",
            database= db_file,
            user="postgres",
            password="Abcd1234")
    except Error as e:
        print(e)

    return conn

def avgCols(conn):
    
    cur = conn.cursor()
    cur.execute("SELECT avg(count(*)) FROM information_schema.columns")

    rows = cur.fetchall()

    for row in rows:
        print(row)

def wordCount(conn):
    cur = conn.cursor()
    cur.execute("""SELECT   UNNEST(string_to_array(*, ' ')) AS value, COUNT(*) AS count""")

    rows = cur.fetchall()

    for row in rows:
        print(row)

def  rowCount(conn):
    cur =conn.cursor()
    cur.execute("""select
                count_rows_of_table(table_schema, table_name)
                from
                information_schema.tables
                '""")

    rows = cur.fetchall()

    for row in rows:
        print(row)