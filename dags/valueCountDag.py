from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import sqlite3
from sqlite3 import Error
import os
import pandas as pd
import numpy as np

WORKDIR = os.getenv("AIRFLOW_HOME")
DATADIR = WORKDIR + "/data/"
DBFILE = WORKDIR + "/my_db.sqlite"

def create_connection():
    try:
        conn = sqlite3.connect(DBFILE)
        print("sqlite3", sqlite3.version, DBFILE)
    except Error as e:
        print(e)
    return conn

def close_connection(conn):
    if not conn: return True
    conn.close()
    return True

def _get_tables():
    conn = create_connection()
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';");
    tables = cur.fetchall()
    #clean tables because out put is ('table_name',)
    for i, table in enumerate(tables):
        tables[i] = table[0]
    close_connection(conn)
    return tables

def _get_value_count_of_tables():
    conn = create_connection()
    cur = conn.cursor()
    tables = _get_tables()
    word_counts = {}
    for i, table_name in enumerate(tables):
        query = "SELECT * FROM '{}'".format(table_name)
        cur.execute(query);
        rows = cur.fetchall()
        for j, row in enumerate(rows):
            for value in row:
                value_as_str = str(value)
                value_as_list = value_as_str.split(" ")
                if value not in word_counts.keys():
                    wc = len(value_as_list)
                    word_counts[value_as_str] = wc
    close_connection(conn)
    return word_counts

def _format_answer(ti):
    answer = ti.xcom_pull(task_ids=['get_value_count_of_tables'])[0]
    ret = "\n|{0:150}|{1:10}|".format("value","count")
    ret += "\n|"
    for i in range(0,150): ret += "-"
    ret += "+"
    for i in range(0,10): ret += "-"
    ret +="|"
    for value, count in answer.items():
        value_as_str = str(value)
        value_to_print = value_as_str
        if len(value_to_print) > 150:
            value_to_print = value_as_str[:144] + "..." + value_as_str[-3:]
        ret += "\n|{0:150}|{1:10}|".format(value_to_print,count)
    return ret

with DAG("value_count_dag",
schedule_interval=None,
start_date=days_ago(1), 
catchup=False) as dag:
    get_tables = PythonOperator(
        task_id = "get_tables",
        python_callable=_get_tables
    )
    
    get_value_count_of_tables = PythonOperator(
        task_id = "get_value_count_of_tables",
        python_callable=_get_value_count_of_tables
    )
    
    format_answer = PythonOperator(
        task_id = "format_answer",
        python_callable=_format_answer
    )
    
    
    
get_tables >> get_value_count_of_tables >> format_answer 