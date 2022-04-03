from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import sqlite3
from sqlite3 import Error
from tabulate import tabulate
import os

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

def _get_row_counts(ti):
    conn = create_connection()
    cur = conn.cursor()
    tables = ti.xcom_pull(task_ids=['get_tables'])[0]
    counts = []
    for table_name in tables:
        query = "SELECT COUNT(*) FROM '{}'".format(table_name)
        cur.execute(query);
        count = cur.fetchall()[0][0]
        counts.append(count)
    close_connection(conn)
    return counts

def _sum_row_counts(ti):
    counts = ti.xcom_pull(task_ids=['get_row_counts'])[0]
    return sum(counts)

def _format_answer(ti):
    answer = ti.xcom_pull(task_ids=['sum_row_counts'])[0]
    ret = "\n" + tabulate([[3, answer]], headers=['Question', 'Answer'], tablefmt='orgtbl')
    return ret

with DAG("row_count_dag",
schedule_interval=None,
start_date=days_ago(1), 
catchup=False) as dag:
    get_tables = PythonOperator(
        task_id = "get_tables",
        python_callable=_get_tables
    )
    
    get_row_counts = PythonOperator(
        task_id = "get_row_counts",
        python_callable=_get_row_counts
    )
    
    sum_row_counts = PythonOperator(
        task_id = "sum_row_counts",
        python_callable=_sum_row_counts
    )
    
    format_answer = PythonOperator(
        task_id = "format_answer",
        python_callable=_format_answer
    )
    
    
get_tables >> get_row_counts >> sum_row_counts >> format_answer 