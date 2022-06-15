import sqlite3 as sql
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def create_tables():
    # This is the path where you want to search
    path = ".\data"
    conn = sql.connect('kishan.db')
    # this is the extension you want to detect
    extension = '.csv'
    fields = []
    rows = []
    for root, dirs_list, files_list in os.walk(path):
        for file_name in files_list:
            if os.path.splitext(file_name)[-1] == extension:
                file_name_path = os.path.join(root, file_name)
                #  print(file_name_path)
                tblname = file_name_path.replace("\\", "_").replace(".csv", "")
                # print(file_name_path.replace("\\", "_"))
                df = pd.read_csv(file_name_path)
                fields.append(df.shape[1])
                rows.append(df.shape[0])
                df.to_sql(tblname, conn, if_exists='append', index=False)

                num_fields = pd.DataFrame([["1", str(sum(fields) / len(fields))]], columns=['Question', 'Answer'])
                num_total = pd.DataFrame([["3", str(sum(rows))]], columns=['Question', 'Answer'])
                print(num_fields)
                print(num_total)
                return num_fields, num_total

default_args = {
    'owner': 'Kishan',
    'start_date': days_ago(1)
}
# Defining the DAG using Context Manager
with DAG(
        'createtables_kishan',
        default_args=default_args,
        schedule_interval=None,
) as dag:
    t1 = PythonOperator(
        task_id='createtable_and_count',
        python_callable=create_tables,
    )





