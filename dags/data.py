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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'retries': 1
}


def csvToDb():
    path = r'/opt/airflow/data'
## Get the full path of all the csv files.
    full_path_list = []
    paths=Path(path).glob('**/*.csv') 
    
    for path in paths:
        full_data_list.append(path)
 
## Convert all data to list of dicts.
    full_data_list =  [n for f in full_path_list for n in csv.DictReader(open(f,'r'))]
 
## SQL database name and initialize the sql connection.
    db_filename = r'/opt/airflow/Pipe.db'
    con = lite.connect(db_filename)
 
## Convert to dataframe and write to sql database.
    pd.DataFrame(full_data_list).to_sql('test', con, flavor='sqlite',
                    schema=None, if_exists='replace', index=True,
                    index_label=None, chunksize=None, dtype=None)
 
## Close the SQL connection
    con.close()


dag = DAG(
     'PipelineDAG',
    default_args=default_args,
    description='Cleaning Data',
    schedule_interval='@once',   
)

t1 = PythonOperator(
    task_id='csv_to_Db',
    provide_context=True,
    python_callable=csvToDb,
    dag=dag,
)

