from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from datetime import datetime

import sqlite3
from sqlite3 import Error

import os

from numpy import full
import pandas as pd

import csv

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
    
def _load_csvs():
    csvs = []
    for root, dirs, files in os.walk(DATADIR): # step through data directory recursively
        for file in files:
            if file.endswith(".csv"): #only check csvs
                full_file_path = os.path.join(root, file)
                if os.stat(full_file_path).st_size == 0: #if file is empty, skip it
                    print("Skipping Empty File", full_file_path)
                else:
                    csvs.append(full_file_path)
    return csvs

#wanted to do this as several different tasks, but couldn't figure out how to pass dataframes between tasks, non json-serializable
def _clean_convert_insert_dataframes(ti):
    csvs = ti.xcom_pull(task_ids=['load_csvs'])[0]
    conn = create_connection()
    failed = {}
    ####
    # The following goes through every file
    # reads in the csv, uses pandas to determine the delimiter and other formating
    # check for duplicate named columns (there were a few csvs with this, just using different caps)
    # which is a problem for sqlite since its case insensitive
    # then add the parsed csv as a table to the sqlfile
    # if any thing throws an exception, keep track of failed csvs.
    # If a csv was cleaned in the loop, take it out of fails 
    # (not a valid path anymore, since I clean aftewards now and changed delim check)
    ####
    
    for full_file_path in csvs:
        file = full_file_path.split("/")[-1]
        print(full_file_path)
        print("\tCREATING TABLE",file[:-4])
        try:
            #Using pandas to determine what the deliminator is.
            curr_csv = pd.read_csv(full_file_path,sep=None, engine='python', encoding='Latin-1',quoting=csv.QUOTE_ALL)
            columns = list(curr_csv)
            columns_lower = list(map(lambda x: x.lower(), columns))
            columns_renamed = list(map(lambda x: x[1] + str(columns_lower[:x[0]].count(x[1]) + 1) if columns_lower.count(x[1]) > 1 else x[1], enumerate(columns_lower)))
            if columns_lower != columns_renamed:
                print(columns, columns_renamed)
                column_rename_zip = dict(zip(columns, columns_renamed))
                curr_csv.rename(columns=column_rename_zip, inplace= True)
                print("Cleaned header with duplicate columns",full_file_path)
            curr_csv.to_sql(file[:-4], con=conn, if_exists='replace', index = False)
            if full_file_path in failed.keys():  # if it managed to clean it, remove it from failed
                print("Data was cleaned, or parsed properly for", full_file_path)
                del failed[full_file_path]
        except Exception as e:  # add anything that failed to be read or added to csv to list of failed files to try again later
            
            print("Failed to read",full_file_path)
            err_str = str(e).rsplit(',',1)
            print(err_str)
            if full_file_path not in failed:
                failed[full_file_path] = [err_str]
            else:
                failed[full_file_path] = failed[full_file_path] + [err_str]
                
    ##############################  
    # if a csv failed to be parsed or place into the sqlite db
    # try to figure out why here          
    # Some csvs fail because the contain extra lines above the header
    # or contain spaces after commas, or are empty.
    # Hardcoded ways to clean/parse them and add them to the db (minus the empty one) 
    # Empty ones seems to not be caught by size check above because it just contains garbage bytes
    ##################################
    for fail in failed.keys():
        print(fail, failed[fail])
        try:
            curr_csv = pd.read_csv(fail,quotechar='"',sep=",", engine='python', encoding='Latin-1',skipinitialspace=True)
            curr_csv.to_sql(file[:-4], con=conn, if_exists='replace', index = False)
            print("\tSuccessfully inserted after trying again!")
        except Exception as e:
            print("\tTried to parse again")
        try:
            curr_csv = pd.read_csv(fail,quotechar='"',sep=",", engine='python', encoding='Latin-1',skipinitialspace=True, skiprows=5)
            curr_csv.to_sql(file[:-4], con=conn, if_exists='replace', index = False)
            print("\tSuccessfully inserted after trying again!")
        except Exception as e:
            print("\tTried to parse again")
        try:
            curr_csv = pd.read_csv(fail,quotechar='"',sep=",", engine='python', encoding='Latin-1',skipinitialspace=True, skiprows=10)
            curr_csv.to_sql(file[:-4], con=conn, if_exists='replace', index = False)
            print("\tSuccessfully inserted after trying again!")
        except Exception as e:
            print("\tTried to parse again")
    close_connection(conn)

with DAG("data_load_dag",
schedule_interval=None,
start_date=days_ago(1),
catchup=False) as dag:
    
    load_csvs = PythonOperator(
        task_id = "load_csvs",
        python_callable=_load_csvs
    )
    
    clean_convert_insert_dataframes = PythonOperator(
        task_id = "clean_convert_insert_dataframes",
        python_callable=_clean_convert_insert_dataframes
    )
    
                

load_csvs >> clean_convert_insert_dataframes