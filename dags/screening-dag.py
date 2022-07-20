import os
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python_operator import PythonOperator
import pandas as pd

# Required default arguments for Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': "2022-07-20",
    'retries': 1
}


def move_data_to_database():
    # Use MySqlHook operator provided by Airflow to create database connection
    mysql = MySqlHook(mysql_conn_id="mysql_conn_id")
    conn = mysql.get_conn()

    # Store path to data directory 
    data_path = r'/opt/airflow/data'

    # Initialize list for holding paths to individual csv files
    list_paths = []
    # Find paths to individual csv files
    p = Path(list_paths).glob('**/*.csv') 
    
    # Save path to individual csv files in a list
    for csv_paths in p:
        list_paths.append(csv_paths)
 
    # Read data in files and convert to dictionaries
    list_data =  [i for j in list_paths for i in csv.DictReader(open(j,'r'))]

    # Push data to MySQL database as a dataframe for pandas use (uses mysql connection created earlier)
    # Iterate through list to create new table for each csv file
    for index,table in enumerate(list_data):
        table_name = "table" + str(index)
        pd.DataFrame(table).to_sql(table_name, conn,
                        schema=None, if_exists='replace', index=True,
                        index_label=None, chunksize=None, dtype=None)


def columnAverage():
    # Use MySqlHook operator provided by Airflow to create database connection
    mysql = MySqlHook(mysql_conn_id="mysql_conn_id")
    conn = mysql.get_conn()
    cursor = conn.cursor()

    # Use information schema to count the number of columns in each table
    # Should produce a table with columns table name and count
    sql = """
    SELECT TABLE_NAME, COUNT(*) AS COUNT 
    FROM TEST_DB.INFORMATION_SCHEMA.COLUMNS
    GROUP BY TABLE_NAME
    """
    cursor.execute(sql)

    result = cursor.fetchall()

    # Iterates across query results to sum all column counts for each table
    sum = 0
    for r in result:
        sum = sum + int(r[1])
    
    # Gets the number of tables by counting rows in query result
    count = len(result)

    # Calculates average using sum and total table count
    average = sum / count

    # Print result in expected format
    answer = """
    |Question|Answer|
    |--------|------|
    |   1    |  {}  |
    """
    print(answer.format(average))


def valueCount():
    # Use MySqlHook operator provided by Airflow to create database connection
    mysql = MySqlHook(mysql_conn_id="mysql_conn_id")
    conn = mysql.get_conn()
    cursor = conn.cursor()
    
    # Lists to store table names and the values in each table
    table_names = []
    items_in_tables = []

    # Query should return a list of all tables in the database
    first_sql = """
    SELECT TABLE_NAME AS TABLE_NAME 
    FROM TEST_DB.INFORMATION_SCHEMA.TABLES
    """
    cursor.execute(first_sql)

    first_result = cursor.fetchall()

    # Iterate across query results to add table names to a list
    for t in first_result:
        table_names.append(t[0])

    # Query each table within the list to determine all values within the table
    for i in table_names:
        second_sql = """
        SELECT * FROM TEST_DB.{}
        """.format(i)
        cursor.execute(second_sql)

        # Takes all items within the table and adds to a list
        # Must iterate across all columns and all rows to get every value (hence nested loop)
        second_result = cursor.fetchall()
        for j in second_result:
            for k in j:
                items_in_tables.append(k)
    
    # Use pandas to gets counts for all unique value across the tables
    df = pd.value_counts(np.array(items_in_tables))

    # Print expected format
    print_value = """
    |   value  |   count  |
    |----------|----------|
    """
    # Iterates across pandas result to pull value and value count for each item
    # Enumerate allows for accessing both the value and the index
    for index,item in enumerate(df.index):
        print_value = print_value + """\n
        |    {}    |    {}    |
        """.format(item, df.values[index])
    
    print(print_value)


def rowCount():
    # Use MySqlHook operator provided by Airflow to create database connection
    mysql = MySqlHook(mysql_conn_id="mysql_conn_id")
    conn = mysql.get_conn()
    cursor = conn.cursor()

    # List to hold table names and variable to hold count
    table_names = []
    count = 0

    # Query should return a list of all table names
    first_sql = """
    SELECT TABLE_NAME AS TABLE_NAME 
    FROM TEST_DB.INFORMATION_SCHEMA.TABLES
    """
    cursor.execute(first_sql)

    first_result = cursor.fetchall()

    # Add all table names to a list
    for t in first_result:
        table_names.append(t[0])

    # Count rows in each table based on table names in list
    for i in table_names:
        second_sql = """
        SELECT COUNT(*) FROM TEST_DB.{}
        """.format(i)

        cursor.execute(second_sql)

        # Add the count result of the above query to the total count of rows
        second_result = cursor.fetchall()
        for j in second_result:
            count = count + j[0]

    # Prints the answer in expected format
    answer = """
    | Question | Answer |
    |----------|--------|
    |    3     |   {}   |
    """.format(count)


# Builds the DAG by giving it an id, description, and schedule
dag = DAG(
    'Screening-DAG',
    default_args=default_args,
    description='Analyzing Data Content',
    schedule_interval='@once',   
)

# Tasks below and used to build the DAG based on above functions
t1 = PythonOperator(
    task_id='directory_to_mysql',
    provide_context=True,
    python_callable=move_data_to_database,
    dag=dag,
)

t2 = PythonOperator(
    task_id='print_count',
    provide_context=True,
    python_callable=valueCount,
    dag=dag,
)

t3 = PythonOperator(
    task_id='print_average',
    provide_context=True,
    python_callable=columnAverage,
    dag=dag,
)

t4 = PythonOperator(
    task_id='print_rows',
    provide_context=True,
    python_callable=rowCount,
    dag=dag,
)

# Builds the graph -- t2, t3, and t4 rely on completion of t1
t1 >> [t2, t3, t4]