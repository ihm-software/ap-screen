import pandas as pd
from airflow import DAG
import glob
import csv, ast, psycopg2
import logging

from airflow.utils.log.logging_mixin import LoggingMixin




conn = psycopg2.connect(
    host="localhost",
    database="imported_data",
    user="dev",
    password="dev")

conn.initialize(logger)

def gather_csv():
    csv_list = []
    path = "path/to/dir/*.csv"
    for fname in glob.glob(path):
        csv_list.append(fname)

    return csv_list

def create_tables(csv_list):
    for i in csv_list:

        f = open(i, 'r')
        reader = csv.reader(f)

        longest, headers, type_list = [], [], []

        for row in reader:
            if len(headers) == 0:
                headers = row
                for col in row:
                    longest.append(0)
                    type_list.append('')
            else:
                for i in range(len(row)):
                # NA is the csv null value
                    if type_list[i] == 'varchar' or row[i] == 'NA':
                        pass
                    else:
                        var_type = dataType(row[i], type_list[i])
                        type_list[i] = var_type
                if len(row[i]) > longest[i]:
                    longest[i] = len(row[i])
        f.close()

        dir_split = i.split('/')
        table_name = dir_split[-1]

        statement = f'create table {table_name} ('

        for i in range(len(headers)):
            if type_list[i] == 'varchar':
                statement = (statement + '\n{} varchar({}),').format(headers[i].lower(), str(longest[i]))
            else:
                statement = (statement + '\n' + '{} {}' + ',').format(headers[i].lower(), type_list[i])

        statement = statement[:-1] + ');'



        cur = conn.cursor()

        cur.execute(statement)
        conn.commit()

        sql = f"""
        copy {table_name} from '{i}'
        """
        cur.execute(sql)
        conn.commit()

def results():
    sql = """
    select 1 as question,
    round ((count(*)::decimal
    / count(distinct t.table_schema || '.' || t.table_name)), 2)
    as answer
    from information_schema.tables t
    left join information_schema.columns c on c.table_schema = t.table_schema
    and c.table_name = t.table_name
    where t.table_schema not in ('information_schema', 'pg_catalog')
    and table_type = 'BASE TABLE'; 
    """

    cur = conn.cursor()

    cur.execute(sql)

    sql = """
    SELECT schemaname,relname,n_live_tup 
    FROM pg_stat_user_tables 
    ORDER BY n_live_tup DESC;   
    """

    cur.execute(sql)


def dataType(val, current_type):
    try:
        # Evaluates numbers to an appropriate type, and strings an error
        t = ast.literal_eval(val)
    except ValueError:
        return 'varchar'
    except SyntaxError:
        return 'varchar'
    if type(t) in [int, float]:
       if (type(t) in [int]) and current_type not in ['float', 'varchar']:
           # Use smallest possible int type
            if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:
               return 'smallint'
            elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:
               return 'int'
            else:
               return 'bigint'
       if type(t) is float and current_type not in ['varchar']:
           return 'decimal'
    else:
        return 'varchar'

def main():
    csv_list = gather_csv()
    create_tables(csv_list)



if __name__ == '__main__':
    main()