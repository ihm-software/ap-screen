#!/bin/sh
sleep 10 && airflow dags trigger field_avg_dag && airflow dags trigger row_count_dag && airflow dags trigger value_count_dag