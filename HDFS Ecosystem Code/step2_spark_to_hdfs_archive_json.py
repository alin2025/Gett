from airflow import DAG
import os
import time
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

with DAG(dag_id='dags_2_hdfs_archive_dag', schedule_interval='@once', start_date=datetime(2021, 12, 17), catchup=False) as dag:

    # Task 1
    dummy_task = DummyOperator(task_id='dags_2_hdfs_archive_dag')

    # Task 2
    bash_task1 = BashOperator(task_id='dags_2_hdfs_archive_dag_start',
	bash_command='python /tmp/pycharm_project_764/venv/step2_spark_to_hdfs_archive_json.py')

    dummy_task >> bash_task1


