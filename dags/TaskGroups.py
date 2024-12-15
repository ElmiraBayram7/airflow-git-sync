from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from group.group_downloads import download_tasks

start_date = datetime(2024, 10, 11)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('my_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    downloads=download_tasks

    check_files=BashOperator(task_id='check_file_exists', bash_command='sha256sum /tmp/dirty_store_transactions.csv /tmp/Churn_Modelling.csv',
                      retries=2, retry_delay=timedelta(seconds=15))

    
    
    downloads>>check_files