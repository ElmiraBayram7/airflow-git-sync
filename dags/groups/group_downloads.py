from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup  # Doğru import

def download_tasks():
    with TaskGroup("downloads", tooltip="Download tasks") as group:
        download_a = BashOperator(
            task_id='download_data_a',
            bash_command='wget -O /tmp/dirty_store_transactions.csv https://github.com/erkansirin78/datasets/raw/master/dirty_store_transactions.csv',
            retries=2,
            retry_delay=timedelta(seconds=15)
        )

        download_b = BashOperator(
            task_id='download_data_b',
            bash_command='wget -O /tmp/Churn_Modelling.csv https://github.com/erkansirin78/datasets/raw/master/Churn_Modelling.csv',
            retries=2,
            retry_delay=timedelta(seconds=15)
        )
    
    return group  # TaskGroup nesnesi döndürülüyor