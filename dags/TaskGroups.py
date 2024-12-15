from airflow import DAG
from airflow.operators.bash import BashOperator
from groups.group_downloads import download_tasks
from datetime import datetime

with DAG('group_dag', start_date=datetime(2022, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:

    #args = {'start_date': dag.start_date, 'schedule_interval': dag.schedule_interval, 'catchup': dag.catchup}

    # `download_tasks` fonksiyonunu çağırıyoruz
    downloads = download_tasks()

    # `check_files` görevi
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    # Görevlerin sıralaması: downloads -> check_files
    downloads >> check_files