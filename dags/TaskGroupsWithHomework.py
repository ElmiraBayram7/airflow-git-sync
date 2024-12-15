import sys
sys.path.append('/opt/airflow/code_base/airflow-git-sync/dags/groups')

from group_downloads_homework import download_tasks
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
import logging

# Başlangıç tarihi ve veritabanı bağlantısı
start_date = datetime(2024, 1, 1)
engine = create_engine('postgresql://train:Ankara06@postgres:5432/traindb')

# DAG için default_args parametreleri
default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

# DAG'ın tanımlanması
with DAG(dag_id='GroupTaskWithHomework', default_args=default_args, schedule_interval='@hourly', catchup=False) as dag:

    # Schema oluşturma görevleri
    t1 = PostgresOperator(
        task_id='create_staging_schema', 
        postgres_conn_id='postgresql_conn',
        sql="CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION train;"
    )

    # Verileri indiren görevleri çağıran task group
    downloads = download_tasks()

    # Yeni schema oluşturma görevleri
    t5 = PostgresOperator(
        task_id='create_serving_schema', 
        postgres_conn_id='postgresql_conn',
        sql="CREATE SCHEMA IF NOT EXISTS serving AUTHORIZATION train;"
    )

    # `v_product_status_track` view'inin oluşturulması
    t6 = PostgresOperator(
        task_id='create_v_product_status_track', 
        postgres_conn_id='postgresql_conn',
        sql="""
        CREATE OR REPLACE VIEW serving.v_product_status_track AS
        SELECT * 
        FROM staging.order_items oi
        JOIN staging.orders o ON oi."orderItemOrderId" = o."orderId"
        JOIN staging.products p ON oi."orderItemProductId" = p."productId"
        """
    )

    # Görevlerin sıralanması: önce schema, sonra veri indirme ve sonrasında view oluşturma
    t1 >> downloads >> t5 >> t6
