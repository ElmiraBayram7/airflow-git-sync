from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
from airflow.utils.task_group import TaskGroup

# PostgreSQL bağlantısı
engine = create_engine('postgresql://train:Ankara06@postgres:5432/traindb')

# CSV'yi PostgreSQL'e yükleyen fonksiyon
def from_github_to_postgresql_staging(**kwargs):
    base_url = kwargs['base_url']
    schema = kwargs['schema']
    table_name = kwargs['table_name']
    engine = kwargs['engine']

    #do this
    df = pd.read_csv(base_url)
    # Veriyi PostgreSQL'e yazma
    df.to_sql(name=table_name, con=engine, schema=schema, if_exists='replace', index=False)



# İndirme görevlerini içeren TaskGroup fonksiyonu
def download_tasks():
    with TaskGroup("downloads", tooltip="Download tasks") as group:
        t1 = PythonOperator(
            task_id='from_github_to_postgresql_staging_orders', 
            python_callable=from_github_to_postgresql_staging,
            op_kwargs={'base_url': "https://raw.githubusercontent.com/erkansirin78/datasets/master/retail_db/orders.csv",
                       'table_name': 'orders', 'schema': 'staging', 'engine': engine}
        )

        t2 = PythonOperator(
            task_id='from_github_to_postgresql_staging_order_items', 
            python_callable=from_github_to_postgresql_staging,
            op_kwargs={'base_url': "https://raw.githubusercontent.com/erkansirin78/datasets/master/retail_db/order_items.csv",
                       'table_name': 'order_items', 'schema': 'staging', 'engine': engine}
        )

        t3 = PythonOperator(
            task_id='from_github_to_postgresql_staging_products', 
            python_callable=from_github_to_postgresql_staging,
            op_kwargs={'base_url': "https://raw.githubusercontent.com/erkansirin78/datasets/master/retail_db/products.csv",
                       'table_name': 'products', 'schema': 'staging', 'engine': engine}
        )

        return group