from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from pipeline.extract import extract_police_api
from pipeline.storeRaw import store_raw_data
from pipeline.transformData import creating_silver_table, filling_silver_layer, silver_to_cloud
from pipeline.goldLayer import creating_gold_table, loading_gold_table, gold_to_s3

default_args = {
    'owner': 'amarachukwu',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='police_etl_dag',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_police_api
    )

    store_raw_task = PythonOperator(
        task_id='store_raw_data',
        python_callable=store_raw_data
    )

    silver_table_task = PythonOperator(
        task_id='creating_silver_table',
        python_callable= creating_silver_table
    )
    fill_silver_table_task = PythonOperator(
        task_id='fill_silver_table',
        python_callable= filling_silver_layer
    )

    silver_cloud_task = PythonOperator(
        task_id='silver_to_cloud',
        python_callable= silver_to_cloud
    )
    gold_table_task = PythonOperator(
        task_id='creating_gold_table',
        python_callable= creating_gold_table
    )

    gold_filling_task = PythonOperator(
        task_id='loading_gold_table',
        python_callable= loading_gold_table
    )

    gold_cloud_task = PythonOperator(
        task_id='gold_to_s3',
        python_callable= gold_to_s3
    )

    extract_task >> store_raw_task >> silver_table_task >> fill_silver_table_task >> silver_cloud_task >> gold_table_task >> gold_filling_task >> gold_cloud_task
