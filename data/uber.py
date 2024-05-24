import airflow
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime
from spark import extract_data
from spark import transform_data
from spark import load_data

# Define DAG parameters
dag = DAG(
    dag_id='uber_etl',
    schedule_interval='@daily',  # Schedule the pipeline to run daily
    start_date=datetime(2024, 5, 23),
    catchup = False,
)

    
extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        dag=dag,
        )
    
transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        dag=dag,
        )
    
load_task = PythonOperator(
        task_id='load_data_to_bigquery',
        python_callable=load_data,
        dag=dag,
        )

# Define task dependencies
extract_task >> transform_task >> load_task
