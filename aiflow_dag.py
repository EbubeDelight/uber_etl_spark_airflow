import airflow
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime
# from spark import extract_data
from spark import transform_data
from spark import load_data
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Define DAG parameters
dag = DAG(
    dag_id='uber_etl',
    schedule_interval='@daily',  # Schedule the pipeline to run daily
    start_date=datetime(2024, 5, 23),
    catchup=False,
)
start = PythonOperator(
    task_id='start',
    python_callable= lambda: print("uber_etl_started"),
    dag=dag,
)

spark_job = SparkSubmitOperator(
    task_id="spark_job",
    conn_id="spark-conn",
    application="spark.py",
    dag=dag
)

end = PythonOperator(
    task_id='end',
    python_callable= lambda: print("uber_etl_ended"),
    dag=dag,
)

# Define task dependencies
start >> spark_job >> end
# extract_task = PythonOperator(
# task_id='extract_data',
# python_callable=extract_data,
# dag=dag,
# )

#transform_task = PythonOperator(
 #   task_id='transform_data',
   # python_callable=transform_data,
  #  dag=dag,
#)

#load_task = PythonOperator(
 #   task_id='load_data_to_bigquery',
  #  python_callable=load_data,
   # dag=dag,
#)