from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from load_dataset import load_dataset_to_snowflake
from create_structured_table import create_structured_table
from load_data import load_data_from_single_column
from create_max_table import create_max_ratings_table
from max_ratings import insert_max_ratings



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 15),
    'retries': 4,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('steam_games_dataset_pipeline', default_args=default_args, schedule_interval=None)

load_dataset_task = PythonOperator(
    task_id='load_dataset',
    python_callable=load_dataset_to_snowflake,
    dag=dag
)

create_structured_table_task = PythonOperator(
    task_id='create_structured_table',
    python_callable= create_structured_table,
    dag = dag
)

load_data_task = PythonOperator(
    task_id = 'load_data',
    python_callable = load_data_from_single_column,
    dag=dag
)

create_max_table_task = PythonOperator(
    task_id='create_max_ratings_table',
    python_callable= create_max_ratings_table,
    dag = dag
)

insert_max_data_task = PythonOperator(
    task_id='insert_max_ratings',
    python_callable=insert_max_ratings,
    dag = dag
)

load_dataset_task >> create_structured_table_task >> load_data_task >> create_max_table_task >> insert_max_data_task