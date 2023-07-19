from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from snowflake.connector import connect
from snowflake_connection import snowflake_conn




def create_max_ratings_table():
    conn = connect(**snowflake_conn)
    cursor = conn.cursor()
    
    # Drop the max_ratings table if it already exists
    cursor.execute("DROP TABLE IF EXISTS max_ratings")
    
    # Create the max_ratings table
    cursor.execute("""
        CREATE TABLE max_ratings (
            year INT,
            max_rating FLOAT
        )
    """)
    # conn.commit
    cursor.close()
    conn.close()