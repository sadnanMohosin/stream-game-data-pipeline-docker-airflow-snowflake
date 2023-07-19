from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from snowflake.connector import connect
from snowflake_connection import snowflake_conn




def create_structured_table():
    conn = connect(**snowflake_conn)
    cursor = conn.cursor()
    
    # Drop the max_ratings table if it already exists
    cursor.execute("DROP TABLE IF EXISTS structured_data")
    
    # Create the max_ratings table
    cursor.execute("""
        CREATE TABLE structured_data (
            id INT,
            name STRING,              
            year STRING,
            metacritic_rating INT,
            reviewer_rating INT,
            positivity_ratio FLOAT,
            to_beat_main FLOAT,
            to_beat_extra FLOAT,
            to_beat_completionist FLOAT,
            extra_content_length FLOAT,
            tags STRING
        )
    """)
    # conn.commit
    cursor.close()
    conn.close()