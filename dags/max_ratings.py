from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from snowflake.connector import connect
from snowflake_connection import snowflake_conn



def insert_max_ratings():
    conn = connect(**snowflake_conn)
    cursor = conn.cursor()

    # Insert max ratings per year from structured_data1 into max_ratings table
    cursor.execute("""
        INSERT INTO max_ratings (year, max_rating)
        SELECT year, MAX(metacritic_rating)
        FROM structured_data
        GROUP BY year
        ORDER BY year DESC
    """)

    cursor.close()
    conn.close()