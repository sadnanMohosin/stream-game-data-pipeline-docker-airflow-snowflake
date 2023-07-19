import json
from datetime import datetime
from airflow import DAG
from snowflake_connection import snowflake_conn  # Import the Snowflake connection function
from airflow.operators.python_operator import PythonOperator
from snowflake.connector import connect


def load_data_from_single_column():
    # Connect to Snowflake
    connection = connect(**snowflake_conn)

    # Extract data from the unstructured_data1 table
    select_query = "SELECT data FROM unstructured_data"
    cursor = connection.cursor()
    cursor.execute(select_query)
    results = cursor.fetchall()

    # Transform and load data into the structured_data1 table
    insert_query = """
        INSERT INTO structured_data (
            id,
            name,              
            year,
            metacritic_rating,
            reviewer_rating,
            positivity_ratio,
            to_beat_main,
            to_beat_extra,
            to_beat_completionist,
            extra_content_length,
            tags
        )
        VALUES (
            
            %(id)s,
            %(name)s,
            %(year)s,
            %(metacritic_rating)s,
            %(reviewer_rating)s,
            %(positivity_ratio)s,
            %(to_beat_main)s,
            %(to_beat_extra)s,
            %(to_beat_completionist)s,
            %(extra_content_length)s,
            %(tags)s
            
            
            
        )
    """

    for row in results:
        data_string = row[0]
        data = json.loads(data_string)  # Parse the data string as JSON

        # Prepare values for the insert query
        values = {
            'extra_content_length': data.get('extra_content_length', None),
            'id': data.get('id', None),
            'metacritic_rating': data.get('metacritic_rating', None),
            'name': data.get('name', None),
            'positivity_ratio': data.get('positivity_ratio', None),
            'reviewer_rating': data.get('reviewer_rating', None),
            'tags': data.get('tags', None),
            'to_beat_completionist': data.get('to_beat_completionist', None),
            'to_beat_extra': data.get('to_beat_extra', None),
            'to_beat_main': data.get('to_beat_main', None),
            'year': data.get('year', None)
        }

        # Handle empty or missing numeric values
        for key in ['extra_content_length', 'id', 'metacritic_rating', 'positivity_ratio', 'reviewer_rating',
                    'to_beat_completionist', 'to_beat_extra', 'to_beat_main', 'year']:
            if values[key] == '':
                values[key] = None

        cursor.execute(insert_query, values)

    # Commit and close the Snowflake connection
    connection.commit()
    cursor.close()
    connection.close()

