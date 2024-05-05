from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import requests
import logging


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract():
    """Function to extract data from JSONPlaceholder API"""
    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url)
    data = response.json()
    if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
        raise ValueError("Extracted data must be a list of dictionaries.")
    return data



def transform(data):
    """Function to transform data. Ensures data is correctly formatted."""
    if not isinstance(data, list) or not all(isinstance(item, dict) and 'id' in item and 'title' in item and 'body' in item for item in data):
        raise ValueError("Data must be a list of dictionaries with keys 'id', 'title', and 'body'.")
    transformed_data = []
    for record in data:
        transformed_data.append({
            'id': record['id'],
            'title': record['title'].capitalize(),  # Capitalize the title
            'body': record['body']
        })
    return transformed_data


def load(transformed_data):
    """Function to load data into a PostgreSQL database"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    insert_query = "INSERT INTO posts (id, title, body) VALUES (%s, %s, %s)"
    for record in transformed_data:
        cursor.execute(insert_query, (record['id'], record['title'], record['body']))
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    'jsonplaceholder_etl',
    default_args=default_args,
    description='A DAG for extracting, transforming, and loading data from JSONPlaceholder',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    t2 = PythonOperator(
    task_id='transform',
    python_callable=transform,
    op_kwargs={'data': '{{ ti.xcom_pull(task_ids="extract") }}'}
    )


    t3 = PythonOperator(
        task_id='load',
        python_callable=load,
        op_kwargs={'data': '{{ ti.xcom_pull(task_ids="transform") }}'}
    )

    t1 >> t2 >> t3
