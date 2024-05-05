from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator

@dag()
def hive_query():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    query = HiveOperator(
        task_id          = "hive_query",
        hive_cli_conn_id = "hive_cli_default",
        hql              = "SELECT * FROM dibimbing.karyawan",
    )

    start_task >> query >> end_task

hive_query()
