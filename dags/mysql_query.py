from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

@dag()
def mysql_query():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    query = MySqlOperator(
        task_id       = "mysql_query",
        mysql_conn_id = "mysql_default",
        sql           = "SELECT * FROM dibimbing.karyawan"
    )

    start_task >> query >> end_task

mysql_query()
