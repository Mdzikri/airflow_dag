from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

@dag()
def mysql_insert():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    insert = MySqlOperator(
        task_id       = "mysql_query",
        mysql_conn_id = "mysql_default",
        sql           = """
            INSERT INTO dibimbing.karyawan (nama) VALUES
            ('lies'),
            ('surya'),
            ('priska')
        """,
    )

    start_task >> insert >> end_task

mysql_insert()
