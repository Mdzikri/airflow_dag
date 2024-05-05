from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator

@dag()
def hive_insert():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    insert = HiveOperator(
        task_id          = "hive_insert",
        hive_cli_conn_id = "hive_cli_default",
        hql              = """
            INSERT INTO dibimbing.karyawan (id, nama) VALUES
            (3, 'lies'),
            (4, 'surya'),
            (5, 'priska')
        """,
    )

    start_task >> insert >> end_task
