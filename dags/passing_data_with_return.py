from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

@dag()
def passing_data_with_return():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    @task
    def sender():
        return {
            "nama"  : "dibimbing",
            "divisi": "DE",
        }

    @task
    def receiver(data):
        print("DATA DARI SENDER:", data)

    start_task >> receiver(sender()) >> end_task

passing_data_with_return()
