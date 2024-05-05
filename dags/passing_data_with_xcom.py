from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context

@dag()
def passing_data_with_xcom():
    start_task  = EmptyOperator(task_id="start_task")
    bridge_task = EmptyOperator(task_id="bridge_task")
    end_task    = EmptyOperator(task_id="end_task")

    @task
    def sender():
        context = get_current_context()
        ti      = context["ti"]
        ti.xcom_push(
            key   = "data",
            value = {
                "nama"  : "dibimbing",
                "divisi": "DE",
            }    
        )

    @task
    def receiver():
        context = get_current_context()
        ti      = context["ti"]
        data    = ti.xcom_pull(
            task_ids = "sender",
            key      = "data"
        )

        print("DATA DARI SENDER:", data)

    start_task >> sender() >> bridge_task >> receiver() >> end_task

passing_data_with_xcom()
