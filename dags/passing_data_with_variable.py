from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
from datetime import datetime

@dag()
def passing_data_with_variable():
    start_task    = EmptyOperator(task_id="start_task")
    bridge_task_1 = EmptyOperator(task_id="bridge_task_1")
    bridge_task_2 = EmptyOperator(task_id="bridge_task_2")
    end_task      = EmptyOperator(task_id="end_task")

    @task
    def prev_variable():
        var = Variable.get("data_variable", deserialize_json=True)
        print("DATA dari variable sebelumnya adalah:", var)

    @task
    def set_variable():
        Variable.set(
            key   = "data_variable",
            value = {
                "nama"    : "dibimbing",
                "divisi"  : "DE",
                "datetime": str(datetime.now()),
            },
            serialize_json = True
        )

    @task
    def get_variable():
        var = Variable.get("data_variable", deserialize_json=True)
        print("DATA dari variable yang baru adalah:", var)

    start_task >> prev_variable() >> bridge_task_1 >> set_variable() >> bridge_task_2 >> get_variable() >> end_task

passing_data_with_variable()
