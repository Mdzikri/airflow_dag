
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta, datetime

@dag()
def sensor_dag():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    wait_dag = ExternalTaskSensor(
        task_id           = "wait_dag",
        external_dag_id   = "sensor_sleep",
        external_task_id  = "end_task",
        execution_date_fn = lambda dt: dt.replace(second=0, microsecond=0),
        poke_interval     = 5,
    )
   
    start_task >> wait_dag >> end_task

sensor_dag()




