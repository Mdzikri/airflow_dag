from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import datetime, timedelta

@dag(schedule_interval="* * * * *", start_date=datetime(204, 2, 20), catchup=False)
def sensor_sleep():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    wait_time = TimeDeltaSensor(
        task_id       = "wait_time",
        delta         = timedelta(seconds=30),
        poke_interval = 5,
    )
   
    start_task >> wait_time >> end_task

sensor_sleep()
