from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from datetime import datetime

@dag(
    dag_id            = "dag_schedule",
    schedule_interval = "30 9 * * *",
    start_date        = datetime(2024, 2, 1),
    catchup           = True,
    tags              = ["exercise"],
)
def main():
    task_1 = EmptyOperator(
        task_id = "task_ke_1"
    )

    task_1

main()
