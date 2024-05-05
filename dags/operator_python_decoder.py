from airflow.decorators import dag, task

@dag()
def operator_python_decorator():
    @task
    def python():
        print("ini adalah operator python dengan decorator")
   
    python()

operator_python_decorator()
