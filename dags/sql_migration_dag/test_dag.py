import datetime

from airflow import DAG
from airflow.decorators import task

with DAG(
        dag_id="test_dag",
        start_date=datetime.datetime(2021, 1, 1),
        schedule="@once",
        max_active_runs=1
):
    @task(task_id='test_task')
    def print_number():
        print('------------------------------')
        print(100000)

    print_number()

