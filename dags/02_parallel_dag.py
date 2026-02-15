"""
2. Create a parallel DAG with at least 4 tasks where each task is run independently.


"""
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="parallel_independent_tasks",
    schedule=None,
    start_date=datetime(2021, 12, 1),
    catchup=False,
    tags=["parallel"],
)
def parallel_independent_tasks():
    @task
    def task_a():
        print("Task A executed independently")

    @task
    def task_b():
        print("Task B executed independently")

    @task
    def task_c():
        print("Task C executed independently")

    @task
    def task_d():
        print("Task D executed independently")

    # Call all four; no args between them → no dependencies → all run in parallel
    task_a()
    task_b()
    task_c()
    task_d()


parallel_independent_tasks()
