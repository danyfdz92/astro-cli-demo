"""
1. Create a DAG with at least 4 tasks that are serially dependent.

This DAG contains four tasks:
- step_one
- step_two
- step_three
- step_four

Schedule - @daily
  - Time: 00:00 (Midnight)
  - Frequency: Once every 24 hours.
  - TimeZone: UTC

Start_date - December 1, 2021

Catchup - False
  - The scheduler will only run the most recent interval

"""
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="serial_pipeline",
    schedule="@daily",
    start_date=datetime(2021, 12, 1),
    catchup=False,
    tags=["serial", "scheduled"],
)
def serial_pipeline():
    @task
    def step_one():
        return "data_from_step_one"

    @task
    def step_two(prev: str) -> str:
        return f"{prev} -> processed_by_step_two"

    @task
    def step_three(prev: str) -> str:
        return f"{prev} -> processed_by_step_three"

    @task
    def step_four(prev: str) -> str:
        print(f"Final result: {prev}")
        return prev

    step_four(step_three(step_two(step_one())))


serial_pipeline()
