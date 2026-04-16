from datetime import datetime
from airflow.decorators import dag, task

@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "taskflow"]
)
def demo_taskflow():

    @task
    def extract():
        return ["apple", "banana", "cherry"]

    @task
    def transform(data):
        return [item.upper() for item in data]

    @task
    def load(data):
        for item in data:
            print(f"Loaded: {item}")

    load(transform(extract()))


dag = demo_taskflow()