import pendulum

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator


@dag(
    "after_trading__instituional_investors",
    schedule="0 22 * * 1,2,3,4,5",
    start_date=pendulum.datetime(2024, 1, 15, tz="Asia/Taipei"),
    tags=["after_trading"],
)
def parse_instituional_investors_after_trading():
    @task.external_python(
        task_id="ingest_institutional_investors_data",
        python="/root/miniconda3/envs/crawler/bin/python3.12",
    )
    def save_institutional_investor():
        from crawler.base_crawler.ingestor import upsert_data

        upsert_data("institutional_investors", "otc")
        upsert_data("institutional_investors", "listed")

    save_institutional_investor()


instituional_investors = parse_instituional_investors_after_trading()
