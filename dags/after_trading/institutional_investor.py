import pendulum

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator


@dag(
    "after_trading__instituional_investors",
    schedule="0 22 * * 1,2,3,4,5",
    start_date=pendulum.datetime(2023, 11, 19, 9, 0, tz="Asia/Taipei"),
    tags=["after_trading"],
)
def parse_instituional_investors_after_trading():
    @task(task_id="ingest_institutional_investors_data")
    def save_institutional_investor():
        from crawler.insitutional_investors import upsert_data

        upsert_data("otc")
        upsert_data("listed")

    save_institutional_investor()


parse_instituional_investors_after_trading()
