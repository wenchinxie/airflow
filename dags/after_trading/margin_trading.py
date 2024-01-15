import pendulum

from airflow import DAG
from airflow.decorators import dag, task


@dag(
    "after_trading__margin_tradings",
    schedule="30 22 * * 1,2,3,4,5",
    start_date=pendulum.datetime(2024, 1, 15, tz="Asia/Taipei"),
    tags=["after_trading"],
)
def parse_margin_trading_after_trading():
    @task.external_python(
        task_id="ingest_margin_trading_data",
        python="/root/miniconda3/envs/crawler/bin/python3.12",
    )
    def save_margin_trading():
        from crawler.base_crawler.ingestor import upsert_data

        upsert_data("margin_trading", "otc")
        upsert_data("margin_trading", "listed")

    save_margin_trading()


margin_trading_to_parse = parse_margin_trading_after_trading()
