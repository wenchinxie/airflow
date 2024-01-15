import pendulum

from airflow.decorators import dag, task

# Define DAG configurations
tasks = [
    "day_transaction",
    "institutional_investors",
    "margin_trading",
    "day_trade",
]

# Create DAGs using a loop
for job in tasks:

    @dag(
        dag_id=f"after_trading__{job}",
        schedule="0 22 * * 1,2,3,4,5",
        start_date=pendulum.datetime(2024, 1, 15, tz="Asia/Taipei"),
        tags=["after_trading"],
    )
    def generic_dag():
        @task.external_python(
            task_id=f"ingest__{job}",
            python="/root/miniconda3/envs/crawler/bin/python3.12",
        )
        def save_data(job):
            from crawler.base_crawler.ingestor import upsert_data

            upsert_data(job, "otc")
            upsert_data(job, "listed")

        save_data(job)

    generic_dag()
