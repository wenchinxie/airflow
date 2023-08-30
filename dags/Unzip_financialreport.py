import pendulum

from airflow import DAG
from airflow.operators.python import ExternalPythonOperator
from airflow.configuration import conf

with DAG(
    "Quarterly_Financial_Report_Saver",
    schedule_interval="@once",
    start_date=pendulum.datetime(2023, 1, 12, 9, 0, tz="Asia/Taipei"),
    catchup=False,
    tags=["Quarterly Financial Report", "Save to db"],
) as dag:

    def call_crawler(crawler_type="", crawler_name=""):
        from Financial_data_crawler import scheduler

        scheduler.main(crawler_type, crawler_name)

    listed_trade_info = ExternalPythonOperator(
        task_id="Unzip_and_save_to_db",
        python=conf.get("core", "virtualenv"),
        python_callable=call_crawler,
        op_kwargs={"crawler_type": "Local", "crawler_name": "TW_FinancialReport"},
    )
