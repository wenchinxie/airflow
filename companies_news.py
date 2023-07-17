import pendulum

from airflow import DAG


with DAG(
    "update_companies_news",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023, 1, 12, 9, 0, tz="Asia/Taipei"),
    catchup=False,
    tags=["News", "Companies"],
) as dag:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import ExternalPythonOperator
    from airflow.configuration import conf

    AT_WEB_PATH = conf.get("core", "virtualenv")

    def call_crawler(crawler_type="", crawler_name=""):
        from Financial_data_crawler import scheduler

        scheduler.main(crawler_type, crawler_name)

    listed_companies_news = ExternalPythonOperator(
        task_id="listed_companiews_news",
        python=AT_WEB_PATH,
        python_callable=call_crawler,
        op_kwargs={
            "crawler_type": "TWSE",
            "crawler_name": "Listed_CompaniesNews",
        },
    )

    otc_companies_news = ExternalPythonOperator(
        task_id="otc_companies_news",
        python=AT_WEB_PATH,
        python_callable=call_crawler,
        op_kwargs={"crawler_type": "TWSE", "crawler_name": "OTC_CompaniesNews"},
    )

    [listed_companies_news, otc_companies_news]
