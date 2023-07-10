import pendulum

from airflow import DAG


with DAG(
    "Update_inds_comps",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    description="",
    schedule_interval="@once",
    start_date=pendulum.datetime(2023, 1, 12, 9, 0, tz="Asia/Taipei"),
    catchup=False,
    tags=["Industries", "Companies"],
) as dag:
    from airflow.operators.python import ExternalPythonOperator
    from airflow.configuration import conf
    from airflow.plugins import router

    def call_inds_scraper():
        from Financial_data_crawler.DataReader import HTTPClient

        inds_df = HTTPClient.IndScraper().scrape_data()

        mysql_conn = router.get_mysql_conn()
        inds_df.to_sql("Inds", mysql_conn, if_exists="replace")

    save_inds_comps_to_db = ExternalPythonOperator(
        task_id="",
        python=conf.get("core", "virtualenv"),
        python_callable=call_inds_scraper,
    )
