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
    tags=["industries", "companies"],
) as dag:
    from airflow.operators.python import ExternalPythonOperator
    from airflow.configuration import conf
    from router import get_db_conn

    def call_inds_scraper(get_db_conn_fnuc):
        from Financial_data_crawler.DataReader import HTTPClient

        inds_df = HTTPClient.IndScraper().scrape_data()

        mysql_conn = get_db_conn_fnuc("mysql_address")
        inds_df.to_sql("Inds", mysql_conn, if_exists="replace")

    save_inds_comps_to_db = ExternalPythonOperator(
        task_id="save_industries_info",
        python=conf.get("core", "virtualenv"),
        python_callable=call_inds_scraper,
        op_args=[get_db_conn],
    )

    save_inds_comps_to_db
