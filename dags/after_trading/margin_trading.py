import pendulum
from airflow import DAG


import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator


@dag(
    "after_trading__margin_tradings",
    schedule="30 22 * * *",
    start_date=pendulum.datetime(2023, 11, 19, 9, 0, tz="Asia/Taipei"),
    tags=["after_trading"],
)
def parse_margin_trading_after_trading():
    from airflow.operators.python import ExternalPythonOperator
    from airflow.configuration import conf
    from router import get_db_conn, upsert_to_db, get_upsert_query

    def save_margin_trading(get_db_conn, upsert_to_db, get_upsert_query, conf_func):
        import pandas as pd

        from Financial_data_crawler.DataReader.DataFrame import DataFrame
        from Financial_data_crawler.DataCleaner.twse_cleaner import (
            TWListed_opendata_cleaner,
            TWOTC_opendata_cleaner,
        )

        mt_otc = DataFrame.get_raw_data(
            conf_func.get("data_api", "otc_margintrading"), parse_dates=["資料日期"]
        )

        cleaned_otc_df = TWOTC_opendata_cleaner.margin_trading_cleaner(mt_otc)
        mt_listed = DataFrame.get_raw_data(
            conf_func.get("data_api", "listed_margintrading")
        ).fillna(0)
        cleaned_listed_df = TWListed_opendata_cleaner.margin_trading_cleaner(mt_listed)

        upload_df = pd.concat([cleaned_listed_df, cleaned_otc_df])
        upload_df["date"].fillna(method="bfill", inplace=True)

        mysql_conn = get_db_conn("mysql_address")
        conflict_cols = ["date", "stock_id"]
        query = get_upsert_query(
            upload_df, "dashboard_margintrading", ",".join(conflict_cols)
        )
        upsert_to_db(mysql_conn, upload_df, query)

    upsert_margin_trading_to_db = ExternalPythonOperator(
        task_id="save_margin_trading",
        python=conf.get("core", "virtualenv"),
        python_callable=save_margin_trading,
        op_args=[get_db_conn, upsert_to_db, get_upsert_query, conf],
    )

    upsert_margin_trading_to_db
