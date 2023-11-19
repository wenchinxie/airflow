import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator


@dag(
    "after_trading__instituional_investors",
    schedule="0 21 * * *",
    start_date=pendulum.datetime(2023, 1, 12, 9, 0, tz="Asia/Taipei"),
    tags=["after_trading"],
)
def parse_instituional_investors_after_trading():
    @task(id="ingest_institutional_investors_data")
    def save_institutional_investor():
        import pandas as pd
        from .processor.instituional_investor import (
            get_concat_listed_ii_df,
            get_otc_ii_df,
        )
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow.configuration import conf

        download_folder = conf.get("core", "download_folder")
        data_lake = conf.get("at_web", "data_lake")

        listed_ii_df = get_concat_listed_ii_df(download_folder, data_lake)
        otc_ii_df = get_otc_ii_df(conf.get("data_api", "otc_institutional_investor"))

        upload_df = pd.concat([listed_ii_df, otc_ii_df])

        hook = PostgresHook(postgres_conn_id="postgres_investment")
        conflict_cols = ["date", "stock_code"]

        hook.insert_rows(
            "institutional_trading",
            upload_df.itertuples(),
            upload_df.columns,
            commit_every=1000,
            replace=True,
            replace_index=conflict_cols,
        )

    save_institutional_investor()


parse_ii_data = parse_instituional_investors_after_trading()
