import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator


@dag(
    "after_trading__instituional_investors",
    schedule="0 21 * * *",
    start_date=pendulum.datetime(2023, 11, 19, 9, 0, tz="Asia/Taipei"),
    tags=["after_trading"],
)
def parse_instituional_investors_after_trading():
    @task(task_id="ingest_institutional_investors_data")
    def save_institutional_investor():
        import pandas as pd
        from after_trading.processor.instituional_investor import (
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
        upload_df["date"] = pd.to_datetime(upload_df["date"], utc=True)
        upload_df["stock_name"] = upload_df["stock_name"].str.strip()
        hook = PostgresHook(postgres_conn_id="postgres_investment")
        conflict_cols = ["date", "stock_code"]

        tuples = list(upload_df.itertuples(index=False, name=None))
        cols = [f'"{col}"' for col in upload_df.columns.to_list()]
        hook.insert_rows(
            "institutional_investors",
            tuples,
            cols,
            commit_every=1000,
            replace=True,
            replace_index=conflict_cols,
        )

    save_institutional_investor()


parse_ii_data = parse_instituional_investors_after_trading()
