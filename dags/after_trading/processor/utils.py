from airflow.providers.postgres.hooks.postgres import PostgresHook


def turn_year_to_ROC_year(date_str):
    year, remains = date_str[:4], date_str[4:]
    roc_year = str(int(year) - 1911)
    return roc_year + remains


def import_data_to_postgres(table, df, replace_index=["stock_code", "occur_date"]):
    hook = PostgresHook(postgres_conn_id="postgres_investment")
    values = list(df.itertuples(index=False, name=None))
    cols = [f'"{col}"' for col in df.columns.to_list()]
    hook.insert_rows(
        table,
        values,
        cols,
        commit_every=1000,
        replace=True,
        replace_index=replace_index,
    )
