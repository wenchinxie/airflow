from typing import List, Any, Tuple
import numpy as np
from sqlalchemy import create_engine

from airflow.plugins_manager import AirflowPlugin
from airflow.configuration import conf


def get_db_conn(conn_name):
    engine = create_engine(conf.get("db_conn", conn_name))
    return engine.connect()


def handle_nan_value_for_insertion(row: List[Any]) -> Tuple[Any]:
    """Transform the data to None value instead of np.nan"""
    return [
        None if isinstance(value, float) and np.isnan(value) else value for value in row
    ]


def get_upsert_query(df, table_name, conflict_str):
    df_cols = list(df.columns)
    insert_cols = ",".join(df_cols)
    # updated_cols = ",".join([f"{col}=excluded.{col}" for col in df_cols])
    values = ",".join(["%s" for _ in range(len(df_cols))])

    query = f"""
    REPLACE INTO {table_name}(
    {insert_cols}
    ) VALUES ({values})
    """
    return query


def upsert_to_db(conn, df, query):
    for row in df.values.tolist():
        insert_row = handle_nan_value_for_insertion(row)
        conn.execute(query, insert_row)

    conn.close()


class AirflowRouterPlugin(AirflowPlugin):
    name = "router"
    macros = [get_db_conn, upsert_to_db]
