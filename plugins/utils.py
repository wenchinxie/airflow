import datetime
import os
import sys

import chardet
import httpx
from bs4 import BeautifulSoup
from faker import Faker

from airflow.plugins_manager import AirflowPlugin
from airflow.providers.postgres.hooks.postgres import PostgresHook


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


def add_relative_path_to_sys(relative_path: str):
    # Define your relative path

    # Convert it to an absolute path
    absolute_path = os.path.abspath(relative_path)

    # Add the absolute path to sys.path
    sys.path.append(absolute_path)


def autodetect(content):
    return chardet.detect(content).get("encoding")


def is_market_open():
    client = httpx.Client(default_encoding=autodetect)
    header = Faker().user_agent()

    response = client.get(
        "https://tw.stock.yahoo.com/quote/2330.TW", headers={"User-Agent": header}
    )
    if response.status_code != 200:
        return False

    soup = BeautifulSoup(response.text, "html.parser")
    market_day = soup.find("span", {"class": "C(#6e7780) Fz(12px) Fw(b)"}).text
    market_day = market_day.split(" ")[2]

    if market_day == "-" or market_day == datetime.datetime.today().strftime(
        "%Y-%m-%d"
    ):
        return True
    return False


class AirflowRouterPlugin(AirflowPlugin):
    name = "utils"
    macros = [add_relative_path_to_sys, is_market_open]
