import pendulum

from airflow import DAG
from airflow.decorators import dag, task


@dag(
    "after_trading__margin_tradings",
    schedule="30 22 * * 1,2,3,4,5",
    start_date=pendulum.datetime(2023, 11, 19, 9, 0, tz="Asia/Taipei"),
    tags=["after_trading"],
)
def parse_margin_trading_after_trading():
    table_name = "margin_trading"

    @task(task_id="create_table_if_not_exists")
    def create_table():
        from database import utils

        utils.create_table_if_not_exists(table_name)

    @task(task_id="save_margin_trading")
    def save_margin_trading():
        from after_trading.processor import marging_trading
        from after_trading.processor.utils import import_data_to_postgres

        for listed in [True, False]:
            df = marging_trading.get_listed_margin_trading_data(listed=listed)
            if not df.empty:
                import_data_to_postgres(table_name, df)

    create_table() >> save_margin_trading()


margin_trading_to_parse = parse_margin_trading_after_trading()
