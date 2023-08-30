import pendulum


from airflow import DAG
from airflow.operators.python import ExternalPythonOperator

AT_WEB_PATH = "/home/wenchin/AT_WEB/bin/python3"

with DAG(
    "TaiwanStockPrice",
    description="Today's info",
    schedule_interval="0 21 * * 1,2,3,4,5",
    start_date=pendulum.datetime(2023, 1, 12, 9, 0, tz="Asia/Taipei"),
    catchup=False,
    tags=["Stock-Daily"],
) as dag:

    def call_crawler(crawler_type="", crawler_name=""):
        from Financial_data_crawler import scheduler

        scheduler.main(crawler_type, crawler_name)

    listed_trade_info = ExternalPythonOperator(
        task_id="Fetch_Listed_Trade_Info",
        python=AT_WEB_PATH,
        python_callable=call_crawler,
        op_kwargs={
            "crawler_type": "TWSE",
            "crawler_name": "Listed_Day_Transaction_Info",
        },
    )

    otc_trade_info = ExternalPythonOperator(
        task_id="Fetch_OTC_Trade_Info",
        python=AT_WEB_PATH,
        python_callable=call_crawler,
        op_kwargs={"crawler_type": "TWSE", "crawler_name": "OTC_Day_Transaction_Info"},
    )

    day_trade = ExternalPythonOperator(
        task_id="Fetch_Day_Trade",
        python=AT_WEB_PATH,
        python_callable=call_crawler,
        op_kwargs={"crawler_type": "TWSE", "crawler_name": "All_Day_Trade"},
    )

    [listed_trade_info, otc_trade_info, day_trade]

with DAG(
    "Parse_sel_broker",
    description="Today's info",
    schedule_interval="0 18 * * 1,2,3,4,5",
    start_date=pendulum.datetime(2023, 1, 12, 9, 0, tz="Asia/Taipei"),
    catchup=False,
    tags=["Sel Broker", "Today"],
) as dag:

    def sel_broker_trans():
        from Financial_data_crawler.DataReader.Selenium import SelBroker

        crawler = SelBroker()
        crawler.parse()

    def save_sel_broker_trans():
        from Financial_data_crawler.DataReader.Selenium import SelBrokerDataCrawler
        import asyncio

        asyncio.run(SelBrokerDataCrawler.run())

    sel_broker = ExternalPythonOperator(
        task_id="Today_Sel_Broker",
        python=AT_WEB_PATH,
        python_callable=sel_broker_trans,
    )

    save_data = ExternalPythonOperator(
        task_id="Save_Sel_Broker",
        python=AT_WEB_PATH,
        python_callable=save_sel_broker_trans,
    )

    sel_broker >> save_data

with DAG(
    "Parse_past_sel_broker",
    description="Today's info",
    schedule_interval="@once",
    start_date=pendulum.datetime(2023, 1, 12, 9, 0, tz="Asia/Taipei"),
    catchup=False,
    tags=["Sel Broker"],
) as dag:

    def past_sel_broker_trans():
        from Financial_data_crawler.DataReader.Selenium import SelBroker

        crawler = SelBroker(auto_date=True)
        print(crawler.urls)
        crawler.parse()

    sel_broker = ExternalPythonOperator(
        task_id="Past_Sel_Broker",
        python=AT_WEB_PATH,
        python_callable=past_sel_broker_trans,
    )

    save_data = ExternalPythonOperator(
        task_id="Save_Sel_Broker",
        python=AT_WEB_PATH,
        python_callable=save_sel_broker_trans,
    )

    sel_broker >> save_data

with DAG(
    "SpreadShareholdings",
    description="Collect Spread Shareholdings",
    schedule_interval="0 21 * * 6",
    start_date=pendulum.datetime(2023, 2, 25, 21, 0, tz="Asia/Taipei"),
    catchup=False,
    tags=["Stock-Weekly"],
) as dag:

    def call_crawler(crawler_type="", crawler_name=""):
        from Financial_data_crawler import scheduler

        scheduler.main(crawler_type, crawler_name)

    day_trade = ExternalPythonOperator(
        task_id="Spread_Shareholdings",
        python=AT_WEB_PATH,
        python_callable=call_crawler,
        op_kwargs={
            "crawler_type": "TWSE",
            "crawler_name": "Listed_Spread_Shareholdings",
        },
    )


with DAG(
    "Save_sel_broker_data",
    schedule_interval="0 0,6 * * *",
    start_date=pendulum.datetime(2023, 1, 12, 9, 0, tz="Asia/Taipei"),
    catchup=False,
    tags=["Sel Broker"],
) as dag:
    save_data = ExternalPythonOperator(
        task_id="Save_Sel_Broker",
        python=AT_WEB_PATH,
        python_callable=save_sel_broker_trans,
    )
    save_data
