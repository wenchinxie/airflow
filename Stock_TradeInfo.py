
from datetime import datetime, timedelta
from textwrap import dedent
import pendulum


# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.decorators import task
# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import ExternalPythonOperator

AT_WEB_PATH='/home/wenchin/AT_WEB/bin/python3'

with DAG(
    
    "TaiwanStockPrice",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization

    description="Today's info",
    #schedule="@daily",
    schedule_interval="0 21 * * *",
    start_date=pendulum.datetime(2023, 1, 12,9,0, tz="Asia/Taipei"),
    catchup=False,
    tags=['Stock-Daily'],

) as dag:
    def call_crawler(crawler_type='',crawler_name=''):

        from Financial_data_crawler import scheduler
        scheduler.main(crawler_type,crawler_name)

    listed_trade_info= ExternalPythonOperator(
        task_id='Fetch_Listed_Trade_Info',
        python=AT_WEB_PATH,
        python_callable=call_crawler,
        op_kwargs={'crawler_type':'TWSE','crawler_name':'Listed_Day_Transaction_Info'}
    )

    otc_trade_info= ExternalPythonOperator(
        task_id='Fetch_OTC_Trade_Info',
        python=AT_WEB_PATH,
        python_callable=call_crawler,
        op_kwargs={'crawler_type':'TWSE','crawler_name':'OTC_Day_Transaction_Info'}
    )

    day_trade= ExternalPythonOperator(
        task_id='Fetch_Day_Trade',
        python=AT_WEB_PATH,
        python_callable=call_crawler,
        op_kwargs={'crawler_type':'TWSE','crawler_name':'All_Day_Trade'}
    )

    sel_broker = BashOperator(
        task_id = "sel_broker",
        bash_command="source /home/wenchin/AT_WEB/bin/activate;"\
        "cd /mnt/c/Users/s3309/AT/Financial_data_crawler/Scrapy/sel_broker/sel_broker;"\
        "scrapy crawl sel_broker"
    )

    [listed_trade_info, otc_trade_info, day_trade]


with DAG(
    
    "Parse_past_sel_broker",

    description="Today's info",
    #schedule="@daily",
    schedule_interval="0 12,4 * * *",
    start_date=pendulum.datetime(2023, 1, 12,9,0, tz="Asia/Taipei"),
    catchup=False,
    tags=['Sel Broker'],

) as dag:

    sel_broker = BashOperator(
        task_id = "sel_broker",
        bash_command="source /home/wenchin/AT_WEB/bin/activate;"\
        "cd /mnt/c/Users/s3309/AT/Financial_data_crawler/Scrapy/sel_broker/sel_broker;"\
        "scrapy crawl sel_broker"
    )
