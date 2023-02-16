
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
    "Raw_Material",
    description="Only crawl the data on cnyes so far",
    #schedule="@daily",
    schedule_interval="0 21 5 * *",
    start_date=pendulum.datetime(2023, 1, 12,9,0, tz="Asia/Taipei"),
    catchup=False,
    tags=["Scarpy",'Raw Material'],

) as dag:

    
    rm=BashOperator(
        task_id='Scrapy_SCI',
        bash_command="source /home/wenchin/AT_WEB/bin/activate;"\
        "cd /mnt/c/Users/s3309/AT/Financial_data_crawler/Scrapy/raw_material/raw_material;"\
        "scrapy crawl sci"
    )

    rm
