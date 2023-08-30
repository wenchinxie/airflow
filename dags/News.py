import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

AT_WEB_PATH = "/home/wenchin/AT_WEB/bin/python3"

with DAG(
    "News_Feed",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    description="Only crawl the data on cnyes so far",
    # schedule="@daily",
    schedule_interval="0 21 * * *",
    start_date=pendulum.datetime(2023, 1, 12, 9, 0, tz="Asia/Taipei"),
    catchup=False,
    tags=["Scarpy", "News"],
) as dag:
    cynes_news = BashOperator(
        task_id="Scrapy_Cynes_News",
        bash_command="source /home/wenchin/AT_WEB/bin/activate;"
        "cd /mnt/c/Users/s3309/AT/Financial_data_crawler/Scrapy/news/news;"
        "scrapy crawl News",
    )
    cynes_news
