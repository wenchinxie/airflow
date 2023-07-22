from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "News_Fetching",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": True,
    },
    description="Only crawl the data on cnyes so far",
    schedule="30 22 * * *",
    start_date=pendulum.datetime(2023, 1, 9, 23, 31, tz="Asia/Taipei"),
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
