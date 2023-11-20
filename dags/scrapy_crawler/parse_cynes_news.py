import pendulum
from airflow.operators.bash import BashOperator
from airflow.decorators import dag
from airflow.models.param import Param
from scrapy_crawler.utils import get_scrapy_crawl_command

today = pendulum.today().to_date_string()


@dag(
    "scrapy__parse_cynes_news",
    schedule="0 21 * * *",
    start_date=pendulum.datetime(2023, 1, 12, 9, 0, tz="Asia/Taipei"),
    catchup=False,
    tags=["Scarpy", "Cynews"],
    params={
        "start_date": Param(today, type="string", format="date"),
        "end_date": Param(today, type="string", format="date"),
    },
)
def parse_cynes_news():
    def get_args():
        start_date = "{{params.start_date}}"
        if start_date != today:
            end_date = "{{params.end_date}}"
            return f"-a start_dt_str={start_date} -a end_dt_str={end_date}"
        return ""

    folder_name = "news"
    spider = "cynews"
    args = get_args()

    command = get_scrapy_crawl_command(folder_name, folder_name, spider, args)
    parse_news = BashOperator(task_id="parse_cynes", bash_command=command)
    parse_news


to_parse = parse_cynes_news()
