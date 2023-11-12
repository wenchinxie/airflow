import pendulum
from airflow.decorators import dag
from scrapy_crawler.utils import get_scrapy_crawl_command
from airflow.operators.bash import BashOperator


@dag(
    "scrapy__parse_raw_material_from_sci",
    schedule="0 21 */5 * *",
    start_date=pendulum.datetime(2023, 1, 12, 9, 0, tz="Asia/Taipei"),
    catchup=False,
    tags=["Scarpy", "Raw Material"],
)
def parse_sci_raw_material():
    folder_name = "raw_material"
    spider = "sci"

    command = get_scrapy_crawl_command(folder_name, folder_name, spider, "")
    parse_sci = BashOperator(task_id="parse_sci_raw_material", bash_command=command)
    parse_sci


to_parse = parse_sci_raw_material()
