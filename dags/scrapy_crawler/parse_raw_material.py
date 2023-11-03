import pendulum
from airflow import DAG


with DAG(
    "scrapy__parse_raw_material_from_sci",
    schedule="0 21 */5 * *",
    start_date=pendulum.datetime(2023, 1, 12, 9, 0, tz="Asia/Taipei"),
    catchup=False,
    tags=["Scarpy", "Raw Material"],
) as dag:

    def parse_sci():
        from crawler.raw_material.raw_material.spiders.sci import SciSpider
        from scrapy.crawler import CrawlerProcess
        from scrapy.utils.project import get_project_settings

        settings = get_project_settings()
        process = CrawlerProcess(settings)
        process.crawl(SciSpider)
        process.start()
