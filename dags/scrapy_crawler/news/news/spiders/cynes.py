import scrapy
import json
import requests
import pendulum
from datetime import datetime
from typing import List
import requests
from news.items import CynewsItem
from Financial_data_crawler.db.clients import MongoClient
from Financial_data_crawler.db.NewsModels import Cynes_News
from Financial_data_crawler.DataCleaner import news_cleaner


def get_timestamp(date_str: str) -> str:
    return pendulum.parse(date_str).timestamp()


def get_start_and_end_time(start_dt_str: str, end_dt_str: str):
    if not (start_dt_str and end_dt_str):
        today_timestamp = pendulum.today().timestamp()
        start, end = today_timestamp, today_timestamp - 86400
    else:
        start, end = get_timestamp(start_dt_str), get_timestamp(end_dt_str)
    return int(round(start)), int(round(end))


def get_urls(start: int, end: int) -> List[str]:
    urls = []

    while start >= end:
        base_url = f"https://news.cnyes.com/api/v3/news/category/tw_stock?startAt={start-86400}&endAt={start}&limit=30&page="
        url = base_url + "1"
        content = json.loads(requests.get(base_url, timeout=60).text)
        last_page = content["items"]["last_page"]

        for page in range(1, last_page + 1):
            linkage = base_url + str(page)
            content = json.loads(requests.get(linkage, timeout=60).text)

            for newsid in content["items"]["data"]:
                url = f"https://news.cnyes.com/news/id/{newsid['newsId']}?exp=a"
                if not url in urls:
                    urls.append(url)

        start -= 86400

    return urls


class CynewsSpider(scrapy.Spider):
    name = "cynews"
    allowed_domains = ["https://www.cnyes.com/"]

    def __init__(self, start_dt_str: str = "", end_dt_str: str = ""):
        start_time, end_time = get_start_and_end_time(start_dt_str, end_dt_str)
        self.start_urls = get_urls(start_time, end_time)

    def parse(self, response):
        headline = response.xpath('//h1[@itemprop="headline"]/text()').get()
        tags = response.xpath('//span[@class="_1E-R"]/text()').getall()
        content = "\b".join(response.xpath("//p/text()").getall()[4:])
        date = response.xpath("//time/text()").get()
        yield CynewsItem(date, headline, content, tags)
