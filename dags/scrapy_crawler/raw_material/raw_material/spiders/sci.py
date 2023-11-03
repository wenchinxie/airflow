from typing import Union
import re
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from raw_material.items import RawMaterialItem


class SciSpider(CrawlSpider):
    name = "sci.com"
    allowed_domains = ["www.sci99.com"]

    start_urls = ["https://www.sci99.com/monitor-94911214-0.html"]
    rules = Rule(
        LinkExtractor(allow=(r"monitor-\d{1,}.*\.html")), callback="parse_item"
    )

    def clean(self, text, convert_to_float: bool = False) -> Union[str, float]:
        """remove all whitespace characters

        Args:
            text (_type_): text from web
            convert_to_float (bool, optional): convert the price into float type. Defaults to False.

        Returns:
            Union[str,float]: float price or str
        """
        cleaned_str = re.sub(r"\s+", "", text)
        striped_str = cleaned_str.strip()

        if convert_to_float:
            return float(striped_str)
        return striped_str

    def parse_item(self, response):
        material_mame = self.clean(
            response.xpath('//div[@class="detect_title"]/h2/text()').get()
        )

        table_rows = response.xpath(
            '//div[@id="Panel1"]/div[@class="div_content"]/div/table/tr'
        )

        dates = [self.clean(row.xpath("td/text()").get()) for row in table_rows]
        prices = [
            self.clean(row.xpath("td/a/text()").get(), convert_to_float=True)
            for row in table_rows
        ]
        for date, price in zip(dates, prices):
            if re.match(r"\d{4}-\d{1,2}-\d{1,2}", date):
                yield RawMaterialItem(date, material_mame, price)
