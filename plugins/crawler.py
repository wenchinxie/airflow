import sys

sys.extend(["/root/crawler"])

from crawler import base_crawler, scrapy_crawler


class AirflowRouterPlugin(AirflowPlugin):
    name = "crawler"
    macros = [base_crawler, scrapy_crawler]
