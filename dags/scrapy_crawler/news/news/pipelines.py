from mongoengine import DateTimeField, StringField, ListField, Document
from itemadapter import ItemAdapter
from mongoengine import connect, disconnect
from dataclasses import asdict


class CynesNews(Document):
    date = DateTimeField()
    headline = StringField()
    tags = ListField()
    content = StringField()

    meta = {"shard_key": ("date", "headline"), "indexes": [("date", "headline")]}


class MongoPipeline:
    _doc_name = "News"

    def open_spider(self, spider):
        username = spider.settings.get("MONGO_USERNAME")
        password = spider.settings.get("MONGO_PASSWORD")
        connect(
            db=self._doc_name,
            username=username,
            password=password,
            host="localhost",
        )

    def close_spider(self, spider):
        disconnect()

    def process_item(self, item, spider):
        item_dict = ItemAdapter(item).asdict()
        news = CynesNews(**item_dict)
        news.save()
        return item
