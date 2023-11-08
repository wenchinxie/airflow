from mongoengine import DateTimeField, StringField, ListField, Document
from itemadapter import ItemAdapter
from airflow.providers.mongo.hooks.mongo import MongoHook


class Cynes_News(Document):
    Date = DateTimeField()
    Headline = StringField()
    Tags = StringField()
    Content = StringField()


class RawMaterialPipeline:
    def process_item(self, item, spider):
        item_dict = ItemAdapter(item).asdict()
        raw_material = RawMaterial(**item_dict)
        raw_material.save()
        return item


class MongoPipeline:
    doc_name = "raw_material"

    def __init__(self, mongo_host, mongo_username, mongo_password):
        self.conn = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def open_spider(self, spider):
        self.client = MongoHook(db=self.doc_name).get_conn()

    def close_spider(self, spider):
        self.conn.close()

    def process_item(self, item, spider):
        item_dict = ItemAdapter(item).asdict()
        raw_material = RawMaterial(**item_dict)
        raw_material.save()
        return item
