from mongoengine import DateTimeField, StringField, FloatField, Document
from itemadapter import ItemAdapter
from mongoengine import connect, disconnect


class RawMaterial(Document):
    date = DateTimeField()
    material_name = StringField()
    price = FloatField()

    meta = {
        "shard_key": ("material_name"),
        "indexes": [("date", "material_name")],
    }


class MongoPipeline:
    _doc_name = "RawMaterial"

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
        RawMaterial.objects(
            date=item_dict["date"], material_name=item_dict["material_name"]
        ).modify(upsert=True, **item_dict)
        return item
