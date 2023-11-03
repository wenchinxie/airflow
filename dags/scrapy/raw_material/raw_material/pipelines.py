from mongoengine import DateTimeField, StringField, FloatField, Document
from itemadapter import ItemAdapter


class RawMaterial(Document):
    date = DateTimeField()
    material_Name = StringField()
    price = FloatField()

    meta = {
        "shard_key": ("date", "material_name"),
        "indexes": ("date", "material_name"),
    }


class RawMaterialPipeline:
    def process_item(self, item, spider):
        item_dict = ItemAdapter(item).asdict()
        raw_material = RawMaterial(**item_dict)
        raw_material.save()
        return item
