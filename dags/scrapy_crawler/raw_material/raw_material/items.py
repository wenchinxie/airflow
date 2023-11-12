from dataclasses import dataclass
from datetime import datetime


@dataclass
class RawMaterialItem:
    date: datetime.date
    material_name: str
    price: float
