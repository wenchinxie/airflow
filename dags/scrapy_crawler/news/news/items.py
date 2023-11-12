from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class CynewsItem:
    date: datetime
    headline: str
    content: str
    tags: list[str] = field(default_factory=list)
