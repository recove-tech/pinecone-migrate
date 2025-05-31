from typing import List, Dict
import numpy as np

from dataclasses import dataclass
from datetime import datetime

from .enums import CategoryType
from .utils import normalize_vector


@dataclass
class Item:
    point_id: str
    id: str
    vinted_id: str
    catalog_id: int
    title: str
    url: str
    image_location: str
    price: float
    currency: str
    brand: str
    size: str
    condition: str
    category_type: CategoryType
    women: bool = False
    num_likes: int = 0
    material_id: int = -1
    pattern_id: int = -1
    color_id: int = -1
    created_at: datetime = None
    updated_at: datetime = None
    unix_created_at: int = 0

    def __post_init__(self):
        now = datetime.now()

        if self.created_at is None:
            self.created_at = now

        if self.updated_at is None:
            self.updated_at = self.created_at

        if self.unix_created_at == 0:
            self.unix_created_at = int(now.timestamp())


@dataclass
class Point:
    id: str
    values: List[float]
    metadata: Item

    def __post_init__(self):
        self._namespace = self.metadata.category_type
        self.values = normalize_vector(self.values)

    @property
    def namespace(self) -> str:
        return self._namespace

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "values": self.values,
            "metadata": self.metadata.__dict__,
        }

    def to_row(self) -> Dict:
        return {
            "point_id": self.id,
            "created_at": datetime.now().isoformat(),
        }
