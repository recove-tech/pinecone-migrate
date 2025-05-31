from typing import List, Dict, Iterable, Tuple
from collections import defaultdict

from pinecone.core.openapi.data.model.scored_vector import ScoredVector

from .models import Item, Point


def create_category_type_mapping(rows: Iterable) -> Tuple[Dict[str, str], List[str]]:
    mapping, point_ids = {}, set()

    for row in rows:
        try:
            item = Item(**row)

            if item.point_id not in mapping:
                mapping[item.point_id] = item.category_type
                point_ids.add(item.point_id)

        except:
            continue

    return mapping, list(point_ids)


def prepare_points(
    points: List[ScoredVector], category_type_mapping: Dict[str, str]
) -> Tuple[Dict[str, List[Dict]], List[Dict]]:
    output_points, rows = defaultdict(list), []

    for point in points:
        try:
            point = _prepare_point(point, category_type_mapping)

            output_points[point.namespace].append(point.to_dict())
            rows.append(point.to_row())

        except:
            continue

    return output_points, rows


def _prepare_point(point: ScoredVector, category_type_mapping: Dict[str, str]) -> Point:
    point_id, values, metadata = point.id, point.values, point.metadata
    category_type = category_type_mapping[point_id]

    metadata["category_type"] = category_type
    metadata["point_id"] = point_id
    metadata = Item(**metadata)

    return Point(id=point_id, values=values, metadata=metadata)
