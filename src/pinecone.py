from typing import List, Dict

import pinecone


BATCH_SIZE = 100


def upload(index: pinecone.Index, points: List[Dict], namespace: str) -> int:
    if len(points) == 0:
        return 0

    total_upserted = 0

    try:
        for i in range(0, len(points), BATCH_SIZE):
            batch = points[i:i + BATCH_SIZE]
            response = index.upsert(vectors=batch, namespace=namespace)
            total_upserted += response.get("upserted_count", 0)

        return total_upserted

    except Exception as e:
        print(e)
        return -1


def fetch_points(
    index: pinecone.Index, point_ids: List[str]
) -> List[pinecone.ScoredVector]:
    response = index.fetch(ids=point_ids)

    return response.vectors.values()
