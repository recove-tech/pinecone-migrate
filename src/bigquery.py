from typing import Dict, Iterable

from google.cloud import bigquery
from google.oauth2 import service_account

from .enums import *


def init_client(credentials_dict: Dict) -> bigquery.Client:
    credentials_dict["private_key"] = credentials_dict["private_key"].replace(
        "\\n", "\n"
    )

    credentials = service_account.Credentials.from_service_account_info(
        credentials_dict
    )

    return bigquery.Client(
        credentials=credentials, project=credentials_dict["project_id"]
    )


def load_rows(client: bigquery.Client, query: str) -> Iterable:
    query_job = client.query(query)

    return query_job.result()


def upload_rows(
    client: bigquery.Client, dataset_id: str, table_id: str, rows: Dict
) -> bool:
    try:
        errors = client.insert_rows_json(
            table=f"{dataset_id}.{table_id}", json_rows=rows
        )

        return len(errors) == 0

    except:
        return False


def query_remaining_points(n: int = -1, shuffle: bool = False) -> str:
    order_by = "RAND()" if shuffle else "created_at DESC"

    query = f"""
    WITH RankedRows AS (
        SELECT 
        p.point_id,
        i.*,
        ROW_NUMBER() OVER (PARTITION BY i.vinted_id ORDER BY vinted_id) as rn
        FROM `{GCP_PROJECT_ID}.{GC_DATASET_ID_VINTED}.{GCP_TABLE_ID_VECTOR}` AS p
        INNER JOIN `{GCP_PROJECT_ID}.{GC_DATASET_ID_VINTED}.{GCP_TABLE_ID_ITEM_ACTIVE}` AS i ON p.item_id = i.id
        LEFT JOIN `{GCP_PROJECT_ID}.{GC_DATASET_ID_PINECONE}.{GCP_TABLE_ID_PROCESSED}` AS pr ON p.point_id = pr.point_id
        WHERE pr.point_id IS NULL AND category_type != ''
    )
    SELECT * EXCEPT(rn)
    FROM RankedRows
    WHERE rn = 1
    ORDER BY {order_by}
    """

    if n > 0:
        query += f"LIMIT {n}"

    return query
