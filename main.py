import os, json, pinecone, random
import src


BATCH_SIZE = 1000
SHUFFLE_ALPHA = 0.3


def display_progress(batch_ix: int, n: int, n_success: int, bq_success: bool):
    print(
        f"Batch: {batch_ix}: "
        f"Processed: {n} | "
        f"Upserted: {n_success} | "
        f"BigQuery Upload: {bq_success}"
    )


def main():
    secrets = json.loads(os.getenv("SECRETS_JSON"))

    global bq_client, pc_index_items, pc_index_vinted

    bq_client = src.bigquery.init_client(secrets["GCP_CREDENTIALS"])
    
    pc_client = pinecone.Pinecone(api_key=secrets.get("PINECONE_API_KEY"))
    pc_index_items = pc_client.Index(src.enums.PINECONE_INDEX_ITEMS)
    pc_index_vinted = pc_client.Index(src.enums.PINECONE_INDEX_VINTED)

    shuffle = random.random() < SHUFFLE_ALPHA
    query = src.bigquery.query_remaining_points(shuffle=shuffle)
    loader = src.bigquery.load_rows(bq_client, query)

    if loader.total_rows == 0:
        return

    batch_ix, n, n_success = 0, 0, 0
    current_batch = []

    for row in loader:
        batch_ix += 1
        current_batch.append(row)
        
        if len(current_batch) == BATCH_SIZE or batch_ix == loader.total_rows:
            mapping, point_ids = src.processing.create_category_type_mapping(current_batch)
            points = src.pinecone.fetch_points(pc_index_items, point_ids)
            points, rows = src.processing.prepare_points(points, mapping)
            n += len(rows)

            for namespace, namespace_points in points.items():
                n_upserted = src.pinecone.upload(
                    index=pc_index_vinted, points=namespace_points, namespace=namespace
                )
                n_success += n_upserted

            bq_success = src.bigquery.upload_rows(
                client=bq_client,
                dataset_id=src.enums.GC_DATASET_ID_PINECONE,
                table_id=src.enums.GCP_TABLE_ID_PROCESSED,
                rows=rows,
            )

            display_progress(batch_ix, n, n_success, bq_success)
            current_batch = []


if __name__ == "__main__":
    main()
