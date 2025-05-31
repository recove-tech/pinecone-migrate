from typing import Literal


GCP_PROJECT_ID = "recove-450509"

GC_DATASET_ID_VINTED = "vinted"
GC_DATASET_ID_PINECONE = "pinecone"

GCP_TABLE_ID_ITEM_ACTIVE = "item_active"
GCP_TABLE_ID_VECTOR = "pinecone"
GCP_TABLE_ID_PROCESSED = "processed"

PINECONE_INDEX_ITEMS = "items"
PINECONE_INDEX_VINTED = "vinted"


CategoryType = Literal[
    "top", "accessories", "bottom", "outerwear", "footwear", "dress", "suit"
]
