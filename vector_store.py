# vector_store.py
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance

COLLECTION_NAME = "voia_vectors"

def get_or_create_vector_store():
    client = QdrantClient(host="localhost", port=6333)

    if not client.collection_exists(COLLECTION_NAME):
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=384, distance=Distance.COSINE)
        )
        print(f"🆕 Colección '{COLLECTION_NAME}' creada.")
    else:
        print(f"✅ Colección '{COLLECTION_NAME}' ya existe.")

    return client
