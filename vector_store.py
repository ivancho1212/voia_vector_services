from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct, PointIdsList


COLLECTION_NAME = "voia_vectors"

client = QdrantClient(host="localhost", port=6333)


def get_or_create_vector_store():
    if not client.collection_exists(COLLECTION_NAME):
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=384, distance=Distance.COSINE)
        )
        print(f"🆕 Colección '{COLLECTION_NAME}' creada.")
    else:
        print(f"✅ Colección '{COLLECTION_NAME}' ya existe.")

    return client


def is_in_qdrant(qdrant_id: str) -> bool:
    try:
        points = client.retrieve(
            collection_name=COLLECTION_NAME,
            ids=[qdrant_id]
        )
        return len(points) > 0
    except Exception:
        return False


def add_point_to_qdrant(qdrant_id: str, vector: list, payload: dict = {}):
    client.upsert(
        collection_name=COLLECTION_NAME,
        points=[
            PointStruct(
                id=qdrant_id,
                vector=vector,
                payload=payload
            )
        ]
    )
    print(f"✅ Punto {qdrant_id} insertado en Qdrant.")


def delete_point_from_qdrant(qdrant_id: str):
    try:
        client.delete(
            collection_name=COLLECTION_NAME,
            points_selector=PointIdsList([qdrant_id])
        )
        print(f"🗑️ Punto {qdrant_id} eliminado de Qdrant.")
    except Exception as e:
        print(f"❌ Error al eliminar punto {qdrant_id}: {e}")


# ✅ Nueva función: Listar puntos en Qdrant
def list_all_points(limit=10):
    points, next_page = client.scroll(
        collection_name=COLLECTION_NAME,
        limit=limit
    )
    return points
