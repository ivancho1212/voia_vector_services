# voia_vector_services/search_vectors.py
from .embedder import get_embedding
from .vector_store import get_or_create_vector_store

def search_vectors(bot_id: int, query: str = "", limit: int = 5):
    """
    Busca los vectores más relevantes para un bot dado y un query opcional.
    Si query está vacío, trae los top documentos del bot.
    """
    client = get_or_create_vector_store()
    vector = get_embedding(query) if query else None

    if vector:
        results = client.search(
            collection_name="voia_vectors",
            query_vector=vector,
            limit=limit,
            query_filter={
                "must": [{"key": "bot_id", "match": {"value": bot_id}}]
            }
        )
        return [r.payload for r in results]
    else:
        points, _ = client.scroll(collection_name="voia_vectors", limit=limit)
        return [p.payload for p in points if p.payload.get("bot_id") == bot_id]
