from voia_vector_services.vector_store import client
from voia_vector_services.embedder import get_embedding

def search_vectors(bot_id: int, query: str = "", limit: int = 5):
    """
    Busca los vectores más relevantes para un bot dado y un query opcional.
    Si query está vacío, trae los top documentos del bot.
    """
    vector = get_embedding(query) if query else None

    search_params = {
        "collection_name": "voia_vectors",
        "limit": limit,
        "filter": {"must": [{"key": "bot_id", "match": {"value": bot_id}}]}
    }

    if vector:
        search_params["query_vector"] = vector
    else:
        # Qdrant requiere vector si se usa search; si quieres traer top docs sin query
        # puedes usar scroll y luego filtrar por bot_id
        from voia_vector_services.vector_store import client
        points, _ = client.scroll(collection_name="voia_vectors", limit=limit)
        return [p.payload for p in points if p.payload.get("bot_id") == bot_id]

    results = client.search(**search_params)
    return [r.payload for r in results]
