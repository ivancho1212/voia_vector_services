# voia_vector_services/main.py
from voia_vector_services.search_vectors import search_vectors
from voia_vector_services.process_documents import process_pending_documents
from voia_vector_services.process_urls import process_pending_urls
from voia_vector_services.process_custom_texts import process_pending_custom_texts

from fastapi import FastAPI, Query   # 👈 Aquí agregamos Query
# Instancia FastAPI
app = FastAPI()

# Endpoints existentes
@app.get("/process_all")
def process_all():
    print("🚀 Procesando PDFs...")
    process_pending_documents()
    print("🌐 Procesando URLs...")
    process_pending_urls()
    print("📝 Procesando textos planos...")
    process_pending_custom_texts()
    return {"status": "success", "message": "All processing tasks have been initiated."}

@app.get("/process_documents")
def process_documents_endpoint():
    print("🚀 Procesando PDFs...")
    process_pending_documents()
    return {"status": "success", "message": "Document processing initiated."}

@app.get("/process_urls")
def process_urls_endpoint():
    print("🌐 Procesando URLs...")
    process_pending_urls()
    return {"status": "success", "message": "URL processing initiated."}

@app.get("/process_texts")
def process_texts_endpoint():
    print("📝 Procesando textos planos...")
    process_pending_custom_texts()
    return {"status": "success", "message": "Custom text processing initiated."}

# 🔹 Nuevo endpoint para búsqueda de vectores
@app.get("/search_vectors")
def search_vectors_endpoint(
    bot_id: int = Query(..., description="ID del bot"),
    query: str = Query("", description="Texto de búsqueda opcional"),
    limit: int = Query(5, description="Cantidad máxima de resultados")
):
    """
    Busca vectores en Qdrant asociados a un bot dado y un query opcional.
    """
    results = search_vectors(bot_id=bot_id, query=query, limit=limit)
    return results
