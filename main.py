# voia_vector_services/main.py
from fastapi import FastAPI, Query, HTTPException  # 👈 Agregar HTTPException
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()
from voia_vector_services.process_documents import process_pending_documents # noqa
from voia_vector_services.process_urls import process_pending_urls # noqa
from voia_vector_services.process_custom_texts import process_pending_custom_texts # noqa
from voia_vector_services.search_vectors import search_vectors # noqa

from fastapi import FastAPI, Query
from pydantic import BaseModel

class SearchRequest(BaseModel):
    query: str
    bot_id: int
    limit: int = 3

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
def process_documents_endpoint(bot_id: int = Query(..., description="ID del bot para procesar documentos")):
    try:
        print(f"🚀 Procesando PDFs para el bot_id: {bot_id}...")
        process_pending_documents(bot_id)
        return {"status": "success", "message": f"Document processing initiated for bot {bot_id}."}
    except Exception as e:
        print(f"❌ Error en process_documents_endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/process_urls")
def process_urls_endpoint(bot_id: int = Query(..., description="ID del bot para procesar URLs")):
    try:
        print(f"🌐 Procesando URLs para el bot_id: {bot_id}...")
        process_pending_urls(bot_id)
        return {"status": "success", "message": f"URL processing initiated for bot {bot_id}."}
    except Exception as e:
        print(f"❌ Error en process_urls_endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/process_texts")
def process_texts_endpoint(bot_id: int = Query(..., description="ID del bot para procesar textos")):
    try:
        print(f"📝 Procesando textos para el bot_id: {bot_id}...")
        process_pending_custom_texts(bot_id)
        return {"status": "success", "message": f"Custom text processing initiated for bot {bot_id}."}
    except Exception as e:
        print(f"❌ Error en process_texts_endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 🔹 Endpoint para búsqueda de vectores (NUEVO, para el chat dinámico)
@app.post("/search")
def search_vectors_endpoint(request: SearchRequest):
    """
    Busca vectores en Qdrant asociados a un bot dado y un query opcional.
    """
    try:
        results = search_vectors(bot_id=request.bot_id, query=request.query, limit=request.limit)
        return {"results": results}
    except Exception as e:
        print(f"❌ Error en el endpoint /search: {e}")
        # Esto devolverá un error 500 al cliente C# con un mensaje específico
        raise HTTPException(status_code=500, detail=f"Error interno en el servicio de búsqueda de Python: {str(e)}")

# 🔹 Endpoint de búsqueda de vectores (ANTIGUO, restaurado para compatibilidad)
@app.get("/search_vectors")
def search_vectors_get_endpoint(
    bot_id: int = Query(..., description="ID del bot"),
    query: str = Query("", description="Texto de búsqueda opcional"),
    limit: int = Query(5, description="Cantidad máxima de resultados")
):
    """
    Busca vectores en Qdrant. Mantenido por compatibilidad con flujos existentes.
    """
    try:
        results = search_vectors(bot_id=bot_id, query=query, limit=limit)
        return results
    except Exception as e:
        print(f"❌ Error en el endpoint /search_vectors: {e}")
        raise HTTPException(status_code=500, detail=f"Error interno en el servicio de búsqueda de Python: {str(e)}")
