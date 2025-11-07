# voia_vector_services/main.py
from fastapi import FastAPI, Query, HTTPException
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()
from voia_vector_services.process_documents import process_pending_documents # noqa
from voia_vector_services.process_urls import process_pending_urls # noqa
from voia_vector_services.process_custom_texts import process_pending_custom_texts # noqa
from voia_vector_services.search_vectors import search_vectors # noqa
from voia_vector_services.sync_qdrant_mysql import validate_bot_endpoint, sync_bot_endpoint, sync_all_bots_endpoint # noqa

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


# ============================================================
# ENDPOINTS DE SINCRONIZACION QDRANT-MYSQL (SOLUTION #6)
# ============================================================

@app.get("/validate/{bot_id}")
def validate_bot(bot_id: int):
    """
    Valida la integridad de Qdrant y MySQL para un bot.
    
    Detecta:
    - Vectores huérfanos (en Qdrant pero no en MySQL)
    - Documentos perdidos (en MySQL pero no en Qdrant)
    - Inconsistencias de hash
    
    Returns: Reporte detallado de discrepancias
    """
    try:
        return validate_bot_endpoint(bot_id)
    except Exception as e:
        print(f"❌ Error en /validate/{bot_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sync/{bot_id}")
def sync_bot(bot_id: int, dry_run: bool = Query(True, description="Si True, solo análisis sin reparar")):
    """
    Sincroniza Qdrant con MySQL para un bot.
    
    Actions:
    - Elimina vectores huérfanos
    - Remarca documentos sin vector para reindexación
    - Repara inconsistencias
    
    Args:
        bot_id: ID del bot
        dry_run: Si True (default), solo reporta; si False, repara
    
    Returns: Resultado de la sincronización
    """
    try:
        return sync_bot_endpoint(bot_id, dry_run=dry_run)
    except Exception as e:
        print(f"❌ Error en /sync/{bot_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sync-all")
def sync_all(dry_run: bool = Query(True, description="Si True, solo análisis sin reparar")):
    """
    Sincroniza Qdrant con MySQL para TODOS los bots.
    
    Args:
        dry_run: Si True (default), solo reporta; si False, repara
    
    Returns: Resultado de sincronización para todos los bots
    """
    try:
        return sync_all_bots_endpoint(dry_run=dry_run)
    except Exception as e:
        print(f"❌ Error en /sync-all: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# ENDPOINT DE DIAGNOSTICO Y LIMPIEZA
# ============================================================

@app.get("/cleanup-qdrant")
def cleanup_qdrant():
    """
    🧹 Limpia la colección Qdrant eliminando todos los puntos (para resolver corrupción).
    ADVERTENCIA: Esto elimina TODOS los embeddings. Deberás reindexar después.
    
    Use solo si hay errores persistentes de lectura en Qdrant.
    """
    try:
        from qdrant_client import QdrantClient
        
        client = QdrantClient(host="localhost", port=6333)
        collection_name = "voia_vectors"
        
        print("🧹 Limpiando colección Qdrant...")
        
        # Obtener info inicial
        collection_info = client.get_collection(collection_name)
        points_count = collection_info.points_count
        
        if points_count == 0:
            return {
                "status": "ok",
                "message": "Colección ya está vacía",
                "points_before": 0,
                "points_after": 0
            }
        
        # Eliminar todos los puntos en lotes
        deleted_total = 0
        offset = 0
        batch_size = 100
        
        while deleted_total < points_count:
            try:
                points, next_offset = client.scroll(
                    collection_name=collection_name,
                    limit=batch_size,
                    offset=0  # Siempre desde el principio después de cada delete
                )
                
                if not points:
                    break
                
                point_ids = [p.id for p in points]
                client.delete(
                    collection_name=collection_name,
                    points_selector={"ids": point_ids}
                )
                
                deleted_total += len(point_ids)
                print(f"   Eliminados {deleted_total}/{points_count} puntos...")
                
                if len(points) < batch_size:
                    break
                    
            except Exception as batch_error:
                print(f"   ⚠️ Error en lote: {str(batch_error)[:80]}")
                break
        
        # Verificar resultado
        collection_info = client.get_collection(collection_name)
        final_count = collection_info.points_count
        
        return {
            "status": "ok" if final_count == 0 else "partial",
            "message": "Colección limpiada exitosamente" if final_count == 0 else f"Limpieza parcial: {final_count} puntos aún quedan",
            "points_before": points_count,
            "points_after": final_count,
            "points_deleted": deleted_total,
            "next_step": "Procesa documentos nuevamente para reindexar"
        }
        
    except Exception as e:
        print(f"❌ Error en /cleanup-qdrant: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/diagnose-qdrant")
def diagnose_qdrant():
    """
    Diagnostica problemas en Qdrant.
    Detecta y reporta:
    - Total de puntos en la colección
    - Puntos sin bot_id
    - Vectores con problemas
    """
    try:
        from qdrant_client import QdrantClient
        
        client = QdrantClient(host="localhost", port=6333)
        collection_name = "voia_vectors"
        
        # Obtener info de colección
        collection_info = client.get_collection(collection_name)
        points_count = collection_info.points_count
        
        print(f"📊 Diagnosticando colección '{collection_name}'...")
        print(f"   Total de puntos: {points_count}")
        
        if points_count == 0:
            return {
                "status": "ok",
                "collection": collection_name,
                "total_points": 0,
                "message": "Colección vacía - lista para indexar"
            }
        
        # Intentar scroll para detectar puntos problemáticos
        problematic_points = []
        
        try:
            points_processed = 0
            
            # Procesar en pequeños lotes
            points, _ = client.scroll(
                collection_name=collection_name,
                limit=10  # Solo primeros 10
            )
            
            for point in points:
                points_processed += 1
                if not point.payload.get("bot_id"):
                    problematic_points.append(point.id)
            
            return {
                "status": "ok",
                "collection": collection_name,
                "total_points": points_count,
                "points_sampled": points_processed,
                "points_without_bot_id": len(problematic_points),
                "recommendation": f"Ejecutar /cleanup-qdrant para limpiar" if problematic_points else "Colección OK"
            }
        
        except Exception as e:
            return {
                "status": "error",
                "collection": collection_name,
                "total_points": points_count,
                "error": str(e)[:200],
                "recommendation": "Colección tiene datos corruptos. Ejecutar /cleanup-qdrant"
            }
    
    except Exception as e:
        print(f"❌ Error en /diagnose-qdrant: {e}")
        raise HTTPException(status_code=500, detail=str(e))
