# voia_vector_services/main.py
from fastapi import FastAPI, Query, HTTPException, BackgroundTasks, Request, Response
from dotenv import load_dotenv
import os
import asyncio
from datetime import datetime, timedelta
import numpy as np

# Cargar variables de entorno
load_dotenv()
import pathlib
print("DEBUG ENV FILES:", list(pathlib.Path().glob(".env")))
print("DB_HOST:", os.getenv("DB_HOST"))
print("DB_PORT:", os.getenv("DB_PORT"))
print("DB_USER:", os.getenv("DB_USER"))
print("DB_PASSWORD:", os.getenv("DB_PASSWORD"))
print("DB_NAME:", os.getenv("DB_NAME"))
from voia_vector_services.process_documents import process_pending_documents # noqa
from voia_vector_services.process_urls import process_pending_urls # noqa
from voia_vector_services.process_custom_texts import process_pending_custom_texts # noqa
from voia_vector_services.search_vectors import search_vectors # noqa
from voia_vector_services.sync_qdrant_mysql import validate_bot_endpoint, sync_bot_endpoint, sync_all_bots_endpoint # noqa
from voia_vector_services.rate_limiting import limiter, setup_rate_limiting, LIMITS # noqa
from voia_vector_services.snapshot_manager import SnapshotManager # noqa
from voia_vector_services.sync_manager import QdrantMySQLSynchronizer # noqa
from voia_vector_services.recovery_manager import RecoveryManager # noqa
from voia_vector_services.embedder import get_embedding

from pydantic import BaseModel

class SearchRequest(BaseModel):
    query: str
    bot_id: int
    limit: int = 3

# Instancia FastAPI
app = FastAPI()

# 🚨 Setup Rate Limiting
setup_rate_limiting(app)

# ✅ FASE 1, 2, 3: Inicializar managers de persistencia
snapshot_manager = SnapshotManager(
    snapshots_dir=os.getenv("QDRANT_SNAPSHOTS_DIR", "./snapshots"),
    compress=True,
    retention_days=30
)

recovery_manager = RecoveryManager(
    snapshots_dir=os.getenv("QDRANT_SNAPSHOTS_DIR", "./snapshots"),
    rpo_hours=int(os.getenv("QDRANT_RPO_HOURS", "1")),
    rto_minutes=int(os.getenv("QDRANT_RTO_MINUTES", "30"))
)

# Worker para snapshots automáticos (programado cada 6 horas en producción)
async def automatic_snapshot_worker():
    """Worker que crea snapshots automáticamente cada N horas."""
    snapshot_interval_hours = int(os.getenv("QDRANT_SNAPSHOT_INTERVAL_HOURS", "6"))
    last_snapshot = None

    while True:
        try:
            now = datetime.now()
            
            if last_snapshot is None or (now - last_snapshot).total_seconds() >= snapshot_interval_hours * 3600:
                print(f"\n⏰ Ejecutando snapshot automático...")
                result = snapshot_manager.create_snapshot(
                    description=f"Automatic snapshot at {now.isoformat()}"
                )
                if result.get("success"):
                    last_snapshot = now
                    print(f"✅ Snapshot automático completado")
                else:
                    print(f"❌ Error en snapshot automático: {result.get('error')}")
            
            # Esperar 1 hora antes de verificar nuevamente
            await asyncio.sleep(3600)
        except Exception as e:
            print(f"❌ Error en automatic_snapshot_worker: {e}")
            await asyncio.sleep(3600)

# Worker para sincronización automática (cada 30 minutos)
async def automatic_sync_worker():
    """Worker que valida sincronización entre Qdrant y MySQL periodicamente."""
    sync_interval_minutes = int(os.getenv("QDRANT_SYNC_INTERVAL_MINUTES", "30"))

    while True:
        try:
            print(f"\n⏰ Ejecutando sincronización automática...")
            from voia_vector_services.db_utils import get_connection
            
            conn = get_connection()
            cursor = conn.cursor(dictionary=True)
            
            # Obtener todos los bots activos
            cursor.execute("SELECT id FROM bots LIMIT 100")
            bots = cursor.fetchall()
            
            total_issues = 0
            for bot in bots:
                try:
                    sync = QdrantMySQLSynchronizer(bot["id"])
                    discrepancies = sync.detect_drift()
                    
                    issue_count = (
                        len(discrepancies.get("orphan_vectors", [])) +
                        len(discrepancies.get("lost_documents", [])) +
                        len(discrepancies.get("hash_mismatches", []))
                    )
                    
                    if issue_count > 0:
                        print(f"   ⚠️ Bot {bot['id']}: {issue_count} discrepancias detectadas")
                        total_issues += issue_count
                    
                    sync.close()
                except Exception as e:
                    print(f"   ⚠️ Error sincronizando bot {bot['id']}: {e}")
            
            cursor.close()
            conn.close()
            
            if total_issues > 0:
                print(f"⚠️ Total de discrepancias: {total_issues}")
            else:
                print(f"✅ Sincronización completa sin problemas")
            
            # Esperar N minutos
            await asyncio.sleep(sync_interval_minutes * 60)
        except Exception as e:
            print(f"❌ Error en automatic_sync_worker: {e}")
            await asyncio.sleep(3600)

# Iniciar workers en background
@app.on_event("startup")
async def startup_event():
    """Inicia tasks asincrónicas al startup de la app."""
    print("\n🚀 Iniciando Voia Vector Services...")
    print("📊 Estado de Persistencia y Recuperación:")
    
    # Mostrar estado de recuperación
    status = recovery_manager.get_recovery_status()
    
    # Iniciar workers automáticos
    enable_auto_snapshot = os.getenv("QDRANT_AUTO_SNAPSHOT", "true").lower() == "true"
    enable_auto_sync = os.getenv("QDRANT_AUTO_SYNC", "true").lower() == "true"
    
    if enable_auto_snapshot:
        asyncio.create_task(automatic_snapshot_worker())
        print("   ✅ Snapshot automático habilitado")
    
    if enable_auto_sync:
        asyncio.create_task(automatic_sync_worker())
        print("   ✅ Sincronización automática habilitada")

# Endpoints existentes
@app.get("/process_all")
@limiter.limit(LIMITS["process_all"])
def process_all(request):
    print("🚀 Procesando PDFs...")
    process_pending_documents()
    print("🌐 Procesando URLs...")
    process_pending_urls()
    print("📝 Procesando textos planos...")
    process_pending_custom_texts()
    return {"status": "success", "message": "All processing tasks have been initiated."}

@app.get("/process_documents")
@limiter.limit(LIMITS["process_documents"])
def process_documents_endpoint(request: Request, bot_id: int = Query(..., description="ID del bot para procesar documentos")):
    try:
        print(f"🚀 Procesando PDFs para el bot_id: {bot_id}...")
        process_pending_documents(bot_id)
        return {"status": "success", "message": f"Document processing initiated for bot {bot_id}."}
    except Exception as e:
        print(f"❌ Error en process_documents_endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/process_urls")
@limiter.limit(LIMITS["process_urls"])
def process_urls_endpoint(request: Request, bot_id: int = Query(..., description="ID del bot para procesar URLs")):
    try:
        print(f"🌐 Procesando URLs para el bot_id: {bot_id}...")
        process_pending_urls(bot_id)
        return {"status": "success", "message": f"URL processing initiated for bot {bot_id}."}
    except Exception as e:
        print(f"❌ Error en process_urls_endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/process_texts")
@limiter.limit(LIMITS["process_texts"])
def process_texts_endpoint(request: Request, bot_id: int = Query(..., description="ID del bot para procesar textos")):
    try:
        print(f"📝 Procesando textos para el bot_id: {bot_id}...")
        process_pending_custom_texts(bot_id)
        return {"status": "success", "message": f"Custom text processing initiated for bot {bot_id}."}
    except Exception as e:
        print(f"❌ Error en process_texts_endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 🔹 Endpoint para búsqueda de vectores (NUEVO, para el chat dinámico)
@app.post("/search")
@limiter.limit(LIMITS["search"])
def search_vectors_endpoint(request, req: SearchRequest):
    """
    Busca vectores en Qdrant asociados a un bot dado y un query opcional.
    """
    try:
        results = search_vectors(bot_id=req.bot_id, query=req.query, limit=req.limit)
        return {"results": results}
    except Exception as e:
        print(f"❌ Error en el endpoint /search: {e}")
        # Esto devolverá un error 500 al cliente C# con un mensaje específico
        raise HTTPException(status_code=500, detail=f"Error interno en el servicio de búsqueda de Python: {str(e)}")

# 🔹 Endpoint de búsqueda de vectores (ANTIGUO, restaurado para compatibilidad)
@app.get("/search_vectors")
@limiter.limit(LIMITS["search"])
def search_vectors_get_endpoint(request,
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
@limiter.limit(LIMITS["validate"])
def validate_bot(request, bot_id: int):
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
@limiter.limit(LIMITS["sync"])
def sync_bot(request, bot_id: int, dry_run: bool = Query(True, description="Si True, solo análisis sin reparar")):
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
@limiter.limit(LIMITS["sync"])
def sync_all(request, dry_run: bool = Query(True, description="Si True, solo análisis sin reparar")):
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


# ============================================================
# ✅ FASE 1, 2, 3: ENDPOINTS DE PERSISTENCIA Y RECUPERACIÓN
# ============================================================

# FASE 1: SNAPSHOTS
# ============================================================

@app.post("/snapshots/create")
@limiter.limit(LIMITS.get("snapshots", "10/hour"))
def create_snapshot_endpoint(request, description: str = Query("", description="Descripción del snapshot")):
    """Crea un snapshot manual de la colección Qdrant."""
    try:
        print(f"📸 Creando snapshot manual...")
        result = snapshot_manager.create_snapshot(description=description)
        
        if not result.get("success"):
            raise HTTPException(status_code=500, detail=result.get("error"))
        
        return result
    except Exception as e:
        print(f"❌ Error creando snapshot: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/snapshots/list")
@limiter.limit(LIMITS.get("snapshots", "10/hour"))
def list_snapshots_endpoint(request):
    """Lista todos los snapshots disponibles."""
    try:
        snapshots = snapshot_manager.list_snapshots()
        stats = snapshot_manager.get_statistics()
        
        return {
            "snapshots": snapshots,
            "statistics": stats
        }
    except Exception as e:
        print(f"❌ Error listando snapshots: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/snapshots/validate/{snapshot_id}")
@limiter.limit(LIMITS.get("snapshots", "10/hour"))
def validate_snapshot_endpoint(request, snapshot_id: str):
    """Valida integridad de un snapshot específico."""
    try:
        result = snapshot_manager.validate_snapshot(snapshot_id)
        
        if not result.get("valid"):
            raise HTTPException(status_code=400, detail=result.get("error"))
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error validando snapshot: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/snapshots/restore/{snapshot_id}")
@limiter.limit(LIMITS.get("snapshots", "3/hour"))
def restore_snapshot_endpoint(request, snapshot_id: str, bot_id: int = Query(None)):
    """Restaura una colección desde un snapshot."""
    try:
        print(f"🔄 Restaurando snapshot {snapshot_id}...")
        result = recovery_manager.restore_from_snapshot(snapshot_id, bot_id=bot_id)
        
        if not result.get("success"):
            raise HTTPException(status_code=500, detail=result.get("error"))
        
        return result
    except Exception as e:
        print(f"❌ Error restaurando snapshot: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# FASE 2: SINCRONIZACIÓN
# ============================================================

@app.get("/sync/detect-drift")
@limiter.limit(LIMITS.get("sync", "5/hour"))
def detect_drift_endpoint(request, bot_id: int = Query(...)):
    """Detecta desincronización entre Qdrant y MySQL para un bot."""
    try:
        sync = QdrantMySQLSynchronizer(bot_id)
        discrepancies = sync.detect_drift()
        stats = sync.get_statistics()
        sync.close()
        
        return {
            "bot_id": bot_id,
            "discrepancies": discrepancies,
            "statistics": stats
        }
    except Exception as e:
        print(f"❌ Error detectando drift: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sync/reconcile")
@limiter.limit(LIMITS.get("sync", "3/hour"))
def reconcile_endpoint(request, bot_id: int = Query(...), auto_fix: bool = Query(False)):
    """Reconcilia datos entre Qdrant y MySQL."""
    try:
        print(f"🔄 Reconciliando bot {bot_id} (auto_fix={auto_fix})...")
        sync = QdrantMySQLSynchronizer(bot_id)
        result = sync.reconcile(auto_fix=auto_fix)
        stats = sync.get_statistics()
        sync.close()
        
        return {
            "reconciliation": result,
            "statistics": stats
        }
    except Exception as e:
        print(f"❌ Error reconciliando: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sync/reconcile-all")
@limiter.limit(LIMITS.get("sync", "2/hour"))
def reconcile_all_endpoint(request, auto_fix: bool = Query(False)):
    """Reconcilia todos los bots."""
    try:
        print(f"🔄 Reconciliando TODOS los bots (auto_fix={auto_fix})...")
        
        from voia_vector_services.db_utils import get_connection
        
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT id FROM bots")
        bots = cursor.fetchall()
        cursor.close()
        conn.close()
        
        results = []
        for bot in bots:
            try:
                sync = QdrantMySQLSynchronizer(bot["id"])
                result = sync.reconcile(auto_fix=auto_fix)
                results.append(result)
                sync.close()
            except Exception as e:
                print(f"⚠️ Error con bot {bot['id']}: {e}")
                results.append({"bot_id": bot["id"], "error": str(e)})
        
        return {
            "total_bots": len(results),
            "results": results,
            "message": f"Reconciled {len(results)} bots"
        }
    except Exception as e:
        print(f"❌ Error reconciliando todos: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# FASE 3: RECUPERACIÓN Y DISASTER RECOVERY
# ============================================================

@app.get("/recovery/status")
@limiter.limit(LIMITS.get("recovery", "10/hour"))
def recovery_status_endpoint(request):
    """Obtiene estado actual del sistema de recuperación."""
    try:
        status = recovery_manager.get_recovery_status()
        return status
    except Exception as e:
        print(f"❌ Error obteniendo status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/recovery/create-point")
@limiter.limit(LIMITS.get("recovery", "5/hour"))
def create_recovery_point_endpoint(request, description: str = Query("Manual Recovery Point")):
    """Crea un punto de recuperación manual."""
    try:
        result = recovery_manager.create_recovery_point(description=description)
        
        if not result.get("success"):
            raise HTTPException(status_code=500, detail=result.get("error"))
        
        return result
    except Exception as e:
        print(f"❌ Error creando punto de recuperación: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/recovery/rollback")
@limiter.limit(LIMITS.get("recovery", "2/hour"))
def rollback_endpoint(request, hours_back: int = Query(1, ge=1, le=72)):
    """Hace rollback a un estado anterior (N horas atrás)."""
    try:
        print(f"⏮️ Iniciando rollback ({hours_back} horas atrás)...")
        result = recovery_manager.rollback_to_previous_state(hours_back=hours_back)
        
        if not result.get("success"):
            raise HTTPException(status_code=500, detail=result.get("error"))
        
        return result
    except Exception as e:
        print(f"❌ Error en rollback: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/recovery/drp")
@limiter.limit(LIMITS.get("recovery", "1/hour"))
def disaster_recovery_procedure_endpoint(request, background_tasks: BackgroundTasks):
    """
    Inicia procedimiento de recuperación ante desastres (DRP).
    
    Operación crítica:
    1. Evalúa capacidad de recuperación
    2. Crea backup inmediato
    3. Valida snapshot
    4. Restaura desde snapshot más reciente
    5. Ejecuta sincronización completa
    6. Genera reporte
    """
    try:
        print(f"🚨 INICIANDO DISASTER RECOVERY PROCEDURE...")
        
        # Ejecutar DRP en background para no bloquear la API
        background_tasks.add_task(recovery_manager.disaster_recovery_procedure)
        
        return {
            "status": "started",
            "message": "Disaster Recovery Procedure iniciado. Monitorear /recovery/status",
            "next_steps": [
                "1. Verificar /recovery/status periodicamente",
                "2. Revisar logs de recuperación",
                "3. Validar integridad de datos manualmente",
                "4. Notificar al equipo de operaciones"
            ]
        }
    except Exception as e:
        print(f"❌ Error iniciando DRP: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/recovery/assess")
@limiter.limit(LIMITS.get("recovery", "10/hour"))
def assess_recovery_capability_endpoint(request):
    """Evalúa capacidad de recuperación del sistema."""
    try:
        capability = recovery_manager.assess_recovery_capability()
        return capability
    except Exception as e:
        print(f"❌ Error evaluando capacidad: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Health check para persistencia
@app.get("/health/persistence")
def health_persistence_endpoint(request):
    """Health check del sistema de persistencia."""
    try:
        # Verificar snapshots
        snapshots = snapshot_manager.list_snapshots()
        
        # Verificar capability
        capability = recovery_manager.assess_recovery_capability()
        
        return {
            "status": "healthy" if capability.get("capable_of_recovery") else "unhealthy",
            "snapshots_available": len(snapshots),
            "rpo_compliant": capability.get("rpo_compliant"),
            "rto_compliant": capability.get("rto_compliant"),
            "latest_snapshot": snapshots[0]["snapshot_id"] if snapshots else None
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        }

@app.post("/embed")
async def embed_endpoint(request: Request):
    text = await request.body()
    text_str = text.decode("utf-8")
    vector = get_embedding(text_str)
    arr = np.array(vector, dtype=np.float32)
    return Response(content=arr.tobytes(), media_type="application/octet-stream")

