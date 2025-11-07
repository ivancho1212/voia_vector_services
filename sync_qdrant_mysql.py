"""
Sincronización entre Qdrant y MySQL para validar consistencia de datos.
Detecta y repara:
- Vectores en Qdrant pero NO en MySQL (huérfanos)
- Documentos en MySQL pero NO en Qdrant (perdidos)
"""

from .db_utils import get_connection
from .vector_store import get_or_create_vector_store
from datetime import datetime
from typing import Dict, List

client = get_or_create_vector_store()


# ============================================================
# VALIDADOR PRINCIPAL - SOLUTION #6
# ============================================================

class QdrantMySQLValidator:
    """Validador de integridad Qdrant↔MySQL para un bot"""

    def __init__(self, bot_id: int):
        self.bot_id = bot_id
        self.client = get_or_create_vector_store()
        self.conn = get_connection()
        self.cursor = self.conn.cursor(dictionary=True)
        self.stats = {
            "orphan_vectors": 0,
            "lost_documents": 0,
            "hash_mismatches": 0,
            "fixed_issues": 0,
        }

    def close(self):
        """Cierra conexion a BD"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn and self.conn.is_connected():
                self.conn.close()
        except:
            pass

    def get_qdrant_vectors(self) -> List[Dict]:
        """Obtiene todos los vectores de Qdrant para el bot_id"""
        try:
            points, _ = self.client.scroll(
                collection_name="voia_vectors",
                limit=10000,
                scroll_filter={
                    "must": [{"key": "bot_id", "match": {"value": self.bot_id}}]
                }
            )
            return [
                {
                    "id": p.id,
                    "payload": p.payload,
                    "content_hash": p.payload.get("content_hash")
                }
                for p in points
            ]
        except Exception as e:
            print(f"Error obteniendo vectores de Qdrant: {e}")
            return []

    def get_mysql_indexed_documents(self) -> Dict[str, List[Dict]]:
        """Obtiene documentos indexados de MySQL"""
        result = {
            "documents": [],
            "urls": [],
            "texts": []
        }

        try:
            self.cursor.execute("""
                SELECT id, qdrant_id, content_hash, indexed, file_name 
                FROM uploaded_documents 
                WHERE bot_id = %s AND indexed IN (1, 2)
            """, (self.bot_id,))
            result["documents"] = self.cursor.fetchall() or []

            self.cursor.execute("""
                SELECT id, qdrant_id, content_hash, indexed, url 
                FROM training_urls 
                WHERE bot_id = %s AND indexed IN (1, 2)
            """, (self.bot_id,))
            result["urls"] = self.cursor.fetchall() or []

            self.cursor.execute("""
                SELECT id, qdrant_id, content_hash, indexed 
                FROM training_custom_texts 
                WHERE bot_id = %s AND indexed IN (1, 2)
            """, (self.bot_id,))
            result["texts"] = self.cursor.fetchall() or []

            return result
        except Exception as e:
            print(f"Error obteniendo documentos de MySQL: {e}")
            return result

    def validate_bot_integrity(self) -> Dict:
        """Valida integridad de datos del bot"""
        print(f"\nValidando bot {self.bot_id}...")

        qdrant_vectors = self.get_qdrant_vectors()
        mysql_docs = self.get_mysql_indexed_documents()

        qdrant_count = len(qdrant_vectors)
        mysql_count = (
            len(mysql_docs["documents"]) +
            len(mysql_docs["urls"]) +
            len(mysql_docs["texts"])
        )

        print(f"  Qdrant: {qdrant_count} vectores")
        print(f"  MySQL: {mysql_count} documentos indexados")

        discrepancies = []

        # Detectar: Vectores en Qdrant sin documento en MySQL
        for qdrant_vec in qdrant_vectors:
            doc_id = qdrant_vec["payload"].get("doc_id")
            vec_hash = qdrant_vec.get("content_hash")

            found = False
            for doc_list in mysql_docs.values():
                for doc in doc_list:
                    if doc.get("id") == doc_id:
                        found = True
                        if doc.get("content_hash") != vec_hash:
                            discrepancies.append({
                                "type": "hash_mismatch",
                                "doc_id": doc_id,
                                "qdrant_hash": vec_hash,
                                "mysql_hash": doc.get("content_hash")
                            })
                        break
                if found:
                    break

            if not found:
                self.stats["orphan_vectors"] += 1
                discrepancies.append({
                    "type": "qdrant_orphan",
                    "qdrant_id": qdrant_vec["id"],
                    "hash": vec_hash,
                    "payload": qdrant_vec["payload"]
                })

        # Detectar: Documentos en MySQL sin vector en Qdrant
        for doc_type_name, doc_list in mysql_docs.items():
            for doc in doc_list:
                if not doc.get("qdrant_id"):
                    self.stats["lost_documents"] += 1
                    discrepancies.append({
                        "type": "mysql_no_qdrant_id",
                        "doc_type": doc_type_name,
                        "doc_id": doc.get("id")
                    })
                    continue

                if not any(v.get("id") == doc.get("qdrant_id") for v in qdrant_vectors):
                    self.stats["lost_documents"] += 1
                    discrepancies.append({
                        "type": "mysql_vector_missing",
                        "doc_type": doc_type_name,
                        "doc_id": doc.get("id"),
                        "qdrant_id": doc.get("qdrant_id")
                    })

        result = {
            "bot_id": self.bot_id,
            "timestamp": datetime.now().isoformat(),
            "qdrant_count": qdrant_count,
            "mysql_count": mysql_count,
            "discrepancies": discrepancies,
            "discrepancy_count": len(discrepancies),
            "orphan_vectors": self.stats["orphan_vectors"],
            "lost_documents": self.stats["lost_documents"],
            "status": "ok" if len(discrepancies) == 0 else "warning",
        }

        print(f"  Discrepancias: {len(discrepancies)}")
        return result

    def sync_qdrant_with_mysql(self, dry_run: bool = True) -> Dict:
        """Sincroniza datos entre Qdrant y MySQL"""
        print(f"\nSincronizando bot {self.bot_id} (dry_run={dry_run})...")

        validation = self.validate_bot_integrity()
        discrepancies = validation["discrepancies"]

        actions = {
            "delete_from_qdrant": [],
            "reimport_to_qdrant": [],
            "repair_hash": [],
            "total_actions": 0,
            "applied": False
        }

        for disc in discrepancies:
            disc_type = disc["type"]

            if disc_type == "qdrant_orphan":
                actions["delete_from_qdrant"].append({
                    "qdrant_id": disc["qdrant_id"],
                    "reason": "No existe documento en MySQL"
                })

            elif disc_type == "mysql_vector_missing" or disc_type == "mysql_no_qdrant_id":
                actions["reimport_to_qdrant"].append({
                    "doc_type": disc["doc_type"],
                    "doc_id": disc["doc_id"],
                    "reason": "Vector faltante en Qdrant"
                })

            elif disc_type == "hash_mismatch":
                actions["repair_hash"].append({
                    "doc_id": disc["doc_id"],
                    "qdrant_hash": disc["qdrant_hash"],
                    "mysql_hash": disc["mysql_hash"]
                })

        # Aplicar cambios si no es dry-run
        if not dry_run and (actions["delete_from_qdrant"] or actions["reimport_to_qdrant"]):
            print("Aplicando cambios...")

            for action in actions["delete_from_qdrant"]:
                try:
                    self.client.delete(
                        collection_name="voia_vectors",
                        points_selector=[action["qdrant_id"]]
                    )
                    self.stats["fixed_issues"] += 1
                    print(f"  Eliminado vector: {action['qdrant_id']}")
                except Exception as e:
                    print(f"  Error: {e}")

            for action in actions["reimport_to_qdrant"]:
                try:
                    doc_id = action["doc_id"]
                    doc_type = action["doc_type"]

                    table_map = {
                        "documents": "uploaded_documents",
                        "urls": "training_urls",
                        "texts": "training_custom_texts"
                    }

                    table = table_map.get(doc_type)
                    if table:
                        self.cursor.execute(
                            f"UPDATE {table} SET indexed = 0 WHERE id = %s",
                            (doc_id,)
                        )
                        self.conn.commit()
                        self.stats["fixed_issues"] += 1
                        print(f"  Remarcado: {doc_type}#{doc_id}")
                except Exception as e:
                    print(f"  Error: {e}")
                    self.conn.rollback()

        actions["total_actions"] = (
            len(actions["delete_from_qdrant"]) +
            len(actions["reimport_to_qdrant"])
        )
        actions["applied"] = not dry_run
        actions["timestamp"] = datetime.now().isoformat()
        actions["dry_run"] = dry_run

        print(f"  Acciones: {actions['total_actions']}")
        print(f"  Reparadas: {self.stats['fixed_issues']}")

        return actions


# ============================================================
# ENDPOINTS FASTAPI - SOLUTION #6
# ============================================================

def validate_bot_endpoint(bot_id: int) -> Dict:
    """GET /validate/{bot_id}"""
    validator = QdrantMySQLValidator(bot_id)
    try:
        result = validator.validate_bot_integrity()
        return {
            "status": "ok",
            "data": result
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "bot_id": bot_id
        }
    finally:
        validator.close()


def sync_bot_endpoint(bot_id: int, dry_run: bool = True) -> Dict:
    """GET/POST /sync/{bot_id}?dry_run={true|false}"""
    validator = QdrantMySQLValidator(bot_id)
    try:
        sync_result = validator.sync_qdrant_with_mysql(dry_run=dry_run)
        return {
            "status": "ok",
            "data": sync_result,
            "message": f"Sincronizacion completada"
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "bot_id": bot_id
        }
    finally:
        validator.close()


def sync_all_bots_endpoint(dry_run: bool = True) -> Dict:
    """GET/POST /sync-all/?dry_run={true|false}"""
    results = {
        "status": "ok",
        "bots": [],
        "total_bots": 0,
        "total_discrepancies": 0,
        "total_fixed": 0,
        "timestamp": datetime.now().isoformat(),
        "dry_run": dry_run
    }

    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT DISTINCT bot_id FROM (
                SELECT bot_id FROM uploaded_documents WHERE indexed IN (1, 2)
                UNION
                SELECT bot_id FROM training_urls WHERE indexed IN (1, 2)
                UNION
                SELECT bot_id FROM training_custom_texts WHERE indexed IN (1, 2)
            ) as bots
            ORDER BY bot_id
        """)

        bots = cursor.fetchall() or []
        cursor.close()
        conn.close()

        results["total_bots"] = len(bots)

        for bot_row in bots:
            bot_id = bot_row["bot_id"]
            validator = QdrantMySQLValidator(bot_id)

            try:
                validation = validator.validate_bot_integrity()
                sync = validator.sync_qdrant_with_mysql(dry_run=dry_run)

                results["bots"].append({
                    "bot_id": bot_id,
                    "validation": validation,
                    "sync": sync
                })

                results["total_discrepancies"] += validation["discrepancy_count"]
                results["total_fixed"] += validator.stats["fixed_issues"]

            except Exception as e:
                results["bots"].append({
                    "bot_id": bot_id,
                    "error": str(e)
                })
            finally:
                validator.close()

        print(f"Sincronizacion de {results['total_bots']} bots completada")

    except Exception as e:
        results["status"] = "error"
        results["error"] = str(e)

    return results


def sync_qdrant_with_mysql(bot_id: int, dry_run=True):
    """
    Valida y sincroniza Qdrant con MySQL para un bot específico.
    
    Args:
        bot_id: ID del bot a sincronizar
        dry_run: Si True, solo reporta; si False, repara
    
    Returns:
        dict: {
            "orphan_vectors": int,  # Vectores sin documento
            "lost_documents": int,  # Documentos sin vector
            "action": "analyzed" | "repaired"
        }
    """
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    
    stats = {
        "bot_id": bot_id,
        "timestamp": datetime.now().isoformat(),
        "orphan_vectors": 0,
        "lost_documents": 0,
        "orphan_details": [],
        "lost_details": [],
        "action": "analyzed" if dry_run else "repaired"
    }
    
    try:
        print(f"\n{'='*60}")
        print(f"🔄 SINCRONIZANDO BOT {bot_id}...")
        print(f"{'='*60}\n")
        
        # ============================================
        # PASO 1: BUSCAR VECTORES HUÉRFANOS
        # ============================================
        print("📍 PASO 1: Buscando vectores sin documento en MySQL...")
        
        qdrant_points = client.scroll(
            collection_name="voia_vectors",
            scroll_filter={
                "must": [{"key": "bot_id", "match": {"value": bot_id}}]
            },
            limit=100  # Paginar si hay muchos
        )
        
        for point in qdrant_points[0]:
            doc_id = point.payload.get("doc_id")
            
            if not doc_id:
                print(f"  ⚠️ Vector {point.id} sin doc_id en payload")
                stats["orphan_vectors"] += 1
                stats["orphan_details"].append({
                    "vector_id": str(point.id),
                    "reason": "missing_doc_id"
                })
                continue
            
            # Buscar en MySQL
            cursor.execute(
                "SELECT id FROM uploaded_documents WHERE id = %s AND bot_id = %s",
                (doc_id, bot_id)
            )
            
            if not cursor.fetchone():
                print(f"  ❌ Vector {point.id} → Doc {doc_id} NOT FOUND en MySQL")
                stats["orphan_vectors"] += 1
                stats["orphan_details"].append({
                    "vector_id": str(point.id),
                    "doc_id": doc_id,
                    "reason": "document_deleted"
                })
                
                # ✅ REPARAR: Eliminar vector huérfano
                if not dry_run:
                    try:
                        client.delete(
                            collection_name="voia_vectors",
                            points_selector=[point.id]
                        )
                        print(f"    ✅ Vector eliminado de Qdrant")
                    except Exception as e:
                        print(f"    ❌ Error eliminando: {e}")
        
        # ============================================
        # PASO 2: BUSCAR DOCUMENTOS PERDIDOS
        # ============================================
        print("\n📍 PASO 2: Buscando documentos sin vector en Qdrant...")
        
        cursor.execute("""
            SELECT id, file_name, indexed, qdrant_id
            FROM uploaded_documents
            WHERE bot_id = %s AND indexed = 1
        """, (bot_id,))
        
        documents = cursor.fetchall()
        
        for doc in documents:
            doc_id = doc['id']
            qdrant_id = doc.get('qdrant_id')
            
            if not qdrant_id:
                print(f"  ⚠️ Doc {doc_id} sin qdrant_id en MySQL")
                stats["lost_documents"] += 1
                stats["lost_details"].append({
                    "doc_id": doc_id,
                    "filename": doc['file_name'],
                    "reason": "missing_qdrant_id"
                })
                
                # ✅ REPARAR: Marcar como no indexado
                if not dry_run:
                    cursor.execute(
                        "UPDATE uploaded_documents SET indexed = 0 WHERE id = %s",
                        (doc_id,)
                    )
                    conn.commit()
                    print(f"    ✅ Doc marcado para reindexar")
                continue
            
            # Buscar en Qdrant
            try:
                points = client.retrieve(
                    collection_name="voia_vectors",
                    ids=[qdrant_id]
                )
                
                if not points:
                    print(f"  ❌ Doc {doc_id} ({doc['file_name']}) → Vector {qdrant_id} NOT FOUND en Qdrant")
                    stats["lost_documents"] += 1
                    stats["lost_details"].append({
                        "doc_id": doc_id,
                        "filename": doc['file_name'],
                        "qdrant_id": qdrant_id,
                        "reason": "vector_deleted"
                    })
                    
                    # ✅ REPARAR: Marcar como no indexado para reprocessar
                    if not dry_run:
                        cursor.execute(
                            "UPDATE uploaded_documents SET indexed = 0, qdrant_id = NULL WHERE id = %s",
                            (doc_id,)
                        )
                        conn.commit()
                        print(f"    ✅ Doc marcado para reindexar")
            
            except Exception as e:
                print(f"  ❌ Error buscando {qdrant_id}: {e}")
        
        # ============================================
        # RESUMEN
        # ============================================
        print(f"\n{'='*60}")
        print(f"📊 RESULTADOS DE SINCRONIZACIÓN:")
        print(f"{'='*60}")
        print(f"  Vectores huérfanos (sin doc): {stats['orphan_vectors']}")
        print(f"  Documentos perdidos (sin vector): {stats['lost_documents']}")
        print(f"  Modo: {'DRY RUN (solo análisis)' if dry_run else 'REPARACIÓN APLICADA'}")
        print(f"{'='*60}\n")
        
        if not dry_run:
            conn.commit()
            print("✅ Cambios guardados en MySQL")
        
        return stats
    
    except Exception as e:
        print(f"❌ Error en sincronización: {e}")
        return {**stats, "error": str(e)}
    
    finally:
        cursor.close()
        conn.close()


def validate_bot_integrity(bot_id: int):
    """
    Valida la integridad completa de un bot:
    - Documentos en MySQL: ¿todos tienen vectores?
    - Vectores en Qdrant: ¿todos tienen documentos?
    - ¿Indices consistentes?
    
    Returns:
        bool: True si todo OK, False si hay problemas
    """
    result = sync_qdrant_with_mysql(bot_id, dry_run=True)
    
    is_valid = result["orphan_vectors"] == 0 and result["lost_documents"] == 0
    
    print(f"\n{'🟢' if is_valid else '🔴'} BOT {bot_id} INTEGRIDAD: {'OK ✅' if is_valid else 'PROBLEMAS ❌'}")
    
    return is_valid


def repair_bot_data(bot_id: int, force=False):
    """
    Repara automáticamente inconsistencias detectadas.
    
    Args:
        bot_id: ID del bot
        force: Si True, repara sin confirmación
    
    Returns:
        dict: Estadísticas de reparación
    """
    print(f"\n⚠️ REPARANDO BOT {bot_id}...")
    
    if not force:
        confirm = input("¿Estás seguro? Esto eliminará datos. (s/n): ")
        if confirm.lower() != 's':
            print("Cancelado.")
            return None
    
    return sync_qdrant_with_mysql(bot_id, dry_run=False)


# ============================================
# FUNCIONES DE MANTENIMIENTO
# ============================================

def cleanup_all_orphans(dry_run=True):
    """
    Limpia todos los vectores huérfanos de todos los bots.
    """
    print("\n🧹 LIMPIANDO VECTORES HUÉRFANOS DE TODOS LOS BOTS...")
    
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        # Obtener todos los bots
        cursor.execute("SELECT DISTINCT bot_id FROM uploaded_documents")
        bots = cursor.fetchall()
        
        total_orphans = 0
        
        for bot in bots:
            bot_id = bot['bot_id']
            result = sync_qdrant_with_mysql(bot_id, dry_run=dry_run)
            total_orphans += result["orphan_vectors"]
        
        print(f"\n📊 TOTAL: {total_orphans} vectores huérfanos")
        if not dry_run:
            print("✅ Limpieza completada")
    
    finally:
        cursor.close()
        conn.close()


def generate_sync_report(output_file="sync_report.txt"):
    """
    Genera reporte de sincronización para todos los bots.
    """
    print(f"\n📄 GENERANDO REPORTE DE SINCRONIZACIÓN...")
    
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    
    with open(output_file, 'w') as f:
        f.write(f"REPORTE DE SINCRONIZACIÓN QDRANT-MYSQL\n")
        f.write(f"Generado: {datetime.now().isoformat()}\n")
        f.write("=" * 80 + "\n\n")
        
        try:
            cursor.execute("SELECT DISTINCT bot_id FROM uploaded_documents")
            bots = cursor.fetchall()
            
            for bot in bots:
                bot_id = bot['bot_id']
                result = sync_qdrant_with_mysql(bot_id, dry_run=True)
                
                f.write(f"\nBOT ID: {bot_id}\n")
                f.write(f"Vectores huérfanos: {result['orphan_vectors']}\n")
                f.write(f"Documentos perdidos: {result['lost_documents']}\n")
                f.write("-" * 80 + "\n")
        
        finally:
            cursor.close()
            conn.close()
    
    print(f"✅ Reporte guardado en: {output_file}")
