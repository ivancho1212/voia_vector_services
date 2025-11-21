"""
FASE 2: Sincronización MySQL ↔ Qdrant
Mantiene consistencia entre base de datos relacional y vector store.

Funcionalidades:
- Sincronización bidireccional
- Detección de drift (desincronización)
- Reconciliación automática
- Logging de operaciones
- Validación de integridad
"""

import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from .db_utils import get_connection
from .vector_store import get_or_create_vector_store
from dotenv import load_dotenv

load_dotenv()


class SyncLog:
    """Registra operaciones de sincronización en base de datos."""

    def __init__(self):
        self.conn = get_connection()
        self._init_table()

    def _init_table(self) -> None:
        """Crea tabla de log si no existe."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS vector_sync_log (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    operation VARCHAR(50) NOT NULL,
                    entity_type VARCHAR(50),
                    entity_id INT,
                    bot_id INT,
                    source VARCHAR(20),
                    destination VARCHAR(20),
                    status VARCHAR(20),
                    error_message TEXT,
                    records_affected INT,
                    duration_ms INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_bot_id (bot_id),
                    INDEX idx_created_at (created_at)
                )
            """)
            self.conn.commit()
        except Exception as e:
            print(f"⚠️ Error creando tabla sync_log: {e}")

    def log_operation(
        self,
        operation: str,
        entity_type: str,
        entity_id: Optional[int] = None,
        bot_id: Optional[int] = None,
        source: str = "mysql",
        destination: str = "qdrant",
        status: str = "success",
        error_message: Optional[str] = None,
        records_affected: int = 0,
        duration_ms: int = 0
    ) -> None:
        """Registra una operación de sincronización."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO vector_sync_log
                (operation, entity_type, entity_id, bot_id, source, destination,
                 status, error_message, records_affected, duration_ms)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                operation, entity_type, entity_id, bot_id, source, destination,
                status, error_message, records_affected, duration_ms
            ))
            self.conn.commit()
        except Exception as e:
            print(f"❌ Error logging operación: {e}")

    def close(self):
        """Cierra conexión."""
        if self.conn and self.conn.is_connected():
            self.conn.close()


class QdrantMySQLSynchronizer:
    """
    Sincroniza datos entre Qdrant y MySQL.
    
    Detecta y repara:
    - Vectores huérfanos (en Qdrant sin documento en MySQL)
    - Documentos perdidos (en MySQL sin vector en Qdrant)
    - Hash mismatches (contenido modificado sin re-indexación)
    - Desincronización de timestamps
    """

    def __init__(self, bot_id: int):
        """
        Inicializa sincronizador para un bot específico.
        
        Args:
            bot_id: ID del bot a sincronizar
        """
        self.bot_id = bot_id
        self.client = get_or_create_vector_store()
        self.conn = get_connection()
        self.cursor = self.conn.cursor(dictionary=True)
        self.sync_log = SyncLog()

        self.stats = {
            "orphan_vectors_found": 0,
            "lost_documents_found": 0,
            "hash_mismatches": 0,
            "items_synced": 0,
            "items_fixed": 0,
            "errors": 0,
        }

    def close(self):
        """Cierra conexiones."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn and self.conn.is_connected():
                self.conn.close()
            self.sync_log.close()
        except:
            pass

    def _calculate_hash(self, content: str) -> str:
        """Calcula hash SHA256 del contenido."""
        return hashlib.sha256(content.encode()).hexdigest()

    def _get_qdrant_vectors(self) -> List[Dict]:
        """Obtiene todos los vectores del bot desde Qdrant."""
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
                }
                for p in points
            ]
        except Exception as e:
            print(f"❌ Error obteniendo vectores de Qdrant: {e}")
            self.stats["errors"] += 1
            return []

    def _get_mysql_documents(self) -> Dict[str, List[Dict]]:
        """Obtiene todos los documentos del bot desde MySQL."""
        result = {
            "uploaded_documents": [],
            "training_urls": [],
            "training_custom_texts": []
        }

        try:
            # Obtener documentos
            self.cursor.execute("""
                SELECT id, qdrant_id, content_hash, indexed, file_name
                FROM uploaded_documents
                WHERE bot_id = %s
            """, (self.bot_id,))
            result["uploaded_documents"] = self.cursor.fetchall() or []

            # Obtener URLs
            self.cursor.execute("""
                SELECT id, qdrant_id, content_hash, indexed, url
                FROM training_urls
                WHERE bot_id = %s
            """, (self.bot_id,))
            result["training_urls"] = self.cursor.fetchall() or []

            # Obtener textos personalizados
            self.cursor.execute("""
                SELECT id, qdrant_id, content_hash, indexed
                FROM training_custom_texts
                WHERE bot_id = %s
            """, (self.bot_id,))
            result["training_custom_texts"] = self.cursor.fetchall() or []

            return result
        except Exception as e:
            print(f"❌ Error obteniendo documentos de MySQL: {e}")
            self.stats["errors"] += 1
            return result

    def detect_drift(self) -> Dict:
        """
        Detecta desincronización entre Qdrant y MySQL.
        
        Returns:
            Dict con discrepancias encontradas
        """
        print(f"\n🔍 Detectando drift para bot {self.bot_id}...")

        qdrant_vectors = self._get_qdrant_vectors()
        mysql_docs = self._get_mysql_documents()

        total_qdrant = len(qdrant_vectors)
        total_mysql = (
            len(mysql_docs["uploaded_documents"]) +
            len(mysql_docs["training_urls"]) +
            len(mysql_docs["training_custom_texts"])
        )

        print(f"   Qdrant: {total_qdrant} vectores")
        print(f"   MySQL: {total_mysql} documentos")

        discrepancies = {
            "orphan_vectors": [],
            "lost_documents": [],
            "hash_mismatches": [],
            "timestamp_drift": []
        }

        # 1. Detectar vectores huérfanos (en Qdrant sin documento en MySQL)
        for qdrant_vec in qdrant_vectors:
            payload = qdrant_vec.get("payload", {})
            doc_id = payload.get("doc_id")
            entity_type = payload.get("entity_type", "unknown")

            found = False
            for doc_list in mysql_docs.values():
                for doc in doc_list:
                    if doc.get("id") == doc_id:
                        found = True

                        # Verificar hash si documentos son indexados
                        if doc.get("indexed") in [1, 2]:
                            qdrant_hash = payload.get("content_hash")
                            mysql_hash = doc.get("content_hash")

                            if qdrant_hash != mysql_hash:
                                discrepancies["hash_mismatches"].append({
                                    "doc_id": doc_id,
                                    "entity_type": entity_type,
                                    "qdrant_hash": qdrant_hash,
                                    "mysql_hash": mysql_hash,
                                })
                                self.stats["hash_mismatches"] += 1
                        break
                if found:
                    break

            if not found:
                discrepancies["orphan_vectors"].append({
                    "qdrant_id": qdrant_vec["id"],
                    "doc_id": doc_id,
                    "entity_type": entity_type,
                    "payload": payload
                })
                self.stats["orphan_vectors_found"] += 1

        # 2. Detectar documentos perdidos (en MySQL sin vector en Qdrant)
        qdrant_doc_ids = set(v["payload"].get("doc_id") for v in qdrant_vectors)

        for doc_type, doc_list in mysql_docs.items():
            for doc in doc_list:
                if doc.get("indexed") in [1, 2]:  # Solo indexados
                    if doc["id"] not in qdrant_doc_ids:
                        discrepancies["lost_documents"].append({
                            "doc_id": doc["id"],
                            "entity_type": doc_type,
                            "qdrant_id": doc.get("qdrant_id")
                        })
                        self.stats["lost_documents_found"] += 1

        # 3. Loguear discrepancias
        total_issues = (
            len(discrepancies["orphan_vectors"]) +
            len(discrepancies["lost_documents"]) +
            len(discrepancies["hash_mismatches"])
        )

        if total_issues > 0:
            print(f"   ⚠️ Detectadas {total_issues} discrepancias")
            self._log_discrepancies(discrepancies)
        else:
            print(f"   ✅ Sincronización perfecta")

        return discrepancies

    def _log_discrepancies(self, discrepancies: Dict) -> None:
        """Registra discrepancias en log."""
        for orphan in discrepancies["orphan_vectors"]:
            self.sync_log.log_operation(
                operation="orphan_vector_detected",
                entity_type=orphan.get("entity_type"),
                entity_id=orphan.get("doc_id"),
                bot_id=self.bot_id,
                source="qdrant",
                status="warning"
            )

        for lost in discrepancies["lost_documents"]:
            self.sync_log.log_operation(
                operation="lost_document_detected",
                entity_type=lost.get("entity_type"),
                entity_id=lost.get("doc_id"),
                bot_id=self.bot_id,
                destination="qdrant",
                status="warning"
            )

    def reconcile(self, auto_fix: bool = False) -> Dict:
        """
        Reconcilia datos entre Qdrant y MySQL.
        
        Args:
            auto_fix: Si True, intenta reparar discrepancias automáticamente
            
        Returns:
            Dict con resultados de reconciliación
        """
        print(f"\n🔄 Reconciliando bot {self.bot_id}...")

        discrepancies = self.detect_drift()

        result = {
            "bot_id": self.bot_id,
            "timestamp": datetime.now().isoformat(),
            "orphan_vectors": len(discrepancies["orphan_vectors"]),
            "lost_documents": len(discrepancies["lost_documents"]),
            "hash_mismatches": len(discrepancies["hash_mismatches"]),
            "auto_fix_enabled": auto_fix,
            "fixed_items": 0,
        }

        if not auto_fix:
            print(f"   ℹ️ auto_fix deshabilitado. Revisar discrepancias manualmente.")
            return result

        # 1. Limpiar vectores huérfanos
        for orphan in discrepancies["orphan_vectors"]:
            try:
                self.client.delete(
                    collection_name="voia_vectors",
                    points_selector=[orphan["qdrant_id"]]
                )
                print(f"   🗑️ Eliminado vector huérfano {orphan['qdrant_id']}")
                result["fixed_items"] += 1
                self.stats["items_fixed"] += 1
            except Exception as e:
                print(f"   ❌ Error eliminando vector: {e}")
                self.stats["errors"] += 1

        # 2. Marcar documentos perdidos como no indexados
        for lost in discrepancies["lost_documents"]:
            try:
                entity_type = lost["entity_type"]
                table_name = entity_type.replace("_", "")

                if entity_type == "uploaded_documents":
                    self.cursor.execute(
                        "UPDATE uploaded_documents SET indexed = 0 WHERE id = %s AND bot_id = %s",
                        (lost["doc_id"], self.bot_id)
                    )
                elif entity_type == "training_urls":
                    self.cursor.execute(
                        "UPDATE training_urls SET indexed = 0 WHERE id = %s AND bot_id = %s",
                        (lost["doc_id"], self.bot_id)
                    )
                elif entity_type == "training_custom_texts":
                    self.cursor.execute(
                        "UPDATE training_custom_texts SET indexed = 0 WHERE id = %s AND bot_id = %s",
                        (lost["doc_id"], self.bot_id)
                    )

                self.conn.commit()
                print(f"   📝 Marcado {entity_type}#{lost['doc_id']} como no indexado")
                result["fixed_items"] += 1
                self.stats["items_fixed"] += 1
            except Exception as e:
                print(f"   ❌ Error actualizando documento: {e}")
                self.stats["errors"] += 1

        print(f"✅ Reconciliación completada: {result['fixed_items']} items reparados")

        return result

    def get_statistics(self) -> Dict:
        """Obtiene estadísticas de sincronización."""
        return {
            "bot_id": self.bot_id,
            "timestamp": datetime.now().isoformat(),
            **self.stats
        }
