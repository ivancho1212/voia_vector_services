"""
FASE 3: Recovery Manager para Qdrant
Mecanismo de recuperación y rollback ante fallos.

Funcionalidades:
- Restauración desde snapshots
- Validación de integridad post-restauración
- Rollback de cambios
- Procedimiento de recuperación ante desastres
- Verificación de punto de recuperación
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, List
from .snapshot_manager import SnapshotManager
from .sync_manager import QdrantMySQLSynchronizer, SyncLog
from .vector_store import get_or_create_vector_store
from .db_utils import get_connection
from dotenv import load_dotenv

load_dotenv()


class RecoveryManager:
    """
    Gestor de recuperación para Qdrant con opciones de rollback y restauración.
    
    Incluye:
    - Punto de recuperación (Recovery Point Objective - RPO)
    - Tiempo de recuperación objetivo (Recovery Time Objective - RTO)
    - Procedimiento de recuperación de desastres (DRP)
    - Validación post-restauración
    """

    def __init__(
        self,
        snapshots_dir: Optional[str] = None,
        rpo_hours: int = 1,
        rto_minutes: int = 30
    ):
        """
        Inicializa RecoveryManager.
        
        Args:
            snapshots_dir: Directorio de snapshots (default: ./snapshots)
            rpo_hours: Recovery Point Objective en horas (default: 1)
            rto_minutes: Recovery Time Objective en minutos (default: 30)
        """
        self.client = get_or_create_vector_store()
        self.snapshot_manager = SnapshotManager(snapshots_dir=snapshots_dir)
        self.sync_log = SyncLog()

        self.rpo_hours = rpo_hours
        self.rto_minutes = rto_minutes

        # Archivo de auditoría de recuperación
        self.recovery_dir = Path(snapshots_dir or "./snapshots") / "recovery"
        self.recovery_dir.mkdir(parents=True, exist_ok=True)
        self.audit_file = self.recovery_dir / "recovery_audit.json"

        self._init_audit_file()

    def _init_audit_file(self) -> None:
        """Inicializa archivo de auditoría."""
        if not self.audit_file.exists():
            audit_data = {
                "created": datetime.now().isoformat(),
                "recoveries": [],
                "rollbacks": []
            }
            with open(self.audit_file, 'w') as f:
                json.dump(audit_data, f, indent=2)

    def _log_recovery_event(self, event_type: str, details: Dict) -> None:
        """Registra evento de recuperación."""
        try:
            with open(self.audit_file, 'r') as f:
                audit_data = json.load(f)

            event = {
                "timestamp": datetime.now().isoformat(),
                "type": event_type,
                **details
            }

            if event_type == "recovery":
                audit_data["recoveries"].append(event)
            elif event_type == "rollback":
                audit_data["rollbacks"].append(event)

            with open(self.audit_file, 'w') as f:
                json.dump(audit_data, f, indent=2, default=str)
        except Exception as e:
            print(f"⚠️ Error logging recovery event: {e}")

    def assess_recovery_capability(self) -> Dict:
        """
        Evalúa capacidad de recuperación del sistema.
        
        Returns:
            Dict con estado de RPO/RTO y capacidad de recuperación
        """
        print("\n📊 Evaluando capacidad de recuperación...")

        snapshots = self.snapshot_manager.list_snapshots()
        stats = self.snapshot_manager.get_statistics()

        if not snapshots:
            print("   ❌ CRÍTICO: No hay snapshots disponibles")
            return {
                "capable_of_recovery": False,
                "rpo_compliant": False,
                "rto_compliant": False,
                "error": "No snapshots available"
            }

        latest_snapshot = snapshots[0]
        snapshot_age_minutes = (
            datetime.now() - datetime.fromisoformat(latest_snapshot["created"])
        ).total_seconds() / 60

        rpo_minutes = self.rpo_hours * 60
        rto_compliant = snapshot_age_minutes <= rto_minutes
        rpo_compliant = snapshot_age_minutes <= rpo_minutes

        print(f"   Snapshot más reciente: {latest_snapshot['snapshot_id']}")
        print(f"   Edad: {snapshot_age_minutes:.1f} minutos")
        print(f"   RPO ({self.rpo_hours}h): {'✅' if rpo_compliant else '❌'}")
        print(f"   RTO ({self.rto_minutes}m): {'✅' if rto_compliant else '❌'}")

        return {
            "capable_of_recovery": True,
            "rpo_compliant": rpo_compliant,
            "rto_compliant": rto_compliant,
            "latest_snapshot": latest_snapshot["snapshot_id"],
            "snapshot_age_minutes": snapshot_age_minutes,
            "total_snapshots": stats["total_snapshots"],
            "total_size_mb": stats["total_size_mb"],
            "recommendations": self._get_recovery_recommendations(
                rpo_compliant, rto_compliant, snapshot_age_minutes
            )
        }

    def _get_recovery_recommendations(
        self,
        rpo_compliant: bool,
        rto_compliant: bool,
        snapshot_age_minutes: float
    ) -> List[str]:
        """Genera recomendaciones basadas en estado actual."""
        recommendations = []

        if not rpo_compliant:
            recommendations.append(
                f"⚠️ RPO violado: último snapshot tiene {snapshot_age_minutes:.0f}m. "
                f"Ejecutar snapshot manual inmediatamente."
            )

        if not rto_compliant:
            recommendations.append(
                f"⚠️ RTO en riesgo: tiempo de recuperación actual {snapshot_age_minutes:.0f}m "
                f"excede objetivo {self.rto_minutes}m. Considerar snapshots más frecuentes."
            )

        if snapshot_age_minutes > self.rpo_hours * 60 * 2:
            recommendations.append(
                "⚠️ CRÍTICO: Último snapshot muy antiguo. Implementar snapshots automáticos inmediatamente."
            )

        if not recommendations:
            recommendations.append("✅ Sistema en estado de recuperación óptimo")

        return recommendations

    def create_recovery_point(self, description: str = "Manual Recovery Point") -> Dict:
        """
        Crea un punto de recuperación (snapshot manual).
        
        Args:
            description: Descripción del punto de recuperación
            
        Returns:
            Dict con resultado de creación
        """
        print(f"\n💾 Creando punto de recuperación: {description}...")

        result = self.snapshot_manager.create_snapshot(description=description)

        if result.get("success"):
            self._log_recovery_event("recovery_point_created", result)
            print(f"✅ Punto de recuperación creado: {result['snapshot_id']}")
        else:
            print(f"❌ Error creando punto de recuperación: {result.get('error')}")

        return result

    def restore_from_snapshot(self, snapshot_id: str, bot_id: Optional[int] = None) -> Dict:
        """
        Restaura desde un snapshot específico.
        
        Procedimiento:
        1. Validar snapshot
        2. Pre-restauración: Estado actual
        3. Restaurar desde snapshot
        4. Post-restauración: Validar integridad
        5. Sincronizar con MySQL si aplica
        6. Registrar evento
        
        Args:
            snapshot_id: ID del snapshot a restaurar
            bot_id: Opcional - si está especificado, sincronizar solo este bot
            
        Returns:
            Dict con resultado de restauración
        """
        print(f"\n🔄 INICIANDO PROCEDIMIENTO DE RESTAURACIÓN")
        print(f"   Snapshot: {snapshot_id}")
        print(f"   Bot: {bot_id or 'todos'}")

        # 1. Validar snapshot
        print("\n1️⃣ Validando snapshot...")
        validation = self.snapshot_manager.validate_snapshot(snapshot_id)

        if not validation.get("valid"):
            error = validation.get("error")
            print(f"   ❌ Snapshot inválido: {error}")
            return {
                "success": False,
                "error": error,
                "step": "validation"
            }

        print(f"   ✅ Snapshot válido ({validation.get('size_bytes')} bytes)")

        # 2. Registrar pre-restauración
        print("\n2️⃣ Registrando estado pre-restauración...")
        try:
            pre_restore_stats = {
                "timestamp": datetime.now().isoformat(),
                "collection_info": str(self.client.get_collection("voia_vectors"))
            }
            print(f"   ✅ Estado capturado")
        except Exception as e:
            print(f"   ⚠️ Error capturando estado: {e}")
            pre_restore_stats = {}

        # 3. Restaurar desde snapshot
        print("\n3️⃣ Restaurando desde snapshot...")
        restore_result = self.snapshot_manager.restore_snapshot(snapshot_id)

        if not restore_result.get("success"):
            print(f"   ❌ Error en restauración: {restore_result.get('error')}")
            return {
                "success": False,
                "error": restore_result.get("error"),
                "step": "restore"
            }

        print(f"   ✅ Snapshot restaurado")

        # 4. Post-restauración: Validar integridad
        print("\n4️⃣ Validando integridad post-restauración...")
        try:
            collection_info = self.client.get_collection("voia_vectors")
            print(f"   ✅ Colección accesible: {collection_info.points_count} puntos")
        except Exception as e:
            print(f"   ❌ Error validando colección: {e}")
            return {
                "success": False,
                "error": str(e),
                "step": "post_validation"
            }

        # 5. Sincronizar con MySQL si aplica
        print("\n5️⃣ Sincronizando con MySQL...")
        sync_results = []

        try:
            conn = get_connection()
            cursor = conn.cursor(dictionary=True)

            # Obtener bots a sincronizar
            if bot_id:
                bots = [{"id": bot_id}]
            else:
                cursor.execute("SELECT id FROM bots LIMIT 100")
                bots = cursor.fetchall()

            for bot in bots:
                try:
                    sync = QdrantMySQLSynchronizer(bot["id"])
                    sync_result = sync.reconcile(auto_fix=False)
                    sync_results.append(sync_result)
                    sync.close()

                    if sync_result.get("hash_mismatches", 0) > 0:
                        print(f"   ⚠️ Bot {bot['id']}: {sync_result['hash_mismatches']} hash mismatches")
                    else:
                        print(f"   ✅ Bot {bot['id']}: Sincronizado")
                except Exception as e:
                    print(f"   ⚠️ Error sincronizando bot {bot['id']}: {e}")

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"   ⚠️ Error en sincronización: {e}")

        # 6. Registrar evento
        recovery_event = {
            "snapshot_id": snapshot_id,
            "bot_id": bot_id,
            "pre_restore_stats": pre_restore_stats,
            "sync_results": sync_results,
            "status": "completed"
        }
        self._log_recovery_event("recovery", recovery_event)

        print(f"\n✅ RESTAURACIÓN COMPLETADA EXITOSAMENTE")

        return {
            "success": True,
            "snapshot_id": snapshot_id,
            "bot_id": bot_id,
            "points_count": collection_info.points_count,
            "sync_results": sync_results,
            "message": "Restauración completada. Validar resultados manualmente."
        }

    def rollback_to_previous_state(self, hours_back: int = 1) -> Dict:
        """
        Rollback a estado anterior (últimas N horas).
        
        Args:
            hours_back: Cuántas horas atrás hacer rollback (default: 1)
            
        Returns:
            Dict con resultado del rollback
        """
        print(f"\n⏮️ INICIANDO ROLLBACK ({hours_back} horas atrás)...")

        snapshots = self.snapshot_manager.list_snapshots()

        if not snapshots:
            return {
                "success": False,
                "error": "No snapshots available for rollback"
            }

        target_time = datetime.now() - timedelta(hours=hours_back)
        target_snapshot = None

        # Encontrar snapshot más cercano a target_time
        for snapshot in snapshots:
            snapshot_time = datetime.fromisoformat(snapshot["created"])
            if snapshot_time <= target_time:
                target_snapshot = snapshot
                break

        if not target_snapshot:
            return {
                "success": False,
                "error": f"No snapshot found from {hours_back} hours ago"
            }

        print(f"   Restaurando: {target_snapshot['snapshot_id']}")
        print(f"   Edad: {(datetime.now() - datetime.fromisoformat(target_snapshot['created'])).total_seconds() / 3600:.1f} horas")

        result = self.restore_from_snapshot(target_snapshot["snapshot_id"])

        if result.get("success"):
            self._log_recovery_event("rollback", {
                "hours_back": hours_back,
                "snapshot_id": target_snapshot["snapshot_id"],
                "result": result
            })

        return result

    def disaster_recovery_procedure(self) -> Dict:
        """
        Procedimiento completo de recuperación ante desastres.
        
        Incluye:
        1. Evaluación de capacidad
        2. Crear backup inmediato
        3. Validar último snapshot
        4. Restaurar desde snapshot más reciente
        5. Ejecutar sincronización completa
        6. Generar reporte
        
        Returns:
            Dict con reporte de DRP
        """
        print("\n🚨 INICIANDO PROCEDIMIENTO DE RECUPERACIÓN ANTE DESASTRES (DRP)")
        print("=" * 60)

        drp_start = datetime.now()

        # 1. Evaluación
        print("\nPaso 1: Evaluando capacidad de recuperación...")
        capability = self.assess_recovery_capability()

        if not capability.get("capable_of_recovery"):
            return {
                "success": False,
                "status": "DRP FAILED",
                "error": "Sistema incapaz de recuperación",
                "capability_assessment": capability
            }

        # 2. Backup inmediato
        print("\nPaso 2: Creando backup inmediato...")
        backup = self.create_recovery_point("DRP - Auto backup")

        # 3. Validar snapshot
        print("\nPaso 3: Validando snapshot...")
        validation = self.snapshot_manager.validate_snapshot(
            capability["latest_snapshot"]
        )

        # 4. Restaurar
        print("\nPaso 4: Restaurando desde snapshot...")
        restore = self.restore_from_snapshot(capability["latest_snapshot"])

        if not restore.get("success"):
            return {
                "success": False,
                "status": "DRP FAILED",
                "error": f"Restauración fallida: {restore.get('error')}",
                "steps_completed": ["assessment", "backup", "validation"]
            }

        # 5. Reporte
        drp_duration = (datetime.now() - drp_start).total_seconds()

        print("\n" + "=" * 60)
        print("✅ PROCEDIMIENTO DRP COMPLETADO")
        print("=" * 60)

        return {
            "success": True,
            "status": "DRP SUCCESS",
            "duration_seconds": drp_duration,
            "steps": {
                "capability_assessment": capability,
                "backup": backup,
                "validation": validation,
                "restore": restore
            },
            "recommendations": [
                "✓ Validar manualmente que los datos se restauraron correctamente",
                "✓ Ejecutar tests de integridad de datos",
                "✓ Notificar al equipo de operaciones",
                "✓ Documentar causa raíz del incidente",
                "✓ Revisar y mejorar procedimientos si es necesario"
            ]
        }

    def get_recovery_status(self) -> Dict:
        """Obtiene estado actual de recuperación del sistema."""
        print("\n📋 ESTADO DE RECUPERACIÓN DEL SISTEMA")
        print("=" * 60)

        capability = self.assess_recovery_capability()
        stats = self.snapshot_manager.get_statistics()

        # Calcular SLA compliance
        snapshots = self.snapshot_manager.list_snapshots()
        snapshot_frequency = None

        if len(snapshots) > 1:
            # Calcular frecuencia promedio
            times = [datetime.fromisoformat(s["created"]) for s in snapshots[:10]]
            time_diffs = [(times[i] - times[i+1]).total_seconds() / 3600 for i in range(len(times)-1)]
            snapshot_frequency = sum(time_diffs) / len(time_diffs) if time_diffs else None

        status = {
            "timestamp": datetime.now().isoformat(),
            "rpo_hours": self.rpo_hours,
            "rto_minutes": self.rto_minutes,
            "capability": capability,
            "snapshot_stats": stats,
            "snapshot_frequency_hours": snapshot_frequency,
            "sla_status": {
                "rpo_compliant": capability.get("rpo_compliant"),
                "rto_compliant": capability.get("rto_compliant"),
                "overall_compliant": (
                    capability.get("rpo_compliant") and
                    capability.get("rto_compliant")
                )
            }
        }

        print(f"RPO Compliant: {'✅' if status['sla_status']['rpo_compliant'] else '❌'}")
        print(f"RTO Compliant: {'✅' if status['sla_status']['rto_compliant'] else '❌'}")
        print(f"Total Snapshots: {stats['total_snapshots']}")
        print(f"Total Size: {stats['total_size_mb']:.1f} MB")

        if snapshot_frequency:
            print(f"Snapshot Frequency: {snapshot_frequency:.1f} hours")

        print("=" * 60)

        return status
