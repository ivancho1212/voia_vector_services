#!/usr/bin/env python3
"""
🧹 CLEANUP SCRIPT: Limpia y reconstruye la colección Qdrant
Elimina puntos corruptos que causan errores de lectura.
"""

from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance

def cleanup_qdrant():
    """
    Limpia la colección Qdrant eliminando todos los puntos corruptos.
    Opción 1: Eliminar todos los puntos (más seguro)
    Opción 2: Recrear la colección completamente
    """
    client = QdrantClient(host="localhost", port=6333)
    collection_name = "voia_vectors"
    
    print("🧹 Iniciando limpieza de Qdrant...")
    
    try:
        # Verificar si la colección existe
        if not client.collection_exists(collection_name):
            print(f"❌ Colección '{collection_name}' no existe.")
            return False
        
        # Obtener información de la colección
        collection_info = client.get_collection(collection_name)
        points_count = collection_info.points_count
        print(f"📊 Colección tiene {points_count} puntos")
        
        if points_count == 0:
            print("✅ Colección ya está vacía. Nada que limpiar.")
            return True
        
        # OPCIÓN 1: Eliminar todos los puntos
        print(f"\n🗑️  Eliminando todos los {points_count} puntos...")
        
        # Usar delete_payload_indexes para limpiar, o scroll + delete
        offset = 0
        batch_size = 100
        deleted_total = 0
        
        while True:
            try:
                # Scroll para obtener puntos
                points, next_offset = client.scroll(
                    collection_name=collection_name,
                    limit=batch_size,
                    offset=offset
                )
                
                if not points:
                    break
                
                # Extraer IDs y eliminar
                point_ids = [p.id for p in points]
                client.delete(
                    collection_name=collection_name,
                    points_selector={
                        "ids": point_ids
                    }
                )
                
                deleted_total += len(point_ids)
                print(f"   Eliminados {deleted_total}/{points_count} puntos...")
                
                offset = next_offset if next_offset else offset + batch_size
                
                if not next_offset or len(points) < batch_size:
                    break
                    
            except Exception as batch_error:
                print(f"   ⚠️  Error en lote: {str(batch_error)[:100]}")
                break
        
        # Verificar que quedó vacía
        collection_info = client.get_collection(collection_name)
        final_count = collection_info.points_count
        
        if final_count == 0:
            print(f"✅ Colección limpiada exitosamente. Puntos eliminados: {deleted_total}")
            print("   Puedes procesar documentos nuevamente ahora.")
            return True
        else:
            print(f"⚠️  Colección aún tiene {final_count} puntos. Intenta nuevamente.")
            return False
            
    except Exception as e:
        print(f"❌ Error durante limpieza: {str(e)[:200]}")
        return False

def recreate_collection():
    """
    Opción más drástica: Elimina y recrea la colección desde cero.
    """
    client = QdrantClient(host="localhost", port=6333)
    collection_name = "voia_vectors"
    
    print("🔄 Recreando colección desde cero...")
    
    try:
        # Eliminar colección si existe
        if client.collection_exists(collection_name):
            print(f"   Eliminando colección '{collection_name}'...")
            client.delete_collection(collection_name)
            print("   ✅ Colección eliminada")
        
        # Recrear colección
        print(f"   Creando nueva colección '{collection_name}'...")
        client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=384, distance=Distance.COSINE)
        )
        print("   ✅ Colección recreada exitosamente")
        
        return True
        
    except Exception as e:
        print(f"❌ Error recreando colección: {str(e)[:200]}")
        return False

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--recreate":
        print("=" * 60)
        print("MODO: Recrear colección completamente")
        print("=" * 60)
        success = recreate_collection()
    else:
        print("=" * 60)
        print("MODO: Limpiar puntos corruptos")
        print("=" * 60)
        success = cleanup_qdrant()
    
    if success:
        print("\n✅ Limpieza completada. Reinicia el servidor de vectores.")
    else:
        print("\n❌ Limpieza falló. Intenta con --recreate para recrear completamente.")
    
    sys.exit(0 if success else 1)
