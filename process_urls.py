import uuid
import hashlib
from voia_vector_services.db import get_connection
from voia_vector_services.vector_store import get_or_create_vector_store
from voia_vector_services.embedder import get_embedding
from voia_vector_services.services.document_processor import process_url
from voia_vector_services.tag_utils import infer_tags_from_payload

def process_pending_urls():
    print("🚀 Iniciando procesamiento de URLs pendientes...")

    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
    except Exception as e:
        print(f"❌ Error conectando a la base de datos: {e}")
        return

    try:
        cursor.execute("""
            SELECT id, url, bot_id, bot_template_id, user_id 
            FROM training_urls 
            WHERE indexed = 0 AND status = 'pending'
        """)
        urls = cursor.fetchall()

        if not urls:
            print("ℹ️ No hay URLs pendientes por procesar.")
            return

        client = get_or_create_vector_store()

        for url_item in urls:
            url_id = url_item['id']
            url = url_item.get('url', '').strip()

            print(f"\n🌐 Procesando URL ID {url_id}: {url}")

            if not url:
                print("⚠️ URL vacía o nula. Marcando como error.")
                cursor.execute("""
                    UPDATE training_urls 
                    SET indexed = -1, status = 'error' 
                    WHERE id = %s
                """, (url_id,))
                conn.commit()
                continue

            try:
                result = process_url(url)
                content = result.get("content", "").strip()

                if not content:
                    print("⚠️ No se extrajo contenido de la URL.")
                    cursor.execute("""
                        UPDATE training_urls 
                        SET indexed = -1, status = 'error' 
                        WHERE id = %s
                    """, (url_id,))
                    conn.commit()
                    continue

                print(f"✅ Contenido extraído ({result['type']}): {content[:300]} ...")

                content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

                cursor.execute("""
                    SELECT COUNT(*) as count FROM training_urls
                    WHERE content_hash = %s AND indexed = 1
                """, (content_hash,))
                if cursor.fetchone()['count'] > 0:
                    print("⏩ URL con contenido idéntico ya fue indexada. Se omite.")
                    cursor.execute("""
                        UPDATE training_urls 
                        SET indexed = 1, status = 'completed' 
                        WHERE id = %s
                    """, (url_id,))
                    conn.commit()
                    continue

                qdrant_id = str(uuid.uuid4())

                # 🎯 Construir payload base
                payload = {
                    "url": url,
                    "type": result["type"],
                    "user_id": url_item.get('user_id'),
                    "bot_id": url_item.get('bot_id'),
                    "bot_template_id": url_item.get('bot_template_id'),
                }

                # 🏷️ Aplicar etiquetas
                tags = infer_tags_from_payload(payload, content)
                payload.update(tags)

                print(f"🏷️ Etiquetas inferidas: {tags}")  # Opcional para debug

                client.upsert(
                    collection_name="voia_vectors",
                    points=[{
                        "id": qdrant_id,
                        "vector": get_embedding(content),
                        "payload": payload
                    }]
                )

                cursor.execute("""
                    UPDATE training_urls 
                    SET indexed = 1, status = 'completed', qdrant_id = %s, content_hash = %s, extracted_text = %s 
                    WHERE id = %s
                """, (qdrant_id, content_hash, content[:10000], url_id))

                conn.commit()
                print("✅ URL procesada y almacenada en Qdrant.")

            except Exception as e:
                print(f"❌ Error procesando la URL {url}: {e}")
                cursor.execute("""
                    UPDATE training_urls 
                    SET indexed = -1, status = 'error' 
                    WHERE id = %s
                """, (url_id,))
                conn.commit()

    except Exception as e:
        print(f"❌ Error general en el procesamiento: {e}")
    finally:
        cursor.close()
        conn.close()
        print("\n🔚 Procesamiento de URLs finalizado.")
