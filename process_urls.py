import uuid
import hashlib
from voia_vector_services.db import get_connection
from voia_vector_services.vector_store import get_or_create_vector_store
from voia_vector_services.embedder import get_embedding
from voia_vector_services.services.document_processor import process_url
from voia_vector_services.tag_utils import infer_tags_from_payload

def process_pending_urls(bot_id: int):
    print(f"🚀 Iniciando procesamiento de URLs pendientes para el bot {bot_id}...")

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT id, url, bot_id, bot_template_id, user_id 
            FROM training_urls 
            WHERE indexed = 0 AND status = 'pending' AND bot_id = %s
        """, (bot_id,))
        urls = cursor.fetchall()

        if not urls:
            print(f"ℹ️ No hay URLs pendientes por procesar para el bot {bot_id}.")
            return

        client = get_or_create_vector_store()

        for url_item in urls:
            url_id = url_item['id']
            url = url_item.get('url', '').strip()

            print(f"\n🌐 Procesando URL ID {url_id}: {url} para el bot {bot_id}")

            if not url:
                print("⚠️ URL vacía o nula. Marcando como fallido.")
                cursor.execute("UPDATE training_urls SET indexed = -1, status = 'failed' WHERE id = %s", (url_id,))
                conn.commit()
                continue

            try:
                result = process_url(url)
                content = result.get("content", "").strip()

                if not content:
                    print("⚠️ No se extrajo contenido de la URL.")
                    cursor.execute("UPDATE training_urls SET indexed = -1, status = 'failed' WHERE id = %s", (url_id,))
                    conn.commit()
                    continue

                print(f"✅ Contenido extraído ({result['type']}): {content[:300]} ...")

                content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

                cursor.execute("""
                    SELECT COUNT(*) as count FROM training_urls
                    WHERE content_hash = %s AND indexed = 1 AND bot_id = %s
                """, (content_hash, bot_id))
                if cursor.fetchone()['count'] > 0:
                    print("⏩ URL con contenido idéntico ya fue indexada para este bot. Se omite.")
                cursor.execute("UPDATE training_urls SET indexed = 2, status = 'processed' WHERE id = %s", (url_id,))
                conn.commit()
                continue

                qdrant_id = str(uuid.uuid4())

                payload = {
                    "url": url,
                    "type": result["type"],
                    "user_id": url_item.get('user_id'),
                    "bot_id": url_item.get('bot_id'),
                    "bot_template_id": url_item.get('bot_template_id'),
                }

                tags = infer_tags_from_payload(payload, content)
                payload.update(tags)

                print(f"🏷️ Etiquetas inferidas: {tags}")

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
                    SET indexed = 1, status = 'processed', qdrant_id = %s, content_hash = %s, extracted_text = %s 
                    WHERE id = %s
                """, (qdrant_id, content_hash, content[:10000], url_id))

                conn.commit()
                print("✅ URL procesada y almacenada en Qdrant.")

            except Exception as e:
                print(f"❌ Error procesando la URL {url}: {e}")
                
                # Determinar si es un error HTTP 404
                error_msg = str(e)
                if "404" in error_msg:
                    print(f"⚠️ La URL no existe o no es accesible: {url}")
                    cursor.execute("""
                        UPDATE training_urls 
                        SET indexed = -1, 
                            status = 'failed',
                            extracted_text = 'URL no encontrada o no accesible (404)'
                        WHERE id = %s
                    """, (url_id,))
                    conn.commit()
                    # Para errores 404, no propagamos la excepción
                    continue
                
                # Para otros errores, marcamos como fallido y propagamos la excepción
                cursor.execute("UPDATE training_urls SET indexed = -1, status = 'failed' WHERE id = %s", (url_id,))
                conn.commit()
                raise Exception(f"Fallo al procesar la URL {url} (ID: {url_id}): {e}")

    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
        print(f"\n🔚 Procesamiento de URLs para el bot {bot_id} finalizado.")
