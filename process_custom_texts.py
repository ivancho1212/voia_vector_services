import uuid
import hashlib
from voia_vector_services.db import get_connection
from voia_vector_services.vector_store import get_or_create_vector_store
from voia_vector_services.embedder import get_embedding
from voia_vector_services.tag_utils import infer_tags_from_payload

client = get_or_create_vector_store()

def process_pending_custom_texts(bot_id: int):
    print(f"🚀 Iniciando procesamiento de textos planos para el bot {bot_id}...")

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT id, content, bot_id, bot_template_id, user_id
            FROM training_custom_texts
            WHERE indexed = 0 AND bot_id = %s
        """, (bot_id,))
        texts = cursor.fetchall()

        if not texts:
            print(f"ℹ️ No hay textos planos pendientes por procesar para el bot {bot_id}.")
            return

        for item in texts:
            try:
                content = item['content'].strip()

                if not content:
                    print(f"⚠️ Texto vacío. ID {item['id']}")
                    cursor.execute("UPDATE training_custom_texts SET indexed = -1 WHERE id = %s", (item['id'],))
                    conn.commit()
                    continue

                content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

                cursor.execute("""
                    SELECT COUNT(*) as count FROM training_custom_texts
                    WHERE content_hash = %s AND indexed = 1 AND bot_id = %s
                """, (content_hash, bot_id))
                if cursor.fetchone()['count'] > 0:
                    print(f"⏩ Texto con contenido idéntico ya fue indexado para este bot. Se omite. ID {item['id']}")
                    cursor.execute("UPDATE training_custom_texts SET indexed = 2 WHERE id = %s", (item['id'],))
                    conn.commit()
                    continue

                qdrant_id = str(uuid.uuid4())

                payload = {
                    "type": "custom_text",
                    "user_id": item.get('user_id'),
                    "bot_id": item.get('bot_id'),
                    "bot_template_id": item.get('bot_template_id'),
                    "source": "training_custom_texts",
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
                    UPDATE training_custom_texts
                    SET indexed = 1, qdrant_id = %s, content_hash = %s
                    WHERE id = %s
                """, (qdrant_id, content_hash, item['id']))

                conn.commit()
                print(f"✅ Texto plano ID {item['id']} procesado y vectorizado.")

            except Exception as e:
                print(f"❌ Error procesando texto ID {item['id']}: {e}")
                cursor.execute("UPDATE training_custom_texts SET indexed = -1 WHERE id = %s", (item['id'],))
                conn.commit()
                raise Exception(f"Fallo al procesar el texto (ID: {item['id']}): {e}")

    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
        print(f"\n🔚 Procesamiento de textos planos para el bot {bot_id} finalizado.")
