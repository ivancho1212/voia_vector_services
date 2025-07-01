import os
import uuid
import hashlib
from PyPDF2 import PdfReader
from db import get_connection
from vector_store import get_or_create_vector_store
from embedder import get_embedding
from tag_utils import infer_tags_from_payload

client = get_or_create_vector_store()

def extract_text_from_pdf(path):
    try:
        reader = PdfReader(path)

        # Verificar si el PDF está cifrado
        if reader.is_encrypted:
            try:
                reader.decrypt("")  # Intenta desbloquear con contraseña vacía
            except Exception:
                raise Exception("Archivo PDF protegido con contraseña, no se puede procesar.")

            if reader.is_encrypted:
                raise Exception("Archivo PDF sigue cifrado, no se puede leer.")

        text = "\n".join(page.extract_text() or '' for page in reader.pages)

        return text.strip()

    except Exception as e:
        print(f"❌ Error leyendo PDF {path}: {e}")
        raise


def index_document(qdrant_id, text, metadata):
    try:
        vector = get_embedding(text)
        client.upsert(
            collection_name="voia_vectors",
            points=[{
                "id": qdrant_id,
                "vector": vector,
                "payload": metadata
            }]
        )
        print(f"✅ Documento indexado en Qdrant con ID: {qdrant_id}")
    except Exception as e:
        print(f"❌ Error al indexar en Qdrant: {e}")
        raise


def handle_invalid_pdf(path, doc_id, user_id, cursor, conn):
    print(f"🗑️ Eliminando archivo inválido: {path}")
    try:
        if os.path.exists(path):
            os.remove(path)
            print("✅ Archivo eliminado.")
        else:
            print("⚠️ Archivo ya no existe.")
    except Exception as e:
        print(f"❌ Error al eliminar archivo: {e}")

    cursor.execute("UPDATE uploaded_documents SET indexed = -1 WHERE id = %s", (doc_id,))
    conn.commit()
    print(f"📢 (Simulado) Notificar a usuario {user_id} que su archivo es inválido.")


def process_pending_documents():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT id, file_path, bot_id, bot_template_id, user_id, file_name 
        FROM uploaded_documents 
        WHERE indexed = 0
    """)
    documents = cursor.fetchall()

    for doc in documents:
        relative_path = doc['file_path'].replace("\\", "/")
        abs_path = os.path.normpath(os.path.join(os.getenv("UPLOAD_DIR"), os.path.basename(relative_path)))

        print(f"📄 Procesando: {abs_path}")

        try:
            content = extract_text_from_pdf(abs_path)

            if content.strip():
                print(f"✅ Texto extraído: {content[:300]} ...")

                content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

                cursor.execute("""
                    SELECT COUNT(*) as count FROM uploaded_documents
                    WHERE content_hash = %s AND indexed = 1
                """, (content_hash,))
                if cursor.fetchone()['count'] > 0:
                    print("⏩ Documento con contenido idéntico ya fue indexado. Se omite.")
                    continue

                qdrant_id = str(uuid.uuid4())

                payload = {
                    "file_name": doc['file_name'],
                    "user_id": doc['user_id'],
                    "bot_id": doc['bot_id'],
                    "bot_template_id": doc['bot_template_id'],
                }

                tags = infer_tags_from_payload(payload, content)
                payload.update(tags)

                index_document(qdrant_id, content, payload)


                cursor.execute("""
                    UPDATE uploaded_documents 
                    SET indexed = 1, qdrant_id = %s, content_hash = %s, extracted_text = %s
                    WHERE id = %s
                """, (qdrant_id, content_hash, content[:10000], doc['id']))

                conn.commit()

            else:
                print("⚠️ No se pudo extraer texto del archivo (vacío o ilegible).")
                handle_invalid_pdf(abs_path, doc['id'], doc['user_id'], cursor, conn)

        except Exception as e:
            print(f"❌ Error al procesar el archivo: {e}")
            handle_invalid_pdf(abs_path, doc['id'], doc['user_id'], cursor, conn)

    cursor.close()
    conn.close()
