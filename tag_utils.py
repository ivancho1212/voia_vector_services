from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Voia Vector Services",
    description="API para procesar documentos, URLs y textos planos, generando embeddings y almacenándolos en Qdrant.",
    version="1.1.0"
)

# ✅ Configuración de CORS (puedes restringir en producción)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Ejemplo: ["http://localhost:3000"] si es frontend local
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ✅ Endpoint raíz: status de la API
@app.get("/")
def read_root():
    return {
        "status": "ok",
        "message": "🚀 API de Voia Vector Services funcionando correctamente.",
        "endpoints": [
            "/process-documents",
            "/process-urls",
            "/process-custom-texts",
            "/process-all"
        ]
    }

    # Aquí continúa el resto del código utilitario y lógica de tags...
import os
from .db_utils import get_connection, get_embedding


# --- Removed duplicate FastAPI app and endpoints. Only one set of endpoints is kept above. ---
import uuid
import hashlib
from .db import get_connection
from .vector_store import get_or_create_vector_store
from .embedder import get_embedding

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

                from .tag_utils import infer_tags_from_payload
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
import os
import uuid
import hashlib
from PyPDF2 import PdfReader
from pdf2image import convert_from_path
import pytesseract
from .db import get_connection
from .vector_store import get_or_create_vector_store
from .embedder import get_embedding

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

def extract_text_from_pdf_with_ocr(path):
    try:
        pages = convert_from_path(path)
        text = ""
        for page in pages:
            text += pytesseract.image_to_string(page)
        return text.strip()
    except Exception as e:
        print(f"❌ Error OCR PDF {path}: {e}")
        return ""

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
    print(f"📢 Notificar a usuario {user_id} que su archivo es inválido.")

import uuid
import hashlib
from db import get_connection
from vector_store import get_or_create_vector_store
from embedder import get_embedding
from services.document_processor import process_url

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

                from .tag_utils import infer_tags_from_payload
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
# voia_vector_services/search_vectors.py
from .embedder import get_embedding
from .vector_store import get_or_create_vector_store

def search_vectors(bot_id: int, query: str = "", limit: int = 5):
    """
    Busca los vectores más relevantes para un bot dado y un query opcional.
    Si query está vacío, trae los top documentos del bot.
    """
    client = get_or_create_vector_store()
    vector = get_embedding(query) if query else None

    if vector:
        results = client.search(
            collection_name="voia_vectors",
            query_vector=vector,
            limit=limit,
            query_filter={
                "must": [{"key": "bot_id", "match": {"value": bot_id}}]
            }
        )
        return [r.payload for r in results]
    else:
        points, _ = client.scroll(collection_name="voia_vectors", limit=limit)
        return [p.payload for p in points if p.payload.get("bot_id") == bot_id]
import re
from .tag_inference import infer_tags_from_payload
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct, PointIdsList


COLLECTION_NAME = "voia_vectors"

client = QdrantClient(host="localhost", port=6333)


def get_or_create_vector_store():
    if not client.collection_exists(COLLECTION_NAME):
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=384, distance=Distance.COSINE)
        )
        print(f"🆕 Colección '{COLLECTION_NAME}' creada.")
    else:
        print(f"✅ Colección '{COLLECTION_NAME}' ya existe.")

    return client


def is_in_qdrant(qdrant_id: str) -> bool:
    try:
        points = client.retrieve(
            collection_name=COLLECTION_NAME,
            ids=[qdrant_id]
        )
        return len(points) > 0
    except Exception:
        return False


def add_point_to_qdrant(qdrant_id: str, vector: list, payload: dict = {}, extracted_text: str = ""):
    if is_in_qdrant(qdrant_id):
        print(f"⏭️ Punto con ID {qdrant_id} ya existe en Qdrant. No se insertará de nuevo.")
        return

    # 🏷️ Agregar etiquetas inferidas al payload
    tags = infer_tags_from_payload(payload, extracted_text)
    payload.update(tags)

    client.upsert(
        collection_name=COLLECTION_NAME,
        points=[
            PointStruct(
                id=qdrant_id,
                vector=vector,
                payload=payload
            )
        ]
    )
    print(f"✅ Punto {qdrant_id} insertado en Qdrant con etiquetas: {tags}")


def delete_point_from_qdrant(qdrant_id: str):
    try:
        client.delete(
            collection_name=COLLECTION_NAME,
            points_selector=PointIdsList([qdrant_id])
        )
        print(f"🗑️ Punto {qdrant_id} eliminado de Qdrant.")
    except Exception as e:
        print(f"❌ Error al eliminar punto {qdrant_id}: {e}")


def list_all_points(limit=10):
    points, next_page = client.scroll(
        collection_name=COLLECTION_NAME,
        limit=limit
    )
    return points
