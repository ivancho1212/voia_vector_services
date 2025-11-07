from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from .process_custom_texts import process_pending_custom_texts

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


# ✅ Procesar documentos PDF
@app.post("/process-documents/")
def process_documents_endpoint():
    try:
        print("🚀 Iniciando procesamiento de documentos PDF...")
        process_pending_documents()
        return {"status": "ok", "message": "✅ Documentos procesados exitosamente."}
    except Exception as e:
        print(f"❌ Error en /process-documents: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ✅ Procesar URLs (sitios web)
@app.post("/process-urls/")
def process_urls_endpoint():
    try:
        print("🚀 Iniciando procesamiento de URLs...")
        process_pending_urls()
        return {"status": "ok", "message": "✅ URLs procesadas exitosamente."}
    except Exception as e:
        print(f"❌ Error en /process-urls: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ✅ Procesar textos planos
@app.post("/process-custom-texts/")
def process_custom_texts_endpoint():
    try:
        print("🚀 Iniciando procesamiento de textos planos...")
        process_pending_custom_texts()
        return {"status": "ok", "message": "✅ Textos planos procesados exitosamente."}
    except Exception as e:
        print(f"❌ Error en /process-custom-texts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ✅ Procesar todo: documentos, URLs y textos planos
@app.post("/process-all/")
def process_all_endpoint():
    try:
        print("🚀 Iniciando procesamiento de documentos, URLs y textos planos...")
        process_pending_documents()
        process_pending_urls()
        process_pending_custom_texts()
        return {"status": "ok", "message": "✅ Documentos, URLs y textos planos procesados exitosamente."}
    except Exception as e:
        print(f"❌ Error en /process-all: {e}")
        raise HTTPException(status_code=500, detail=str(e))
import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

def get_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
    )
from sentence_transformers import SentenceTransformer

# Carga del modelo de embeddings
model = SentenceTransformer("all-MiniLM-L6-v2")

def get_embedding(text):
    # Convierte el texto en vector y lo devuelve como lista
    return model.encode(text).tolist()
# voia_vector_services/main.py
from fastapi import FastAPI, Query, HTTPException  # 👈 Agregar HTTPException
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()
# from process_documents import process_pending_documents # noqa  # Deshabilitado: rompe el flujo por ciclos/imports
# from process_urls import process_pending_urls # noqa
# from process_custom_texts import process_pending_custom_texts # noqa
# from search_vectors import search_vectors # noqa

from fastapi import FastAPI, Query
from pydantic import BaseModel

class SearchRequest(BaseModel):
    query: str
    bot_id: int
    limit: int = 3

# Instancia FastAPI
app = FastAPI()

# Endpoints existentes
@app.get("/process_all")
def process_all():
    print("🚀 Procesando PDFs...")
    process_pending_documents()
    print("🌐 Procesando URLs...")
    process_pending_urls()
    print("📝 Procesando textos planos...")
    process_pending_custom_texts()
    return {"status": "success", "message": "All processing tasks have been initiated."}

@app.get("/process_documents")
def process_documents_endpoint(bot_id: int = Query(..., description="ID del bot para procesar documentos")):
    try:
        print(f"🚀 Procesando PDFs para el bot_id: {bot_id}...")
        process_pending_documents(bot_id)
        return {"status": "success", "message": f"Document processing initiated for bot {bot_id}."}
    except Exception as e:
        print(f"❌ Error en process_documents_endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/process_urls")
def process_urls_endpoint(bot_id: int = Query(..., description="ID del bot para procesar URLs")):
    try:
        print(f"🌐 Procesando URLs para el bot_id: {bot_id}...")
        process_pending_urls(bot_id)
        return {"status": "success", "message": f"URL processing initiated for bot {bot_id}."}
    except Exception as e:
        print(f"❌ Error en process_urls_endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/process_texts")
def process_texts_endpoint(bot_id: int = Query(..., description="ID del bot para procesar textos")):
    try:
        print(f"📝 Procesando textos para el bot_id: {bot_id}...")
        process_pending_custom_texts(bot_id)
        return {"status": "success", "message": f"Custom text processing initiated for bot {bot_id}."}
    except Exception as e:
        print(f"❌ Error en process_texts_endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 🔹 Endpoint para búsqueda de vectores (NUEVO, para el chat dinámico)
@app.post("/search")
def search_vectors_endpoint(request: SearchRequest):
    """
    Busca vectores en Qdrant asociados a un bot dado y un query opcional.
    """
    try:
        results = search_vectors(bot_id=request.bot_id, query=request.query, limit=request.limit)
        return {"results": results}
    except Exception as e:
        print(f"❌ Error en el endpoint /search: {e}")
        # Esto devolverá un error 500 al cliente C# con un mensaje específico
        raise HTTPException(status_code=500, detail=f"Error interno en el servicio de búsqueda de Python: {str(e)}")

# 🔹 Endpoint de búsqueda de vectores (ANTIGUO, restaurado para compatibilidad)
@app.get("/search_vectors")
def search_vectors_get_endpoint(
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
import uuid
import hashlib
# from db import get_connection
# from vector_store import get_or_create_vector_store
# from embedder import get_embedding
# from tag_utils import infer_tags_from_payload

# client = get_or_create_vector_store()

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
import os
import uuid
import hashlib
from PyPDF2 import PdfReader
from pdf2image import convert_from_path
import pytesseract
# from db import get_connection
# from vector_store import get_or_create_vector_store
# from embedder import get_embedding
# from tag_utils import infer_tags_from_payload

# client = get_or_create_vector_store()  # Deshabilitado: función no disponible por import

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

def process_pending_documents(bot_id: int):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT id, file_path, bot_id, bot_template_id, user_id, file_name 
            FROM uploaded_documents 
            WHERE indexed = 0 AND bot_id = %s
        """, (bot_id,))
        documents = cursor.fetchall()

        if not documents:
            print(f"✅ No hay documentos pendientes para el bot {bot_id}.")
            return

        for doc in documents:
            # Corregir escape en rutas
            # Usar la ruta base configurada o inferirla
            net_root = os.getenv('DOTNET_ROOT_PATH', 'C:/Users/Ivan Herrera/Documents/VIA/Api')
            relative_path = doc['file_path'].replace('\\', '/')
            abs_path = os.path.normpath(os.path.join(net_root, relative_path))

            print(f"📄 Procesando: {abs_path} para el bot {bot_id}")
            print(f"🔍 Detalles de la ruta:")
            print(f"  - NET_ROOT: {net_root}")
            print(f"  - Ruta relativa: {relative_path}")
            print(f"  - Ruta absoluta: {abs_path}")
            print(f"  - ¿Archivo existe?: {os.path.exists(abs_path)}")

            try:
                content = extract_text_from_pdf(abs_path)

                if not content.strip():
                    print("⚠️ Texto vacío, intentando OCR...")
                    content = extract_text_from_pdf_with_ocr(abs_path)

                if content.strip():
                    print(f"✅ Texto extraído: {content[:300]} ...")
                    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

                    cursor.execute("""
                        SELECT COUNT(*) as count FROM uploaded_documents
                        WHERE content_hash = %s AND indexed = 1 AND bot_id = %s
                    """, (content_hash, bot_id))
                    if cursor.fetchone()['count'] > 0:
                        print("⏩ Documento con contenido idéntico ya fue indexado para este bot. Se omite.")
                        cursor.execute("UPDATE uploaded_documents SET indexed = 2 WHERE id = %s", (doc['id'],))
                        conn.commit()
                        continue

                    qdrant_id = str(uuid.uuid4())

                    # ✅ METADATA COMPLETO para mejor trazabilidad
                    base_payload = {
                        # Identificadores
                        "doc_id": doc['id'],
                        "bot_id": doc['bot_id'],
                        "user_id": doc['user_id'],
                        "bot_template_id": doc['bot_template_id'],
                        
                        # Fuente y tipo
                        "source": "url",  # "document", "url", "text"
                        "url": doc['file_path'],  # URL guardada
                        "file_name": doc['file_name'],
                        
                        # Contenido
                        "original_text": content[:500],  # Primeros 500 chars para preview
                        "text_length": len(content),
                        "chunk_number": 1,
                        
                        # Metadata
                        "processed_at": __import__('datetime').datetime.now().isoformat(),
                        "content_hash": content_hash,
                    }
                    
                    # Agregar tags inferidos
                    tags = infer_tags_from_payload(base_payload, content)
                    payload = {**base_payload, **tags}

                    index_document(qdrant_id, content, payload)

                    cursor.execute("""
                        UPDATE uploaded_documents 
                        SET indexed = 1, qdrant_id = %s, content_hash = %s, extracted_text = %s
                        WHERE id = %s
                    """, (qdrant_id, content_hash, content[:10000], doc['id']))

                    conn.commit()
                else:
                    print("❌ No se pudo extraer texto del archivo (vacío o ilegible).")
                    handle_invalid_pdf(abs_path, doc['id'], doc['user_id'], cursor, conn)

            except Exception as e:
                # ✅ MANEJO DE ERROR: marcar como fallido (indexed = -1) para evitar ciclo infinito
                print(f"❌ Error al procesar el archivo: {e}")
                try:
                    cursor.execute("""
                        UPDATE uploaded_documents 
                        SET indexed = -1, error_message = %s
                        WHERE id = %s
                    """, (str(e)[:500], doc['id']))
                    conn.commit()
                    print(f"📌 Documento {doc['id']} marcado como FALLIDO (indexed=-1)")
                except Exception as db_error:
                    print(f"❌ Error actualizando DB: {db_error}")
                    conn.rollback()
                # Continuar con siguiente documento en lugar de fallar
                continue

    finally:
        cursor.close()
        conn.close()
import uuid
import hashlib
from datetime import datetime
# from db import get_connection
# from vector_store import get_or_create_vector_store
# from embedder import get_embedding
from .services.document_processor import process_url
from .text_chunking import split_into_chunks
# from tag_utils import infer_tags_from_payload

def process_pending_urls(bot_id: int):
    """
    ✅ SOLUTION #3 + #4: Procesamiento de URLs con error handling robusto y metadata completo.
    
    Características:
    - Try-catch INDIVIDUAL por URL
    - Actualiza indexed = -1 en error
    - Continúa batch incluso si una URL falla
    - Metadata COMPLETO en cada resultado
    """
    print(f"🚀 Iniciando procesamiento de URLs pendientes para el bot {bot_id}...")

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    
    processed_count = 0
    failed_count = 0

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

            print(f"\n🌐 Procesando URL ID {url_id}: {url}")

            # ✅ SOLUTION #3: TRY-CATCH INDIVIDUAL POR URL
            try:
                if not url:
                    raise Exception("URL vacía o nula")

                result = process_url(url)
                content = result.get("content", "").strip()

                if not content:
                    raise Exception("No se extrajo contenido de la URL")

                print(f"✅ Contenido extraído ({result['type']}): {len(content)} caracteres")

                content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

                # Verificar duplicados
                cursor.execute("""
                    SELECT COUNT(*) as count FROM training_urls
                    WHERE content_hash = %s AND indexed = 1 AND bot_id = %s
                """, (content_hash, bot_id))
                if cursor.fetchone()['count'] > 0:
                    print("⏩ Contenido duplicado, marcando como procesado (indexed=2)")
                    cursor.execute("UPDATE training_urls SET indexed = 2, status = 'processed' WHERE id = %s", (url_id,))
                    conn.commit()
                    continue

                # ✅ SOLUTION #4: METADATA COMPLETO - Chunking inteligente
                chunks = split_into_chunks(content, chunk_size=512, overlap=50, sentence_aware=True)
                print(f"   📦 Dividido en {len(chunks)} chunks")

                indexed_chunk_ids = []
                for chunk_idx, chunk in enumerate(chunks, 1):
                    chunk_qdrant_id = str(uuid.uuid4())
                    
                    # ✅ Payload con METADATA COMPLETO
                    payload = {
                        # Identificadores
                        "doc_id": url_id,
                        "bot_id": url_item.get('bot_id'),
                        "user_id": url_item.get('user_id'),
                        "bot_template_id": url_item.get('bot_template_id'),
                        
                        # Fuente y tipo
                        "source": "url",
                        "source_type": result.get("type", "unknown"),
                        "url": url,
                        
                        # Contenido
                        "original_text": chunk[:500],
                        "text_length": len(chunk),
                        "chunk_number": chunk_idx,
                        "total_chunks": len(chunks),
                        
                        # Metadata temporal
                        "processed_at": datetime.now().isoformat(),
                        "content_hash": content_hash,
                        "indexed_status": "indexed",
                        "error_message": None,
                    }
                    
                    # Agregar tags inferidos
                    tags = infer_tags_from_payload(payload, chunk)
                    payload.update(tags)

                    # Indexar chunk
                    client.upsert(
                        collection_name="voia_vectors",
                        points=[{
                            "id": chunk_qdrant_id,
                            "vector": get_embedding(chunk),
                            "payload": payload
                        }]
                    )
                    indexed_chunk_ids.append(chunk_qdrant_id)

                # Actualizar URL como indexada
                cursor.execute("""
                    UPDATE training_urls 
                    SET indexed = 1, status = 'processed', qdrant_id = %s, 
                        content_hash = %s, extracted_text = %s
                    WHERE id = %s
                """, (indexed_chunk_ids[0], content_hash, content[:10000], url_id))
                conn.commit()
                
                print(f"   ✅ {len(chunks)} chunks indexados")
                processed_count += 1

            except Exception as e:
                # ✅ SOLUTION #3: ERROR HANDLING - Marcar URL como fallida (indexed = -1)
                error_msg = str(e)[:500]
                print(f"❌ Error en URL {url_id}: {error_msg}")
                failed_count += 1
                
                try:
                    cursor.execute("""
                        UPDATE training_urls 
                        SET indexed = -1, status = 'failed', error_message = %s
                        WHERE id = %s
                    """, (error_msg, url_id))
                    conn.commit()
                    print(f"   📌 Marcada como FALLIDA (indexed=-1), continuando batch...")
                except Exception as db_error:
                    print(f"   ⚠️ Error actualizando DB: {db_error}")
                    conn.rollback()
                
                # ✅ CONTINUAR CON SIGUIENTE URL (NO FALLAR BATCH)
                continue

    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
        print(f"\n🔚 Procesamiento completado: {processed_count} exitosos, {failed_count} fallos")
# voia_vector_services/search_vectors.py
# from .embedder import get_embedding
# from .vector_store import get_or_create_vector_store

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
from typing import Dict, List

def infer_tags_from_payload(payload: Dict, extracted_text: str = "") -> Dict:
    tags = {}

    file_name = payload.get("file_name", "").lower()
    text = extracted_text.lower()

    def match_keywords(text: str, keywords: List[str]) -> bool:
        return any(re.search(rf"\b{re.escape(kw)}\b", text) for kw in keywords)

    # -----------------------------
    # Tipos de documento
    # -----------------------------
    tipos_documento = {
        "autorización": ["autorizacion", "autorización", "autorisacion", "autorizacón", "autorizaciòn"],
        "contrato": ["contrato", "contarto", "contratp", "contratto"],
        "certificado": ["certificado", "certificdo", "cert", "constancia"],
        "factura": ["factura", "recibo", "cuenta de cobro"],
        "información empresarial": ["informacion", "quienes somos", "perfil empresarial", "presentación", "empresa"]
    }

    for tipo, keywords in tipos_documento.items():
        if match_keywords(file_name, keywords) or match_keywords(text, keywords):
            tags["tipo"] = tipo
            break

    # -----------------------------
    # Temas
    # -----------------------------
    temas = {
        "nómina": ["nómina", "nomina", "descuento por nómina", "liquidación de nómina"],
        "salud": ["salud", "eps", "historia clínica", "centro médico", "procedimiento médico"],
        "financiero": ["préstamo", "cuota", "descuento", "interés", "deuda", "pago", "cartera"],
        "legal": ["demandas", "proceso judicial", "abogado", "juez", "código penal", "sentencia"],
        "educación": ["colegio", "universidad", "certificado de estudio", "boletín", "notas"],
        "laboral": ["trabajo", "empleo", "contratación", "vacaciones", "licencia"],
        "inmobiliario": ["arriendo", "inmueble", "propiedad", "contrato de arrendamiento"],
        "tecnología": ["software", "sistema", "plataforma", "aplicación", "soporte técnico"],
        "vehículos": ["vehículo", "soat", "licencia de conducción", "revisión técnico-mecánica", "matrícula vehicular"],
        "tributario": ["renta", "DIAN", "impuesto", "retención", "declaración"],
    }

    for tema, palabras in temas.items():
        if match_keywords(text, palabras):
            tags["tema"] = tema
            break

    # -----------------------------
    # Sectores económicos
    # -----------------------------
    sectores = {
        "tasación": ["tasación", "avaluo", "avalúo", "valor comercial", "peritaje", "inspección vehicular"],
        "automotriz": ["vehículos", "taller", "automotor", "siniestro", "accidente de tránsito"],
        "salud": ["eps", "clínica", "médico", "psicología", "odontología"],
        "educativo": ["universidad", "colegio", "institución educativa", "certificado académico"],
        "financiero": ["entidad financiera", "banco", "pago", "deuda", "cuenta"],
        "legal": ["tribunal", "juez", "proceso", "firma de abogados", "sentencia"],
        "tecnología": ["startup", "aplicación", "plataforma digital", "software"],
        "logística": ["transporte", "entrega", "mensajería", "camión"],
    }

    for sector, keywords in sectores.items():
        if match_keywords(text, keywords):
            if "sectores" not in tags:
                tags["sectores"] = []
            tags["sectores"].append(sector)

    # -----------------------------
    # Firma electrónica o física
    # -----------------------------
    firma_keywords = [
        "firma", "firmado", "firma electrónica", "firmado electrónicamente", "firma digital"
    ]
    if match_keywords(text, firma_keywords):
        tags["requiere_firma"] = True

    # -----------------------------
    # Especialidades médicas (si aplica)
    # -----------------------------
    especialidades = {
        "psicología": ["psicología", "psicoterapia", "consulta psicológica"],
        "odontología": ["odontología", "dentista", "odontograma"],
        "medicina general": ["consulta médica", "médico general", "valoración médica"],
        "oftalmología": ["oftalmología", "visión", "examen visual", "optometría"],
    }

    for esp, keywords in especialidades.items():
        if match_keywords(text, keywords):
            tags["especialidad"] = esp
            break

    return tags
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct, PointIdsList
# from tag_utils import infer_tags_from_payload  # ✅

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
