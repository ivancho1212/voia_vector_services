import os
import uuid
import hashlib
from datetime import datetime
from PyPDF2 import PdfReader
from pdf2image import convert_from_path
import pytesseract
from .db_utils import get_connection, get_embedding
from .vector_store import get_or_create_vector_store
from .tag_inference import infer_tags_from_payload
from .text_chunking import split_into_chunks  # ✅ NUEVO


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

def process_pending_documents(bot_id: int):
    """
    ✅ SOLUTION #3 + #4: Procesamiento de documentos con error handling robusto.
    
    Características:
    - Try-catch INDIVIDUAL por documento
    - Actualiza indexed = -1 en error (evita ciclos infinitos)
    - Continúa batch incluso si un documento falla
    - Metadata COMPLETO en cada chunk (doc_id, source, timestamps, etc)
    """
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    
    processed_count = 0
    failed_count = 0

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
            # ✅ SOLUTION #3: TRY-CATCH INDIVIDUAL POR DOCUMENTO
            try:
                # Corregir escape en rutas
                net_root = os.getenv('DOTNET_ROOT_PATH', 'C:/Users/Ivan Herrera/Documents/VIA/Api')
                relative_path = doc['file_path'].replace('\\', '/')
                abs_path = os.path.normpath(os.path.join(net_root, relative_path))

                print(f"\n📄 Procesando documento {doc['id']}: {abs_path}")

                if not os.path.exists(abs_path):
                    raise Exception(f"Archivo no encontrado: {abs_path}")

                content = extract_text_from_pdf(abs_path)

                if not content.strip():
                    print("⚠️ Texto vacío, intentando OCR...")
                    content = extract_text_from_pdf_with_ocr(abs_path)

                if not content.strip():
                    raise Exception("No se pudo extraer texto (vacío/ilegible)")

                print(f"✅ Texto extraído: {len(content)} caracteres")
                content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

                # Verificar duplicados
                cursor.execute("""
                    SELECT COUNT(*) as count FROM uploaded_documents
                    WHERE content_hash = %s AND indexed = 1 AND bot_id = %s
                """, (content_hash, bot_id))
                if cursor.fetchone()['count'] > 0:
                    print("⏩ Contenido duplicado, marcando como procesado (indexed=2)")
                    cursor.execute("UPDATE uploaded_documents SET indexed = 2 WHERE id = %s", (doc['id'],))
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
                        "doc_id": doc['id'],
                        "bot_id": doc['bot_id'],
                        "user_id": doc['user_id'],
                        "bot_template_id": doc['bot_template_id'],
                        
                        # Fuente y tipo
                        "source": "document",
                        "source_type": "pdf",
                        "file_name": doc['file_name'],
                        
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
                    index_document(chunk_qdrant_id, chunk, payload)
                    indexed_chunk_ids.append(chunk_qdrant_id)

                # Actualizar documento como indexado
                cursor.execute("""
                    UPDATE uploaded_documents 
                    SET indexed = 1, qdrant_id = %s, content_hash = %s, 
                        extracted_text = %s, chunks_count = %s
                    WHERE id = %s
                """, (indexed_chunk_ids[0], content_hash, content[:10000], len(chunks), doc['id']))
                conn.commit()
                
                print(f"   ✅ {len(chunks)} chunks indexados")
                processed_count += 1

            except Exception as e:
                # ✅ SOLUTION #3: ERROR HANDLING - Marcar documento como fallido (indexed = -1)
                error_msg = str(e)[:500]
                print(f"❌ Error en documento {doc['id']}: {error_msg}")
                failed_count += 1
                
                try:
                    cursor.execute("""
                        UPDATE uploaded_documents 
                        SET indexed = -1, error_message = %s
                        WHERE id = %s
                    """, (error_msg, doc['id']))
                    conn.commit()
                    print(f"   📌 Marcado como FALLIDO (indexed=-1), continuando batch...")
                except Exception as db_error:
                    print(f"   ⚠️ Error actualizando DB: {db_error}")
                    conn.rollback()
                
                # ✅ CONTINUAR CON SIGUIENTE DOCUMENTO (NO FALLAR BATCH)
                continue

    finally:
        cursor.close()
        conn.close()
        print(f"\n🔚 Procesamiento completado: {processed_count} exitosos, {failed_count} fallos")
