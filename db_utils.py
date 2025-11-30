import mysql.connector
from dotenv import load_dotenv
import os
import time
import random
from sentence_transformers import SentenceTransformer

load_dotenv()

def get_connection():
    print("DB_HOST:", os.getenv("DB_HOST"))
    print("DB_PORT:", os.getenv("DB_PORT"))
    print("DB_USER:", os.getenv("DB_USER"))
    print("DB_PASSWORD:", os.getenv("DB_PASSWORD"))
    print("DB_NAME:", os.getenv("DB_NAME"))
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
    )

# Carga del modelo de embeddings
model = SentenceTransformer("all-MiniLM-L6-v2")

def get_embedding(text, max_length=8000, max_retries=3):
    """
    Genera embedding con reintentos automáticos y manejo de textos largos.
    
    Args:
        text: Texto a vectorizar
        max_length: Máximo de caracteres (si excede, trunca)
        max_retries: Número de reintentos en caso de error
    
    Returns:
        list: Vector de embedding (384 dimensiones)
    
    Raises:
        Exception: Si falla después de max_retries intentos
    """
    # ✅ Limitar longitud si es muy largo
    if len(text) > max_length:
        print(f"⚠️ Texto muy largo ({len(text)} chars), truncando a {max_length}")
        text = text[:max_length]
    
    # ✅ Reintentos exponenciales
    for attempt in range(max_retries):
        try:
            embedding = model.encode(text, convert_to_numpy=True)
            return embedding.tolist()
        
        except RuntimeError as e:
            if "out of memory" in str(e).lower():
                print(f"❌ Intento {attempt + 1}/{max_retries}: Memoria insuficiente")
                
                # Liberar caché de GPU/CPU
                try:
                    import torch
                    torch.cuda.empty_cache()
                    print("🧹 Caché limpiado")
                except:
                    pass
                
                # Si es el último intento, fallar
                if attempt == max_retries - 1:
                    raise Exception(f"Fallo de memoria después de {max_retries} reintentos")
                
                # Esperar antes de reintentar (backoff exponencial)
                wait_time = 2 ** attempt
                print(f"⏳ Esperando {wait_time}s antes de reintento...")
                time.sleep(wait_time)
            else:
                raise e
        
        except Exception as e:
            print(f"❌ Intento {attempt + 1}/{max_retries}: Error - {str(e)[:100]}")
            
            if attempt == max_retries - 1:
                raise Exception(f"Fallo generando embedding después de {max_retries} intentos: {str(e)[:200]}")
            
            # Esperar antes de reintentar
            wait_time = 2 ** attempt
            print(f"⏳ Esperando {wait_time}s antes de reintento...")
            time.sleep(wait_time)


def get_embedding_with_fallback(text, max_length=8000):
    """
    Genera embedding con fallback a embedding aleatorio si falla.
    Útil para asegurar que el procesamiento continúe incluso si SentenceTransformer falla.
    """
    try:
        return get_embedding(text, max_length=max_length, max_retries=3)
    except Exception as e:
        print(f"⚠️ No se pudo generar embedding, usando fallback aleatorio: {str(e)[:100]}")
        # Generar embedding aleatorio como fallback (384 dimensiones)
        return [random.random() for _ in range(384)]


def batch_get_embeddings(texts, max_length=8000, batch_size=10):
    """
    Genera embeddings en lotes para mejor rendimiento.
    
    Args:
        texts: Lista de textos
        max_length: Máximo de caracteres por texto
        batch_size: Cantidad de textos a procesar juntos
    
    Returns:
        list: Lista de embeddings
    """
    embeddings = []
    
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        print(f"📦 Procesando lote {i // batch_size + 1} ({len(batch)} textos)...")
        
        for text in batch:
            try:
                embedding = get_embedding(text, max_length=max_length, max_retries=2)
                embeddings.append(embedding)
            except Exception as e:
                embeddings.append([random.random() for _ in range(384)])
    
    return embeddings
