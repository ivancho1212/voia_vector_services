"""
Chunking inteligente para documentos.
Divide textos largos en fragmentos óptimos para embedding y búsqueda.
"""

import re
from typing import List, Tuple


def split_into_chunks(
    text: str,
    chunk_size: int = 512,
    overlap: int = 50,
    sentence_aware: bool = True
) -> List[str]:
    """
    Divide texto en chunks inteligentes.
    
    Args:
        text: Texto a dividir
        chunk_size: Tamaño objetivo en caracteres
        overlap: Superposición entre chunks (para contexto)
        sentence_aware: Si True, respeta límites de oraciones
    
    Returns:
        Lista de chunks
    """
    if len(text) <= chunk_size:
        return [text.strip()]
    
    chunks = []
    
    if sentence_aware:
        # ✅ Opción 1: Dividir por oraciones
        sentences = re.split(r'(?<=[.!?])\s+', text)
        current_chunk = ""
        
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
            
            # Si agregar esta oración excede el límite
            if len(current_chunk) + len(sentence) + 1 > chunk_size and current_chunk:
                chunks.append(current_chunk.strip())
                # Iniciar nuevo chunk con overlap
                current_chunk = " ".join(current_chunk.split()[-5:]) + " " + sentence
            else:
                current_chunk += " " + sentence if current_chunk else sentence
        
        if current_chunk.strip():
            chunks.append(current_chunk.strip())
    
    else:
        # ✅ Opción 2: Dividir por caracteres (rápido)
        for i in range(0, len(text), chunk_size - overlap):
            chunk = text[i:i + chunk_size]
            if chunk.strip():
                chunks.append(chunk.strip())
    
    return chunks


def chunk_with_metadata(
    text: str,
    chunk_size: int = 512,
    overlap: int = 50,
    doc_id: int = None,
    source: str = "document"
) -> List[Tuple[str, dict]]:
    """
    Divide texto y retorna chunks con metadata.
    
    Returns:
        Lista de (chunk_text, metadata) tuples
    """
    chunks = split_into_chunks(text, chunk_size, overlap, sentence_aware=True)
    
    result = []
    for i, chunk in enumerate(chunks):
        metadata = {
            "chunk_number": i + 1,
            "total_chunks": len(chunks),
            "chunk_size": len(chunk),
            "doc_id": doc_id,
            "source": source
        }
        result.append((chunk, metadata))
    
    return result


def optimize_chunks_for_search(chunks: List[str]) -> List[dict]:
    """
    Optimiza chunks para búsqueda.
    - Detecta chunks duplicados
    - Filtra chunks muy cortos
    - Normaliza espacios
    """
    optimized = []
    seen = set()
    
    for i, chunk in enumerate(chunks):
        # Normalizar
        normalized = " ".join(chunk.split()).lower()
        
        # Filtrar duplicados
        if normalized in seen:
            print(f"  ⏭️ Chunk {i + 1} es duplicado, se omite")
            continue
        
        # Filtrar muy cortos (<50 chars)
        if len(chunk) < 50:
            print(f"  ⏭️ Chunk {i + 1} muy corto ({len(chunk)} chars), se omite")
            continue
        
        seen.add(normalized)
        optimized.append({
            "text": chunk,
            "length": len(chunk),
            "order": i
        })
    
    print(f"  📊 Original: {len(chunks)} chunks → Optimizado: {len(optimized)} chunks")
    return optimized


class TextChunker:
    """
    Clase para chunking avanzado con cache y estadísticas.
    """
    
    def __init__(self, chunk_size: int = 512, overlap: int = 50):
        self.chunk_size = chunk_size
        self.overlap = overlap
        self.stats = {
            "documents_processed": 0,
            "total_chunks_created": 0,
            "duplicates_removed": 0,
            "short_chunks_removed": 0
        }
    
    def process_document(self, text: str, doc_id: int = None) -> List[dict]:
        """
        Procesa un documento completo.
        """
        print(f"\n📄 Procesando documento {doc_id or 'unknown'}...")
        print(f"   Tamaño: {len(text):,} caracteres")
        
        # Dividir en chunks
        chunks = split_into_chunks(
            text,
            chunk_size=self.chunk_size,
            overlap=self.overlap,
            sentence_aware=True
        )
        
        print(f"   Chunks creados: {len(chunks)}")
        
        # Optimizar
        optimized = optimize_chunks_for_search(chunks)
        
        # Actualizar stats
        self.stats["documents_processed"] += 1
        self.stats["total_chunks_created"] += len(chunks)
        self.stats["duplicates_removed"] += len(chunks) - len(optimized)
        
        return optimized
    
    def process_batch(self, documents: List[Tuple[str, int]]) -> List[dict]:
        """
        Procesa múltiples documentos.
        
        Args:
            documents: Lista de (text, doc_id) tuples
        
        Returns:
            Lista de chunks procesados
        """
        all_chunks = []
        
        for text, doc_id in documents:
            chunks = self.process_document(text, doc_id)
            all_chunks.extend(chunks)
        
        print(f"\n✅ Lote completado:")
        print(f"   Documentos: {self.stats['documents_processed']}")
        print(f"   Chunks totales: {self.stats['total_chunks_created']}")
        print(f"   Duplicados eliminados: {self.stats['duplicates_removed']}")
        
        return all_chunks
    
    def get_stats(self) -> dict:
        """Retorna estadísticas."""
        return self.stats.copy()


# ============================================
# FUNCIONES DE UTILIDAD
# ============================================

def estimate_chunks_count(text_length: int, chunk_size: int = 512) -> int:
    """
    Estima cuántos chunks tendrá un texto.
    """
    return max(1, text_length // chunk_size)


def analyze_text_for_chunking(text: str) -> dict:
    """
    Analiza un texto antes de chunking.
    """
    sentences = len(re.split(r'(?<=[.!?])\s+', text))
    paragraphs = len(text.split('\n\n'))
    words = len(text.split())
    
    return {
        "total_chars": len(text),
        "total_words": words,
        "total_sentences": sentences,
        "total_paragraphs": paragraphs,
        "avg_sentence_length": len(text) // sentences if sentences > 0 else 0,
        "avg_word_length": len(text) // words if words > 0 else 0
    }


def find_optimal_chunk_size(text: str, target_chunks: int = 5) -> int:
    """
    Calcula el chunk_size óptimo para obtener un número específico de chunks.
    """
    analysis = analyze_text_for_chunking(text)
    optimal_size = analysis["total_chars"] // target_chunks
    return max(256, optimal_size)  # Mínimo 256 chars
