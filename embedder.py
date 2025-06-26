from sentence_transformers import SentenceTransformer

# Carga del modelo de embeddings
model = SentenceTransformer("all-MiniLM-L6-v2")

def get_embedding(text):
    # Convierte el texto en vector y lo devuelve como lista
    return model.encode(text).tolist()
