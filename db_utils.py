import mysql.connector
from dotenv import load_dotenv
import os
from sentence_transformers import SentenceTransformer

load_dotenv()

def get_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
    )

# Carga del modelo de embeddings
model = SentenceTransformer("all-MiniLM-L6-v2")

def get_embedding(text):
    # Convierte el texto en vector y lo devuelve como lista
    return model.encode(text).tolist()
