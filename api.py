from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from voia_vector_services.process_documents import process_pending_documents
from voia_vector_services.process_urls import process_pending_urls
from voia_vector_services.process_custom_texts import process_pending_custom_texts

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
