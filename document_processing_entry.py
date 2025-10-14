from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Voia Vector Services Document Endpoints",
    description="Endpoints para procesar documentos PDF, URLs y textos planos.",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/process-documents/")
def process_documents_endpoint():
    try:
        print("🚀 Iniciando procesamiento de documentos PDF...")
        from .process_documents import process_pending_documents
        process_pending_documents()
        return {"status": "ok", "message": "✅ Documentos procesados exitosamente."}
    except Exception as e:
        print(f"❌ Error en /process-documents: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/process-urls/")
def process_urls_endpoint():
    try:
        print("🚀 Iniciando procesamiento de URLs...")
        from .process_urls import process_pending_urls
        process_pending_urls()
        return {"status": "ok", "message": "✅ URLs procesadas exitosamente."}
    except Exception as e:
        print(f"❌ Error en /process-urls: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/process-custom-texts/")
def process_custom_texts_endpoint():
    try:
        print("🚀 Iniciando procesamiento de textos planos...")
        from .process_custom_texts import process_pending_custom_texts
        process_pending_custom_texts()
        return {"status": "ok", "message": "✅ Textos planos procesados exitosamente."}
    except Exception as e:
        print(f"❌ Error en /process-custom-texts: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/process-all/")
def process_all_endpoint():
    try:
        print("🚀 Iniciando procesamiento de documentos, URLs y textos planos...")
        from .process_documents import process_pending_documents
        process_pending_documents()
        from .process_urls import process_pending_urls
        process_pending_urls()
        from .process_custom_texts import process_pending_custom_texts
        process_pending_custom_texts()
        return {"status": "ok", "message": "✅ Documentos, URLs y textos planos procesados exitosamente."}
    except Exception as e:
        print(f"❌ Error en /process-all: {e}")
        raise HTTPException(status_code=500, detail=str(e))
