from fastapi import FastAPI, HTTPException
from process_documents import process_pending_documents

app = FastAPI()

@app.post("/process-documents/")
def process_documents_endpoint():
    try:
        process_pending_documents()
        return {"status": "ok", "message": "Documentos procesados exitosamente."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al procesar documentos: {str(e)}")
