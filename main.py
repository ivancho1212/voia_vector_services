from fastapi import FastAPI
from process_documents import process_pending_documents
from process_urls import process_pending_urls
from process_custom_texts import process_pending_custom_texts

# This is the FastAPI application instance.
app = FastAPI()

# Define an endpoint that listens for a GET request.
@app.get("/process_all")
def process_all():
    print("🚀 Procesando PDFs...")
    process_pending_documents()

    print("🌐 Procesando URLs...")
    process_pending_urls()

    print("📝 Procesando textos planos...")
    process_pending_custom_texts()

    return {"status": "success", "message": "All processing tasks have been initiated."}

# You can also create separate endpoints for each task.
@app.get("/process_documents")
def process_documents_endpoint():
    print("🚀 Procesando PDFs...")
    process_pending_documents()
    return {"status": "success", "message": "Document processing initiated."}

@app.get("/process_urls")
def process_urls_endpoint():
    print("🌐 Procesando URLs...")
    process_pending_urls()
    return {"status": "success", "message": "URL processing initiated."}

@app.get("/process_texts")
def process_texts_endpoint():
    print("📝 Procesando textos planos...")
    process_pending_custom_texts()
    return {"status": "success", "message": "Custom text processing initiated."}