from process_documents import process_pending_documents
from process_urls import process_pending_urls
from process_custom_texts import process_pending_custom_texts


def main():
    print("🚀 Procesando PDFs...")
    process_pending_documents()

    print("🌐 Procesando URLs...")
    process_pending_urls()

    print("📝 Procesando textos planos...")
    process_pending_custom_texts()


if __name__ == "__main__":
    main()
