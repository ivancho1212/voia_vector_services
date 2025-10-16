from voia_vector_services.services.scraper_service import scrape_website

def process_url(url: str) -> dict:
    url = url.lower()

    if url.startswith("http"):
        print(f"[INFO] Procesando como Página Web: {url}")
        try:
            text = scrape_website(url)
            if not text:
                print(f"[SCRAPER WARNING] No se extrajo texto de la URL: {url}")
            return {"type": "Web", "content": text}
        except Exception as e:
            print(f"[SCRAPER ERROR] Fallo al procesar la URL {url}: {e}")
            return {"type": "Web", "content": ""}

    print(f"[ERROR] Tipo desconocido para la URL: {url}")
    return {"type": "Unknown", "content": ""}
