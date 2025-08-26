from voia_vector_services.services.scraper_service import scrape_website

def process_url(url: str) -> dict:
    url = url.lower()

    if url.startswith("http"):
        print(f"[INFO] Procesando como Página Web: {url}")
        text = scrape_website(url)
        return {"type": "Web", "content": text}

    print(f"[ERROR] Tipo desconocido para la URL: {url}")
    return {"type": "Unknown", "content": ""}
