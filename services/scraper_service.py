import requests
from bs4 import BeautifulSoup


def scrape_website(url: str) -> str:
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
    except Exception as e:
        print(f"[SCRAPER ERROR] Error al obtener la URL {url}: {e}")
        return ""

    soup = BeautifulSoup(response.text, "html.parser")

    # Extraer texto de más etiquetas relevantes
    tags = ['p', 'h1', 'h2', 'h3', 'li', 'span', 'article', 'section', 'div']
    text_chunks = []
    for tag in tags:
        for elem in soup.find_all(tag):
            t = elem.get_text(separator=" ", strip=True)
            if t:
                text_chunks.append(t)

    # Unir y limpiar el texto
    text = ' '.join(text_chunks)
    text = ' '.join(text.split())  # Normalizar espacios

    # Logging de longitud de texto
    print(f"[SCRAPER] URL: {url} | Longitud texto extraído: {len(text)}")

    return text.strip()
