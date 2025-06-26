import requests
from bs4 import BeautifulSoup


def scrape_website(url: str) -> str:
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    text = ' '.join([p.get_text() for p in soup.find_all(['p', 'h1', 'h2', 'h3'])])

    return text.strip()
