import requests
from bs4 import BeautifulSoup
import json
import time
import re
from pathlib import Path

# -----------------------------------------------------------
# CONFIGURACIÓN
# -----------------------------------------------------------

SEARCH_QUERY = "data science"
BASE_SEARCH_URL = "https://www.goodreads.com/search"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

HEADERS = {"User-Agent": USER_AGENT}

OUTPUT_PATH = Path("landing/goodreads_books.json")

# Control de páginas y volumen de datos
PAGES_TO_SCRAPE = 3      # 3 páginas ~ 60 libros
MAX_BOOKS = 80           # límite superior opcional
SLEEP_LIST = 0.4         # segundos entre resultados
SLEEP_PAGE = 1.0         # segundos entre páginas


# -----------------------------------------------------------
# FUNCIONES AUXILIARES
# -----------------------------------------------------------

def clean_text(text):
    if not text:
        return None
    return re.sub(r"\s+", " ", text).strip()


def extract_isbn_from_book_page(book_url):
    """
    Entra en la página del libro y extrae ISBN10 / ISBN13 si existen.
    """
    try:
        resp = requests.get(book_url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        isbn10, isbn13 = None, None
        info_rows = soup.select("div#bookDataBox .clearFloats")

        for row in info_rows:
            heading_el = row.select_one(".infoBoxRowTitle")
            value_el = row.select_one(".infoBoxRowItem")

            heading = clean_text(heading_el.get_text()) if heading_el else ""
            value = clean_text(value_el.get_text()) if value_el else ""

            if "ISBN" in heading:
                # Ejemplo de texto: "ISBN 1491957662 (ISBN13: 9781491957660)"
                match_10 = re.search(r"\b(\d{10})\b", value)
                match_13 = re.search(r"\b(\d{13})\b", value)

                if match_10:
                    isbn10 = match_10.group(1)
                if match_13:
                    isbn13 = match_13.group(1)

        return isbn10, isbn13

    except Exception:
        return None, None


# -----------------------------------------------------------
# SCRAPING PRINCIPAL
# -----------------------------------------------------------

def scrape_goodreads():
    print(f"[INFO] Iniciando scraping ampliado de Goodreads…\n")

    books = []

    for page in range(1, PAGES_TO_SCRAPE + 1):
        url = f"{BASE_SEARCH_URL}?q={SEARCH_QUERY.replace(' ', '+')}&page={page}"

        print(f"[INFO] Scrapeando página {page}: {url}")
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        rows = soup.select("table.tableList tr")
        print(f"[INFO] Libros encontrados en esta página: {len(rows)}")

        for row in rows:
            if len(books) >= MAX_BOOKS:
                break

            # Título
            title_el = row.select_one("a.bookTitle span")
            title = clean_text(title_el.get_text()) if title_el else None

            # Autor
            author_el = row.select_one("a.authorName span")
            author = clean_text(author_el.get_text()) if author_el else None

            # Rating y nº de ratings
            rating_el = row.select_one("span.minirating")
            rating, ratings_count = None, None
            if rating_el:
                rating_text = clean_text(rating_el.get_text())
                match_rating = re.search(r"([0-5]\.\d+)", rating_text)
                match_count = re.search(r"(\d[\d,]*) ratings", rating_text)

                if match_rating:
                    rating = float(match_rating.group(1))
                if match_count:
                    ratings_count = int(match_count.group(1).replace(",", ""))

            # URL del libro
            link_el = row.select_one("a.bookTitle")
            book_url = None
            if link_el:
                book_url = "https://www.goodreads.com" + link_el.get("href")

            # ISBN desde página de detalle
            isbn10, isbn13 = None, None
            if book_url:
                time.sleep(0.6)  # pausa ética
                isbn10, isbn13 = extract_isbn_from_book_page(book_url)

            books.append(
                {
                    "title": title,
                    "author": author,
                    "rating": rating,
                    "ratings_count": ratings_count,
                    "book_url": book_url,
                    "isbn10": isbn10,
                    "isbn13": isbn13,
                }
            )

            print(f"[OK] Libro añadido: {title}")
            time.sleep(SLEEP_LIST)

        if len(books) >= MAX_BOOKS:
            break

        print("[INFO] Pausando antes de la siguiente página…\n")
        time.sleep(SLEEP_PAGE)

    # Guardar JSON
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(books, f, ensure_ascii=False, indent=4)

    print(f"\n[FIN] Scraping completado. Total libros obtenidos: {len(books)}")
    print(f"[GUARDADO] Archivo: {OUTPUT_PATH}")


if __name__ == "__main__":
    scrape_goodreads()
