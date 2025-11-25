import os
import json
import csv
import time
from pathlib import Path
from urllib.parse import quote_plus

import requests
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent

INPUT_JSON = BASE_DIR / "landing" / "goodreads_books.json"
OUTPUT_CSV = BASE_DIR / "landing" / "googlebooks_books.csv"

GOOGLE_BOOKS_BASE_URL = "https://www.googleapis.com/books/v1/volumes"

load_dotenv()
API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY")

REQUEST_DELAY = 0.5  # pausa entre consultas
MAX_RETRIES = 3
TIMEOUT = 12


def build_query(book):
    isbn13 = book.get("isbn13")
    isbn10 = book.get("isbn10")
    title = book.get("title") or ""
    author = book.get("author") or ""

    if isbn13:
        return f"isbn:{isbn13}"
    if isbn10:
        return f"isbn:{isbn10}"

    # Fallback por título + autor
    q = f"intitle:{quote_plus(title)}"
    if author:
        q += f"+inauthor:{quote_plus(author)}"

    return q


def fetch_google_books(query):
    params = {"q": query, "maxResults": 5}
    if API_KEY:
        params["key"] = API_KEY

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(
                GOOGLE_BOOKS_BASE_URL, params=params, timeout=TIMEOUT
            )
            resp.raise_for_status()
            data = resp.json()
            items = data.get("items", [])
            return items[0] if items else None
        except Exception as e:
            print(f"[WARN] Error intento {attempt + 1}/{MAX_RETRIES}: {e}")
            time.sleep(1.2)

    return None


def normalize_list(value):
    if not value:
        return None
    if isinstance(value, list):
        return " | ".join(str(v) for v in value)
    return str(value)


def enrich_books():
    print("[INFO] Iniciando enriquecimiento Google Books…")

    if not INPUT_JSON.exists():
        raise FileNotFoundError(f"No se encuentra {INPUT_JSON}")

    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        books = json.load(f)

    rows = []

    for i, book in enumerate(books, start=1):
        title = book.get("title")
        print(f"\n[INFO] Libro {i}/{len(books)} → {title}")

        query = build_query(book)
        if not query:
            print("[WARN] Query vacía, saltando libro.")
            continue

        volume = fetch_google_books(query)
        time.sleep(REQUEST_DELAY)

        if not volume:
            print("[WARN] Sin resultados en Google Books.")
            continue

        info = volume.get("volumeInfo", {})
        sale = volume.get("saleInfo", {})

        # ISBNs desde Google Books
        isbn10 = None
        isbn13 = None
        for ident in info.get("industryIdentifiers", []):
            if ident.get("type") == "ISBN_10":
                isbn10 = ident.get("identifier")
            if ident.get("type") == "ISBN_13":
                isbn13 = ident.get("identifier")

        # Rellenar con los de Goodreads si faltan
        isbn10 = isbn10 or book.get("isbn10")
        isbn13 = isbn13 or book.get("isbn13")

        # Precio
        price = sale.get("listPrice") or sale.get("retailPrice") or {}
        price_amount = price.get("amount")
        price_currency = price.get("currencyCode")

        rows.append(
            {
                "gb_id": volume.get("id"),
                "title": info.get("title"),
                "subtitle": info.get("subtitle"),
                "authors": normalize_list(info.get("authors")),
                "publisher": info.get("publisher"),
                "pub_date": info.get("publishedDate"),
                "language": info.get("language"),
                "categories": normalize_list(info.get("categories")),
                "isbn13": isbn13,
                "isbn10": isbn10,
                "price_amount": price_amount,
                "price_currency": price_currency,
            }
        )

        print(f"[OK] Enriquecido con ID: {volume.get('id')}")

    # Guardar CSV
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        print("[WARN] No hay filas enriquecidas; no se genera CSV.")
        return

    fieldnames = list(rows[0].keys())

    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, delimiter=";", fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n[FIN] Enriquecimiento completado → {len(rows)} filas")
    print(f"[GUARDADO] {OUTPUT_CSV}")


if __name__ == "__main__":
    enrich_books()