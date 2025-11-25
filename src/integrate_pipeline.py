import json
import re
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from utils_isbn import validate_isbn
from utils_quality import (
    metric_null_percentage,
    metric_duplicates,
    check_numeric_range,
)

# -----------------------------------------------------------
# RUTAS
# -----------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent

GOODREADS_JSON = BASE_DIR / "landing" / "goodreads_books.json"
GOOGLEBOOKS_CSV = BASE_DIR / "landing" / "googlebooks_books.csv"

DIM_BOOK_PARQUET = BASE_DIR / "standard" / "dim_book.parquet"
DETAIL_PARQUET = BASE_DIR / "standard" / "book_source_detail.parquet"

QUALITY_JSON = BASE_DIR / "docs" / "quality_metrics.json"
SCHEMA_MD = BASE_DIR / "docs" / "schema.md"


# -----------------------------------------------------------
# NORMALIZADORES
# -----------------------------------------------------------

def normalize_date(date_str):
    """Convierte una fecha a ISO-8601 (YYYY-MM-DD)."""
    if not isinstance(date_str, str):
        return None
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_str):
            return date_str
        if re.fullmatch(r"\d{4}-\d{2}", date_str):
            return f"{date_str}-01"
        if re.fullmatch(r"\d{4}", date_str):
            return f"{date_str}-01-01"
    except:
        return None
    return None


def normalize_language(lang):
    if not isinstance(lang, str):
        return None
    return lang.strip().lower()


def normalize_currency(cur):
    if not isinstance(cur, str):
        return None
    return cur.strip().upper()


def normalize_list_field(value):
    if not value or not isinstance(value, str):
        return []
    tokens = [v.strip() for v in re.split(r"[\|,]", value) if v.strip()]
    return list(dict.fromkeys(tokens))


# -----------------------------------------------------------
# PIPELINE PRINCIPAL
# -----------------------------------------------------------

def integrate_pipeline():
    print("[INFO] Iniciando integración del pipeline…")

    # -------------------------------------------------------
    # 1. Leer fuentes
    # -------------------------------------------------------
    df_gd = pd.read_json(GOODREADS_JSON)
    df_gb = pd.read_csv(GOOGLEBOOKS_CSV, delimiter=";")

    df_gd["source"] = "goodreads"
    df_gb["source"] = "googlebooks"
    df_gd["row_id"] = df_gd.index + 1
    df_gb["row_id"] = df_gb.index + 1

    # -------------------------------------------------------
    # 2. Normalización Goodreads
    # -------------------------------------------------------
    df_gd = df_gd.rename(columns={"author": "author_principal"})
    df_gd["authors_list"] = df_gd["author_principal"].apply(
        lambda x: [x] if isinstance(x, str) else []
    )
    df_gd["categories_list"] = [[] for _ in range(len(df_gd))]
    df_gd["pub_date_normalized"] = None
    df_gd["language_normalized"] = None
    df_gd["price_currency_normalized"] = None

    # -------------------------------------------------------
    # 3. Normalización Google Books
    # -------------------------------------------------------
    df_gb["pub_date_normalized"] = df_gb["pub_date"].apply(normalize_date)
    df_gb["language_normalized"] = df_gb["language"].apply(normalize_language)
    df_gb["price_currency_normalized"] = df_gb["price_currency"].apply(normalize_currency)
    df_gb["authors_list"] = df_gb["authors"].apply(normalize_list_field)
    df_gb["categories_list"] = df_gb["categories"].apply(normalize_list_field)

    # -------------------------------------------------------
    # 4. Unificar fuentes
    # -------------------------------------------------------
    df_all = pd.concat([df_gd, df_gb], ignore_index=True, sort=False)

    # -------------------------------------------------------
    # 5. Definir ID candidato (clave provisional)
    # -------------------------------------------------------
    def compute_candidate_id(row):
        if pd.notna(row.get("isbn13")):
            return str(row["isbn13"]).strip()

        key = f"{row.get('title','')}_{row.get('author_principal','')}_{row.get('publisher','')}"
        key = re.sub(r"\s+", "_", key.lower())
        return key

    df_all["book_id_candidato"] = df_all.apply(compute_candidate_id, axis=1)

    # -------------------------------------------------------
    # 6. Deduplicación + Reglas de supervivencia
    # -------------------------------------------------------
    dim_rows = []

    grouped = df_all.groupby("book_id_candidato")

    for book_id, group in grouped:
        winner = group.iloc[0].copy()

        # ► Mejor título (el de mayor longitud)
        titles = group["title"].dropna().astype(str).reset_index(drop=True)
        if len(titles) > 0:
            title_lengths = titles.str.len()
            longest_idx = title_lengths.idxmax()
            winner["title"] = titles.loc[longest_idx]
        else:
            winner["title"] = None

        # ► Autor principal (primer no nulo)
        authors_p = group["author_principal"].dropna()
        if len(authors_p):
            winner["author_principal"] = authors_p.iloc[0]

        # ► Unir autores
        all_authors = []
        for a in group["authors_list"]:
            all_authors.extend(a)
        winner["authors"] = list(dict.fromkeys(all_authors))

        # ► Unir categorías
        all_cat = []
        for c in group["categories_list"]:
            all_cat.extend(c)
        winner["categories"] = list(dict.fromkeys(all_cat))

        # ► Precio
        prices = group["price_amount"].dropna()
        winner["price_amount"] = prices.iloc[-1] if len(prices) else None

        currencies = group["price_currency_normalized"].dropna()
        winner["price_currency"] = currencies.iloc[-1] if len(currencies) else None

        # ► Idioma
        langs = group["language_normalized"].dropna()
        winner["language"] = langs.iloc[0] if len(langs) else None

        # ► Fecha publicación
        dates = group["pub_date_normalized"].dropna()
        winner["fecha_publicacion"] = dates.iloc[0] if len(dates) else None

        # ► Editorial
        pubs = group["publisher"].dropna()
        winner["editorial"] = pubs.iloc[0] if len(pubs) else None

        # ► Año
        if winner["fecha_publicacion"]:
            winner["anio_publicacion"] = int(str(winner["fecha_publicacion"])[:4])
        else:
            winner["anio_publicacion"] = None

        # ► Validación ISBN
        winner["isbn13_valido"] = validate_isbn(winner.get("isbn13"))

        # ► Fuente ganadora
        winner["fuente_ganadora"] = group["source"].iloc[0]

        # ► Timestamp
        winner["ts_ultima_actualizacion"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        # ► ID final
        winner["book_id"] = book_id

        dim_rows.append(winner)

    df_dim = pd.DataFrame(dim_rows)

    # -------------------------------------------------------
    # 7. Modelo canónico dim_book.parquet
    # -------------------------------------------------------
    df_dim_out = pd.DataFrame()

    df_dim_out["book_id"] = df_dim["book_id"].astype(str)
    df_dim_out["titulo"] = df_dim["title"]
    df_dim_out["titulo_normalizado"] = df_dim["title"].astype(str).str.lower().str.strip()
    df_dim_out["autor_principal"] = df_dim["author_principal"]
    df_dim_out["autores"] = df_dim["authors"]
    df_dim_out["editorial"] = df_dim["editorial"]
    df_dim_out["anio_publicacion"] = df_dim["anio_publicacion"]
    df_dim_out["fecha_publicacion"] = df_dim["fecha_publicacion"]
    df_dim_out["idioma"] = df_dim["language"]
    df_dim_out["isbn10"] = df_dim.get("isbn10")
    df_dim_out["isbn13"] = df_dim.get("isbn13")
    df_dim_out["paginas"] = None
    df_dim_out["formato"] = None
    df_dim_out["categorias"] = df_dim["categories"]
    df_dim_out["precio"] = df_dim["price_amount"]
    df_dim_out["moneda"] = df_dim["price_currency"]
    df_dim_out["fuente_ganadora"] = df_dim["fuente_ganadora"]
    df_dim_out["ts_ultima_actualizacion"] = df_dim["ts_ultima_actualizacion"]

    # -------------------------------------------------------
    # 8. book_source_detail
    # -------------------------------------------------------
    df_detail = df_all.copy()
    df_detail["timestamp_ingesta"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # -------------------------------------------------------
    # 9. Guardar Parquet
    # -------------------------------------------------------
    DIM_BOOK_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    DETAIL_PARQUET.parent.mkdir(parents=True, exist_ok=True)

    pq.write_table(pa.Table.from_pandas(df_dim_out), DIM_BOOK_PARQUET)
    pq.write_table(pa.Table.from_pandas(df_detail), DETAIL_PARQUET)

    # -------------------------------------------------------
    # 10. quality_metrics.json
    # -------------------------------------------------------
    quality = {
        "registros_goodreads": len(df_gd),
        "registros_googlebooks": len(df_gb),
        "libros_finales_dim": len(df_dim_out),
        "pct_nulos_titulo": metric_null_percentage(df_dim_out, "titulo"),
        "pct_nulos_isbn13": metric_null_percentage(df_dim_out, "isbn13"),
        "pct_nulos_precio": metric_null_percentage(df_dim_out, "precio"),
        "duplicados_por_book_id_candidato": metric_duplicates(df_all, ["book_id_candidato"]),
        "precio_valido_rango": check_numeric_range(df_dim_out, "precio", min_val=0),
        "filas_por_fuente": df_all["source"].value_counts().to_dict(),
    }

    with open(QUALITY_JSON, "w", encoding="utf-8") as f:
        json.dump(quality, f, indent=4)

    # -------------------------------------------------------
    # 11. schema.md
    # -------------------------------------------------------
    schema = """
# Schema del modelo canónico

## standard/dim_book.parquet

- book_id
- titulo
- titulo_normalizado
- autor_principal
- autores
- editorial
- anio_publicacion
- fecha_publicacion
- idioma
- isbn10
- isbn13
- paginas
- formato
- categorias
- precio
- moneda
- fuente_ganadora
- ts_ultima_actualizacion

## standard/book_source_detail.parquet

Incluye:
- source_name
- source_file
- row_number
- book_id_candidato
- timestamp_ingesta
- + todos los campos originales
"""
    SCHEMA_MD.write_text(schema.strip(), encoding="utf-8")

    print("\n[FIN] Integración completada.")
    print(f"[OK] dim_book.parquet → {DIM_BOOK_PARQUET}")
    print(f"[OK] book_source_detail.parquet → {DETAIL_PARQUET}")
    print(f"[OK] quality_metrics.json → {QUALITY_JSON}")
    print(f"[OK] schema.md → {SCHEMA_MD}")


if __name__ == "__main__":
    integrate_pipeline()
