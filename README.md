# Proyecto: Mini-pipeline de libros (Goodreads + Google Books)

Este proyecto implementa un **pipeline de datos completo** con tres bloques:

1. **Extracción (Scraping Goodreads → JSON)**
2. **Enriquecimiento (Google Books API → CSV)**
3. **Integración / Normalización / Calidad (JSON + CSV → Parquet + métricas)**

El objetivo es obtener una muestra de libros desde Goodreads, enriquecerla con datos de Google Books y consolidar todo en un **modelo canónico** en formato Parquet, acompañado de métricas de calidad y documentación de esquema.

---

## 1. Requisitos y entorno

### 1.1. Versión de Python

El proyecto se ha desarrollado y probado con:

- **Python 3.x** (recomendado 3.10+)

### 1.2. Dependencias

Las dependencias se declaran en `requirements.txt`:

```txt
requests
beautifulsoup4
lxml
pandas
numpy
pyarrow
python-dotenv
```

Para instalarlas (desde la carpeta `books-pipeline/`):

```bash
pip install -r requirements.txt
```

---

## 2. Estructura del proyecto

```text
books-pipeline/
├─ README.md
├─ requirements.txt
│
├─ landing/
│  ├─ goodreads_books.json
│  └─ googlebooks_books.csv
│
├─ standard/
│  ├─ dim_book.parquet
│  └─ book_source_detail.parquet
│
├─ docs/
│  ├─ quality_metrics.json
│  └─ schema.md
│
└─ src/
   ├─ scrape_goodreads.py
   ├─ enrich_googlebooks.py
   ├─ integrate_pipeline.py
   ├─ utils_isbn.py
   └─ utils_quality.py
   
```

---

## 3. Variables de entorno (Google Books API)

El Bloque 2 utiliza la **Google Books API**.  
La clave se gestiona mediante un archivo `.env`.

En la raíz del proyecto (`books-pipeline/`), crea un archivo `.env` con:

```env
GOOGLE_BOOKS_API_KEY=TU_CLAVE_DE_API_AQUI
```

---

## 4. Ejecución del pipeline

### 4.1. Bloque 1 — Scraping Goodreads → JSON

```bash
python src/scrape_goodreads.py
```

Genera:

```
landing/goodreads_books.json
```

### 4.2. Bloque 2 — Enriquecimiento Google Books → CSV

```bash
python src/enrich_googlebooks.py
```

Genera:

```
landing/googlebooks_books.csv
```

### 4.3. Bloque 3 — Integración / Normalización → Parquet + métricas

```bash
python src/integrate_pipeline.py
```

Genera:

```
standard/dim_book.parquet
standard/book_source_detail.parquet
docs/quality_metrics.json
docs/schema.md
```

---

## 5. Descripción del pipeline

### 5.1. Scraping Goodreads

- Usa selectores CSS:  
  `table.tableList tr`, `a.bookTitle span`, `a.authorName span`, `span.minirating`
- Extrae: título, autor, rating, nº ratings, URL del libro, ISBN10/ISBN13.
- Scraping ético: pausas de 0.4–1.0 s y User-Agent realista.

### 5.2. Enriquecimiento Google Books

- Busca por orden: `isbn13` → `isbn10` → `title+author`.
- Extrae: title, subtitle, authors, publisher, pub_date, language, categories, precio, ISBN.
- Guarda CSV con codificación UTF-8 y separador `;`.

### 5.3. Integración / Normalización

- Combina JSON + CSV en un modelo unificado.
- Normaliza:
  - fechas → ISO-8601  
  - idioma → BCP-47  
  - moneda → ISO-4217  
  - autores/categorías → listas deduplicadas
- Deduplicación basada en:
  - ISBN13 si existe  
  - Si no, `title+author+publisher`
- Reglas de supervivencia:
  - Título más largo  
  - Primer autor no nulo  
  - Uniones de listas  
  - Precio más reciente  
- Genera Parquet + métricas + esquema.

---

## 6. Utilidades (`utils_isbn.py` y `utils_quality.py`)

### 6.1. utils_isbn.py

Funciones:

- `clean_isbn()`
- `is_valid_isbn10()`
- `is_valid_isbn13()`
- `validate_isbn()`

### 6.2. utils_quality.py

Funciones:

- `metric_null_percentage()`
- `metric_unique_values()`
- `metric_duplicates()`
- `check_numeric_range()`
- `check_required_columns()`

---

## 7. Salidas finales del proyecto

| Archivo                             | Descripción |
|-------------------------------------|-------------|
| `landing/goodreads_books.json`      | Datos brutos obtenidos del scraping |
| `landing/googlebooks_books.csv`     | Datos enriquecidos desde Google Books |
| `standard/dim_book.parquet`         | Modelo canónico depurado |
| `standard/book_source_detail.parquet` | Detalle por fuente para auditoría |
| `docs/quality_metrics.json`         | Métricas de calidad del pipeline |
| `docs/schema.md`                    | Esquema formal del modelo |

---

## 8. Conclusiones

Este pipeline implementa un flujo de extracción, enriquecimiento, validación, normalización y publicación siguiendo buenas prácticas de ingeniería de datos:

- Separación clara por capas (`landing/`, `standard/`, `docs/`).
- Normalización semántica y sintáctica.
- Detección de duplicados y supervivencia por reglas.
- Registro de calidad y trazabilidad.
- Estandarización del modelo final en Parquet.

