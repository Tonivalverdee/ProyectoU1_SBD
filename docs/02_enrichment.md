# Bloque 2 — Enriquecimiento con Google Books API

## Objetivo
Ampliar los datos de Goodreads con metadata completa de Google Books:
- título/subtítulo
- autores
- editorial
- fecha publicación
- idioma
- categorías
- ISBN normalizados
- precio y moneda

Salida:
```
landing/googlebooks_books.csv
```

## Funcionamiento

### 1. Cargar goodreads_books.json
Se lee como fuente principal.

### 2. Construcción de la consulta
Orden de prioridad:
1. `isbn:ISBN13`
2. `isbn:ISBN10`
3. `intitle:TITULO+inauthor:AUTOR`

### 3. Petición a Google Books API
Endpoint:
```
https://www.googleapis.com/books/v1/volumes
```

### 4. Extracción de datos
Desde `volumeInfo`, `saleInfo`, `industryIdentifiers`.

### 5. Normalización parcial
- Autores → "A | B | C"
- Categorías → "X | Y"

### 6. CSV final
Separador `;`, UTF-8.
