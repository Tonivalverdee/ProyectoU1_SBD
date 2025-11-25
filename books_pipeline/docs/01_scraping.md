# Bloque 1 — Scraping de Goodreads

Este bloque implementa la fase de **extracción de datos (Extract)** mediante web scraping en la plataforma Goodreads, obteniendo una muestra de libros a partir de una búsqueda.

## Objetivo del bloque
Obtener:
- título
- autor principal
- rating
- número de valoraciones
- URL del libro
- ISBN10 / ISBN13

Guardado en:
```
landing/goodreads_books.json
```

## Pasos del Bloque
### 1. Realizar búsqueda pública
Ejemplo:
```
https://www.goodreads.com/search?q=data+science
```

### 2. Extraer tabla de resultados
Selectores:
- `table.tableList tr`
- `a.bookTitle span`
- `a.authorName span`
- `span.minirating`

### 3. Acceder a la ficha del libro
Selectores:
- `#bookDataBox .clearFloats`
- `.infoBoxRowTitle`
- `.infoBoxRowItem`

### 4. Scraping ético
- Pausas
- User-Agent realista
- Límite de páginas

### 5. Salida del bloque
Archivo JSON con un libro por registro.
