# Bloque 3 — Integración, Normalización y Modelo Canónico

## Objetivo
Unir Goodreads + Google Books, limpiar, deduplicar y generar datasets en Parquet listos para análisis.

Salidas:
```
standard/dim_book.parquet
standard/book_source_detail.parquet
docs/quality_metrics.json
docs/schema.md
```

## Pasos del bloque

### 1. Cargar datos de landing/
Incluye:
- source
- row_id

### 2. Normalizar campos
- fechas → ISO
- idioma → BCP-47
- moneda → ISO-4217
- autores/categorías → listas

### 3. book_id_candidato
Regla:
- usar ISBN13 si existe
- si no, `titulo+autor+editorial` normalizado

### 4. Deduplicación
Reglas:
- título más largo
- primer autor no nulo
- unión de listas
- precio más reciente
- idioma no nulo
- editorial no nula

### 5. Modelo canónico
Campos:
- book_id
- titulo
- autor_principal
- autores
- editorial
- anio_publicacion
- fecha_publicacion
- idioma
- isbn10 / isbn13
- categorias
- precio / moneda
- ts_ultima_actualizacion

### 6. Trazabilidad
`book_source_detail.parquet` contiene todos los valores originales.

### 7. Calidad
`quality_metrics.json` calcula nulos, duplicados, etc.

### 8. Esquema
`schema.md` describe el modelo.
