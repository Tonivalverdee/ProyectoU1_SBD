# Métricas de Calidad del Pipeline

El pipeline genera:
```
docs/quality_metrics.json
```

## ¿Qué contiene?

### 1. Conteos
- registros_goodreads
- registros_googlebooks
- libros_finales_dim

### 2. Porcentaje de nulos
- titulo
- isbn13
- precio

### 3. Duplicados por book_id_candidato

### 4. Validación de rango
- precio >= 0

### 5. Distribución por fuente
- goodreads
- googlebooks

## Utilidad
Permite validar:
- integridad
- consistencia
- calidad del scraping
- efectividad del enriquecimiento
