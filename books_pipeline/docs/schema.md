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