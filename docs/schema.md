# Documentación del Esquema - Books Pipeline

## Fecha de generación
2025-11-16T19:22:41.610617

## Modelo de datos

### dim_book.parquet
Tabla dimensional de libros (1 fila por libro único)

| Campo | Tipo | Nullable | Formato | Ejemplo | Descripción |
|-------|------|----------|---------|---------|-------------|
| book_id | string | No | isbn13 o hash | 9780134685991 | Identificador único del libro |
| titulo | string | No | - | Data Science for Business | Título del libro |
| titulo_normalizado | string | No | lowercase, sin acentos | data science for business | Título normalizado para matching |
| autor_principal | string | Sí | - | Foster Provost | Autor principal |
| autores | string | Sí | separado por comas | Foster Provost, Tom Fawcett | Lista completa de autores |
| editorial | string | Sí | - | O'Reilly Media | Editorial |
| anio_publicacion | integer | Sí | YYYY | 2013 | Año de publicación |
| fecha_publicacion | string | Sí | ISO-8601 | 2013-07-27 | Fecha completa de publicación |
| idioma | string | Sí | BCP-47 | en | Código de idioma |
| isbn10 | string | Sí | 10 dígitos | 1449361323 | ISBN-10 validado |
| isbn13 | string | Sí | 13 dígitos | 9781449361327 | ISBN-13 validado |
| categoria | string | Sí | separado por comas | Computers, Data Science | Categorías del libro |
| precio | float | Sí | decimal | 39.99 | Precio de venta |
| moneda | string | Sí | ISO-4217 | USD | Código de moneda |
| rating_promedio | float | Sí | 0.0-5.0 | 4.12 | Rating promedio |
| numero_ratings | integer | Sí | >= 0 | 1543 | Número de valoraciones |
| fuente_ganadora | string | Sí | - | googlebooks | Fuente que aportó más datos |
| ts_ultima_actualizacion | string | No | ISO-8601 | 2025-11-15T10:30:00 | Timestamp de última actualización |

### book_source_detail.parquet
Detalle por fuente y registro original

| Campo | Tipo | Nullable | Descripción |
|-------|------|----------|-------------|
| source_id | string | No | Identificador único del registro fuente |
| source_name | string | No | Nombre de la fuente (goodreads, googlebooks) |
| source_file | string | No | Ruta del archivo fuente |
| row_number | integer | No | Número de fila en el archivo original |
| book_id_candidato | string | Sí | ID candidato antes de deduplicación |
| titulo_original | string | Sí | Título tal como aparece en la fuente |
| autor_original | string | Sí | Autor(es) tal como aparece en la fuente |
| validado | boolean | No | Indica si el registro pasó validaciones |
| timestamp_ingesta | string | No | Timestamp de ingesta del registro |

## Fuentes de datos

### Prioridades de fuente
1. **googlebooks** - Prioridad alta para datos estructurados (ISBN, editorial, fecha)
2. **goodreads** - Prioridad alta para datos de engagement (ratings, número de valoraciones)

### Reglas de deduplicación

**Clave primaria de duplicado:**
- isbn13 (preferente)
- Si no hay ISBN13: hash(titulo_normalizado + autor_normalizado + editorial)

**Reglas de supervivencia:**
- **Título**: Se elige el más completo (mayor longitud)
- **Precio**: Se elige el más reciente (por timestamp)
- **Autores/Categorías**: Se hace unión de ambas fuentes sin duplicados
- **Fechas**: Se prefiere Google Books (más estructuradas)
- **Ratings**: Se prefiere Goodreads (más confiables)

### Normalización aplicada

**Fechas:**
- Formato: ISO-8601 (YYYY-MM-DD)
- Ejemplo: 2025-11-15

**Idioma:**
- Formato: BCP-47 (códigos de 2-3 letras)
- Ejemplos: es, en, en-US, pt-BR

**Moneda:**
- Formato: ISO-4217 (códigos de 3 letras)
- Ejemplos: EUR, USD, GBP

**Precios:**
- Formato: Decimal con punto como separador
- Ejemplo: 39.99

**ISBN:**
- Validados con algoritmo de checksum
- ISBN-10 convertido a ISBN-13 cuando es posible

## Métricas de calidad

Ver `quality_metrics.json` para métricas detalladas de esta ejecución.

## Generado por
Books Pipeline v1.0
Pipeline de integración de datos de libros
