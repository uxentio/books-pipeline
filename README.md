# Books Pipeline - Mini-pipeline de libros

**Proyecto:** Mini-pipeline de Extracción → Enriquecimiento → Integración  
**Autor:** Antonio Ferrer Martínez  
**Asignatura:** IA_y_BD - SISTEMAS BIG DATA - 25/26  
**Centro:** CIFP Carlos III  
**Fecha:** Noviembre 2025

## Descripción

Este proyecto implementa un pipeline completo de datos para libros, integrando información desde dos fuentes:
- **Goodreads** (web scraping): ratings, número de valoraciones, URLs
- **Google Books API**: metadatos estructurados, ISBN, precios, categorías

El pipeline realiza extracción, enriquecimiento, normalización semántica, deduplicación y genera un modelo dimensional estándar en formato Parquet.

## Estructura del Proyecto

```
books-pipeline/
├── README.md                    # Este archivo
├── requirements.txt             # Dependencias Python
├── .env.example                 # Plantilla para variables de entorno
├── landing/                     # Datos sin procesar (solo lectura)
│   ├── goodreads_books.json    # Resultado del scraping
│   └── googlebooks_books.csv   # Resultado del enriquecimiento
├── standard/                    # Datos estandarizados
│   ├── dim_book.parquet        # Tabla dimensional de libros
│   └── book_source_detail.parquet  # Detalle por fuente
├── docs/                        # Documentación
│   ├── schema.md               # Documentación del modelo
│   └── quality_metrics.json    # Métricas de calidad
└── src/                         # Código fuente
    ├── scrape_goodreads.py     # Ejercicio 1: Scraping
    ├── enrich_googlebooks.py   # Ejercicio 2: Enriquecimiento
    ├── integrate_pipeline.py   # Ejercicio 3: Integración
    ├── utils_quality.py        # Utilidades de calidad
    └── utils_isbn.py           # Utilidades para ISBN
```

## Requisitos

- Python 3.10+
- Conexión a Internet
- (Opcional) API Key de Google Books para mayor límite de requests

### Instalación de dependencias

```bash
pip install -r requirements.txt
```

### Librerías necesarias

- `requests`: HTTP requests para scraping y API calls
- `beautifulsoup4`: Parsing de HTML
- `lxml`: Parser rápido para BeautifulSoup
- `pandas`: Manipulación de datos
- `pyarrow`: Lectura/escritura de archivos Parquet
- `numpy`: Operaciones numéricas
- `python-dotenv`: Gestión de variables de entorno

## Configuración (Opcional)

Para usar la API de Google Books con límites extendidos:

1. Obtener API Key en: https://console.cloud.google.com/apis/credentials
2. Copiar `.env.example` a `.env`
3. Añadir tu API Key al archivo `.env`

```bash
cp .env.example .env
# Editar .env y añadir tu GOOGLE_BOOKS_API_KEY
```

## Ejecución

### Opción 1: Ejecutar todo el pipeline

```bash
# Desde el directorio books-pipeline/
cd src

# Paso 1: Scraping de Goodreads
python scrape_goodreads.py

# Paso 2: Enriquecimiento con Google Books
python enrich_googlebooks.py

# Paso 3: Integración y estandarización
python integrate_pipeline.py
```

### Opción 2: Ejecutar paso a paso

**Ejercicio 1 - Scraping:**
```bash
python src/scrape_goodreads.py
```
- Busca libros sobre "data science" en Goodreads
- Extrae 10-15 libros con título, autor, rating, ratings_count, URL, ISBN
- Guarda en `landing/goodreads_books.json`

**Ejercicio 2 - Enriquecimiento:**
```bash
python src/enrich_googlebooks.py
```
- Lee libros del JSON generado en el paso anterior
- Busca cada libro en Google Books API (preferentemente por ISBN)
- Enriquece con metadatos adicionales
- Guarda en `landing/googlebooks_books.csv`

**Ejercicio 3 - Integración:**
```bash
python src/integrate_pipeline.py
```
- Lee ambas fuentes de `landing/`
- Normaliza datos (fechas ISO-8601, idioma BCP-47, moneda ISO-4217)
- Deduplica con reglas de supervivencia
- Genera artefactos en `standard/` y `docs/`

## Metadatos Técnicos

### Scraping de Goodreads (Ejercicio 1)

**URLs utilizadas:**
- Base: `https://www.goodreads.com`
- Búsqueda: `https://www.goodreads.com/search?q=data+science`

**Selectores CSS:**
- Título: `.BookCard__title a` o `h1.Text__title1`
- Autor: `.ContributorLink__name` o `a.authorName`
- Rating: `.RatingStatistics__rating`
- Número de ratings: `span[data-testid="ratingsCount"]`
- URL del libro: `.BookCard__title a[href]`
- ISBN: `meta[property="books:isbn"]`

**User-Agent:**
```
Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36
```

**Fecha de scraping:** Variable (se registra en metadata del JSON)

**Número de registros:** 10-15 libros

**Pausas implementadas:** 1.0 segundos entre requests

### Enriquecimiento con Google Books (Ejercicio 2)

**API endpoint:**
```
https://www.googleapis.com/books/v1/volumes
```

**Estrategia de búsqueda:**
1. Búsqueda por ISBN13 (más precisa)
2. Búsqueda por ISBN10 (alternativa)
3. Búsqueda por título + autor (fallback)
4. Búsqueda solo por título (último recurso)

**Formato CSV:**
- Separador: coma (`,`)
- Codificación: UTF-8
- Cabecera: sí
- Campos: gb_id, title, subtitle, authors, publisher, pub_date, language, categories, isbn13, isbn10, price_amount, price_currency

**Hipótesis de mapeo:**
- Google Books puede no tener ISBN para todos los libros
- Los precios solo están disponibles para libros en venta
- Las categorías pueden ser múltiples (separadas por comas)
- Las fechas pueden tener diferentes precisiones (año, mes-año, fecha completa)

### Integración y Normalización (Ejercicio 3)

**Normalizaciones aplicadas:**

1. **Fechas** → ISO-8601 (YYYY-MM-DD)
   - Ejemplos: `2025-11-15`, `2023-07-27`
   - Precisión variable: año solo, año-mes, fecha completa

2. **Idioma** → BCP-47
   - Ejemplos: `es`, `en`, `en-US`, `pt-BR`
   - Códigos de 2-3 letras

3. **Moneda** → ISO-4217
   - Ejemplos: `EUR`, `USD`, `GBP`, `JPY`
   - Códigos de 3 letras en mayúsculas

4. **Precios** → Decimal con separador punto
   - Ejemplo: `39.99`

5. **ISBN** → Validados con algoritmo de checksum
   - ISBN-13: validación con módulo 10
   - ISBN-10: validación con módulo 11
   - Conversión automática de ISBN-10 a ISBN-13

**Modelo canónico:**
- ID preferente: `isbn13`
- ID alternativo: hash MD5 de (titulo_normalizado + autor_normalizado + editorial)
- Todos los campos en formato `snake_case`

**Reglas de deduplicación:**

Clave primaria:
- Mismo ISBN-13, o
- Mismo hash de (titulo_normalizado, autor_normalizado, editorial)

Reglas de supervivencia:
1. **Título:** Se elige el más completo (mayor longitud)
2. **Precio:** Se elige el más reciente (por timestamp de ingesta)
3. **Autores/Categorías:** Unión de ambas fuentes sin duplicados
4. **Fechas:** Preferencia a Google Books (más estructuradas)
5. **Ratings:** Preferencia a Goodreads (más confiables)
6. **Provenance:** Se registra la fuente ganadora por campo

**Prioridad de fuentes:**
1. Google Books (datos estructurados)
2. Goodreads (datos de engagement)

## Decisiones Clave

### Arquitectura
- **Sin modificar landing/**: Los archivos originales nunca se tocan
- **Uso de staging/**: Archivos temporales en directorio separado
- **Logs por archivo y regla**: Trazabilidad completa
- **Fallos "suaves"**: Registros con errores se marcan pero no detienen el pipeline

### Calidad de Datos
- **Aserciones bloqueantes:** Detienen el pipeline si fallan validaciones críticas
  - Completitud de título ≥ 90%
  - book_id único en dim_book
  - Tipos de datos válidos para campos críticos

- **Métricas registradas:**
  - % de completitud por campo
  - % de fechas/idiomas/monedas válidas
  - Número de duplicados encontrados y eliminados
  - Filas por fuente

### Manejo de ISBNs
- Validación rigurosa con algoritmos de checksum
- Conversión automática ISBN-10 → ISBN-13
- Extracción de ISBNs desde texto si no está en campo estructurado
- Fallback a clave hash si no hay ISBN disponible

## Artefactos Generados

### 1. dim_book.parquet
Tabla dimensional de libros (1 fila por libro único)
- Formato: Apache Parquet
- Campos: 18 columnas
- Clave primaria: book_id

### 2. book_source_detail.parquet
Detalle por fuente y registro original
- Formato: Apache Parquet
- Incluye campos originales mapeados
- Flags de validación
- Timestamps de ingesta

### 3. quality_metrics.json
Métricas de calidad de la ejecución
- Timestamp de ejecución
- Conteos por fuente
- % de completitud por campo
- % de validez de formatos
- Duplicados encontrados
- Warnings y errores

### 4. schema.md
Documentación del modelo de datos
- Descripción de cada campo
- Tipos de datos y formatos
- Ejemplos
- Reglas de negocio
- Reglas de deduplicación y supervivencia

## Consejos para Uso

1. **Primera ejecución:** Ejecutar los 3 scripts en orden
2. **Re-ejecución:** Los archivos en `landing/` se sobrescriben automáticamente
3. **Debug:** Revisar `quality_metrics.json` para identificar problemas
4. **Personalización:** Modificar el término de búsqueda en `scrape_goodreads.py`

## Limitaciones Conocidas

1. **Goodreads:** La estructura HTML puede cambiar (requeriría actualizar selectores)
2. **Rate limiting:** Pausas implementadas pero puede requerir ajuste
3. **Google Books API:** Límite de 1000 requests/día sin API key
4. **ISBNs:** No todos los libros tienen ISBN disponible públicamente
5. **Precios:** Solo disponibles para libros en venta activa

## Métricas de Éxito

- ✓ Estructura del repositorio completa y correcta
- ✓ ≥10 libros scraped con campos completos
- ✓ JSON válido con metadatos documentados
- ✓ CSV enriquecido con búsqueda prioritaria por ISBN
- ✓ Modelo canónico bien definido
- ✓ Normalizaciones aplicadas correctamente
- ✓ Deduplicación con reglas de supervivencia
- ✓ Aserciones y métricas de calidad
- ✓ Artefactos Parquet + documentación completa

## Autor

**Antonio Ferrer Martínez**  
Estudiante de IA_y_BD - SISTEMAS BIG DATA - 25/26  
CIFP Carlos III

## Licencia

Este proyecto es parte de un trabajo académico.
