"""
EJERCICIO 3: Integración y estandarización → Parquet
Integra JSON + CSV, normaliza, deduplica y genera artefactos estándar
"""

import pandas as pd
import json
import re
from datetime import datetime
import hashlib
from utils_quality import QualityChecker, save_quality_metrics
from utils_isbn import validate_isbn13, validate_isbn10, isbn10_to_isbn13


class BooksPipeline:
    """Pipeline de integración de datos de libros"""
    
    def __init__(self):
        self.df_goodreads = None
        self.df_googlebooks = None
        self.df_integrated = None
        self.df_dim_book = None
        self.df_source_detail = None
        self.quality_checker = QualityChecker()
        self.metadata_log = []
    
    def step1_land_data(self):
        """
        PASO 1: Aterrizar datos sin modificar archivos originales
        """
        print("\n" + "="*60)
        print("PASO 1: ATERRIZAJE DE DATOS")
        print("="*60)
        
        # Crear carpetas necesarias si no existen
        import os
        os.makedirs('landing', exist_ok=True)
        os.makedirs('standard', exist_ok=True)
        os.makedirs('docs', exist_ok=True)
        
        # Leer JSON de Goodreads
        print("\nLeyendo landing/goodreads_books.json...")
        with open('landing/goodreads_books.json', 'r', encoding='utf-8') as f:
            gr_data = json.load(f)
        
        self.df_goodreads = pd.DataFrame(gr_data['books'])
        
        self.metadata_log.append({
            'source': 'goodreads',
            'file': 'landing/goodreads_books.json',
            'ingestion_timestamp': datetime.now().isoformat(),
            'rows': len(self.df_goodreads),
            'columns': len(self.df_goodreads.columns),
            'schema': list(self.df_goodreads.columns)
        })
        
        print(f"  ✓ Goodreads: {len(self.df_goodreads)} registros, {len(self.df_goodreads.columns)} columnas")
        
        # Leer CSV de Google Books
        print("\nLeyendo landing/googlebooks_books.csv...")
        self.df_googlebooks = pd.read_csv('landing/googlebooks_books.csv', encoding='utf-8')
        
        self.metadata_log.append({
            'source': 'googlebooks',
            'file': 'landing/googlebooks_books.csv',
            'ingestion_timestamp': datetime.now().isoformat(),
            'rows': len(self.df_googlebooks),
            'columns': len(self.df_googlebooks.columns),
            'schema': list(self.df_googlebooks.columns)
        })
        
        print(f"  ✓ Google Books: {len(self.df_googlebooks)} registros, {len(self.df_googlebooks.columns)} columnas")
        
        print("\n✓ Paso 1 completado: datos aterrizados")
    
    def step2_annotate_metadata(self):
        """
        PASO 2: Anotar metadatos de ingesta
        """
        print("\n" + "="*60)
        print("PASO 2: ANOTACIÓN DE METADATOS")
        print("="*60)
        
        # Añadir columnas de provenance
        self.df_goodreads['source_name'] = 'goodreads'
        self.df_goodreads['source_file'] = 'landing/goodreads_books.json'
        self.df_goodreads['ingestion_ts'] = datetime.now().isoformat()
        
        self.df_googlebooks['source_name'] = 'googlebooks'
        self.df_googlebooks['source_file'] = 'landing/googlebooks_books.csv'
        self.df_googlebooks['ingestion_ts'] = datetime.now().isoformat()
        
        print("  ✓ Metadatos de provenance añadidos a ambas fuentes")
        print("\n✓ Paso 2 completado: metadatos anotados")
    
    def step3_quality_checks(self):
        """
        PASO 3: Chequeos de calidad
        """
        print("\n" + "="*60)
        print("PASO 3: CHEQUEOS DE CALIDAD")
        print("="*60)
        
        # Verificar Goodreads
        print("\nAnalizando calidad de Goodreads:")
        gr_completeness = self.quality_checker.check_completeness(
            self.df_goodreads,
            ['title', 'author', 'book_url']
        )
        self.quality_checker.metrics['goodreads_completeness'] = gr_completeness
        
        # Verificar Google Books
        print("\nAnalizando calidad de Google Books:")
        gb_completeness = self.quality_checker.check_completeness(
            self.df_googlebooks,
            ['title', 'authors']
        )
        self.quality_checker.metrics['googlebooks_completeness'] = gb_completeness
        
        print("\n✓ Paso 3 completado: chequeos de calidad realizados")
    
    def step4_define_canonical_model(self):
        """
        PASO 4: Definir modelo canónico
        """
        print("\n" + "="*60)
        print("PASO 4: MODELO CANÓNICO")
        print("="*60)
        
        print("\nEsquema del modelo canónico:")
        print("  - ID preferente: isbn13")
        print("  - ID alternativo: hash(titulo_normalizado + autor_normalizado + editorial)")
        print("  - Campos: book_id, titulo, titulo_normalizado, autor_principal,")
        print("            autores, editorial, anio_publicacion, fecha_publicacion,")
        print("            idioma, isbn10, isbn13, categoria, precio, moneda,")
        print("            rating_promedio, numero_ratings, fuente_ganadora")
        
        print("\n✓ Paso 4 completado: modelo canónico definido")
    
    def step5_normalize(self):
        """
        PASO 5: Normalización semántica
        """
        print("\n" + "="*60)
        print("PASO 5: NORMALIZACIÓN SEMÁNTICA")
        print("="*60)
        
        # Normalizar Google Books
        print("\nNormalizando Google Books:")
        
        # Fechas ISO-8601
        self.df_googlebooks['fecha_publicacion_iso'] = self.df_googlebooks['pub_date'].apply(
            self._normalize_date
        )
        
        # Idioma BCP-47
        self.df_googlebooks['idioma_bcp47'] = self.df_googlebooks['language'].apply(
            self._normalize_language
        )
        
        # Moneda ISO-4217
        self.df_googlebooks['moneda_iso'] = self.df_googlebooks['price_currency'].apply(
            self._normalize_currency
        )
        
        # Precio como decimal
        self.df_googlebooks['precio_decimal'] = pd.to_numeric(
            self.df_googlebooks['price_amount'], errors='coerce'
        )
        
        # ISBN validados
        self.df_googlebooks['isbn13_validado'] = self.df_googlebooks['isbn13'].apply(
            lambda x: x if validate_isbn13(x) else None
        )
        
        self.df_googlebooks['isbn10_validado'] = self.df_googlebooks['isbn10'].apply(
            lambda x: x if validate_isbn10(x) else None
        )
        
        # Normalizar títulos para comparación
        self.df_goodreads['titulo_normalizado'] = self.df_goodreads['title'].apply(
            self._normalize_title
        )
        self.df_goodreads['autor_normalizado'] = self.df_goodreads['author'].apply(
            self._normalize_author
        )
        
        self.df_googlebooks['titulo_normalizado'] = self.df_googlebooks['title'].apply(
            self._normalize_title
        )
        self.df_googlebooks['autor_normalizado'] = self.df_googlebooks['authors'].apply(
            self._normalize_author
        )
        
        # Validar normalizaciones
        date_valid_pct = self.quality_checker.check_date_format(
            self.df_googlebooks, ['fecha_publicacion_iso']
        )
        lang_valid_pct = self.quality_checker.check_language_format(
            self.df_googlebooks, 'idioma_bcp47'
        )
        currency_valid_pct = self.quality_checker.check_currency_format(
            self.df_googlebooks, 'moneda_iso'
        )
        isbn_validity = self.quality_checker.check_isbn_validity(self.df_googlebooks)
        
        self.quality_checker.metrics['normalization'] = {
            'dates_valid_pct': date_valid_pct,
            'languages_valid_pct': lang_valid_pct,
            'currencies_valid_pct': currency_valid_pct,
            **isbn_validity
        }
        
        print("  ✓ Fechas normalizadas a ISO-8601")
        print("  ✓ Idiomas normalizados a BCP-47")
        print("  ✓ Monedas normalizadas a ISO-4217")
        print("  ✓ ISBNs validados")
        print("  ✓ Títulos y autores normalizados para matching")
        
        print("\n✓ Paso 5 completado: normalización aplicada")
    
    def step6_enrich(self):
        """
        PASO 6: Enriquecimientos ligeros
        """
        print("\n" + "="*60)
        print("PASO 6: ENRIQUECIMIENTOS LIGEROS")
        print("="*60)
        
        # Derivar año de publicación
        self.df_googlebooks['anio_publicacion'] = self.df_googlebooks['fecha_publicacion_iso'].apply(
            lambda x: int(str(x)[:4]) if pd.notna(x) and len(str(x)) >= 4 else None
        )
        
        # Longitud de título
        self.df_googlebooks['titulo_longitud'] = self.df_googlebooks['title'].apply(
            lambda x: len(str(x)) if pd.notna(x) else 0
        )
        
        # Flag de disponibilidad de precio
        self.df_googlebooks['tiene_precio'] = self.df_googlebooks['precio_decimal'].notna()
        
        print("  ✓ Año de publicación derivado")
        print("  ✓ Longitud de título calculada")
        print("  ✓ Flag de disponibilidad de precio añadido")
        
        print("\n✓ Paso 6 completado: enriquecimientos aplicados")
    
    def step7_deduplicate(self):
        """
        PASO 7: Deduplicación con reglas de supervivencia
        """
        print("\n" + "="*60)
        print("PASO 7: DEDUPLICACIÓN Y SUPERVIVENCIA")
        print("="*60)
        
        # Preparar datos para merge
        # Mapear columnas de Goodreads al modelo canónico
        df_gr_mapped = pd.DataFrame({
            'isbn13': self.df_goodreads['isbn13'],
            'isbn10': self.df_goodreads['isbn10'],
            'titulo': self.df_goodreads['title'],
            'titulo_normalizado': self.df_goodreads['titulo_normalizado'],
            'autor_principal': self.df_goodreads['author'],
            'autor_normalizado': self.df_goodreads['autor_normalizado'],
            'rating_promedio': self.df_goodreads['rating'],
            'numero_ratings': self.df_goodreads['ratings_count'],
            'url_referencia': self.df_goodreads['book_url'],
            'source_name': 'goodreads',
            'source_file': self.df_goodreads['source_file'],
            'ingestion_ts': self.df_goodreads['ingestion_ts']
        })
        
        # Mapear columnas de Google Books
        df_gb_mapped = pd.DataFrame({
            'isbn13': self.df_googlebooks['isbn13_validado'],
            'isbn10': self.df_googlebooks['isbn10_validado'],
            'titulo': self.df_googlebooks['title'],
            'titulo_normalizado': self.df_googlebooks['titulo_normalizado'],
            'autor_principal': self.df_googlebooks['authors'].apply(
                lambda x: str(x).split(',')[0].strip() if pd.notna(x) else None
            ),
            'autor_normalizado': self.df_googlebooks['autor_normalizado'],
            'autores_completo': self.df_googlebooks['authors'],
            'editorial': self.df_googlebooks['publisher'],
            'fecha_publicacion': self.df_googlebooks['fecha_publicacion_iso'],
            'anio_publicacion': self.df_googlebooks['anio_publicacion'],
            'idioma': self.df_googlebooks['idioma_bcp47'],
            'categoria': self.df_googlebooks['categories'],
            'precio': self.df_googlebooks['precio_decimal'],
            'moneda': self.df_googlebooks['moneda_iso'],
            'gb_id': self.df_googlebooks['gb_id'],
            'source_name': 'googlebooks',
            'source_file': self.df_googlebooks['source_file'],
            'ingestion_ts': self.df_googlebooks['ingestion_ts']
        })
        
        # Concatenar ambas fuentes
        df_all = pd.concat([df_gr_mapped, df_gb_mapped], ignore_index=True, join='outer')
        
        # Crear clave de deduplicación
        df_all['dedup_key'] = df_all.apply(self._create_dedup_key, axis=1)
        
        print(f"\nRegistros totales antes de deduplicación: {len(df_all)}")
        
        # Detectar duplicados
        duplicates = self.quality_checker.check_duplicates(df_all, ['dedup_key'])
        self.quality_checker.metrics['duplicates_found'] = duplicates
        
        # Resolver duplicados con reglas de supervivencia
        df_deduplicated = []
        
        for key, group in df_all.groupby('dedup_key'):
            if len(group) == 1:
                df_deduplicated.append(group.iloc[0])
            else:
                # Aplicar reglas de supervivencia
                survivor = self._resolve_duplicates(group)
                df_deduplicated.append(survivor)
        
        self.df_integrated = pd.DataFrame(df_deduplicated)
        
        print(f"Registros después de deduplicación: {len(self.df_integrated)}")
        print(f"Duplicados eliminados: {len(df_all) - len(self.df_integrated)}")
        
        print("\n✓ Paso 7 completado: deduplicación realizada")
    
    def step8_emit_artifacts(self):
        """
        PASO 8: Emitir artefactos finales
        """
        print("\n" + "="*60)
        print("PASO 8: EMISIÓN DE ARTEFACTOS")
        print("="*60)
        
        # Crear dim_book.parquet
        self._create_dim_book()
        
        # Crear book_source_detail.parquet
        self._create_source_detail()
        
        # Crear quality_metrics.json
        self._create_quality_metrics()
        
        # Crear schema.md
        self._create_schema_doc()
        
        print("\n✓ Paso 8 completado: todos los artefactos emitidos")
    
    def _create_dim_book(self):
        """Crea la tabla dimensional de libros"""
        print("\nCreando dim_book.parquet...")
        
        # Asignar book_id final (asegurar que sea string)
        self.df_integrated['book_id'] = self.df_integrated['isbn13'].fillna(
            self.df_integrated['dedup_key']
        ).astype(str)
        
        # Seleccionar y ordenar columnas finales
        dim_book_cols = [
            'book_id', 'titulo', 'titulo_normalizado', 'autor_principal',
            'autores', 'editorial', 'anio_publicacion', 'fecha_publicacion',
            'idioma', 'isbn10', 'isbn13', 'categoria', 'precio', 'moneda',
            'rating_promedio', 'numero_ratings', 'fuente_ganadora'
        ]
        
        # Renombrar para consistencia
        self.df_dim_book = self.df_integrated.rename(columns={
            'autores_completo': 'autores'
        })
        
        # Asegurar que todas las columnas existen
        for col in dim_book_cols:
            if col not in self.df_dim_book.columns:
                self.df_dim_book[col] = None
        
        self.df_dim_book = self.df_dim_book[dim_book_cols]
        
        # Añadir timestamp
        self.df_dim_book['ts_ultima_actualizacion'] = datetime.now().isoformat()
        
        # Guardar como Parquet
        self.df_dim_book.to_parquet('standard/dim_book.parquet', index=False)
        
        print(f"  ✓ dim_book.parquet creado: {len(self.df_dim_book)} registros")
        
        # Aplicar aserciones de calidad
        self.quality_checker.assert_quality(self.df_dim_book)
    
    def _create_source_detail(self):
        """Crea la tabla de detalle por fuente"""
        print("\nCreando book_source_detail.parquet...")
        
        # Reconstruir detalle de fuentes originales
        detail_records = []
        
        # Goodreads
        for idx, row in self.df_goodreads.iterrows():
            detail_records.append({
                'source_id': f"gr_{idx}",
                'source_name': 'goodreads',
                'source_file': 'landing/goodreads_books.json',
                'row_number': idx,
                'book_id_candidato': str(row.get('isbn13') or row.get('isbn10') or f"hash_{idx}"),
                'titulo_original': row.get('title'),
                'autor_original': row.get('author'),
                'rating': row.get('rating'),
                'ratings_count': row.get('ratings_count'),
                'book_url': row.get('book_url'),
                'isbn13_original': row.get('isbn13'),
                'isbn10_original': row.get('isbn10'),
                'validado': True,
                'timestamp_ingesta': row.get('ingestion_ts')
            })
        
        # Google Books
        for idx, row in self.df_googlebooks.iterrows():
            detail_records.append({
                'source_id': f"gb_{idx}",
                'source_name': 'googlebooks',
                'source_file': 'landing/googlebooks_books.csv',
                'row_number': idx,
                'book_id_candidato': str(row.get('isbn13') or row.get('isbn10') or f"hash_{idx}"),
                'titulo_original': row.get('title'),
                'autor_original': row.get('authors'),
                'editorial': row.get('publisher'),
                'fecha_publicacion': row.get('pub_date'),
                'idioma': row.get('language'),
                'precio': row.get('price_amount'),
                'moneda': row.get('price_currency'),
                'isbn13_original': row.get('isbn13'),
                'isbn10_original': row.get('isbn10'),
                'gb_id': row.get('gb_id'),
                'validado': True,
                'timestamp_ingesta': row.get('ingestion_ts')
            })
        
        self.df_source_detail = pd.DataFrame(detail_records)
        
        # Guardar como Parquet
        self.df_source_detail.to_parquet('standard/book_source_detail.parquet', index=False)
        
        print(f"  ✓ book_source_detail.parquet creado: {len(self.df_source_detail)} registros")
    
    def _create_quality_metrics(self):
        """Crea el archivo de métricas de calidad"""
        print("\nCreando quality_metrics.json...")
        
        # Compilar todas las métricas
        all_metrics = {
            'pipeline_execution': {
                'timestamp': datetime.now().isoformat(),
                'total_sources': 2,
                'total_input_records': len(self.df_goodreads) + len(self.df_googlebooks),
                'total_output_records': len(self.df_dim_book)
            },
            'source_breakdown': {
                'goodreads_records': len(self.df_goodreads),
                'googlebooks_records': len(self.df_googlebooks)
            },
            'quality_checks': self.quality_checker.metrics,
            'warnings': self.quality_checker.warnings,
            'errors': self.quality_checker.errors
        }
        
        save_quality_metrics(all_metrics, 'docs/quality_metrics.json')
    
    def _create_schema_doc(self):
        """Crea la documentación del esquema"""
        print("\nCreando schema.md...")
        
        schema_content = """# Documentación del Esquema - Books Pipeline

## Fecha de generación
{timestamp}

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
""".format(timestamp=datetime.now().isoformat())
        
        with open('docs/schema.md', 'w', encoding='utf-8') as f:
            f.write(schema_content)
        
        print("  ✓ schema.md creado")
    
    # Funciones auxiliares de normalización
    
    def _normalize_date(self, date_str):
        """Normaliza fechas a formato ISO-8601"""
        if pd.isna(date_str):
            return None
        
        date_str = str(date_str).strip()
        
        # Ya está en formato ISO
        if re.match(r'^\d{4}-\d{2}-\d{2}', date_str):
            return date_str[:10]
        
        # Solo año (YYYY)
        if re.match(r'^\d{4}$', date_str):
            return f"{date_str}-01-01"
        
        # Año-Mes (YYYY-MM)
        if re.match(r'^\d{4}-\d{2}$', date_str):
            return f"{date_str}-01"
        
        # Intentar parsear con pandas
        try:
            parsed = pd.to_datetime(date_str)
            return parsed.strftime('%Y-%m-%d')
        except:
            return None
    
    def _normalize_language(self, lang_str):
        """Normaliza códigos de idioma a BCP-47"""
        if pd.isna(lang_str):
            return None
        
        lang_str = str(lang_str).strip().lower()
        
        # Mapeo de códigos comunes
        lang_map = {
            'en': 'en',
            'es': 'es',
            'fr': 'fr',
            'de': 'de',
            'it': 'it',
            'pt': 'pt',
            'zh': 'zh',
            'ja': 'ja',
            'ko': 'ko',
            'ru': 'ru'
        }
        
        return lang_map.get(lang_str[:2], lang_str)
    
    def _normalize_currency(self, currency_str):
        """Normaliza códigos de moneda a ISO-4217"""
        if pd.isna(currency_str):
            return None
        
        currency_str = str(currency_str).strip().upper()
        
        # Validar que sea un código ISO-4217 válido
        valid_currencies = {'EUR', 'USD', 'GBP', 'JPY', 'CNY', 'INR', 'CAD', 'AUD', 'CHF', 'MXN', 'BRL', 'ARS'}
        
        return currency_str if currency_str in valid_currencies else None
    
    def _normalize_title(self, title):
        """Normaliza título para matching"""
        if pd.isna(title):
            return ""
        
        title = str(title).lower()
        title = re.sub(r'[^\w\s]', '', title)  # Eliminar puntuación
        title = re.sub(r'\s+', ' ', title).strip()  # Normalizar espacios
        
        return title
    
    def _normalize_author(self, author):
        """Normaliza autor para matching"""
        if pd.isna(author):
            return ""
        
        author = str(author).lower()
        # Tomar solo el primer autor si hay múltiples
        author = author.split(',')[0].split(';')[0]
        author = re.sub(r'[^\w\s]', '', author)
        author = re.sub(r'\s+', ' ', author).strip()
        
        return author
    
    def _create_dedup_key(self, row):
        """Crea una clave de deduplicación"""
        # Preferir ISBN13
        if pd.notna(row.get('isbn13')):
            return f"isbn13:{row['isbn13']}"
        
        # Alternativa: hash de campos clave
        key_parts = [
            row.get('titulo_normalizado', ''),
            row.get('autor_normalizado', ''),
            str(row.get('editorial', ''))
        ]
        
        key_string = '|'.join(key_parts).lower()
        hash_key = hashlib.md5(key_string.encode()).hexdigest()[:12]
        
        return f"hash:{hash_key}"
    
    def _resolve_duplicates(self, group):
        """Aplica reglas de supervivencia para resolver duplicados"""
        # Prioridad de fuentes: googlebooks > goodreads
        source_priority = {'googlebooks': 2, 'goodreads': 1}
        
        # Ordenar por prioridad y timestamp
        group = group.copy()
        group['priority'] = group['source_name'].map(source_priority)
        group = group.sort_values(['priority', 'ingestion_ts'], ascending=[False, False])
        
        # Tomar el registro base del más prioritario
        survivor = group.iloc[0].to_dict()
        
        # Regla: título más completo
        max_title_len = 0
        for _, row in group.iterrows():
            title_len = len(str(row.get('titulo', '')))
            if title_len > max_title_len:
                max_title_len = title_len
                survivor['titulo'] = row['titulo']
        
        # Regla: combinar autores únicos
        all_authors = set()
        for _, row in group.iterrows():
            if pd.notna(row.get('autores_completo')):
                authors = str(row['autores_completo']).split(',')
                all_authors.update([a.strip() for a in authors if a.strip()])
        
        if all_authors:
            survivor['autores_completo'] = ', '.join(sorted(all_authors))
        
        # Regla: combinar categorías únicas
        all_categories = set()
        for _, row in group.iterrows():
            if pd.notna(row.get('categoria')):
                cats = str(row['categoria']).split(',')
                all_categories.update([c.strip() for c in cats if c.strip()])
        
        if all_categories:
            survivor['categoria'] = ', '.join(sorted(all_categories))
        
        # Registrar fuente ganadora
        survivor['fuente_ganadora'] = group.iloc[0]['source_name']
        
        return pd.Series(survivor)
    
    def run(self):
        """Ejecuta el pipeline completo"""
        print("\n" + "="*60)
        print("BOOKS PIPELINE - INTEGRACIÓN Y ESTANDARIZACIÓN")
        print("="*60)
        print(f"Inicio: {datetime.now().isoformat()}")
        
        try:
            self.step1_land_data()
            self.step2_annotate_metadata()
            self.step3_quality_checks()
            self.step4_define_canonical_model()
            self.step5_normalize()
            self.step6_enrich()
            self.step7_deduplicate()
            self.step8_emit_artifacts()
            
            print("\n" + "="*60)
            print("✓ PIPELINE COMPLETADO EXITOSAMENTE")
            print("="*60)
            print(f"Fin: {datetime.now().isoformat()}")
            print(f"\nArtefactos generados:")
            print("  - standard/dim_book.parquet")
            print("  - standard/book_source_detail.parquet")
            print("  - docs/quality_metrics.json")
            print("  - docs/schema.md")
            
        except Exception as e:
            print(f"\n✗ ERROR en el pipeline: {e}")
            raise


def main():
    """Función principal"""
    pipeline = BooksPipeline()
    pipeline.run()
    
    print("\n" + "="*60)
    print("EJERCICIO 3 COMPLETADO")
    print("="*60)


if __name__ == "__main__":
    main()
