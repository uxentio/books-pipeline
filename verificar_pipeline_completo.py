"""
VERIFICACIÓN COMPLETA DEL PIPELINE DE LIBROS
Comprueba que todo funcione correctamente después de los cambios
"""

import pandas as pd
import json
import os
from datetime import datetime

# Colores para la terminal
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'

def print_header(text):
    print("\n" + "=" * 80)
    print(f"{BLUE}{text}{RESET}")
    print("=" * 80)

def print_ok(text):
    print(f"  {GREEN}✓{RESET} {text}")

def print_error(text):
    print(f"  {RED}✗{RESET} {text}")

def print_warning(text):
    print(f"  {YELLOW}⚠{RESET} {text}")

def print_info(text):
    print(f"    {text}")

# Contadores
total_checks = 0
passed_checks = 0
failed_checks = 0
warnings = 0

def check(condition, ok_msg, error_msg):
    global total_checks, passed_checks, failed_checks
    total_checks += 1
    if condition:
        print_ok(ok_msg)
        passed_checks += 1
        return True
    else:
        print_error(error_msg)
        failed_checks += 1
        return False

def warn(msg):
    global warnings
    print_warning(msg)
    warnings += 1

# ============================================================================
# INICIO DE VERIFICACIONES
# ============================================================================

print("\n" + "=" * 80)
print(f"{BLUE}VERIFICACIÓN COMPLETA DEL PIPELINE DE LIBROS{RESET}")
print("=" * 80)
print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# ============================================================================
# 1. VERIFICAR ESTRUCTURA DE DIRECTORIOS
# ============================================================================
print_header("1. ESTRUCTURA DE DIRECTORIOS")

required_dirs = ['landing', 'standard', 'docs', 'src']
for dir_name in required_dirs:
    check(
        os.path.isdir(dir_name),
        f"Directorio '{dir_name}/' existe",
        f"Directorio '{dir_name}/' NO existe"
    )

# ============================================================================
# 2. VERIFICAR ARCHIVOS DE ENTRADA (LANDING)
# ============================================================================
print_header("2. ARCHIVOS DE ENTRADA (LANDING)")

# Goodreads JSON
if os.path.exists('landing/goodreads_books.json'):
    try:
        with open('landing/goodreads_books.json', 'r', encoding='utf-8') as f:
            gr_data = json.load(f)
        
        if 'books' in gr_data:
            num_books = len(gr_data['books'])
            check(
                10 <= num_books <= 15,
                f"goodreads_books.json tiene {num_books} libros (✓ rango esperado)",
                f"goodreads_books.json tiene {num_books} libros (esperado: 10-15)"
            )
            
            # Verificar campos requeridos
            required_fields = ['title', 'author', 'rating', 'ratings_count', 'book_url']
            if num_books > 0:
                first_book = gr_data['books'][0]
                missing_fields = [f for f in required_fields if f not in first_book]
                check(
                    len(missing_fields) == 0,
                    f"Todos los campos requeridos presentes: {', '.join(required_fields)}",
                    f"Faltan campos: {', '.join(missing_fields)}"
                )
        else:
            print_error("goodreads_books.json no tiene formato correcto (falta 'books')")
            failed_checks += 1
    except Exception as e:
        print_error(f"Error leyendo goodreads_books.json: {e}")
        failed_checks += 1
else:
    print_error("goodreads_books.json NO existe")
    failed_checks += 1

# Google Books CSV
if os.path.exists('landing/googlebooks_books.csv'):
    try:
        df_gb = pd.read_csv('landing/googlebooks_books.csv')
        num_records = len(df_gb)
        
        check(
            10 <= num_records <= 15,
            f"googlebooks_books.csv tiene {num_records} registros (✓ rango esperado)",
            f"googlebooks_books.csv tiene {num_records} registros (esperado: 10-15)"
        )
        
        # Verificar columnas esperadas
        expected_cols = ['title', 'authors', 'publisher', 'pub_date', 'isbn13', 'isbn10']
        missing_cols = [col for col in expected_cols if col not in df_gb.columns]
        check(
            len(missing_cols) == 0,
            f"Todas las columnas esperadas presentes",
            f"Faltan columnas: {', '.join(missing_cols)}"
        )
    except Exception as e:
        print_error(f"Error leyendo googlebooks_books.csv: {e}")
        failed_checks += 1
else:
    print_error("googlebooks_books.csv NO existe")
    failed_checks += 1

# ============================================================================
# 3. VERIFICAR ARCHIVOS DE SALIDA (STANDARD)
# ============================================================================
print_header("3. ARCHIVOS DE SALIDA (STANDARD)")

# dim_book.parquet
if os.path.exists('standard/dim_book.parquet'):
    try:
        df_dim = pd.read_parquet('standard/dim_book.parquet')
        num_records = len(df_dim)
        
        print_ok(f"dim_book.parquet existe ({num_records} registros)")
        
        # Verificar columnas requeridas
        required_cols = [
            'book_id', 'titulo', 'titulo_normalizado', 'autor_principal',
            'autores', 'editorial', 'anio_publicacion', 'fecha_publicacion',
            'idioma', 'isbn10', 'isbn13', 'categoria', 'precio', 'moneda',
            'rating_promedio', 'numero_ratings', 'fuente_ganadora', 'ts_ultima_actualizacion'
        ]
        
        missing_cols = [col for col in required_cols if col not in df_dim.columns]
        check(
            len(missing_cols) == 0,
            f"Todas las columnas requeridas presentes ({len(required_cols)} columnas)",
            f"Faltan columnas: {', '.join(missing_cols)}"
        )
        
        # Verificar que book_id no tenga nulos
        check(
            df_dim['book_id'].notna().all(),
            "Columna 'book_id' sin valores nulos",
            f"Columna 'book_id' tiene {df_dim['book_id'].isna().sum()} valores nulos"
        )
        
        # Verificar que titulo no tenga nulos
        check(
            df_dim['titulo'].notna().all(),
            "Columna 'titulo' sin valores nulos",
            f"Columna 'titulo' tiene {df_dim['titulo'].isna().sum()} valores nulos"
        )
        
        # Verificar que autores sea una columna (no autores_completo)
        check(
            'autores' in df_dim.columns,
            "Columna 'autores' presente (nombre correcto)",
            "Columna 'autores' NO presente"
        )
        
        if 'autores_completo' in df_dim.columns:
            warn("Columna 'autores_completo' presente (debería ser 'autores')")
        
        # Verificar normalización de fechas (ISO-8601)
        if 'fecha_publicacion' in df_dim.columns:
            fechas_validas = df_dim['fecha_publicacion'].dropna()
            if len(fechas_validas) > 0:
                # Verificar formato YYYY-MM-DD
                fecha_sample = str(fechas_validas.iloc[0])
                check(
                    len(fecha_sample) == 10 and fecha_sample[4] == '-' and fecha_sample[7] == '-',
                    f"Fechas en formato ISO-8601 (ej: {fecha_sample})",
                    f"Fechas NO en formato ISO-8601 (ej: {fecha_sample})"
                )
        
        # Verificar normalización de idiomas (BCP-47)
        if 'idioma' in df_dim.columns:
            idiomas = df_dim['idioma'].dropna().unique()
            if len(idiomas) > 0:
                # Los códigos BCP-47 son 2-3 letras en minúscula
                idiomas_validos = all(
                    isinstance(lang, str) and 2 <= len(lang) <= 5 and lang.islower()
                    for lang in idiomas
                )
                if idiomas_validos:
                    print_ok(f"Idiomas en formato BCP-47 (ej: {idiomas[0]})")
                else:
                    print_error(f"Idiomas NO en formato BCP-47 (ej: {idiomas[0]})")
                    failed_checks += 1
        
        # Verificar normalización de monedas (ISO-4217)
        if 'moneda' in df_dim.columns:
            monedas = df_dim['moneda'].dropna().unique()
            if len(monedas) > 0:
                # Los códigos ISO-4217 son 3 letras en mayúscula
                monedas_validas = all(
                    isinstance(curr, str) and len(curr) == 3 and curr.isupper()
                    for curr in monedas
                )
                if monedas_validas:
                    print_ok(f"Monedas en formato ISO-4217 (ej: {monedas[0]})")
                else:
                    print_error(f"Monedas NO en formato ISO-4217 (ej: {monedas[0]})")
                    failed_checks += 1
        
        # Verificar fuente_ganadora
        if 'fuente_ganadora' in df_dim.columns:
            fuentes = df_dim['fuente_ganadora'].dropna().unique()
            check(
                all(f in ['goodreads', 'googlebooks'] for f in fuentes),
                f"Fuentes ganadoras válidas: {', '.join(fuentes)}",
                f"Fuentes ganadoras inválidas encontradas"
            )
        
        print_info(f"Resumen dim_book.parquet:")
        print_info(f"  - Total registros: {len(df_dim)}")
        print_info(f"  - Total columnas: {len(df_dim.columns)}")
        print_info(f"  - Registros con ISBN13: {df_dim['isbn13'].notna().sum()}")
        print_info(f"  - Registros con precio: {df_dim['precio'].notna().sum()}")
        print_info(f"  - Registros con rating: {df_dim['rating_promedio'].notna().sum()}")
        
    except Exception as e:
        print_error(f"Error leyendo dim_book.parquet: {e}")
        failed_checks += 1
else:
    print_error("dim_book.parquet NO existe")
    failed_checks += 1

# book_source_detail.parquet
if os.path.exists('standard/book_source_detail.parquet'):
    try:
        df_detail = pd.read_parquet('standard/book_source_detail.parquet')
        num_records = len(df_detail)
        
        print_ok(f"book_source_detail.parquet existe ({num_records} registros)")
        
        # Verificar que tenga ambas fuentes
        if 'source_name' in df_detail.columns:
            fuentes = df_detail['source_name'].value_counts()
            
            check(
                'goodreads' in fuentes,
                f"Contiene registros de Goodreads ({fuentes.get('goodreads', 0)} registros)",
                "NO contiene registros de Goodreads"
            )
            
            check(
                'googlebooks' in fuentes,
                f"Contiene registros de Google Books ({fuentes.get('googlebooks', 0)} registros)",
                "NO contiene registros de Google Books"
            )
            
            # Verificar balance
            gr_count = fuentes.get('goodreads', 0)
            gb_count = fuentes.get('googlebooks', 0)
            
            if gr_count > 0 and gb_count > 0:
                check(
                    gr_count == gb_count,
                    f"Balance correcto: {gr_count} Goodreads, {gb_count} Google Books",
                    f"Desbalance: {gr_count} Goodreads, {gb_count} Google Books"
                )
        
        print_info(f"Resumen book_source_detail.parquet:")
        print_info(f"  - Total registros: {len(df_detail)}")
        print_info(f"  - Total columnas: {len(df_detail.columns)}")
        
    except Exception as e:
        print_error(f"Error leyendo book_source_detail.parquet: {e}")
        failed_checks += 1
else:
    print_error("book_source_detail.parquet NO existe")
    failed_checks += 1

# ============================================================================
# 4. VERIFICAR DOCUMENTACIÓN
# ============================================================================
print_header("4. DOCUMENTACIÓN")

# quality_metrics.json
if os.path.exists('docs/quality_metrics.json'):
    try:
        with open('docs/quality_metrics.json', 'r', encoding='utf-8') as f:
            metrics = json.load(f)
        
        print_ok("quality_metrics.json existe y es válido")
        
        # Verificar secciones
        expected_sections = ['pipeline_execution', 'source_breakdown', 'quality_checks']
        missing_sections = [s for s in expected_sections if s not in metrics]
        
        if len(missing_sections) == 0:
            print_ok(f"Todas las secciones presentes: {', '.join(expected_sections)}")
        else:
            warn(f"Faltan secciones en metrics: {', '.join(missing_sections)}")
        
    except Exception as e:
        print_error(f"Error leyendo quality_metrics.json: {e}")
        failed_checks += 1
else:
    print_error("quality_metrics.json NO existe")
    failed_checks += 1

# schema.md
check(
    os.path.exists('docs/schema.md'),
    "schema.md existe",
    "schema.md NO existe"
)

# ============================================================================
# 5. VERIFICAR INTEGRIDAD DE DATOS
# ============================================================================
print_header("5. INTEGRIDAD DE DATOS")

if os.path.exists('standard/dim_book.parquet') and os.path.exists('standard/book_source_detail.parquet'):
    df_dim = pd.read_parquet('standard/dim_book.parquet')
    df_detail = pd.read_parquet('standard/book_source_detail.parquet')
    
    # Verificar que el número de registros en detail sea razonable
    expected_detail = len(df_dim) * 2  # Aproximadamente, antes de deduplicar
    
    if abs(len(df_detail) - expected_detail) < 5:
        print_ok(f"Número de registros en detail es coherente ({len(df_detail)} ≈ {expected_detail})")
    else:
        warn(f"Número de registros en detail inusual ({len(df_detail)}, esperado ≈ {expected_detail})")
    
    # Verificar deduplicación
    if 'book_id' in df_dim.columns:
        duplicates = df_dim['book_id'].duplicated().sum()
        check(
            duplicates == 0,
            f"Sin duplicados en book_id (0 duplicados)",
            f"Hay {duplicates} book_ids duplicados"
        )

# ============================================================================
# RESUMEN FINAL
# ============================================================================
print_header("RESUMEN DE VERIFICACIÓN")

print(f"\n{BLUE}Estadísticas:{RESET}")
print(f"  Total de verificaciones: {total_checks}")
print(f"  {GREEN}✓ Pasadas: {passed_checks}{RESET}")
print(f"  {RED}✗ Fallidas: {failed_checks}{RESET}")
print(f"  {YELLOW}⚠ Advertencias: {warnings}{RESET}")

print(f"\n{BLUE}Resultado:{RESET}")
if failed_checks == 0:
    if warnings == 0:
        print(f"  {GREEN}✓✓✓ TODO PERFECTO - El pipeline funciona correctamente{RESET}")
    else:
        print(f"  {YELLOW}✓ FUNCIONAL CON ADVERTENCIAS - El pipeline funciona pero revisa las advertencias{RESET}")
else:
    print(f"  {RED}✗ HAY PROBLEMAS - {failed_checks} verificaciones fallaron{RESET}")

print("\n" + "=" * 80 + "\n")
