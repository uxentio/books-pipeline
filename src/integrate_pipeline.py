"""
EJERCICIO 3: Integración y estandarización → Parquet (standard/)

VERSIÓN MEJORADA - Compatible con ejecución desde raíz o src/
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import hashlib
import re
import os


class DataIntegrator:
    """Integra datos de Goodreads y Google Books en un modelo canónico"""
    
    def __init__(self, landing_dir=None, standard_dir=None, docs_dir=None):
        # Detectar directorio base automáticamente
        current_dir = Path.cwd()
        
        # Si estamos en src/, subir un nivel
        if current_dir.name == 'src':
            base_dir = current_dir.parent
        else:
            base_dir = current_dir
        
        # Usar directorios proporcionados o calcular relativos al base_dir
        self.landing_dir = Path(landing_dir) if landing_dir else base_dir / "landing"
        self.standard_dir = Path(standard_dir) if standard_dir else base_dir / "standard"
        self.docs_dir = Path(docs_dir) if docs_dir else base_dir / "docs"
        
        # Crear directorios si no existen
        self.standard_dir.mkdir(exist_ok=True)
        self.docs_dir.mkdir(exist_ok=True)
        
        # Dataframes
        self.goodreads_df = None
        self.googlebooks_df = None
        self.dim_book = None
        self.book_source_detail = None
        
        # Métricas
        self.metrics = {
            'execution_date': datetime.now().isoformat(),
            'pipeline_execution': {},
            'source_breakdown': {},
            'source_files': {},
            'record_counts': {},
            'quality_checks': {},
            'data_quality': {},
            'deduplication': {}
        }
    
    def normalize_title_for_matching(self, title):
        """Normaliza un título para facilitar el emparejamiento"""
        if pd.isna(title) or not title:
            return ""
        
        title = str(title).lower()
        title = title.split(':')[0]
        title = re.sub(r'[^a-z\s]', '', title)
        title = ' '.join(title.split())
        
        return title.strip()
    
    def extract_year_from_date(self, date_str):
        """Extrae el año de una fecha"""
        if pd.isna(date_str):
            return None
        
        date_str = str(date_str)
        match = re.search(r'\b(19|20)\d{2}\b', date_str)
        if match:
            return int(match.group())
        
        return None
    
    def load_goodreads_data(self):
        """Carga datos de Goodreads"""
        json_path = self.landing_dir / "goodreads_books.json"
        
        print(f"Cargando: {json_path}")
        
        if not json_path.exists():
            raise FileNotFoundError(f"No se encuentra el archivo: {json_path}")
        
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        books = data.get('books', [])
        self.goodreads_df = pd.DataFrame(books)
        
        self.goodreads_df['source_index'] = range(len(self.goodreads_df))
        self.goodreads_df['titulo_normalizado'] = self.goodreads_df['title'].apply(
            self.normalize_title_for_matching
        )
        
        print(f"  ✓ {len(self.goodreads_df)} registros cargados de Goodreads")
        
        self.metrics['source_files']['goodreads'] = {
            'file': str(json_path),
            'records': len(self.goodreads_df),
            'load_date': datetime.now().isoformat()
        }
        
        self.metrics['source_breakdown']['goodreads'] = {
            'total_records': len(self.goodreads_df),
            'records_with_isbn': int(self.goodreads_df['isbn13'].notna().sum() if 'isbn13' in self.goodreads_df.columns else 0),
            'records_with_rating': int(self.goodreads_df['rating'].notna().sum() if 'rating' in self.goodreads_df.columns else 0)
        }
        
        return self.goodreads_df
    
    def load_googlebooks_data(self):
        """Carga datos de Google Books"""
        csv_path = self.landing_dir / "googlebooks_books.csv"
        
        print(f"Cargando: {csv_path}")
        
        if not csv_path.exists():
            raise FileNotFoundError(f"No se encuentra el archivo: {csv_path}")
        
        self.googlebooks_df = pd.read_csv(csv_path)
        
        self.googlebooks_df['source_index'] = range(len(self.googlebooks_df))
        self.googlebooks_df['titulo_normalizado'] = self.googlebooks_df['title'].apply(
            self.normalize_title_for_matching
        )
        
        print(f"  ✓ {len(self.googlebooks_df)} registros cargados de Google Books")
        
        self.metrics['source_files']['googlebooks'] = {
            'file': str(csv_path),
            'records': len(self.googlebooks_df),
            'load_date': datetime.now().isoformat()
        }
        
        self.metrics['source_breakdown']['googlebooks'] = {
            'total_records': len(self.googlebooks_df),
            'records_with_isbn': int(self.googlebooks_df['isbn13'].notna().sum()),
            'records_with_price': int(self.googlebooks_df['price_amount'].notna().sum())
        }
        
        return self.googlebooks_df
    
    def match_books_by_title(self):
        """Empareja libros de Goodreads con Google Books usando título normalizado"""
        print("\nEmparejando libros por título normalizado...")
        
        matches = []
        
        for idx, gr_row in self.goodreads_df.iterrows():
            gr_title_norm = gr_row['titulo_normalizado']
            
            gb_match = self.googlebooks_df[
                self.googlebooks_df['titulo_normalizado'] == gr_title_norm
            ]
            
            if len(gb_match) > 0:
                gb_row = gb_match.iloc[0]
                matches.append({
                    'goodreads_index': gr_row['source_index'],
                    'googlebooks_index': gb_row['source_index'],
                    'matched_by': 'title',
                    'confidence': 'high'
                })
                print(f"  ✓ Match: '{gr_row['title'][:50]}...'")
            else:
                print(f"  ⚠ No match: '{gr_row['title'][:50]}...'")
                matches.append({
                    'goodreads_index': gr_row['source_index'],
                    'googlebooks_index': None,
                    'matched_by': None,
                    'confidence': None
                })
        
        matches_df = pd.DataFrame(matches)
        matched_count = matches_df['googlebooks_index'].notna().sum()
        
        print(f"\n  Resultado: {matched_count}/{len(self.goodreads_df)} libros emparejados")
        
        self.metrics['deduplication']['matching'] = {
            'total_books': len(self.goodreads_df),
            'matched': int(matched_count),
            'unmatched': int(len(self.goodreads_df) - matched_count),
            'match_rate': f"{(matched_count / len(self.goodreads_df) * 100):.1f}%"
        }
        
        return matches_df
    
    def create_unified_books(self, matches_df):
        """Crea un DataFrame unificado combinando datos de ambas fuentes"""
        print("\nCreando registros unificados...")
        
        unified_books = []
        
        for idx, match in matches_df.iterrows():
            gr_idx = match['goodreads_index']
            gb_idx = match['googlebooks_index']
            
            gr_book = self.goodreads_df.iloc[gr_idx]
            
            if pd.notna(gb_idx):
                gb_book = self.googlebooks_df.iloc[int(gb_idx)]
                has_gb_data = True
            else:
                gb_book = None
                has_gb_data = False
            
            # SUPERVIVENCIA: Combinar datos
            
            if has_gb_data and pd.notna(gb_book.get('isbn13')):
                isbn13 = str(gb_book['isbn13'])
                isbn_source = 'googlebooks'
            elif pd.notna(gr_book.get('isbn13')):
                isbn13 = gr_book['isbn13']
                isbn_source = 'goodreads'
            else:
                isbn13 = None
                isbn_source = None
            
            if has_gb_data and pd.notna(gb_book.get('isbn10')):
                isbn10 = str(gb_book['isbn10'])
            elif pd.notna(gr_book.get('isbn10')):
                isbn10 = gr_book['isbn10']
            else:
                isbn10 = None
            
            gr_title = gr_book['title']
            gb_title = gb_book['title'] if has_gb_data else None
            
            if gb_title and len(str(gb_title)) > len(str(gr_title)):
                titulo = gb_title
                titulo_source = 'googlebooks'
            else:
                titulo = gr_title
                titulo_source = 'goodreads'
            
            gr_author = gr_book['author']
            gb_authors = gb_book['authors'] if has_gb_data and pd.notna(gb_book.get('authors')) else None
            
            if gb_authors:
                autores_list = [a.strip() for a in str(gb_authors).split(',')]
                autor_principal = autores_list[0]
                autores_completo = autores_list
                autor_source = 'googlebooks'
            else:
                autor_principal = gr_author
                autores_completo = [gr_author] if gr_author else []
                autor_source = 'goodreads'
            
            rating_promedio = gr_book.get('rating')
            numero_ratings = gr_book.get('ratings_count')
            rating_source = 'goodreads' if pd.notna(rating_promedio) else None
            
            editorial = gb_book.get('publisher') if has_gb_data else None
            fecha_publicacion = gb_book.get('pub_date') if has_gb_data else None
            anio_publicacion = self.extract_year_from_date(fecha_publicacion) if fecha_publicacion else None
            idioma = gb_book.get('language') if has_gb_data else None
            
            categorias_raw = gb_book.get('categories') if has_gb_data else None
            if pd.notna(categorias_raw):
                if isinstance(categorias_raw, str):
                    categoria = [c.strip() for c in categorias_raw.split(',')]
                else:
                    categoria = [str(categorias_raw)]
            else:
                categoria = []
            
            precio = gb_book.get('price_amount') if has_gb_data else None
            moneda = gb_book.get('price_currency') if has_gb_data else None
            precio_source = 'googlebooks' if pd.notna(precio) else None
            
            goodreads_url = gr_book.get('book_url')
            gb_id = gb_book.get('gb_id') if has_gb_data else None
            
            if isbn13:
                book_id = f"ISBN13:{isbn13}"
            elif isbn10:
                book_id = f"ISBN10:{isbn10}"
            else:
                hash_input = f"{titulo}|{autor_principal}|{editorial or ''}"
                book_id = f"HASH:{hashlib.md5(hash_input.encode()).hexdigest()[:12]}"
            
            fuentes_score = {
                'goodreads': (1 if rating_promedio else 0) + (1 if gr_author else 0),
                'googlebooks': (1 if isbn13 else 0) + (1 if editorial else 0) + (1 if precio else 0)
            }
            fuente_ganadora = max(fuentes_score, key=fuentes_score.get)
            
            unified_book = {
                'book_id': book_id,
                'titulo': titulo,
                'titulo_normalizado': self.normalize_title_for_matching(titulo),
                'autor_principal': autor_principal,
                'autores': autores_completo,
                'editorial': editorial,
                'anio_publicacion': anio_publicacion,
                'fecha_publicacion': fecha_publicacion,
                'idioma': idioma,
                'isbn10': isbn10,
                'isbn13': isbn13,
                'categoria': categoria,
                'rating_promedio': rating_promedio,
                'numero_ratings': numero_ratings,
                'precio': precio,
                'moneda': moneda,
                'goodreads_url': goodreads_url,
                'google_books_id': gb_id,
                'fuente_ganadora': fuente_ganadora,
                'fuente_titulo': titulo_source,
                'fuente_isbn': isbn_source,
                'fuente_autor': autor_source,
                'fuente_rating': rating_source,
                'fuente_precio': precio_source,
                'tiene_datos_goodreads': True,
                'tiene_datos_googlebooks': has_gb_data,
                'ts_ultima_actualizacion': datetime.now().isoformat()
            }
            
            unified_books.append(unified_book)
        
        unified_df = pd.DataFrame(unified_books)
        
        print(f"  ✓ {len(unified_df)} libros unificados creados")
        print(f"  ✓ {unified_df['tiene_datos_googlebooks'].sum()} con datos de Google Books")
        print(f"  ✓ {(~unified_df['tiene_datos_googlebooks']).sum()} solo con datos de Goodreads")
        
        return unified_df
    
    def create_dim_book(self, unified_df):
        """Crea la tabla dimensional dim_book"""
        print("\nCreando dim_book.parquet...")
        
        self.dim_book = unified_df[[
            'book_id',
            'titulo',
            'titulo_normalizado',
            'autor_principal',
            'autores',
            'editorial',
            'anio_publicacion',
            'fecha_publicacion',
            'idioma',
            'isbn10',
            'isbn13',
            'categoria',
            'rating_promedio',
            'numero_ratings',
            'precio',
            'moneda',
            'goodreads_url',
            'google_books_id',
            'fuente_ganadora',
            'fuente_titulo',
            'ts_ultima_actualizacion'
        ]].copy()
        
        self.dim_book['autores'] = self.dim_book['autores'].apply(
            lambda x: ','.join(x) if isinstance(x, list) else str(x) if pd.notna(x) else None
        )
        
        self.dim_book['categoria'] = self.dim_book['categoria'].apply(
            lambda x: ','.join(x) if isinstance(x, list) and len(x) > 0 else None
        )
        
        output_path = self.standard_dir / "dim_book.parquet"
        self.dim_book.to_parquet(output_path, index=False)
        
        print(f"  ✓ dim_book.parquet guardado ({len(self.dim_book)} registros)")
        print(f"  ✓ {len(self.dim_book.columns)} columnas incluidas")
        
        self.metrics['record_counts']['dim_book_total'] = len(self.dim_book)
        self.metrics['record_counts']['dim_book_with_isbn'] = int(self.dim_book['isbn13'].notna().sum())
        self.metrics['record_counts']['dim_book_with_price'] = int(self.dim_book['precio'].notna().sum())
        self.metrics['record_counts']['dim_book_with_rating'] = int(self.dim_book['rating_promedio'].notna().sum())
        
        return self.dim_book
    
    def create_book_source_detail(self, matches_df):
        """Crea la tabla de detalle por fuente"""
        print("\nCreando book_source_detail.parquet...")
        
        detail_records = []
        
        for idx, gr_row in self.goodreads_df.iterrows():
            match = matches_df[matches_df['goodreads_index'] == gr_row['source_index']]
            
            if len(match) > 0:
                gb_idx = match.iloc[0]['googlebooks_index']
                if pd.notna(gb_idx):
                    gb_book = self.googlebooks_df.iloc[int(gb_idx)]
                    isbn13_for_id = gb_book.get('isbn13')
                else:
                    isbn13_for_id = None
            else:
                isbn13_for_id = None
            
            if pd.notna(isbn13_for_id):
                book_id = f"ISBN13:{isbn13_for_id}"
            else:
                hash_input = f"{gr_row['title']}|{gr_row['author']}|"
                book_id = f"HASH:{hashlib.md5(hash_input.encode()).hexdigest()[:12]}"
            
            detail_records.append({
                'source_id': f"GR_{idx}",
                'source_name': 'goodreads',
                'source_file': 'goodreads_books.json',
                'source_index': int(gr_row['source_index']),
                'book_id': book_id,
                'titulo_original': gr_row['title'],
                'autor_original': gr_row['author'],
                'rating': gr_row.get('rating'),
                'ratings_count': gr_row.get('ratings_count'),
                'url': gr_row.get('book_url'),
                'isbn10': gr_row.get('isbn10'),
                'isbn13': gr_row.get('isbn13'),
                'ts_ingesta': datetime.now().isoformat()
            })
        
        for idx, gb_row in self.googlebooks_df.iterrows():
            match = matches_df[matches_df['googlebooks_index'] == gb_row['source_index']]
            
            if len(match) > 0:
                isbn13_for_id = gb_row.get('isbn13')
            else:
                isbn13_for_id = None
            
            if pd.notna(isbn13_for_id):
                book_id = f"ISBN13:{isbn13_for_id}"
            else:
                hash_input = f"{gb_row['title']}|{gb_row.get('authors', '')}|{gb_row.get('publisher', '')}"
                book_id = f"HASH:{hashlib.md5(hash_input.encode()).hexdigest()[:12]}"
            
            detail_records.append({
                'source_id': f"GB_{idx}",
                'source_name': 'googlebooks',
                'source_file': 'googlebooks_books.csv',
                'source_index': int(gb_row['source_index']),
                'book_id': book_id,
                'titulo_original': gb_row['title'],
                'autor_original': gb_row.get('authors'),
                'editorial': gb_row.get('publisher'),
                'fecha_publicacion': gb_row.get('pub_date'),
                'idioma': gb_row.get('language'),
                'isbn10': gb_row.get('isbn10'),
                'isbn13': gb_row.get('isbn13'),
                'precio': gb_row.get('price_amount'),
                'moneda': gb_row.get('price_currency'),
                'google_books_id': gb_row.get('gb_id'),
                'ts_ingesta': datetime.now().isoformat()
            })
        
        self.book_source_detail = pd.DataFrame(detail_records)
        
        output_path = self.standard_dir / "book_source_detail.parquet"
        self.book_source_detail.to_parquet(output_path, index=False)
        
        gr_count = int((self.book_source_detail['source_name'] == 'goodreads').sum())
        gb_count = int((self.book_source_detail['source_name'] == 'googlebooks').sum())
        
        print(f"  ✓ book_source_detail.parquet guardado ({len(self.book_source_detail)} registros)")
        print(f"    - {gr_count} de Goodreads")
        print(f"    - {gb_count} de Google Books")
        
        self.metrics['record_counts']['source_detail_total'] = len(self.book_source_detail)
        self.metrics['record_counts']['source_detail_goodreads'] = gr_count
        self.metrics['record_counts']['source_detail_googlebooks'] = gb_count
        
        return self.book_source_detail
    
    def generate_quality_metrics(self):
        """Genera métricas de calidad"""
        print("\nGenerando métricas de calidad...")
        
        if self.dim_book is not None:
            total_records = len(self.dim_book)
            
            self.metrics['data_quality'] = {
                'percent_valid_titles': round((self.dim_book['titulo'].notna().sum() / total_records) * 100, 2),
                'percent_valid_isbns': round((self.dim_book['isbn13'].notna().sum() / total_records) * 100, 2),
                'percent_with_rating': round((self.dim_book['rating_promedio'].notna().sum() / total_records) * 100, 2),
                'percent_with_price': round((self.dim_book['precio'].notna().sum() / total_records) * 100, 2),
                'percent_with_googlebooks_data': round((self.dim_book['google_books_id'].notna().sum() / total_records) * 100, 2),
                'percent_with_year': round((self.dim_book['anio_publicacion'].notna().sum() / total_records) * 100, 2)
            }
            
            self.metrics['quality_checks'] = {
                'total_books': total_records,
                'books_with_complete_metadata': int(
                    (self.dim_book['titulo'].notna() & 
                     self.dim_book['autor_principal'].notna() & 
                     self.dim_book['isbn13'].notna()).sum()
                ),
                'books_with_rating': int(self.dim_book['rating_promedio'].notna().sum()),
                'books_with_price': int(self.dim_book['precio'].notna().sum())
            }
        
        metrics_path = self.docs_dir / "quality_metrics.json"
        with open(metrics_path, 'w', encoding='utf-8') as f:
            json.dump(self.metrics, f, indent=2, ensure_ascii=False)
        
        print(f"  ✓ quality_metrics.json guardado")
        
        return self.metrics
    
    def run(self):
        """Ejecuta el pipeline completo"""
        start_time = datetime.now()
        
        print("="*80)
        print("INTEGRACIÓN Y ESTANDARIZACIÓN - Ejercicio 3")
        print("="*80)
        
        try:
            self.load_goodreads_data()
            self.load_googlebooks_data()
            
            matches_df = self.match_books_by_title()
            unified_df = self.create_unified_books(matches_df)
            self.create_dim_book(unified_df)
            self.create_book_source_detail(matches_df)
            self.generate_quality_metrics()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.metrics['execution_summary'] = {
                'status': 'success',
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'execution_time_seconds': round(duration, 2)
            }
            
            self.metrics['pipeline_execution'] = {
                'status': 'completed',
                'duration_seconds': round(duration, 2),
                'timestamp': end_time.isoformat()
            }
            
            metrics_path = self.docs_dir / "quality_metrics.json"
            with open(metrics_path, 'w', encoding='utf-8') as f:
                json.dump(self.metrics, f, indent=2, ensure_ascii=False)
            
            print("\n" + "="*80)
            print("✓ INTEGRACIÓN COMPLETADA EXITOSAMENTE")
            print("="*80)
            print(f"\nArchivos generados:")
            print(f"  • {self.standard_dir}/dim_book.parquet ({len(self.dim_book)} registros)")
            print(f"  • {self.standard_dir}/book_source_detail.parquet ({len(self.book_source_detail)} registros)")
            print(f"  • {self.docs_dir}/quality_metrics.json")
            print(f"\nTiempo de ejecución: {duration:.2f} segundos")
            print("="*80)
            
        except Exception as e:
            print(f"\n❌ ERROR: {e}")
            import traceback
            traceback.print_exc()
            
            self.metrics['execution_summary'] = {
                'status': 'failed',
                'error': str(e),
                'end_time': datetime.now().isoformat()
            }
            
            self.metrics['pipeline_execution'] = {
                'status': 'failed',
                'error': str(e)
            }
            
            metrics_path = self.docs_dir / "quality_metrics.json"
            with open(metrics_path, 'w', encoding='utf-8') as f:
                json.dump(self.metrics, f, indent=2, ensure_ascii=False)


def main():
    """Función principal"""
    integrator = DataIntegrator()
    integrator.run()


if __name__ == "__main__":
    main()
