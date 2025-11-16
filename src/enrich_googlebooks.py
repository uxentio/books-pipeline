"""
EJERCICIO 2: Enriquecimiento con Google Books API → CSV
Busca cada libro del JSON en Google Books y guarda información adicional en CSV
"""

import requests
import json
import time
import csv
import os
from dotenv import load_dotenv


class GoogleBooksEnricher:
    """Enriquece datos de libros usando la API de Google Books"""
    
    def __init__(self, api_key=None):
        load_dotenv()
        self.api_key = api_key or os.getenv('GOOGLE_BOOKS_API_KEY')
        self.base_url = "https://www.googleapis.com/books/v1/volumes"
        self.books_enriched = []
        
        if self.api_key:
            print("✓ Usando API Key de Google Books (mejor límite de requests)")
        else:
            print("⚠ No se encontró API Key - usando límite gratuito reducido")
    
    def enrich_from_json(self, json_path):
        """
        Lee el JSON de Goodreads y enriquece cada libro
        """
        print(f"Leyendo datos de: {json_path}")
        
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        books = data.get('books', [])
        print(f"Se encontraron {len(books)} libros para enriquecer")
        
        for idx, book in enumerate(books, 1):
            print(f"\n[{idx}/{len(books)}] Procesando: {book.get('title', 'Sin título')}")
            
            enriched = self._search_google_books(book)
            
            if enriched:
                self.books_enriched.append(enriched)
                print(f"  ✓ Libro enriquecido exitosamente")
            else:
                print(f"  ⚠ No se encontró información en Google Books")
            
            # Pausa para respetar límites de API
            time.sleep(0.5)
        
        print(f"\n✓ Enriquecimiento completado: {len(self.books_enriched)} libros")
    
    def _search_google_books(self, book):
        """
        Busca un libro en Google Books API
        Prioridad: ISBN13 > ISBN10 > título+autor
        """
        # Intentar búsqueda por ISBN (más precisa)
        if book.get('isbn13'):
            result = self._query_api(f"isbn:{book['isbn13']}")
            if result:
                return result
        
        if book.get('isbn10'):
            result = self._query_api(f"isbn:{book['isbn10']}")
            if result:
                return result
        
        # Fallback: búsqueda por título y autor
        if book.get('title') and book.get('author'):
            query = f"intitle:{book['title']}+inauthor:{book['author']}"
            result = self._query_api(query)
            if result:
                return result
        
        # Último intento: solo título
        if book.get('title'):
            result = self._query_api(f"intitle:{book['title']}")
            if result:
                return result
        
        return None
    
    def _query_api(self, query):
        """
        Realiza una query a la API de Google Books
        """
        params = {
            'q': query,
            'maxResults': 1,
            'printType': 'books'
        }
        
        if self.api_key:
            params['key'] = self.api_key
        
        try:
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('totalItems', 0) == 0:
                return None
            
            # Extraer información del primer resultado
            item = data['items'][0]
            return self._extract_book_info(item)
            
        except requests.RequestException as e:
            print(f"    Error en API request: {e}")
            return None
    
    def _extract_book_info(self, item):
        """
        Extrae campos relevantes de un item de Google Books
        """
        volume_info = item.get('volumeInfo', {})
        sale_info = item.get('saleInfo', {})
        
        # Extraer ISBNs
        isbn13 = None
        isbn10 = None
        
        for identifier in volume_info.get('industryIdentifiers', []):
            if identifier.get('type') == 'ISBN_13':
                isbn13 = identifier.get('identifier')
            elif identifier.get('type') == 'ISBN_10':
                isbn10 = identifier.get('identifier')
        
        # Extraer precio
        price_amount = None
        price_currency = None
        
        if sale_info.get('saleability') == 'FOR_SALE':
            retail_price = sale_info.get('retailPrice', {})
            price_amount = retail_price.get('amount')
            price_currency = retail_price.get('currencyCode')
        
        # Construir registro
        book_data = {
            'gb_id': item.get('id'),
            'title': volume_info.get('title'),
            'subtitle': volume_info.get('subtitle'),
            'authors': ', '.join(volume_info.get('authors', [])),
            'publisher': volume_info.get('publisher'),
            'pub_date': volume_info.get('publishedDate'),
            'language': volume_info.get('language'),
            'categories': ', '.join(volume_info.get('categories', [])),
            'isbn13': isbn13,
            'isbn10': isbn10,
            'price_amount': price_amount,
            'price_currency': price_currency
        }
        
        return book_data
    
    def save_to_csv(self, output_path):
        """
        Guarda los datos enriquecidos en CSV
        """
        if not self.books_enriched:
            print("⚠ No hay datos para guardar")
            return
        
        fieldnames = [
            'gb_id', 'title', 'subtitle', 'authors', 'publisher',
            'pub_date', 'language', 'categories', 'isbn13', 'isbn10',
            'price_amount', 'price_currency'
        ]
        
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.books_enriched)
        
        print(f"\n✓ Datos guardados en: {output_path}")
        print(f"  - Formato: CSV")
        print(f"  - Codificación: UTF-8")
        print(f"  - Separador: coma (,)")
        print(f"  - Total de registros: {len(self.books_enriched)}")
        print(f"  - Campos: {len(fieldnames)}")
        
        # Mostrar estadísticas de mapeo
        print("\n  Estadísticas de mapeo:")
        for field in ['isbn13', 'isbn10', 'price_amount']:
            non_null = sum(1 for book in self.books_enriched if book.get(field))
            pct = (non_null / len(self.books_enriched)) * 100
            print(f"    - {field}: {non_null}/{len(self.books_enriched)} ({pct:.1f}%)")


def main():
    """Función principal para ejecutar el enriquecimiento"""
    
    # Crear carpetas necesarias si no existen
    import os
    os.makedirs('landing', exist_ok=True)
    
    # Inicializar enriquecedor
    enricher = GoogleBooksEnricher()
    
    # Procesar JSON de Goodreads
    input_json = "landing/goodreads_books.json"
    enricher.enrich_from_json(input_json)
    
    # Guardar CSV
    output_csv = "landing/googlebooks_books.csv"
    enricher.save_to_csv(output_csv)
    
    print("\n" + "="*60)
    print("EJERCICIO 2 COMPLETADO")
    print("="*60)


if __name__ == "__main__":
    main()
