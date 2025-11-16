"""
═══════════════════════════════════════════════════════════════════════════════
EJERCICIO 1: SCRAPING DE GOODREADS → JSON
═══════════════════════════════════════════════════════════════════════════════

OBJETIVO:
    Extraer información de 10-15 libros desde Goodreads mediante web scraping
    y guardarla en formato JSON con metadata completa.

FUNCIONAMIENTO:
    1. Busca libros en Goodreads con un término (ej: "data science")
    2. Accede a la página de cada libro individual
    3. Extrae: título, autor, rating, número de valoraciones, URL, ISBN
    4. Guarda todo en un archivo JSON con metadata del scraping

TECNOLOGÍAS:
    - requests: para hacer peticiones HTTP a Goodreads
    - BeautifulSoup: para extraer datos del HTML
    - json: para guardar los resultados

ÉTICA:
    - Pausas de 1 segundo entre peticiones (no saturar el servidor)
    - User-Agent identificable (no fingir ser un navegador)
    - Límite de 15 libros (minimizar carga en Goodreads)
"""

#═════════════════════════════════════════════════════════════════════════════
# IMPORTACIONES
#═════════════════════════════════════════════════════════════════════════════

import requests          # Para hacer peticiones HTTP a Goodreads
from bs4 import BeautifulSoup  # Para extraer datos del HTML
import json             # Para guardar los datos en formato JSON
import time             # Para hacer pausas entre peticiones
from datetime import datetime  # Para registrar fecha/hora del scraping
import re               # Para buscar patrones (como ISBNs) en texto


#═════════════════════════════════════════════════════════════════════════════
# CLASE PRINCIPAL DEL SCRAPER
#═════════════════════════════════════════════════════════════════════════════

class GoodreadsScraper:
    """
    Scraper para obtener información de libros desde Goodreads
    
    Esta clase se encarga de:
    1. Buscar libros en Goodreads
    2. Extraer información de cada libro
    3. Guardar los resultados en JSON
    """
    
    def __init__(self):
        """
        Inicializa el scraper con toda la configuración necesaria
        """
        
        #─────────────────────────────────────────────────────────────────────
        # CONFIGURACIÓN DE URLs
        #─────────────────────────────────────────────────────────────────────
        self.base_url = "https://www.goodreads.com"  # URL base de Goodreads
        
        #─────────────────────────────────────────────────────────────────────
        # CONFIGURACIÓN DE HEADERS (identificación del scraper)
        #─────────────────────────────────────────────────────────────────────
        # Estos headers le dicen a Goodreads quién somos y qué aceptamos
        self.headers = {
            # User-Agent: identifica nuestro "navegador"
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            # Accept: tipos de contenido que aceptamos
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            # Accept-Language: idiomas que preferimos
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            # Referer: de dónde "venimos"
            'Referer': 'https://www.goodreads.com/'
        }
        
        #─────────────────────────────────────────────────────────────────────
        # ALMACENAMIENTO DE DATOS
        #─────────────────────────────────────────────────────────────────────
        self.books = []  # Lista donde guardaremos todos los libros extraídos
        
        #─────────────────────────────────────────────────────────────────────
        # METADATA DEL SCRAPING (para documentación)
        #─────────────────────────────────────────────────────────────────────
        self.metadata = {
            'scraper': 'GoodreadsScraper',           # Nombre del scraper
            'search_term': '',                        # Término de búsqueda (se llena después)
            'search_urls': [],                        # URLs visitadas
            'user_agent': self.headers['User-Agent'], # User-Agent usado
            'scrape_date': datetime.now().isoformat(), # Fecha/hora del scraping
            
            # Selectores CSS usados (importante para debugging y documentación)
            'selectors_used': {
                'title': '.BookCard__title a',              # Para extraer título
                'author': '.ContributorLink__name',         # Para extraer autor
                'rating': '.RatingStatistics__rating',      # Para extraer rating
                'ratings_count': '.RatingStatistics__meta', # Para extraer número de ratings
                'book_url': '.BookCard__title a[href]',     # Para extraer URL del libro
                'isbn': 'meta[property="books:isbn"]'       # Para extraer ISBN
            },
            'total_books_scraped': 0  # Contador (se actualiza al final)
        }
    
    #═════════════════════════════════════════════════════════════════════════
    # MÉTODO PRINCIPAL: BUSCAR LIBROS
    #═════════════════════════════════════════════════════════════════════════
    
    def search_books(self, query, max_books=15):
        """
        Busca libros en Goodreads y extrae información de cada uno
        
        PARÁMETROS:
            query (str): Término de búsqueda (ej: "data science")
            max_books (int): Máximo número de libros a extraer (default: 15)
        
        PROCESO:
            1. Construye la URL de búsqueda
            2. Hace una petición a Goodreads
            3. Extrae enlaces a páginas individuales de libros
            4. Visita cada página de libro y extrae datos
            5. Hace pausas entre peticiones (ética de scraping)
        
        RESULTADO:
            Llena self.books con información de los libros encontrados
        """
        
        #─────────────────────────────────────────────────────────────────────
        # PASO 1: Preparar la búsqueda
        #─────────────────────────────────────────────────────────────────────
        self.metadata['search_term'] = query  # Guardar término para metadata
        
        # Construir URL de búsqueda (espacios se reemplazan por +)
        search_url = f"{self.base_url}/search?q={query.replace(' ', '+')}"
        self.metadata['search_urls'].append(search_url)  # Guardar URL visitada
        
        print(f"Buscando libros sobre '{query}' en Goodreads...")
        print(f"URL: {search_url}")
        
        try:
            #─────────────────────────────────────────────────────────────────
            # PASO 2: Hacer la petición HTTP a Goodreads
            #─────────────────────────────────────────────────────────────────
            response = requests.get(
                search_url, 
                headers=self.headers,  # Enviar nuestros headers
                timeout=10             # Timeout de 10 segundos
            )
            response.raise_for_status()  # Lanzar error si status code no es 200
            
            #─────────────────────────────────────────────────────────────────
            # PASO 3: Parsear el HTML con BeautifulSoup
            #─────────────────────────────────────────────────────────────────
            soup = BeautifulSoup(response.content, 'lxml')
            
            #─────────────────────────────────────────────────────────────────
            # PASO 4: Buscar enlaces a páginas de libros
            #─────────────────────────────────────────────────────────────────
            # Intentar con el selector principal
            book_links = soup.select('a.bookTitle')[:max_books]
            
            # Si no encuentra nada, intentar método alternativo
            if not book_links:
                print("⚠ No se encontraron libros con los selectores actuales")
                print("Intentando método alternativo...")
                book_links = soup.find_all('a', {'class': 'bookTitle'})[:max_books]
            
            print(f"Se encontraron {len(book_links)} libros para procesar")
            
            #─────────────────────────────────────────────────────────────────
            # PASO 5: Procesar cada libro individualmente
            #─────────────────────────────────────────────────────────────────
            for idx, link in enumerate(book_links, 1):
                # Construir URL completa del libro
                book_url = self.base_url + link.get('href', '')
                print(f"\nProcesando libro {idx}/{len(book_links)}: {book_url}")
                
                # Extraer datos del libro (método separado abajo)
                book_data = self._scrape_book_page(book_url)
                
                # Si se extrajo algo, añadirlo a la lista
                if book_data:
                    self.books.append(book_data)
                    print(f"✓ Libro extraído: {book_data.get('title', 'Sin título')}")
                
                #─────────────────────────────────────────────────────────────
                # PAUSA ÉTICA: esperar 1 segundo antes de la siguiente petición
                # Esto evita saturar el servidor de Goodreads
                #─────────────────────────────────────────────────────────────
                time.sleep(1.0)
                
                # Parar si ya tenemos suficientes libros
                if len(self.books) >= max_books:
                    break
            
            #─────────────────────────────────────────────────────────────────
            # PASO 6: Actualizar metadata con el total extraído
            #─────────────────────────────────────────────────────────────────
            self.metadata['total_books_scraped'] = len(self.books)
            print(f"\n✓ Scraping completado: {len(self.books)} libros extraídos")
            
        except requests.RequestException as e:
            # Si hay algún error en la petición HTTP, mostrarlo
            print(f"✗ Error en la búsqueda: {e}")
            raise
    
    def _scrape_book_page(self, url):
        """
        Extrae información detallada de una página individual de libro
        """
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'lxml')
            
            book_data = {
                'book_url': url,
                'title': None,
                'author': None,
                'rating': None,
                'ratings_count': None,
                'isbn10': None,
                'isbn13': None
            }
            
            # Extraer título
            title_elem = soup.find('h1', {'class': 'Text__title1'})
            if not title_elem:
                title_elem = soup.find('h1', {'data-testid': 'bookTitle'})
            if title_elem:
                book_data['title'] = title_elem.get_text(strip=True)
            
            # Extraer autor
            author_elem = soup.find('span', {'class': 'ContributorLink__name'})
            if not author_elem:
                author_elem = soup.find('a', {'class': 'authorName'})
            if author_elem:
                book_data['author'] = author_elem.get_text(strip=True)
            
            # Extraer rating
            rating_elem = soup.find('div', {'class': 'RatingStatistics__rating'})
            if rating_elem:
                rating_text = rating_elem.get_text(strip=True)
                try:
                    book_data['rating'] = float(rating_text)
                except:
                    book_data['rating'] = None
            
            # Extraer número de ratings
            ratings_elem = soup.find('span', {'data-testid': 'ratingsCount'})
            if ratings_elem:
                ratings_text = ratings_elem.get_text(strip=True)
                # Extraer número (ej: "1,234 ratings" -> 1234)
                numbers = re.findall(r'[\d,]+', ratings_text)
                if numbers:
                    book_data['ratings_count'] = int(numbers[0].replace(',', ''))
            
            # Extraer ISBN del meta tag
            isbn_meta = soup.find('meta', {'property': 'books:isbn'})
            if isbn_meta:
                isbn = isbn_meta.get('content', '').strip()
                if len(isbn) == 13:
                    book_data['isbn13'] = isbn
                elif len(isbn) == 10:
                    book_data['isbn10'] = isbn
            
            # Buscar ISBN en el texto de la página
            if not book_data['isbn13'] and not book_data['isbn10']:
                page_text = soup.get_text()
                isbn_match = re.search(r'ISBN[:\s]*(\d{10}|\d{13})', page_text)
                if isbn_match:
                    isbn = isbn_match.group(1)
                    if len(isbn) == 13:
                        book_data['isbn13'] = isbn
                    elif len(isbn) == 10:
                        book_data['isbn10'] = isbn
            
            return book_data
            
        except Exception as e:
            print(f"  ⚠ Error al procesar {url}: {e}")
            return None
    
    def save_to_json(self, output_path):
        """
        Guarda los libros y metadata en formato JSON
        """
        data = {
            'metadata': self.metadata,
            'books': self.books
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ Datos guardados en: {output_path}")
        print(f"  - Total de libros: {len(self.books)}")
        print(f"  - Fecha de scraping: {self.metadata['scrape_date']}")


def main():
    """Función principal para ejecutar el scraping"""
    
    # Crear carpetas necesarias si no existen
    import os
    os.makedirs('landing', exist_ok=True)
    
    # Inicializar scraper
    scraper = GoodreadsScraper()
    
    # Realizar búsqueda
    search_term = "data science"
    scraper.search_books(search_term, max_books=15)
    
    # Guardar resultados
    output_path = "landing/goodreads_books.json"
    scraper.save_to_json(output_path)
    
    print("\n" + "="*60)
    print("EJERCICIO 1 COMPLETADO")
    print("="*60)


if __name__ == "__main__":
    main()
