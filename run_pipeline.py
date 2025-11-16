#!/usr/bin/env python3
"""
Script maestro para ejecutar el pipeline completo de libros
Ejecuta los 3 ejercicios en orden: Scraping → Enriquecimiento → Integración
"""

import sys
import os
from datetime import datetime

# Añadir src al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))


def print_banner(text):
    """Imprime un banner decorativo"""
    print("\n" + "="*70)
    print(f"  {text}")
    print("="*70 + "\n")


def main():
    """Ejecuta el pipeline completo"""
    
    print_banner("BOOKS PIPELINE - EJECUCIÓN COMPLETA")
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # Importar módulos
        from scrape_goodreads import main as scrape_main
        from enrich_googlebooks import main as enrich_main
        from integrate_pipeline import main as integrate_main
        
        # Ejercicio 1: Scraping
        print_banner("EJERCICIO 1: SCRAPING DE GOODREADS")
        scrape_main()
        
        # Ejercicio 2: Enriquecimiento
        print_banner("EJERCICIO 2: ENRIQUECIMIENTO CON GOOGLE BOOKS")
        enrich_main()
        
        # Ejercicio 3: Integración
        print_banner("EJERCICIO 3: INTEGRACIÓN Y ESTANDARIZACIÓN")
        integrate_main()
        
        # Resumen final
        print_banner("PIPELINE COMPLETADO EXITOSAMENTE")
        print("Archivos generados:")
        print("  📄 landing/goodreads_books.json")
        print("  📄 landing/googlebooks_books.csv")
        print("  📊 standard/dim_book.parquet")
        print("  📊 standard/book_source_detail.parquet")
        print("  📋 docs/quality_metrics.json")
        print("  📖 docs/schema.md")
        print(f"\nFin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("\n" + "="*70)
        print("✅ PIPELINE COMPLETADO EXITOSAMENTE")
        print("="*70)
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
