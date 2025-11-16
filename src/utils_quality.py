"""
Utilidades para control de calidad de datos
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json


class QualityChecker:
    """Clase para realizar chequeos de calidad en datasets"""
    
    def __init__(self):
        self.metrics = {}
        self.errors = []
        self.warnings = []
    
    def check_completeness(self, df, required_fields):
        """
        Verifica completitud de campos requeridos
        """
        metrics = {}
        for field in required_fields:
            if field not in df.columns:
                self.errors.append(f"Campo requerido ausente: {field}")
                metrics[field] = 0.0
            else:
                non_null_pct = (df[field].notna().sum() / len(df)) * 100
                metrics[field] = round(non_null_pct, 2)
                
                if non_null_pct < 90:
                    self.warnings.append(f"Campo {field} tiene solo {non_null_pct:.1f}% de completitud")
        
        return metrics
    
    def check_date_format(self, df, date_fields):
        """
        Verifica que las fechas estén en formato ISO-8601
        """
        metrics = {}
        for field in date_fields:
            if field not in df.columns:
                continue
            
            valid_count = 0
            total_count = df[field].notna().sum()
            
            if total_count == 0:
                metrics[field] = 0.0
                continue
            
            for val in df[field].dropna():
                if self._is_valid_iso_date(val):
                    valid_count += 1
            
            pct_valid = (valid_count / total_count) * 100 if total_count > 0 else 0
            metrics[field] = round(pct_valid, 2)
            
            if pct_valid < 100:
                self.warnings.append(f"Campo {field}: {pct_valid:.1f}% fechas válidas ISO-8601")
        
        return metrics
    
    def check_language_format(self, df, language_field='idioma'):
        """
        Verifica que los códigos de idioma sigan BCP-47
        """
        if language_field not in df.columns:
            return 0.0
        
        valid_count = 0
        total_count = df[language_field].notna().sum()
        
        if total_count == 0:
            return 0.0
        
        # Patrones BCP-47 comunes: es, en, en-US, pt-BR, etc.
        import re
        bcp47_pattern = r'^[a-z]{2,3}(-[A-Z]{2})?$'
        
        for val in df[language_field].dropna():
            if re.match(bcp47_pattern, str(val)):
                valid_count += 1
        
        pct_valid = (valid_count / total_count) * 100 if total_count > 0 else 0
        
        if pct_valid < 100:
            self.warnings.append(f"Idioma: {pct_valid:.1f}% códigos BCP-47 válidos")
        
        return round(pct_valid, 2)
    
    def check_currency_format(self, df, currency_field='moneda'):
        """
        Verifica que las monedas sigan ISO-4217
        """
        if currency_field not in df.columns:
            return 0.0
        
        valid_currencies = {'EUR', 'USD', 'GBP', 'JPY', 'CNY', 'INR', 'CAD', 'AUD', 'CHF', 'MXN', 'BRL', 'ARS'}
        
        valid_count = 0
        total_count = df[currency_field].notna().sum()
        
        if total_count == 0:
            return 0.0
        
        for val in df[currency_field].dropna():
            if str(val).upper() in valid_currencies:
                valid_count += 1
        
        pct_valid = (valid_count / total_count) * 100 if total_count > 0 else 0
        
        if pct_valid < 100:
            self.warnings.append(f"Moneda: {pct_valid:.1f}% códigos ISO-4217 válidos")
        
        return round(pct_valid, 2)
    
    def check_duplicates(self, df, key_fields):
        """
        Detecta duplicados basándose en campos clave
        """
        if not all(field in df.columns for field in key_fields):
            return 0
        
        duplicates = df.duplicated(subset=key_fields, keep=False).sum()
        
        if duplicates > 0:
            self.warnings.append(f"Se encontraron {duplicates} registros duplicados")
        
        return int(duplicates)
    
    def check_isbn_validity(self, df):
        """
        Verifica validez de ISBN-13 e ISBN-10
        """
        from utils_isbn import validate_isbn13, validate_isbn10
        
        metrics = {}
        
        if 'isbn13' in df.columns:
            valid_count = 0
            total_count = df['isbn13'].notna().sum()
            
            for val in df['isbn13'].dropna():
                if validate_isbn13(val):
                    valid_count += 1
            
            pct_valid = (valid_count / total_count) * 100 if total_count > 0 else 0
            metrics['isbn13_valid_pct'] = round(pct_valid, 2)
        
        if 'isbn10' in df.columns:
            valid_count = 0
            total_count = df['isbn10'].notna().sum()
            
            for val in df['isbn10'].dropna():
                if validate_isbn10(val):
                    valid_count += 1
            
            pct_valid = (valid_count / total_count) * 100 if total_count > 0 else 0
            metrics['isbn10_valid_pct'] = round(pct_valid, 2)
        
        return metrics
    
    def _is_valid_iso_date(self, date_str):
        """Verifica si una fecha está en formato ISO-8601"""
        if pd.isna(date_str):
            return False
        
        try:
            # Intentar parsear como ISO-8601
            pd.to_datetime(date_str, format='ISO8601')
            return True
        except:
            # Intentar formatos ISO comunes
            formats = ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%SZ']
            for fmt in formats:
                try:
                    datetime.strptime(str(date_str), fmt)
                    return True
                except:
                    continue
            return False
    
    def generate_report(self, df, dataset_name):
        """
        Genera un reporte completo de calidad
        """
        report = {
            'dataset': dataset_name,
            'timestamp': datetime.now().isoformat(),
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'metrics': {},
            'errors': self.errors.copy(),
            'warnings': self.warnings.copy()
        }
        
        # Añadir métricas acumuladas
        report['metrics'].update(self.metrics)
        
        return report
    
    def assert_quality(self, df, min_title_completeness=90):
        """
        Aserciones bloqueantes - detienen el pipeline si fallan
        """
        # Verificar que title tenga al menos 90% de completitud
        if 'titulo' in df.columns:
            title_completeness = (df['titulo'].notna().sum() / len(df)) * 100
            assert title_completeness >= min_title_completeness, \
                f"Completitud de título ({title_completeness:.1f}%) por debajo del mínimo ({min_title_completeness}%)"
        
        # Verificar que book_id sea único
        if 'book_id' in df.columns:
            duplicates = df['book_id'].duplicated().sum()
            assert duplicates == 0, f"Se encontraron {duplicates} book_id duplicados en dim_book"
        
        # Verificar tipos de datos críticos
        if 'anio_publicacion' in df.columns:
            # Solo verificar los que no son None/NaN
            non_null_years = df['anio_publicacion'].dropna()
            if len(non_null_years) > 0:
                non_numeric = non_null_years.apply(lambda x: not str(x).replace('.', '', 1).isdigit()).sum()
                if non_numeric > 0:
                    print(f"⚠ Advertencia: {non_numeric} años de publicación no numéricos (se permiten nulos)")
        
        print("✓ Todas las aserciones de calidad pasaron correctamente")


def save_quality_metrics(metrics_dict, output_path):
    """
    Guarda las métricas de calidad en formato JSON
    """
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(metrics_dict, f, ensure_ascii=False, indent=2)
    
    print(f"✓ Métricas de calidad guardadas en: {output_path}")
