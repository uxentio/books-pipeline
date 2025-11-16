"""
Utilidades para validación y manipulación de ISBN
"""

def clean_isbn(isbn_str):
    """Limpia un string de ISBN eliminando guiones y espacios"""
    if not isbn_str:
        return None
    return ''.join(c for c in str(isbn_str) if c.isdigit() or c.upper() == 'X')


def validate_isbn13(isbn):
    """
    Valida un ISBN-13
    Returns: True si es válido, False si no
    """
    if not isbn:
        return False
    
    isbn = clean_isbn(isbn)
    
    if len(isbn) != 13:
        return False
    
    if not isbn.isdigit():
        return False
    
    try:
        # Algoritmo de validación ISBN-13
        total = 0
        for i, digit in enumerate(isbn[:-1]):
            weight = 1 if i % 2 == 0 else 3
            total += int(digit) * weight
        
        check_digit = (10 - (total % 10)) % 10
        return int(isbn[-1]) == check_digit
    except:
        return False


def validate_isbn10(isbn):
    """
    Valida un ISBN-10
    Returns: True si es válido, False si no
    """
    if not isbn:
        return False
    
    isbn = clean_isbn(isbn)
    
    if len(isbn) != 10:
        return False
    
    try:
        # Algoritmo de validación ISBN-10
        total = 0
        for i, char in enumerate(isbn[:-1]):
            total += int(char) * (10 - i)
        
        check_char = isbn[-1]
        if check_char.upper() == 'X':
            check_value = 10
        else:
            check_value = int(check_char)
        
        total += check_value
        return total % 11 == 0
    except:
        return False


def isbn10_to_isbn13(isbn10):
    """
    Convierte ISBN-10 a ISBN-13
    """
    if not isbn10:
        return None
    
    isbn10 = clean_isbn(isbn10)
    
    if len(isbn10) != 10:
        return None
    
    # ISBN-13 = 978 + primeros 9 dígitos del ISBN-10 + dígito de control recalculado
    isbn13_base = '978' + isbn10[:-1]
    
    # Calcular dígito de control
    total = 0
    for i, digit in enumerate(isbn13_base):
        weight = 1 if i % 2 == 0 else 3
        total += int(digit) * weight
    
    check_digit = (10 - (total % 10)) % 10
    
    return isbn13_base + str(check_digit)


def extract_isbn(text):
    """
    Intenta extraer ISBN-10 o ISBN-13 de un texto
    Returns: tuple (isbn13, isbn10) o (None, None)
    """
    if not text:
        return None, None
    
    import re
    
    # Buscar patrones de ISBN-13
    isbn13_pattern = r'(?:ISBN[-:]?\s*(?:13)?:?\s*)?(?:978|979)[-\s]?\d{1,5}[-\s]?\d{1,7}[-\s]?\d{1,7}[-\s]?\d{1}'
    isbn13_matches = re.findall(isbn13_pattern, str(text))
    
    for match in isbn13_matches:
        cleaned = clean_isbn(match)
        if validate_isbn13(cleaned):
            return cleaned, None
    
    # Buscar patrones de ISBN-10
    isbn10_pattern = r'(?:ISBN[-:]?\s*(?:10)?:?\s*)?\d{1,5}[-\s]?\d{1,7}[-\s]?\d{1,7}[-\s]?[\dX]'
    isbn10_matches = re.findall(isbn10_pattern, str(text))
    
    for match in isbn10_matches:
        cleaned = clean_isbn(match)
        if len(cleaned) == 10 and validate_isbn10(cleaned):
            isbn13 = isbn10_to_isbn13(cleaned)
            return isbn13, cleaned
    
    return None, None
