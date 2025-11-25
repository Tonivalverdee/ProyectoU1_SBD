import re

def clean_isbn(isbn: str | None) -> str | None:
    """Elimina guiones, espacios y caracteres no válidos."""
    if not isbn or not isinstance(isbn, str):
        return None
    return re.sub(r"[^0-9Xx]", "", isbn)


def is_valid_isbn10(isbn: str | None) -> bool:
    """Valida ISBN-10 usando el checksum estándar."""
    isbn = clean_isbn(isbn)
    if not isbn or len(isbn) != 10:
        return False

    total = 0
    for i, char in enumerate(isbn[:-1], start=1):
        if not char.isdigit():
            return False
        total += i * int(char)

    check = isbn[-1]
    if check.upper() == "X":
        total += 10 * 10
    elif check.isdigit():
        total += 10 * int(check)
    else:
        return False

    return total % 11 == 0


def is_valid_isbn13(isbn: str | None) -> bool:
    """Valida ISBN-13 usando el algoritmo estándar."""
    isbn = clean_isbn(isbn)
    if not isbn or len(isbn) != 13 or not isbn.isdigit():
        return False

    total = 0
    for i, digit in enumerate(isbn):
        n = int(digit)
        if i % 2 == 0:
            total += n
        else:
            total += 3 * n

    return total % 10 == 0


def validate_isbn(isbn: str | None) -> bool:
    """Valida un ISBN combinando ISBN-10 e ISBN-13."""
    return is_valid_isbn10(isbn) or is_valid_isbn13(isbn)
