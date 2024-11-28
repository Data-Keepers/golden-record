import re


def valid_email(email: str) -> bool:
    pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'

    return bool(re.match(pattern, email))


def valid_phone(phone: str) -> bool:
    pattern = r'\d.*\d.*\d.*\d.*\d.*\d.*\d.*\d.*\d.*\d'

    return bool(re.match(pattern, phone))


def valid_inn(inn: str) -> bool:
    pattern = r'\d.*\d.*\d.*\d.*\d.*\d.*\d.*\d.*\d.*\d.*\d'

    return bool(re.match(pattern, inn))


def valid_snils(snils: str) -> bool:
    pattern = r'\d.*\d.*\d.*\d.*\d.*\d.*\d.*\d.*\d.*\d.*\d.*\d'

    return bool(re.match(pattern, snils))


VALID_RECORDS = {
    "contact_phone": valid_phone,
    "contact_email": valid_email,
    "client_inn": valid_inn,
    "client_snils": valid_snils
}
