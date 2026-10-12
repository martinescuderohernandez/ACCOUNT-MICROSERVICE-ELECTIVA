import uuid
import secrets


def generate_uuid() -> str:
    """Generates a UUID4 string."""
    return str(uuid.uuid4())


def generate_account_number(prefix: str = "100") -> str:
    """
    Generates a unique random 12-digit account number.
    e.g. 100748291038
    """
    random_digits = "".join(str(secrets.randbelow(10)) for _ in range(9))
    return f"{prefix}{random_digits}"
