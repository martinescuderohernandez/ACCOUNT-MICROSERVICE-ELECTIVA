import re
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict, field_validator


class AccountTypeEnum(str, Enum):
    SAVINGS = "SAVINGS"
    CHECKING = "CHECKING"


class CurrencyEnum(str, Enum):
    COP = "COP"
    USD = "USD"
    EUR = "EUR"


class AccountStatusEnum(str, Enum):
    ACTIVE = "ACTIVE"
    BLOCKED = "BLOCKED"
    INACTIVE = "INACTIVE"


class AccountCreate(BaseModel):
    user_id: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="ID del usuario propietario de la cuenta (UUID o identificador alfanumérico)",
        examples=["usr-123e4567-e89b-12d3-a456-426614174000"]
    )
    account_type: str = Field(
        ...,
        description="Tipo de cuenta permitido ('SAVINGS' o 'CHECKING')",
        examples=["SAVINGS"]
    )
    account_number: Optional[str] = Field(
        None,
        max_length=20,
        description="Número de cuenta opcional (entre 6 y 20 caracteres alfanuméricos/guiones). Si no se envía, se generará automáticamente.",
        examples=["100987654321"]
    )
    balance: Decimal = Field(
        default=Decimal("0.00"),
        ge=Decimal("0.00"),
        le=Decimal("999999999999.99"),
        description="Saldo inicial de la cuenta (mayor o igual a 0.00, máx 2 decimales)",
        examples=[150000.50]
    )
    currency: str = Field(
        default="COP",
        min_length=3,
        max_length=3,
        description="Código de moneda ISO 4217 permitido ('COP', 'USD', 'EUR')",
        examples=["COP"]
    )
    user_email: Optional[str] = Field(
        None,
        max_length=120,
        description="Correo electrónico del usuario para notificaciones (debe ser un formato válido)",
        examples=["usuario@ejemplo.com"]
    )

    @field_validator("user_id")
    @classmethod
    def validate_user_id(cls, v: str) -> str:
        cleaned = v.strip()
        if not cleaned:
            raise ValueError("El campo 'user_id' no puede estar vacío ni contener solo espacios.")
        return cleaned

    @field_validator("account_type")
    @classmethod
    def validate_account_type(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("El campo 'account_type' es obligatorio.")
        normalized = v.strip().upper()
        allowed = [t.value for t in AccountTypeEnum]
        if normalized not in allowed:
            raise ValueError(f"Tipo de cuenta no permitido: '{v}'. Debe ser uno de {allowed}.")
        return normalized

    @field_validator("account_number")
    @classmethod
    def validate_account_number(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        cleaned = v.strip()
        if not cleaned:
            return None
        if len(cleaned) < 6 or len(cleaned) > 20:
            raise ValueError("El campo 'account_number' debe tener entre 6 y 20 caracteres.")
        if not re.match(r"^[A-Za-z0-9\-]+$", cleaned):
            raise ValueError("El campo 'account_number' solo puede contener caracteres alfanuméricos y guiones.")
        return cleaned

    @field_validator("balance")
    @classmethod
    def validate_balance(cls, v: Decimal) -> Decimal:
        if v < Decimal("0.00"):
            raise ValueError("El balance inicial no puede ser negativo.")
        if v > Decimal("999999999999.99"):
            raise ValueError("El balance excede el límite máximo permitido (999,999,999,999.99).")
        if (v * 100) % 1 != 0:
            raise ValueError("El balance no puede tener más de 2 cifras decimales.")
        return v

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("El campo 'currency' es obligatorio.")
        normalized = v.strip().upper()
        allowed = [c.value for c in CurrencyEnum]
        if normalized not in allowed:
            raise ValueError(f"Moneda no permitida: '{v}'. Debe ser una de {allowed}.")
        return normalized

    @field_validator("user_email")
    @classmethod
    def validate_user_email(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        cleaned = v.strip().lower()
        if not cleaned:
            return None
        email_regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        if not re.match(email_regex, cleaned):
            raise ValueError(f"El correo electrónico '{v}' no tiene un formato válido.")
        return cleaned


class AccountResponse(BaseModel):
    id: str
    account_number: str
    user_id: str
    account_type: str
    balance: Decimal
    currency: str
    status: str
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class AccountCreatedEvent(BaseModel):
    event: str = "ACCOUNT_CREATED"
    event_id: str
    timestamp: str
    data: dict
