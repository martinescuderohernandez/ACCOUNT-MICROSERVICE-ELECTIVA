from datetime import datetime
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict


class AccountCreate(BaseModel):
    user_id: str = Field(..., description="ID del usuario propietario de la cuenta (UUID o string)")
    account_type: str = Field(..., description="Tipo de cuenta (ej: SAVINGS, CHECKING)", examples=["SAVINGS"])
    account_number: Optional[str] = Field(None, max_length=20, description="Número de cuenta opcional. Si no se envía, se generará automáticamente.")
    balance: Decimal = Field(default=Decimal("0.00"), ge=0, description="Saldo inicial de la cuenta")
    currency: str = Field(default="COP", min_length=3, max_length=3, description="Código de moneda (ISO 4217, ej: COP)")
    user_email: Optional[str] = Field(None, description="Correo electrónico del usuario para que notifiers microservice envíe la notificación")


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
