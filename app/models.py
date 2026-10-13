from datetime import datetime
from decimal import Decimal
from sqlalchemy import Column, String, Numeric, DateTime, Index
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Account(Base):
    __tablename__ = "accounts"

    id = Column(String(36), primary_key=True, doc="UUID primary key")
    account_number = Column(String(20), unique=True, nullable=False, doc="Unique account number")
    user_id = Column(String(50), nullable=False, index=True, doc="ID of the account owner")
    account_type = Column(String(20), nullable=False, doc="Account type (e.g. SAVINGS, CHECKING)")
    balance = Column(Numeric(15, 2), nullable=False, default=Decimal("0.00"), doc="Current balance")
    currency = Column(String(3), nullable=False, default="COP", doc="Currency code (e.g. COP, USD)")
    status = Column(String(20), nullable=False, default="ACTIVE", doc="Account status (ACTIVE, BLOCKED, etc.)")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, doc="Creation timestamp")
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow, doc="Last update timestamp")

    __table_args__ = (
        Index("idx_user_id", "user_id"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "account_number": self.account_number,
            "user_id": self.user_id,
            "account_type": self.account_type,
            "balance": float(self.balance) if self.balance is not None else 0.0,
            "currency": self.currency,
            "status": self.status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
