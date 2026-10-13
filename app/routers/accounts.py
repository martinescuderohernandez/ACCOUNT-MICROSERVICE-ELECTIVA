import logging
from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, Path, Query
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from app.database import get_db
from app.models import Account
from app.schemas import AccountCreate, AccountResponse
from app.utils import generate_uuid, generate_account_number
from app.kafka_producer import kafka_service
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/accounts", tags=["Accounts"])


@router.post(
    "",
    response_model=AccountResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Crear nueva cuenta bancaria",
    description="Crea una cuenta en TiDB (sync) y publica un evento en Kafka para el notifiers microservice (async)."
)
def create_account(
    payload: AccountCreate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    account_id = generate_uuid()
    account_number = payload.account_number or generate_account_number()
    now = datetime.utcnow()

    new_account = Account(
        id=account_id,
        account_number=account_number,
        user_id=payload.user_id,
        account_type=payload.account_type,
        balance=payload.balance,
        currency=payload.currency,
        status="ACTIVE",
        created_at=now,
        updated_at=now
    )

    # 1. Inserción Síncrona en la Base de Datos (TiDB / MySQL)
    try:
        db.add(new_account)
        db.commit()
        db.refresh(new_account)
        logger.info(f"Account {new_account.id} ({new_account.account_number}) created in TiDB.")
    except IntegrityError as e:
        db.rollback()
        logger.error(f"Integrity error creating account: {e}")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Ya existe una cuenta con el número '{account_number}' o ID duplicado."
        )
    except Exception as e:
        db.rollback()
        logger.error(f"Database error creating account: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al guardar la cuenta en la base de datos: {str(e)}"
        )

    # 2. Publicación Asíncrona en Kafka (account-created-topic)
    event_payload = {
        "event": "ACCOUNT_CREATED",
        "event_id": generate_uuid(),
        "timestamp": now.isoformat(),
        "data": {
            "id": new_account.id,
            "account_number": new_account.account_number,
            "user_id": new_account.user_id,
            "user_email": payload.user_email,
            "account_type": new_account.account_type,
            "balance": float(new_account.balance),
            "currency": new_account.currency,
            "status": new_account.status,
            "created_at": new_account.created_at.isoformat()
        }
    }

    background_tasks.add_task(
        kafka_service.send_event,
        topic=settings.KAFKA_TOPIC_ACCOUNT_CREATED,
        value=event_payload,
        key=new_account.id
    )

    return new_account


@router.get(
    "/{account_id}",
    response_model=AccountResponse,
    summary="Obtener cuenta por ID"
)
def get_account_by_id(
    account_id: str = Path(..., min_length=1, max_length=50, description="ID único de la cuenta"),
    db: Session = Depends(get_db)
):
    account = db.query(Account).filter(Account.id == account_id).first()
    if not account:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cuenta con ID '{account_id}' no encontrada."
        )
    return account


@router.get(
    "",
    response_model=List[AccountResponse],
    summary="Listar cuentas"
)
def list_accounts(
    user_id: Optional[str] = Query(None, min_length=1, max_length=50, description="Filtrar por ID del usuario"),
    limit: int = Query(50, ge=1, le=100, description="Número máximo de cuentas a retornar (1 a 100)"),
    offset: int = Query(0, ge=0, description="Número de registros a omitir para paginación"),
    db: Session = Depends(get_db)
):
    query = db.query(Account)
    if user_id:
        query = query.filter(Account.user_id == user_id.strip())
    return query.offset(offset).limit(limit).all()
