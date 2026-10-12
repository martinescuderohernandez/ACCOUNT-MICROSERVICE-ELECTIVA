import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.database import init_db
from app.kafka_producer import kafka_service
from app.routers import accounts

# Configure logging
logging.basicConfig(
    level=logging.INFO if settings.DEBUG else logging.WARNING,
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup ---
    logger.info(f"Starting {settings.APP_NAME}...")
    # Initialize DB schema
    init_db()
    # Connect to Kafka cluster
    await kafka_service.start()

    yield

    # --- Shutdown ---
    logger.info(f"Shutting down {settings.APP_NAME}...")
    await kafka_service.stop()


app = FastAPI(
    title=settings.APP_NAME,
    description="Microservicio de Cuentas (Account Microservice) con integración a TiDB y Kafka (Aiven).",
    version="1.0.0",
    lifespan=lifespan
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include Routers
app.include_router(accounts.router)


@app.get("/", tags=["Health"])
def root():
    return {
        "service": settings.APP_NAME,
        "status": "online",
        "docs_url": "/docs",
        "kafka_status": "connected" if kafka_service.is_connected else "disconnected",
        "kafka_topic": settings.KAFKA_TOPIC_ACCOUNT_CREATED
    }


@app.get("/health", tags=["Health"])
def health_check():
    return {
        "status": "healthy",
        "database": "TiDB / MySQL configured",
        "kafka": {
            "connected": kafka_service.is_connected,
            "bootstrap_servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "topic": settings.KAFKA_TOPIC_ACCOUNT_CREATED
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=settings.PORT, reload=settings.DEBUG)
