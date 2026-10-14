from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Application Config
    APP_NAME: str = "Account Microservice"
    APP_ENV: str = "development"
    DEBUG: bool = True
    PORT: int = 8000

    # Database Config (MySQL / TiDB)
    DB_HOST: str = "127.0.0.1"
    DB_PORT: int = 4000
    DB_USER: str = "root"
    DB_PASSWORD: str = ""
    DB_NAME: str = "accounts_db"
    DB_SSL_CA: Optional[str] = None
    DATABASE_URL: Optional[str] = None

    @property
    def sqlalchemy_database_url(self) -> str:
        if self.DATABASE_URL:
            return self.DATABASE_URL
        # Construct standard MySQL/TiDB URL using pymysql
        url = f"mysql+pymysql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}?charset=utf8mb4"
        return url

    # Kafka Config (Aiven)
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka-11a18fa7-tercer-corte-electiva.d.aivencloud.com:27119"
    KAFKA_TOPIC_ACCOUNT_CREATED: str = "account-created-topic"
    KAFKA_SECURITY_PROTOCOL: str = "SSL"
    KAFKA_SSL_CAFILE: Optional[str] = None
    KAFKA_SSL_CERTFILE: Optional[str] = None
    KAFKA_SSL_KEYFILE: Optional[str] = None
    KAFKA_SSL_CA_BASE64: Optional[str] = None
    KAFKA_SSL_CERT_BASE64: Optional[str] = None
    KAFKA_SSL_KEY_BASE64: Optional[str] = None
    KAFKA_SASL_MECHANISM: Optional[str] = None
    KAFKA_SASL_USERNAME: Optional[str] = None
    KAFKA_SASL_PASSWORD: Optional[str] = None
    KAFKA_ENABLED: bool = True

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


settings = Settings()
