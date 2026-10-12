import json
import logging
import ssl
from typing import Any, Optional
from aiokafka import AIOKafkaProducer
from app.config import settings

logger = logging.getLogger(__name__)


class KafkaService:
    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self.is_connected: bool = False

    def _create_ssl_context(self) -> Optional[ssl.SSLContext]:
        """Configures SSL context for Aiven Kafka or other secure brokers."""
        if settings.KAFKA_SECURITY_PROTOCOL not in ("SSL", "SASL_SSL"):
            return None

        # Create client SSL context for server authentication
        ca_file = settings.KAFKA_SSL_CAFILE
        if ca_file:
            context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=ca_file)
        else:
            context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)

        # In modern Aiven clusters, disable hostname verification if custom hostnames/SANs are used
        context.check_hostname = False

        # Load client certificate and private key for mutual TLS (mTLS)
        if settings.KAFKA_SSL_CERTFILE and settings.KAFKA_SSL_KEYFILE:
            logger.info(
                f"Loading mTLS client certificates: cert='{settings.KAFKA_SSL_CERTFILE}', key='{settings.KAFKA_SSL_KEYFILE}'"
            )
            context.load_cert_chain(
                certfile=settings.KAFKA_SSL_CERTFILE,
                keyfile=settings.KAFKA_SSL_KEYFILE
            )

        return context

    async def start(self):
        """Initializes and starts the AIOKafkaProducer."""
        if not settings.KAFKA_ENABLED:
            logger.info("Kafka is disabled via KAFKA_ENABLED=False.")
            return

        try:
            ssl_context = self._create_ssl_context()
            kwargs = {
                "bootstrap_servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "value_serializer": lambda v: json.dumps(v, default=str).encode("utf-8"),
                "security_protocol": settings.KAFKA_SECURITY_PROTOCOL,
            }

            if ssl_context:
                kwargs["ssl_context"] = ssl_context

            if settings.KAFKA_SASL_MECHANISM:
                kwargs["sasl_mechanism"] = settings.KAFKA_SASL_MECHANISM
                kwargs["sasl_plain_username"] = settings.KAFKA_SASL_USERNAME
                kwargs["sasl_plain_password"] = settings.KAFKA_SASL_PASSWORD

            self.producer = AIOKafkaProducer(**kwargs)
            await self.producer.start()
            self.is_connected = True
            logger.info(f"Connected to Kafka cluster at {settings.KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            self.is_connected = False
            logger.error(
                f"Failed to connect to Kafka ({settings.KAFKA_BOOTSTRAP_SERVERS}): {e}. "
                "Events will be logged until connection is established."
            )

    async def stop(self):
        """Stops the AIOKafkaProducer."""
        if self.producer and self.is_connected:
            try:
                await self.producer.stop()
                logger.info("Kafka producer stopped successfully.")
            except Exception as e:
                logger.error(f"Error stopping Kafka producer: {e}")
            finally:
                self.is_connected = False

    async def send_event(self, topic: str, value: Any, key: Optional[str] = None):
        """Sends a JSON message asynchronously to the designated Kafka topic."""
        if not settings.KAFKA_ENABLED:
            logger.info(f"[SIMULATED KAFKA] Topic: {topic} | Message: {value}")
            return

        if not self.is_connected or not self.producer:
            logger.warning(
                f"[KAFKA NOT CONNECTED] Could not publish event to topic '{topic}'. "
                f"Payload: {value}"
            )
            return

        try:
            encoded_key = key.encode("utf-8") if key else None
            record_metadata = await self.producer.send_and_wait(
                topic=topic,
                value=value,
                key=encoded_key
            )
            logger.info(
                f"Kafka event published to '{topic}' partition {record_metadata.partition} "
                f"offset {record_metadata.offset}"
            )
        except Exception as e:
            logger.error(f"Failed to send event to Kafka topic '{topic}': {e}")


kafka_service = KafkaService()
