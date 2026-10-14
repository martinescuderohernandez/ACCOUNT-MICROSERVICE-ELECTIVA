import base64
import json
import logging
import os
import ssl
import tempfile
from typing import Any, Optional
from aiokafka import AIOKafkaProducer
from app.config import settings

logger = logging.getLogger(__name__)


class KafkaService:
    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self.is_connected: bool = False

    @staticmethod
    def _resolve_cert(file_path: Optional[str], base64_content: Optional[str], default_filename: str) -> Optional[str]:
        """
        Resolves certificate path with fallback logic:
        1. If configured file_path exists on disk, use it.
        2. If Render Secret File exists (/etc/secrets/<filename>), use it.
        3. If Base64 string is provided, decode and save to /tmp/kafka_certs/<filename>.
        4. Fallback to original file_path.
        """
        if file_path and os.path.isfile(file_path):
            return file_path

        render_path = os.path.join("/etc/secrets", default_filename)
        if os.path.isfile(render_path):
            logger.info(f"Using Render Secret File for {default_filename} at {render_path}")
            return render_path

        if base64_content and base64_content.strip():
            try:
                target_dir = os.path.join(tempfile.gettempdir(), "kafka_certs")
                os.makedirs(target_dir, exist_ok=True)
                target_file = os.path.join(target_dir, default_filename)
                decoded_data = base64.b64decode(base64_content.strip())
                with open(target_file, "wb") as f:
                    f.write(decoded_data)
                os.chmod(target_file, 0o600)
                logger.info(f"Successfully decoded Base64 certificate into {target_file}")
                return target_file
            except Exception as e:
                logger.error(f"Error decoding Base64 certificate for {default_filename}: {e}")

        return file_path

    def _create_ssl_context(self) -> Optional[ssl.SSLContext]:
        """Configures SSL context for Aiven Kafka or other secure brokers."""
        if settings.KAFKA_SECURITY_PROTOCOL not in ("SSL", "SASL_SSL"):
            return None

        # Resolve certificates with automatic fallbacks
        ca_file = self._resolve_cert(settings.KAFKA_SSL_CAFILE, settings.KAFKA_SSL_CA_BASE64, "ca.pem")
        cert_file = self._resolve_cert(settings.KAFKA_SSL_CERTFILE, settings.KAFKA_SSL_CERT_BASE64, "service.cert")
        key_file = self._resolve_cert(settings.KAFKA_SSL_KEYFILE, settings.KAFKA_SSL_KEY_BASE64, "service.key")

        # Create client SSL context for server authentication
        if ca_file and os.path.isfile(ca_file):
            context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=ca_file)
        else:
            context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)

        # In modern Aiven clusters, disable hostname verification if custom hostnames/SANs are used
        context.check_hostname = False

        # Load client certificate and private key for mutual TLS (mTLS)
        if cert_file and key_file and os.path.isfile(cert_file) and os.path.isfile(key_file):
            logger.info(
                f"Loading mTLS client certificates: cert='{cert_file}', key='{key_file}'"
            )
            context.load_cert_chain(
                certfile=cert_file,
                keyfile=key_file
            )
        else:
            if settings.KAFKA_SECURITY_PROTOCOL == "SSL":
                logger.warning(
                    f"mTLS certificates not ready (cert='{cert_file}', key='{key_file}'). "
                    "Ensure files are present in ./certs, in Render /etc/secrets/, or via Base64 env vars."
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
