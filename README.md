# Account Microservice (ACCOUNT-MICROSERVICE-ELECTIVA)

Microservicio bancario construido con **FastAPI** para la gestión y creación de cuentas de usuario. Implementa persistencia síncrona en base de datos distribuida **TiDB / MySQL** y publicación asíncrona de eventos hacia **Apache Kafka (Aiven)** para el envío de correos mediante el microservicio notificador (*notifiers microservice*).

---

## 🏗 Arquitectura y Flujo

1. **Cliente / Frontend** envía una petición `POST /accounts` con los datos de creación de cuenta.
2. **FastAPI (Síncrono)**:
   - Valida la estructura mediante **Pydantic**.
   - Genera UUID y número de cuenta único si no fueron provistos.
   - Inserta y confirma la transacción en la tabla `accounts` de **TiDB / MySQL**.
3. **Kafka Producer (Asíncrono)**:
   - Mediante `BackgroundTasks` y `aiokafka`, emite un evento JSON al tópico `account-created-topic` sin bloquear el tiempo de respuesta del cliente HTTP.
4. **Respuesta Inmediata**: Retorna `201 Created` con el detalle de la cuenta creada.
5. **Notifiers Microservice**: Consume el evento de Kafka y despacha el correo electrónico de bienvenida/notificación al usuario.

---

## 🗄 Estructura de Base de Datos (TiDB / MySQL)

La tabla se crea o verifica automáticamente al iniciar la aplicación con la siguiente definición DDL:

```sql
CREATE TABLE IF NOT EXISTS accounts (
    id VARCHAR(36) PRIMARY KEY,
    account_number VARCHAR(20) NOT NULL UNIQUE,
    user_id VARCHAR(36) NOT NULL,
    account_type VARCHAR(20) NOT NULL,
    balance DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    currency VARCHAR(3) NOT NULL DEFAULT 'COP',
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    INDEX idx_user_id (user_id)
);
```

---

## ⚙️ Configuración del Entorno (.env)

Copia el archivo de plantilla `.env.example` a `.env` y ajusta tus credenciales:

```bash
cp .env.example .env
```

### Parámetros Principales

```env
# Servidor
APP_NAME="Account Microservice"
PORT=8000
DEBUG=True

# TiDB / MySQL
DB_HOST=gateway01.us-east-1.prod.aws.tidbcloud.com  # o tu host de TiDB
DB_PORT=4000
DB_USER=tu_usuario
DB_PASSWORD=tu_password
DB_NAME=accounts_db

# Kafka (Aiven)
KAFKA_BOOTSTRAP_SERVERS=kafka-11a18fa7-tercer-corte-electiva.d.aivencloud.com:27119
KAFKA_TOPIC_ACCOUNT_CREATED=account-created-topic
KAFKA_SECURITY_PROTOCOL=SSL

# Certificados Aiven (si usas mTLS):
KAFKA_SSL_CAFILE=./certs/ca.pem
KAFKA_SSL_CERTFILE=./certs/service.cert
KAFKA_SSL_KEYFILE=./certs/service.key

# O si usas SASL_SSL:
# KAFKA_SECURITY_PROTOCOL=SASL_SSL
# KAFKA_SASL_MECHANISM=SCRAM-SHA-256
# KAFKA_SASL_USERNAME=avnadmin
# KAFKA_SASL_PASSWORD=tu_password
```

---

## 🚀 Instalación y Ejecución

### 1. Activar entorno virtual
```bash
source mi_entorno/bin/activate
```

### 2. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 3. Ejecutar la API
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Accede a la documentación interactiva Swagger en:
- **Swagger UI**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **ReDoc**: [http://localhost:8000/redoc](http://localhost:8000/redoc)

---

## 🧪 Pruebas Unitarias
Para correr los tests con base de datos en memoria y mock de Kafka:
```bash
pytest -v
```

---

## 📬 Endpoints de la API

### 1. `POST /accounts`
Crea una nueva cuenta e inicia el evento en Kafka.

#### Request Body (`application/json`):
```json
{
  "user_id": "usr-d41d8cd9-8f00-4b11-a8e5",
  "account_type": "SAVINGS",
  "account_number": "100123456789",  // Opcional, auto-generado si no se envía
  "balance": 250000.00,               // Opcional, por defecto 0.00
  "currency": "COP",                  // Opcional, por defecto "COP"
  "user_email": "cliente@banco.com"   // Opcional, para el servicio de notificaciones
}
```

#### Response (`201 Created`):
```json
{
  "id": "b1fa6f33-6628-4ad0-b856-11f87b8f9e21",
  "account_number": "100123456789",
  "user_id": "usr-d41d8cd9-8f00-4b11-a8e5",
  "account_type": "SAVINGS",
  "balance": 250000.00,
  "currency": "COP",
  "status": "ACTIVE",
  "created_at": "2026-10-11T19:00:00",
  "updated_at": "2026-10-11T19:00:00"
}
```

### 2. Mensaje emitido a Kafka (`account-created-topic`)
```json
{
  "event": "ACCOUNT_CREATED",
  "event_id": "a931cb7f-0bc5-4e78-9b88-ec94cf5ea45f",
  "timestamp": "2026-10-11T19:00:00.123456",
  "data": {
    "id": "b1fa6f33-6628-4ad0-b856-11f87b8f9e21",
    "account_number": "100123456789",
    "user_id": "usr-d41d8cd9-8f00-4b11-a8e5",
    "user_email": "cliente@banco.com",
    "account_type": "SAVINGS",
    "balance": 250000.0,
    "currency": "COP",
    "status": "ACTIVE",
    "created_at": "2026-10-11T19:00:00"
  }
}
```

### 3. Otros Endpoints
- `GET /accounts`: Listar cuentas creadas (soporta filtro `?user_id=...`).
- `GET /accounts/{id}`: Consultar el estado y saldo de una cuenta por ID.
- `GET /health`: Monitoreo del estado de conexión de TiDB y Kafka.