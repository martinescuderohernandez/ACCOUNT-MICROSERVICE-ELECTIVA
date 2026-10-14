# ==========================================
# Dockerfile for Account Microservice
# ==========================================
FROM python:3.11-slim

# Prevent Python from writing .pyc files & keep logs unbuffered for real-time output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000

# Set container working directory
WORKDIR /app

# Install system dependencies:
# - ca-certificates: Required for secure SSL/TLS validation with TiDB Cloud and Aiven Kafka
# - curl: Useful for health checks
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create runtime directory for certs (if mounted or generated dynamically)
RUN mkdir -p /app/certs

# Copy application source code
COPY app/ ./app

# Create non-root user for security best practices
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

USER appuser

# Expose default port
EXPOSE 8000

# Run uvicorn evaluating $PORT at runtime (Render injects dynamic $PORT e.g. 10000)
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
