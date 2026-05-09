FROM alpine:3.19

# Python3, pip, curl (healthcheck), su-exec y tzdata para gestionar la zona horaria
RUN apk add --no-cache \
    curl \
    ca-certificates \
    python3 \
    py3-pip \
    su-exec \
    tzdata \
    && adduser -D -u 1000 appuser

WORKDIR /app

# Instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt

# Copiar el código
COPY . .

# Hacer ejecutable el entrypoint
RUN chmod +x /app/entrypoint.sh

# Asegurar que el directorio interno existe inicialmente
RUN mkdir -p /app/data && chown -R appuser:appuser /app

EXPOSE 8000

ENTRYPOINT ["/app/entrypoint.sh"]
