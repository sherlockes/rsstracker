#!/bin/sh
set -e

# Garantiza que el directorio de datos existe y pertenece a appuser
# (necesario cuando el volumen de Docker lo crea como root)
mkdir -p /app/data
chown -R appuser:appuser /app/data

# Cambia a appuser y arranca uvicorn
exec su-exec appuser uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
