FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_HOST=0.0.0.0 \
    APP_PORT=8091 \
    APP_DATA_DIR=/app/data \
    APP_RUNTIME_DIR=/app/runtime

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pycryptodome

COPY dev_server.py /app/dev_server.py
COPY backend_modules /app/backend_modules
COPY runtime /app/runtime

RUN mkdir -p /app/data /app/runtime

EXPOSE 8091

CMD ["python3", "dev_server.py"]