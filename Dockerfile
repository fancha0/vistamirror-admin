FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_HOST=0.0.0.0 \
    APP_PORT=8091 \
    APP_DATA_DIR=/app/data \
    APP_RUNTIME_DIR=/app/runtime

WORKDIR /app

# Optional dependency for enterprise WeCom callback decrypt support.
RUN pip install --no-cache-dir pycryptodome

COPY dev_server.py /app/dev_server.py
COPY backend_modules /app/backend_modules
COPY runtime /app/runtime
COPY docs /app/docs

RUN mkdir -p /app/data /app/runtime && \
    addgroup --system app && adduser --system --ingroup app app && \
    chown -R app:app /app

USER app

EXPOSE 8091

CMD ["python3", "dev_server.py"]
