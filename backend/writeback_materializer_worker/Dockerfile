FROM python:3.11-slim

WORKDIR /app

# Install minimal build tools (some shared deps include native wheels/extensions).
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip setuptools wheel

# Shared package (SSoT config + storage clients)
COPY shared /app/shared
RUN cd /app/shared && mv pyproject.toml pyproject.toml.bak && cd /app && pip install ./shared && cd /app/shared && mv pyproject.toml.bak pyproject.toml

# Sanity check for required deps
RUN python -c "import boto3, httpx; print('writeback-materializer-worker deps OK')"

COPY writeback_materializer_worker /app/writeback_materializer_worker

ENV PYTHONPATH=/app:$PYTHONPATH

CMD ["python", "-m", "writeback_materializer_worker.main"]
