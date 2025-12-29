FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip setuptools wheel

COPY shared /app/shared
RUN cd /app/shared && mv pyproject.toml pyproject.toml.bak && cd /app && pip install ./shared && cd /app/shared && mv pyproject.toml.bak pyproject.toml

COPY ingest_reconciler_worker/requirements.txt .
RUN sed -i '/-e .*shared/d' requirements.txt && \
    (if [ -s requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi)

RUN python -c "import uvicorn; import fastapi; print('Ingest reconciler deps OK')"

COPY ingest_reconciler_worker /app/ingest_reconciler_worker

ENV PYTHONPATH=/app

EXPOSE 8012

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8012/health || exit 1

CMD ["python", "-m", "ingest_reconciler_worker.main"]
