FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip setuptools wheel

COPY shared /app/shared
RUN cd /app/shared && mv pyproject.toml pyproject.toml.bak && cd /app && pip install ./shared && cd /app/shared && mv pyproject.toml.bak pyproject.toml

COPY pipeline_scheduler /app/pipeline_scheduler

ENV PYTHONPATH=/app:$PYTHONPATH

CMD ["python", "-m", "pipeline_scheduler.main"]
