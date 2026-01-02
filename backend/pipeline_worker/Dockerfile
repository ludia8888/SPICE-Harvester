FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip setuptools wheel

COPY shared /app/shared
RUN cd /app/shared && mv pyproject.toml pyproject.toml.bak && cd /app && pip install ./shared && cd /app/shared && mv pyproject.toml.bak pyproject.toml

COPY pipeline_worker/requirements.txt .
RUN sed -i '/-e .*shared/d' requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY data_connector /app/data_connector
COPY pipeline_worker /app/pipeline_worker

ENV PYTHONPATH=/app:$PYTHONPATH

CMD ["python", "-m", "pipeline_worker.main"]
