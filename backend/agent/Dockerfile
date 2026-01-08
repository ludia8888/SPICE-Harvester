# Agent Service Dockerfile
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip setuptools wheel

COPY shared /app/shared

RUN cd /app/shared && mv pyproject.toml pyproject.toml.bak && cd /app && pip install ./shared && cd /app/shared && mv pyproject.toml.bak pyproject.toml

COPY agent/requirements.txt .
RUN sed -i '/-e .*shared/d' requirements.txt && \
    (if [ -s requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi)

COPY agent /app/agent

EXPOSE 8004

ENV PYTHONPATH=/app

CMD ["python", "-m", "uvicorn", "agent.main:app", "--host", "0.0.0.0", "--port", "8004", "--log-level", "info"]
