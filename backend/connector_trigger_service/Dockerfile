FROM python:3.11-slim

WORKDIR /app

RUN pip install --upgrade pip setuptools wheel

# Copy shared modules first
COPY shared /app/shared

# Install shared package (temporarily hide pyproject.toml to force setup.py usage)
RUN cd /app/shared && mv pyproject.toml pyproject.toml.bak && cd /app && pip install ./shared && cd /app/shared && mv pyproject.toml.bak pyproject.toml

# Copy service requirements and install
COPY connector_trigger_service/requirements.txt .
RUN sed -i '/-e .*shared/d' requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy data_connector module (google sheets adapter)
COPY data_connector /app/data_connector

# Copy service code
COPY connector_trigger_service /app/connector_trigger_service

ENV PYTHONPATH=/app:$PYTHONPATH

CMD ["python", "-m", "connector_trigger_service.main"]

