FROM python:3.11-slim

WORKDIR /app

# Upgrade pip and install build tools
RUN pip install --upgrade pip setuptools wheel

# Copy shared modules first
COPY shared /app/shared

# Install shared package (temporarily hide pyproject.toml to force setup.py usage)
RUN cd /app/shared && mv pyproject.toml pyproject.toml.bak && cd /app && pip install ./shared && cd /app/shared && mv pyproject.toml.bak pyproject.toml

# Copy service requirements and install
COPY google_sheets_worker/requirements.txt .
RUN sed -i '/-e .*shared/d' requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy data_connector module (used for registry + fetcher utils)
COPY data_connector /app/data_connector

# Copy service code
COPY google_sheets_worker /app/google_sheets_worker

# Set Python path
ENV PYTHONPATH=/app:$PYTHONPATH

CMD ["python", "-m", "google_sheets_worker.main"]

