FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install build tools
RUN pip install --upgrade pip setuptools wheel

# Copy shared modules first
COPY shared /app/shared
COPY oms /app/oms

# Install shared package (temporarily hide pyproject.toml to force setup.py usage)
RUN cd /app/shared && mv pyproject.toml pyproject.toml.bak && cd /app && pip install ./shared && cd /app/shared && mv pyproject.toml.bak pyproject.toml

# Copy service requirements and install
COPY instance_worker/requirements.txt .

# Remove shared package reference (already installed above)
RUN sed -i '/-e .*shared/d' requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code
COPY instance_worker /app/instance_worker

# Set Python path
ENV PYTHONPATH=/app:$PYTHONPATH

# Run the service
CMD ["python", "-m", "instance_worker.main"]