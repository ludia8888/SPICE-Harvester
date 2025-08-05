FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy shared modules first
COPY shared /app/shared
COPY oms /app/oms

# Install shared package
RUN pip install -e /app/shared

# Copy service requirements and install
COPY instance_worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code
COPY instance_worker /app/instance_worker

# Set Python path
ENV PYTHONPATH=/app:$PYTHONPATH

# Run the service
CMD ["python", "-m", "instance_worker.main"]