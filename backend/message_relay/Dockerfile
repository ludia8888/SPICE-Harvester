FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy shared modules first
COPY shared /app/shared

# Install shared package
RUN pip install -e /app/shared

# Copy service requirements and install
COPY message_relay/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code
COPY message_relay /app/message_relay

# Set Python path
ENV PYTHONPATH=/app:$PYTHONPATH

# Run the service
CMD ["python", "-m", "message_relay.main"]