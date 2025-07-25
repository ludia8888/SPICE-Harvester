FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY bff/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared modules first
COPY shared /app/shared

# Copy application code
COPY bff /app/bff

# Create data directory for SQLite
RUN mkdir -p /app/data

# Set Python path
ENV PYTHONPATH=/app

# Expose port
EXPOSE 8002

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8002/health || exit 1

# Run the application
CMD ["uvicorn", "bff.main:app", "--host", "0.0.0.0", "--port", "8002", "--log-level", "info"]