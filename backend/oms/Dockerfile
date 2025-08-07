FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install build tools
RUN pip install --upgrade pip setuptools wheel

# Copy shared modules first
COPY shared /app/shared

# Install shared package (temporarily hide pyproject.toml to force setup.py usage)
RUN cd /app/shared && mv pyproject.toml pyproject.toml.bak && cd /app && pip install ./shared && cd /app/shared && mv pyproject.toml.bak pyproject.toml

# Copy requirements
COPY oms/requirements.txt .

# Remove shared package reference (already installed above) and install remaining dependencies if any
RUN sed -i '/-e .*shared/d' requirements.txt && \
    (if [ -s requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi)

# Verify critical dependencies are installed
RUN python -c "import uvicorn; import fastapi; import terminusdb_client; import asyncpg; import confluent_kafka; print('All critical dependencies installed successfully!')"

# Copy application code
COPY oms /app/oms

# Create data directory
RUN mkdir -p /app/data

# Set Python path
ENV PYTHONPATH=/app:/app/shared

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application
CMD ["python", "-m", "uvicorn", "oms.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]