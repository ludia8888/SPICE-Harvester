FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install build tools
RUN pip install --upgrade pip setuptools wheel

# Copy shared modules first
COPY shared /app/shared
RUN mkdir -p /app/backend

# Copy MCP modules for backend.mcp imports
COPY mcp /app/backend/mcp

# Install shared package (temporarily hide pyproject.toml to force setup.py usage)
RUN cd /app/shared && mv pyproject.toml pyproject.toml.bak && cd /app && pip install ./shared && cd /app/shared && mv pyproject.toml.bak pyproject.toml

# Copy requirements first for better caching
COPY bff/requirements.txt .

# Remove shared package reference (already installed above) and install remaining dependencies if any
RUN sed -i '/-e .*shared/d' requirements.txt && \
    (if [ -s requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi)

# Verify critical dependencies are installed
RUN python -c "import uvicorn; import fastapi; import aiosqlite; print('All critical dependencies installed successfully!')"

# Copy application code
COPY bff /app/bff

# Copy data_connector module
COPY data_connector /app/data_connector

# Create data directory for SQLite
RUN mkdir -p /app/data

# Set Python path
ENV PYTHONPATH=/app

# Expose port
EXPOSE 8002

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8002/api/v1/health || exit 1

# Run the application
CMD ["python", "-m", "uvicorn", "bff.main:app", "--host", "0.0.0.0", "--port", "8002", "--log-level", "info"]
