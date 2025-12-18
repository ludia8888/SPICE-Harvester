# 🔥 THINK ULTRA! Funnel Service Dockerfile
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

# Copy requirements first for better caching
COPY funnel/requirements.txt .

# Remove shared package reference (already installed above)
RUN sed -i '/-e .*shared/d' requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY funnel /app/funnel
COPY data_connector /app/data_connector

# Expose port
EXPOSE 8003

# Set PYTHONPATH to ensure modules can be imported
ENV PYTHONPATH=/app

# Run the application
CMD ["python", "-m", "uvicorn", "funnel.main:app", "--host", "0.0.0.0", "--port", "8003", "--log-level", "info"]
