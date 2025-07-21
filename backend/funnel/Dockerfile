# 🔥 THINK ULTRA! Funnel Service Dockerfile
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire backend directory (contains all services)
COPY . .

# Expose port
EXPOSE 8003

# Set PYTHONPATH to ensure modules can be imported
ENV PYTHONPATH=/app

# Run the application
CMD ["python", "-m", "funnel.main"]