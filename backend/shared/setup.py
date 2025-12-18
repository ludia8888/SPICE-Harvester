#!/usr/bin/env python3
"""
Setup script for spice-shared package

This provides backward compatibility for editable installs
with older pip versions that don't fully support pyproject.toml
"""

from setuptools import setup, find_packages

# Read requirements from pyproject.toml would be ideal,
# but for simplicity we'll just use setuptools
setup(
    name="spice-shared",
    version="0.1.0",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        # 🚀 Web Framework - MSA Core Stack
        "fastapi==0.104.1",
        "uvicorn[standard]==0.24.0",
        "httpx==0.25.2",
        
        # 📋 Data Validation & Settings
        "pydantic==2.5.0",
        "pydantic-settings==2.1.0",
        "python-dotenv==1.0.0",
        
        # 🗄️ Database & Caching
        "redis[hiredis]==5.0.1",
        "elasticsearch==8.11.0",
        "aiohttp==3.9.5",  # Required for Elasticsearch async client (also required by aioboto3/aiobotocore)
        "asyncpg==0.29.0",
        "aiosqlite==0.19.0",
        
        # 📬 Message Queue
        "confluent-kafka==2.3.0",
        
        # 🧠 Graph Database
        "terminusdb-client==10.2.0",
        
        # ☁️ Storage
        "boto3==1.38.27",
        "botocore==1.38.27",
        "aioboto3==15.0.0",
        
        # 🔐 Authentication & Security
        "python-jose[cryptography]==3.3.0",
        "passlib[bcrypt]==1.7.4",
        "bcrypt==4.1.1",
        
        # 🌐 HTTP & File Handling
        "python-multipart==0.0.6",
        "aiofiles==23.2.1",
        
        # 📊 Data Processing
        "pandas==2.1.3",
        "numpy==1.24.3",
        "openpyxl==3.1.2",
        
        # 🔍 Validation
        "email-validator==2.0.0",
        "phonenumbers==8.13.0",
        
        # 🔗 Google APIs
        "google-auth==2.27.0",
        "google-auth-oauthlib==1.2.0",
        "google-auth-httplib2==0.2.0",
        "google-api-python-client==2.116.0",
        
        # 📝 Logging
        "python-json-logger==2.0.7",
        
        # 📊 Observability - OpenTelemetry
        "opentelemetry-api==1.23.0",
        "opentelemetry-sdk==1.23.0",
        "opentelemetry-instrumentation==0.44b0",
        "opentelemetry-instrumentation-fastapi==0.44b0",
        "opentelemetry-instrumentation-httpx==0.44b0",
        "opentelemetry-instrumentation-asyncio==0.44b0",
        "opentelemetry-exporter-otlp==1.23.0",
        "opentelemetry-exporter-jaeger==1.21.0",
        "prometheus-client==0.19.0",
    ],
    package_data={
        "shared": ["py.typed"],
    },
)
