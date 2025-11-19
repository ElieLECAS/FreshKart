FROM python:3.11-slim

WORKDIR /app

# Install system dependencies including Java (required for PySpark)
RUN apt-get update && apt-get install -y \
    gcc \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables (default-jdk crée un lien symbolique)
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the import script
COPY import_data.py .

# Set working directory for data access but keep script in /app
WORKDIR /data
