# Use official lightweight Python image
FROM python:3.11-slim

# Set working directory 
WORKDIR /app

# Installingg system dependencies if required (sqlite3 is built-in with python)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements or install libraries directly

RUN pip install --no-cache-dir requests

#all python source code into the container
COPY . /app/

# Initializing the database upon container build/startup
RUN python database.py

# Default command to check status or keep alive
CMD ["python", "pipeline.py", "status"]