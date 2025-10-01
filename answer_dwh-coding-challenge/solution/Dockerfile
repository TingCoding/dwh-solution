# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

# Install deps
COPY solution/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY solution/main.py .

# Copy data dari root repo
COPY data ./data

# Env (opsional)
ENV DATA_ROOT=/app

CMD ["python", "main.py"]
