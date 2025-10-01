FROM python:3.11-slim

WORKDIR /app

COPY solution/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY solution/main.py .

COPY data ./data

ENV DATA_ROOT=/app

CMD ["python", "main.py"]
