FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir fastapi uvicorn pymongo

COPY telemetry_api.py /app/telemetry_api.py

EXPOSE 8000

CMD ["uvicorn", "telemetry_api:app", "--host", "0.0.0.0", "--port", "8000"]