FROM python:3.10-slim

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir fastapi google-cloud-pubsub google-cloud-storage uvicorn python-multipart

ENV PORT=8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]