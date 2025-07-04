import pika
import json
import uuid
import os
import shutil
from fastapi import FastAPI, Form, File, UploadFile, HTTPException
from typing import List

app = FastAPI(title="API Gateway")

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
QUEUE_NAME = 'q.operations.received'
TEMP_UPLOADS_DIR = "/tmp/uploads"

os.makedirs(TEMP_UPLOADS_DIR, exist_ok=True)

def publish_event(event: dict):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_publish(
        exchange='',
        routing_key=QUEUE_NAME,
        body=json.dumps(event),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    connection.close()

@app.post("/api/v1/operaciones", status_code=202)
async def create_operation(
    metadata: str = Form(...),
    files: List[UploadFile] = File(...)
):
    operation_id = str(uuid.uuid4())
    operation_temp_path = os.path.join(TEMP_UPLOADS_DIR, operation_id)
    os.makedirs(operation_temp_path, exist_ok=True)

    temp_file_paths = {}
    try:
        for file in files:
            file_path = os.path.join(operation_temp_path, file.filename)
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            temp_file_paths[file.filename] = file_path

        event = {
            "operation_id": operation_id,
            "metadata": json.loads(metadata),
            "temp_file_paths": temp_file_paths
        }

        publish_event(event)
        return {"status": "processing_queued", "operation_id": operation_id}
    except Exception as e:
        # Limpieza en caso de error
        if os.path.exists(operation_temp_path):
            shutil.rmtree(operation_temp_path)
        raise HTTPException(status_code=500, detail=f"Error: {e}")

@app.get("/health")
def health_check():
    return {"status": "ok"}