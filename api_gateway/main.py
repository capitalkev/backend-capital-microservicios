import pika
import json
import uuid
import os
import shutil
from fastapi import FastAPI, Form, File, UploadFile, HTTPException
from typing import List

# --- Configuración ---
app = FastAPI(title="API Gateway", description="Punto de entrada para todas las operaciones.")
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
QUEUE_NAME = 'q.operations.received'
TEMP_UPLOADS_DIR = "/tmp/uploads"

# --- Lógica de Negocio ---
def publish_event(event_body: dict):
    """Publica un evento en la cola de operaciones recibidas."""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_publish(
        exchange='',
        routing_key=QUEUE_NAME,
        body=json.dumps(event_body),
        properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
    )
    connection.close()
    print(f" [x] Evento 'OperationReceived' enviado para op_id: {event_body['operation_id']}")

# --- Endpoints ---
@app.on_event("startup")
def startup_event():
    os.makedirs(TEMP_UPLOADS_DIR, exist_ok=True)

@app.post("/api/v1/operaciones", status_code=202, summary="Crea una nueva operación de factoring")
async def create_operation(metadata: str = Form(...), files: List[UploadFile] = File(...)):
    """
    Recibe los archivos y metadatos, los guarda temporalmente y dispara
    un evento para que el backend comience el procesamiento asíncrono.
    """
    operation_id = str(uuid.uuid4())
    operation_temp_path = os.path.join(TEMP_UPLOADS_DIR, operation_id)
    os.makedirs(operation_temp_path)

    file_paths = {file.filename: os.path.join(operation_temp_path, file.filename) for file in files}
    
    try:
        for file in files:
            with open(file_paths[file.filename], "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)

        event_body = {
            "operation_id": operation_id,
            "metadata": json.loads(metadata),
            "file_paths": file_paths
        }
        publish_event(event_body)
        return {"status": "processing_queued", "operation_id": operation_id}
    except Exception as e:
        if os.path.exists(operation_temp_path):
            shutil.rmtree(operation_temp_path)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health", summary="Verifica el estado del servicio")
def health_check():
    return {"status": "ok", "service": "API Gateway"}
