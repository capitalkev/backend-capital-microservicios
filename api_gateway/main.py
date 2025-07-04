import json
import os
import uuid
from fastapi import FastAPI, Form, File, UploadFile, HTTPException
from typing import List
from google.cloud import storage, pubsub_v1

# --- Configuración de la App y Clientes de Google Cloud ---
app = FastAPI(title="API Gateway (Modo Prueba Directa)")
storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()

# --- Variables de Entorno ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = "operaciones-capital-express-uploads"
COMMAND_TOPIC_NAME = "commands-parse-xml" 
topic_path = publisher.topic_path(PROJECT_ID, COMMAND_TOPIC_NAME)

@app.post("/api/v1/operaciones", status_code=202)
async def create_operation(
    metadata: str = Form(...),
    files: List[UploadFile] = File(...)
):
    operation_id = str(uuid.uuid4())
    xml_file = next((f for f in files if f.filename.lower().endswith('.xml')), None)

    if not xml_file:
        raise HTTPException(status_code=400, detail="No se encontró un archivo XML en la subida.")

    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        # Sube solo el archivo XML para esta prueba
        blob_name = f"{operation_id}/{xml_file.filename}"
        blob = bucket.blob(blob_name)
        blob.upload_from_file(xml_file.file, content_type=xml_file.content_type)
        gcs_xml_path = f"gs://{BUCKET_NAME}/{blob_name}"

        # Prepara el COMANDO directo para el parser
        command_to_publish = {
            "operation_id": operation_id,
            "xml_file_path": gcs_xml_path 
        }

        # Publica el comando en Pub/Sub
        command_data_bytes = json.dumps(command_to_publish).encode("utf-8")
        future = publisher.publish(topic_path, command_data_bytes)
        future.result()
        
        print(f"[API Gateway] Comando de parseo publicado para op: {operation_id}")
        return {"status": "parse_command_sent", "operation_id": operation_id, "file_path": gcs_xml_path}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en la subida o publicación: {str(e)}")

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "API Gateway"}