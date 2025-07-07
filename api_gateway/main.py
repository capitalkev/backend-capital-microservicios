import json
import os
import uuid
import datetime
from fastapi import FastAPI, Form, File, UploadFile, HTTPException
from typing import List
from google.cloud import storage, pubsub_v1

# --- Configuración ---
app = FastAPI(title="API Gateway")
storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "operaciones-peru")
BUCKET_NAME = "capitalexpress-operations" # Usando el nombre de tu nueva lógica
EVENT_TOPIC_NAME = "operations-received"  # El tópico inicial y genérico
topic_path = publisher.topic_path(PROJECT_ID, EVENT_TOPIC_NAME)

@app.post("/api/v1/submit-operation", status_code=202)
async def submit_operation(
    tasa: float = Form(...),
    comision: float = Form(...),
    adelanto: float = Form(...),
    cuenta_bancaria: str = Form(...),
    correos: str = Form(""),
    xml_file: UploadFile = File(...),
    pdf_file: UploadFile = File(...),
    respaldo_files: List[UploadFile] = File(...)
):
    """
    Recibe todos los datos de la operación desde el frontend,
    sube los archivos a GCS en carpetas estructuradas y publica
    un único evento enriquecido para el orquestador.
    """
    operation_id = f"OP-{uuid.uuid4().hex[:8].upper()}"
    print(f"Iniciando nueva operación: {operation_id}")

    # Helper para subir archivos
    def upload_file(file: UploadFile, subfolder: str) -> str:
        try:
            blob_path = f"{operation_id}/{subfolder}/{file.filename}"
            blob = storage_client.bucket(BUCKET_NAME).blob(blob_path)
            blob.upload_from_file(file.file, content_type=file.content_type)
            file.file.seek(0)
            gcs_path = f"gs://{BUCKET_NAME}/{blob_path}"
            print(f"Archivo subido: {gcs_path}")
            return gcs_path
        except Exception as e:
            print(f"ERROR subiendo {file.filename}: {e}")
            raise HTTPException(status_code=500, detail=f"Error al subir el archivo {file.filename}.")

    try:
        # 1. Subir todos los archivos a GCS en sus carpetas correspondientes
        xml_path = upload_file(xml_file, "xml")
        pdf_path = upload_file(pdf_file, "pdf")
        respaldo_paths = [upload_file(f, "respaldos") for f in respaldo_files]
        
        all_file_paths = {
            "xml": xml_path,
            "pdf": pdf_path,
            "respaldos": respaldo_paths
        }

        # 2. Construir el evento enriquecido para el orquestador
        event_to_publish = {
            "operation_id": operation_id,
            "status": "RECEIVED",
            "received_at": datetime.datetime.utcnow().isoformat(),
            "file_paths": all_file_paths,
            "operation_details": {
                "tasa": tasa,
                "comision": comision,
                "adelanto": adelanto,
                "cuenta_bancaria": cuenta_bancaria,
                "correos_adicionales": correos.split(',') if correos else []
            }
        }

        # 3. Publicar el evento inicial
        future = publisher.publish(topic_path, json.dumps(event_to_publish).encode("utf-8"))
        future.result()
        
        print(f"[API Gateway] Evento de recepción publicado para op: {operation_id}")
        return {"status": "received", "operation_id": operation_id}

    except Exception as e:
        print(f"ERROR en API Gateway para op {operation_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "API Gateway"}