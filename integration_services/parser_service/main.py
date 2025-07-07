import base64
import json
import os
from fastapi import FastAPI, Request, HTTPException
from google.cloud import pubsub_v1, storage
from parser import extract_invoice_data

app = FastAPI(
    title="Parser Service",
    description="Extrae datos de facturas XML desde Cloud Storage y publica el resultado.",
    version="1.0.0"
)

# Configuración de Pub/Sub
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
RESULT_TOPIC_NAME = "events-invoices-parsed"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, RESULT_TOPIC_NAME)

# Cliente de Cloud Storage
storage_client = storage.Client()

def read_xml_from_gcs(gcs_path):
    if not gcs_path.startswith("gs://"):
        raise ValueError("Ruta GCS inválida")

    parts = gcs_path.replace("gs://", "").split("/", 1)
    bucket_name = parts[0]
    blob_name = parts[1]

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.download_as_bytes()

@app.post("/", summary="Procesa un mensaje de Pub/Sub")
async def handle_pubsub_message(request: Request):
    envelope = await request.json()
    if not envelope or "message" not in envelope:
        raise HTTPException(status_code=400, detail="Payload de Pub/Sub inválido")

    pubsub_message = envelope["message"]
    command = json.loads(base64.b64decode(pubsub_message["data"]).decode("utf-8"))
    
    op_id = command.get("operation_id")
    xml_gcs_path = command.get("xml_file_path")
    
    if not op_id or not xml_gcs_path:
        raise HTTPException(status_code=400, detail="Falta 'operation_id' o 'xml_file_path'")
        
    print(f"[Parser Service] Procesando operación {op_id} desde {xml_gcs_path}")

    try:
        # 1. Descargar contenido del archivo XML desde Cloud Storage
        xml_bytes = read_xml_from_gcs(xml_gcs_path)

        # 2. Parsear la factura
        invoice_data = extract_invoice_data(xml_bytes)

        # 3. Preparar resultado
        result_event = {
            "operation_id": op_id,
            "status": "SUCCESS",
            "parsed_invoice_data": invoice_data
        }
        print(f"[Parser Service] Éxito: {invoice_data.get('document_id')}")

    except Exception as e:
        print(f"[Parser Service] ERROR en {op_id}: {e}")
        result_event = {
            "operation_id": op_id,
            "status": "ERROR",
            "source_service": "Parser Service",
            "error_message": str(e)
        }

    finally:
        # 4. Publicar resultado
        event_data_bytes = json.dumps(result_event).encode("utf-8")
        future = publisher.publish(topic_path, event_data_bytes)
        future.result()
        print(f"[Parser Service] Resultado publicado para op: {op_id}")

    return {"status": "processed"}

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "Parser Service"}
