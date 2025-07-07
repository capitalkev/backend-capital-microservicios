import base64
import json
import os
from fastapi import FastAPI, Request, HTTPException
from google.cloud import pubsub_v1, storage
from parser import extract_invoice_data

app = FastAPI(title="Parser Service")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "operaciones-peru")
RESULT_TOPIC_NAME = "events-invoices-parsed"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, RESULT_TOPIC_NAME)
storage_client = storage.Client()

def read_xml_from_gcs(gcs_path):
    parts = gcs_path.replace("gs://", "").split("/", 1)
    bucket = storage_client.bucket(parts[0])
    blob = bucket.blob(parts[1])
    return blob.download_as_bytes()

@app.post("/", status_code=204)
async def handle_pubsub_message(request: Request):
    envelope = await request.json()
    message = envelope.get("message")
    if not message: raise HTTPException(status_code=400, detail="Payload inválido")

    command = json.loads(base64.b64decode(message["data"]).decode("utf-8"))
    op_id = command.get("operation_id")
    xml_path = command.get("xml_file_path")
    
    if not op_id or not xml_path: return ""
        
    print(f"[Parser Service] Procesando op {op_id} desde {xml_path}")

    try:
        xml_bytes = read_xml_from_gcs(xml_path)
        invoice_data = extract_invoice_data(xml_bytes)
        result_event = {
            "operation_id": op_id, "status": "SUCCESS",
            "parsed_invoice_data": invoice_data
        }
    except Exception as e:
        print(f"[Parser Service] ERROR en {op_id}: {e}")
        result_event = {
            "operation_id": op_id, "status": "ERROR",
            "error_message": str(e)
        }

    future = publisher.publish(topic_path, json.dumps(result_event).encode("utf-8"))
    future.result()
    print(f"[Parser Service] Resultado publicado para op: {op_id}")
    return ""