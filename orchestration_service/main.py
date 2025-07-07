import base64
import json
import os
from fastapi import FastAPI, Request, HTTPException, Depends
from sqlalchemy.orm import Session
from google.cloud import pubsub_v1, firestore

import models, repository, database

# --- Configuración ---
app = FastAPI(title="Orchestration Service")
publisher = pubsub_v1.PublisherClient()
db_firestore = firestore.Client()
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "operaciones-peru")
FIRESTORE_COLLECTION = "operations_status"

@app.on_event("startup")
def on_startup():
    database.Base.metadata.create_all(bind=database.engine)

def publish_command(topic_name: str, data: dict):
    topic_path = publisher.topic_path(PROJECT_ID, topic_name)
    future = publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
    future.result()
    print(f"  -> Comando '{topic_name}' publicado para op: {data['operation_id']}")

def update_firestore_status(op_id: str, status: str, details: dict = None):
    doc_ref = db_firestore.collection(FIRESTORE_COLLECTION).document(op_id)
    update_data = {"status": status, "last_updated": firestore.SERVER_TIMESTAMP}
    if details:
        update_data["details"] = details
    doc_ref.set(update_data, merge=True)
    print(f"  -> Estado en Firestore actualizado a: {status}")

@app.post("/", status_code=204)
async def handle_pubsub_message(request: Request, db: Session = Depends(database.get_db)):
    envelope = await request.json()
    message = envelope.get("message")
    if not message:
        raise HTTPException(status_code=400, detail="Payload de Pub/Sub inválido.")
    
    source_topic = message.get("attributes", {}).get("googclient_delivery_topic", "unknown").split("/")[-1]
    event_data = json.loads(base64.b64decode(message["data"]).decode("utf-8"))
    op_id = event_data.get("operation_id")
    
    if not op_id:
        return ""

    print(f"[Orquestador] Evento '{source_topic}' recibido para op: {op_id}")
    repo = repository.OperationRepository(db)
    
    try:
        if source_topic == "operations-received":
            repo.create_log(operation_id=op_id, file_paths=event_data.get("file_paths", {}))
            update_firestore_status(op_id, "RECIBIDO")
            repo.update_status(op_id, "PARSING_XML")
            update_firestore_status(op_id, "PROCESANDO_FACTURA")
            
            xml_path = next((p for p in event_data.get("file_paths", {}).values() if p.lower().endswith('.xml')), None)
            if xml_path:
                publish_command("commands-parse-xml", {
                    "operation_id": op_id,
                    "xml_file_path": xml_path
                })

        elif source_topic == "events-invoices-parsed":
            if event_data.get("status") == "SUCCESS":
                repo.update_status(op_id, "VALIDATING_CAVALI")
                update_firestore_status(op_id, "VALIDANDO_CON_CAVALI")
                # Aquí publicarías el comando para Cavali
                # publish_command("commands-validate-cavali", {...})
            else:
                error = event_data.get("error_message")
                repo.update_status(op_id, "ERROR_PARSING", error)
                update_firestore_status(op_id, "ERROR_PROCESANDO_FACTURA", {"error": error})
        
        # Aquí puedes seguir agregando lógica para otros eventos futuros...

    except Exception as e:
        error_msg = f"Error crítico en Orquestador: {e}"
        print(f"ERROR para op {op_id}: {error_msg}")
        repo.update_status(op_id, "ERROR_ORCHESTRATION", error_msg)
        update_firestore_status(op_id, "ERROR_INESPERADO", {"error": error_msg})
    
    return ""