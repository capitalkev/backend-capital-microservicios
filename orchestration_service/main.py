import base64
import json
import os
from fastapi import FastAPI, Request, HTTPException, Depends
from google.cloud import pubsub_v1
from sqlalchemy.orm import Session

# --- SECCIÓN CORREGIDA ---
# Se quitan los puntos de las importaciones para que sean absolutas.
# Python buscará los archivos en el mismo directorio.
import database
import repository
# --- FIN DE LA SECCIÓN CORREGIDA ---

# --- Configuración ---
app = FastAPI(title="Orchestration Service")
publisher = pubsub_v1.PublisherClient()
PROJECT_ID = os.getenv("GCP_PROJECT_ID")

# Crea la tabla en la base de datos si no existe
database.Base.metadata.create_all(bind=database.engine)

@app.post("/")
async def handle_pubsub_message(request: Request, db: Session = Depends(database.get_db)):
    envelope = await request.json()
    pubsub_message = envelope["message"]
    event_data = json.loads(base64.b64decode(pubsub_message["data"]).decode("utf-8"))
    
    op_id = event_data.get("operation_id")
    print(f"[Orquestador] Evento recibido para op: {op_id}")
    
    repo = repository.OperationRepository(db)

    # Lógica de orquestación (simplificada para el ejemplo)
    if pubsub_message["attributes"]["type"] == "OperationReceived":
        repo.create_log(op_id, event_data.get("file_paths", {}))
        repo.update_status(op_id, "PARSING")

        xml_path = next((path for filename, path in event_data.get("file_paths", {}).items() if filename.lower().endswith('.xml')), None)
        
        if xml_path:
            command = {"operation_id": op_id, "xml_file_path": xml_path}
            topic_path = publisher.topic_path(PROJECT_ID, "commands-parse-xml")
            publisher.publish(topic_path, json.dumps(command).encode("utf-8"))
            print(f"[Orquestador] Comando de parseo publicado para op: {op_id}")
        else:
            repo.update_status(op_id, "FAILED_NO_XML")
    
    return {"status": "processed"}

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "Orchestration Service"}