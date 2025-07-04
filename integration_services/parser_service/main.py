import base64
import json
import os
from fastapi import FastAPI, Request, HTTPException
from google.cloud import pubsub_v1

# Importa la lógica de parseo del archivo local
from parser import extract_invoice_data

# --- Configuración de la App y Pub/Sub ---
app = FastAPI(
    title="Parser Service",
    description="Extrae datos de facturas XML recibidas vía Pub/Sub.",
    version="1.0.0"
)

# Configuración de Pub/Sub para publicar el resultado
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
RESULT_TOPIC_NAME = "events-invoices-parsed"  # Tema donde se publicará el resultado
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, RESULT_TOPIC_NAME)

@app.post("/", summary="Procesa un mensaje de Pub/Sub")
async def handle_pubsub_message(request: Request):
    """
    Recibe un mensaje de Pub/Sub, lee el archivo XML, lo parsea,
    y publica el resultado en un nuevo tema.
    """
    envelope = await request.json()
    if not envelope or "message" not in envelope:
        raise HTTPException(status_code=400, detail="Payload de Pub/Sub inválido")

    pubsub_message = envelope["message"]
    command = json.loads(base64.b64decode(pubsub_message["data"]).decode("utf-8"))
    
    op_id = command.get("operation_id")
    xml_path = command.get("xml_file_path")
    
    if not op_id or not xml_path:
        raise HTTPException(status_code=400, detail="El comando no contiene 'operation_id' o 'xml_file_path'")
        
    print(f"[Parser Service] Mensaje recibido para parsear XML de op: {op_id}")

    try:
        # 1. Leer el contenido del archivo XML
        with open(xml_path, 'rb') as f:
            xml_content_bytes = f.read()
            
        # 2. Llamar a la función de parseo
        invoice_data = extract_invoice_data(xml_content_bytes)
        
        # 3. Preparar el evento de resultado
        result_event = {
            "operation_id": op_id,
            "status": "SUCCESS",
            "parsed_invoice_data": invoice_data
        }
        print(f"[Parser Service] Éxito al parsear la factura: {invoice_data.get('document_id')}")

    except Exception as e:
        print(f"[Parser Service] ERROR procesando op {op_id}: {e}")
        result_event = {
            "operation_id": op_id,
            "status": "ERROR",
            "source_service": "Parser Service",
            "error_message": str(e)
        }
        
    finally:
        # 4. Publicar el resultado (éxito o error)
        event_data_bytes = json.dumps(result_event).encode("utf-8")
        future = publisher.publish(topic_path, event_data_bytes)
        future.result() # Espera a que se complete la publicación
        print(f"[Parser Service] Evento publicado en '{RESULT_TOPIC_NAME}' para op: {op_id}")

    # 5. Devolver un 200 OK a Pub/Sub
    return {"status": "processed"}

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "Parser Service"}