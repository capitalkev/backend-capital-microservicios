import base64
import json
import os
import requests
import time
from fastapi import FastAPI, Request
from google.cloud import pubsub_v1
from typing import List, Dict, Any

app = FastAPI(title="Cavali Service")

# --- Configuración ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "operaciones-peru")
publisher = pubsub_v1.PublisherClient()
EVENT_TOPIC_PATH = publisher.topic_path(PROJECT_ID, "events-cavali-validated")

# Lee la configuración de Cavali de las variables de entorno inyectadas
CAVALI_CONFIG = {
    "client_id": os.getenv("CAVALI_CLIENT_ID"),
    "client_secret": os.getenv("CAVALI_CLIENT_SECRET"),
    "scope": os.getenv("CAVALI_SCOPE"),
    "token_url": os.getenv("CAVALI_TOKEN_URL"),
    "api_key": os.getenv("CAVALI_API_KEY"),
    "block_url": os.getenv("CAVALI_BLOCK_URL"),
    "status_url": os.getenv("CAVALI_STATUS_URL"),
    "batch_size": 30,
    "enabled": os.getenv('CAVALI_ENABLED', 'true').lower() == 'true'
}

# --- Lógica de Negocio (de cavali_adapter.py) ---

def get_access_token() -> str:
    data = {
        "grant_type": "client_credentials",
        "client_id": CAVALI_CONFIG["client_id"],
        "client_secret": CAVALI_CONFIG["client_secret"],
        "scope": CAVALI_CONFIG["scope"]
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    print("Obteniendo token de Cavali...")
    response = requests.post(CAVALI_CONFIG["token_url"], data=data, headers=headers, timeout=30)
    response.raise_for_status()
    print("Token de Cavali obtenido exitosamente.")
    return response.json()["access_token"]

def send_batch(self, batch: List[Dict[str, Any]], headers: Dict[str, str], batch_number: int) -> Dict[str, Any]:
        """
        Envía un único lote a Cavali (paso 1) y consulta su estado (paso 2).
        """
        resultado_bloqueo = None
        try:
            # PASO 1: Enviar el lote para bloqueo
            invoice_xml_list = [
                {"name": f['filename'], "fileXml": base64.b64encode(f['content_bytes']).decode("utf-8")}
                for f in batch
            ]
            process_number = int(time.time()) + batch_number
            payload_bloqueo = {
                "processDetail": {"processNumber": process_number},
                "invoiceXMLDetail": {"invoiceXML": invoice_xml_list}
            }
            
            print(f"Enviando Lote #{batch_number} ({len(batch)} facturas) a Cavali...")
            response_bloqueo = requests.post(self.block_url, json=payload_bloqueo, headers=headers, timeout=60)
            response_bloqueo.raise_for_status()
            resultado_bloqueo = response_bloqueo.json()
            print(f"Respuesta de Bloqueo para Lote #{batch_number}: {resultado_bloqueo}")

            id_proceso = resultado_bloqueo.get("response", {}).get("idProceso")
            if not id_proceso:
                raise ValueError("La respuesta de bloqueo de Cavali no contiene 'idProceso'.")

        except requests.exceptions.RequestException as e:
            print(f"Error en el Paso 1 (Bloqueo) para el Lote #{batch_number}: {e}")
            return {"bloqueo_resultado": {"status": "error", "message": str(e)}, "estado_resultado": None}
        
        try:
            payload_estado = {"ProcessFilter": {"idProcess": id_proceso}}
            print(f"Consultando estado del proceso {id_proceso} (Lote #{batch_number})...")
            response_estado = requests.post(self.status_url, json=payload_estado, headers=headers, timeout=30)
            response_estado.raise_for_status()
            resultado_estado = response_estado.json()
            print(f"Respuesta de Estado para Lote #{batch_number}: {resultado_estado}")
        
        except requests.exceptions.RequestException as e:
            print(f"Error en el Paso 2 (Estado) para el Lote #{batch_number}: {e}")
            return {"bloqueo_resultado": resultado_bloqueo, "estado_resultado": {"status": "error", "message": str(e)}}

        return {
            "bloqueo_resultado": resultado_bloqueo,
            "estado_resultado": resultado_estado
        }

def validate_invoices_in_batches(self, xml_files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Divide la lista completa de XML en lotes y procesa cada uno.
        Retorna una lista con todas las respuestas combinadas de Cavali.
        """
        if not self.is_enabled:
            print("ADVERTENCIA: La integración con Cavali está deshabilitada (CAVALI_ENABLED=false). Omitiendo el envío.")
            return {}

        if not xml_files:
            print("Lote de XML vacío. No se envía a Cavali.")
            return [{"status": "skipped", "message": "No XML files to process."}]

        token = self._get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "x-api-key": self.api_key,
            "Content-Type": "application/json"
        }

        # Dividir la lista de archivos en lotes (chunks)
        batches = [xml_files[i:i + self.BATCH_SIZE] for i in range(0, len(xml_files), self.BATCH_SIZE)]
        all_results = []
        
        print(f"Total de {len(xml_files)} XML a procesar en {len(batches)} lote(s) de hasta {self.BATCH_SIZE} c/u.")

        for i, batch in enumerate(batches):
            batch_result = self._send_batch(batch, headers, i + 1)
            all_results.append(batch_result)
            if i < len(batches) - 1:
                print("Siguiente lote...")
        
        return all_results


@app.post("/", status_code=204)
async def handle_pubsub_message(request: Request):
    envelope = await request.json()
    message = envelope.get("message")
    if not message: return ""

    command = json.loads(base64.b64decode(message["data"]).decode("utf-8"))
    op_id = command.get("operation_id")
    # El orquestador debe enviar los XML como una lista de diccionarios:
    # [{"filename": "...", "content_bytes": "base64-encoded-content"}, ...]
    xml_files_data = command.get("xml_files_data", [])

    print(f"[Cavali Service] Comando recibido para op: {op_id}")
    result_event = {"operation_id": op_id}

    if not CAVALI_CONFIG["enabled"]:
        result_event.update({"status": "SKIPPED", "cavali_results": "Service disabled"})
    elif not xml_files_data:
        result_event.update({"status": "SKIPPED", "cavali_results": "No XML files provided"})
    else:
        try:
            # Aquí la lógica completa de tu adapter
            cavali_responses = validate_invoices_in_batches(xml_files_data)
            result_event.update({"status": "SUCCESS", "cavali_results": cavali_responses})
            print(f"[Cavali Service] Éxito para op: {op_id}")
        except Exception as e:
            print(f"[Cavali Service] ERROR para op {op_id}: {e}")
            result_event.update({"status": "ERROR", "error_message": str(e)})

    publisher.publish(EVENT_TOPIC_PATH, json.dumps(result_event).encode("utf-8"))
    return ""