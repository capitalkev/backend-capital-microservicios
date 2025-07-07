import base64
import json
import os
import requests
import datetime
from fastapi import FastAPI, Request
from google.cloud import pubsub_v1
from typing import List, Dict, Any

app = FastAPI(title="Trello Service")

# --- Configuración ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "operaciones-peru")
publisher = pubsub_v1.PublisherClient()
EVENT_TOPIC_PATH = publisher.topic_path(PROJECT_ID, "events-trello-created")

TRELLO_CONFIG = {
    "api_key": os.getenv("TRELLO_API_KEY"),
    "api_token": os.getenv("TRELLO_API_TOKEN"),
    "list_id": os.getenv("TRELLO_LIST_ID"),
    "label_ids": os.getenv("TRELLO_LABEL_IDS", "")
}

def _format_number(num: float) -> str:
    return "{:,.2f}".format(num)

def _sanitize_name(name: str) -> str:
    return name.strip() if name else "—"

def create_operation_card(card_details: Dict[str, Any]) -> str:
    print("Creando tarjeta en Trello...")
    
    current_date = datetime.datetime.now().strftime('%d.%m')
    debtors_info = card_details.get("debtors_info", {})
    operation_amounts = card_details.get("operation_amounts", {})
    
    debtors_str = ', '.join(_sanitize_name(name) for name in debtors_info.values() if name) or 'Ninguno'
    amount_str = ', '.join(f"{currency} {_format_number(amount)}" for currency, amount in operation_amounts.items()) or "0.00"

    card_title = (
        f"🤖 {current_date} // CLIENTE: {_sanitize_name(card_details.get('client_name'))} // "
        f"DEUDOR: {debtors_str} // MONTO: {amount_str} // "
        f"{card_details.get('initials', '')} // OP: {card_details.get('operation_id', '')[:8]}"
    )
    
    debtors_markdown = '\n'.join(f"- RUC {ruc}: {_sanitize_name(name)}" for ruc, name in debtors_info.items()) or '- Ninguno'

    card_description = (
        f"**ID Operación:** {card_details.get('operation_id')}\n\n"
        f"**Deudores:**\n{debtors_markdown}\n\n"
        f"**Tasa:** {card_details.get('tasa', 0)}%\n"
        f"**Comisión:** {card_details.get('comision', 0)}\n"
        f"**Monto Operación:** {amount_str}\n\n"
        f"**Carpeta Drive:** {card_details.get('drive_folder_url', 'N/A')}\n\n"
        f"**Errores:** {', '.join(card_details.get('errors', [])) or 'Ninguno'}"
    )
        
    url_card = "https://api.trello.com/1/cards"
    auth_params = {'key': TRELLO_CONFIG['api_key'], 'token': TRELLO_CONFIG['api_token']}
    card_payload = {
        'idList': TRELLO_CONFIG['list_id'],
        'name': card_title,
        'desc': card_description,
        'pos': 'top',
        'idLabels': TRELLO_CONFIG['label_ids']
    }
        
    response = requests.post(url_card, params=auth_params, json=card_payload)
    response.raise_for_status()
    card_data = response.json()
    card_url = card_data['url']
    print(f"Tarjeta creada exitosamente: {card_url}")
    return card_url

@app.post("/", status_code=204)
async def handle_pubsub_message(request: Request):
    envelope = await request.json()
    message = envelope.get("message")
    if not message: return ""

    command = json.loads(base64.b64decode(message["data"]).decode("utf-8"))
    op_id = command.get("operation_id")
    # El orquestador debe enviar un diccionario "card_details" con toda la data
    card_details = command.get("card_details", {})
    card_details["operation_id"] = op_id # Aseguramos que el op_id esté

    print(f"[Trello Service] Comando recibido para op: {op_id}")
    result_event = {"operation_id": op_id}

    if not card_details:
        result_event.update({"status": "SKIPPED", "message": "No card details provided"})
    else:
        try:
            card_url = create_operation_card(card_details)
            result_event.update({"status": "SUCCESS", "trello_card_url": card_url})
        except Exception as e:
            print(f"[Trello Service] ERROR para op {op_id}: {e}")
            result_event.update({"status": "ERROR", "error_message": str(e)})

    publisher.publish(EVENT_TOPIC_PATH, json.dumps(result_event).encode("utf-8"))
    return ""