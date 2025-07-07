import base64
import json
import os
import io
from fastapi import FastAPI, Request
from google.cloud import pubsub_v1, storage
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

app = FastAPI(title="Drive Service")

# --- Configuración ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "operaciones-peru")
publisher = pubsub_v1.PublisherClient()
EVENT_TOPIC_PATH = publisher.topic_path(PROJECT_ID, "events-drive-archived")
storage_client = storage.Client()
CLIENT_SECRETS_JSON = os.getenv("CLIENT_SECRETS_FILE")
OAUTH_TOKEN_JSON = os.getenv("OAUTH_TOKEN_FILE")
DRIVE_PARENT_FOLDER_ID = os.getenv("DRIVE_PARENT_FOLDER_ID") # Asegúrate de crear este secreto

def get_drive_service():
    client_config = json.loads(CLIENT_SECRETS_JSON).get("installed")
    token_info = json.loads(OAUTH_TOKEN_JSON)
    creds = Credentials.from_authorized_user_info(info=token_info, client_config=client_config)
    return build('drive', 'v3', credentials=creds)

# --- Lógica de Negocio (de google_drive_adapter.py) ---
def archive_operation_files(drive_service, operation_id: str, file_paths: list) -> str:
    print(f"[{operation_id}] Iniciando subida a Google Drive...")
    folder_name = f"Operacion_{operation_id}"
    
    folder_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [DRIVE_PARENT_FOLDER_ID]
    }
    folder = drive_service.files().create(body=folder_metadata, fields='id, webViewLink').execute()
    folder_id = folder.get('id')
    folder_url = folder.get('webViewLink')

    for gcs_path in file_paths:
        try:
            filename = os.path.basename(gcs_path)
            bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
            blob = storage_client.bucket(bucket_name).blob(blob_name)
            file_content = blob.download_as_bytes()
            
            file_metadata = {'name': filename, 'parents': [folder_id]}
            media = MediaIoBaseUpload(io.BytesIO(file_content), resumable=True)
            drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        except Exception as e:
            print(f"ADVERTENCIA: Falló la subida de {filename}. Error: {e}")
            
    return folder_url

@app.post("/", status_code=204)
async def handle_pubsub_message(request: Request):
    envelope = await request.json()
    message = envelope.get("message")
    if not message: return ""

    command = json.loads(base64.b64decode(message["data"]).decode("utf-8"))
    op_id = command.get("operation_id")
    file_paths = command.get("file_paths", [])

    result_event = {"operation_id": op_id}
    print(f"[Drive Service] Comando recibido para op: {op_id}")
    try:
        service = get_drive_service()
        folder_url = archive_operation_files(service, op_id, file_paths)
        result_event.update({"status": "SUCCESS", "drive_folder_url": folder_url})
    except Exception as e:
        print(f"[Drive Service] ERROR para op {op_id}: {e}")
        result_event.update({"status": "ERROR", "error_message": str(e)})

    publisher.publish(EVENT_TOPIC_PATH, json.dumps(result_event).encode("utf-8"))
    return ""