import pika
import json
import os
import threading
from fastapi import FastAPI
# Aquí importarías tu adaptador real. Ej:
# from your_project.drive_adapter import GoogleDriveAdapter

# --- Configuración ---
app = FastAPI(title="Drive Service", description="Microservicio para interactuar con Google Drive.")
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')

# --- Lógica del Worker (Consumidor de RabbitMQ) ---
def worker_logic():
    """Esta función se ejecuta en un hilo separado para no bloquear la API."""
    COMMAND_QUEUE = 'q.commands.archive.drive'
    EVENT_QUEUE = 'q.events.files.archived'

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=COMMAND_QUEUE, durable=True)
    channel.queue_declare(queue=EVENT_QUEUE, durable=True)

    def callback(ch, method, properties, body):
        command = json.loads(body)
        op_id = command.get("operation_id")
        file_paths = command.get("file_paths", [])
        
        print(f"[Drive Service] Comando recibido para archivar op: {op_id}")

        try:
            # --- LÓGICA REAL ---
            # 1. Instanciar tu adaptador
            # drive_adapter = GoogleDriveAdapter() 
            # 2. Llamar a tu método de negocio
            # folder_url = drive_adapter.archive_operation_files(op_id, file_paths)
            
            # --- SIMULACIÓN ---
            print(f"Simulando subida de {len(file_paths)} archivos a Drive...")
            folder_url = f"https://drive.google.com/drive/folders/fake-folder-id-for-{op_id}"
            
            # Publicar evento de éxito
            result_event = {"operation_id": op_id, "status": "SUCCESS", "drive_folder_url": folder_url}
            channel.basic_publish(exchange='', routing_key=EVENT_QUEUE, body=json.dumps(result_event))
            print(f"[Drive Service] Éxito. URL: {folder_url}")

        except Exception as e:
            # Publicar evento de error
            error_event = {"operation_id": op_id, "status": "ERROR", "error_message": str(e)}
            channel.basic_publish(exchange='', routing_key=EVENT_QUEUE, body=json.dumps(error_event))
            print(f"[Drive Service] ERROR: {e}")
        
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=COMMAND_QUEUE, on_message_callback=callback)
    print('[Drive Service] Worker iniciado, esperando comandos...')
    channel.start_consuming()

# --- API y Arranque ---
@app.on_event("startup")
def startup_event():
    """Al iniciar la API, arranca el consumidor en un hilo demonio."""
    thread = threading.Thread(target=worker_logic, daemon=True)
    thread.start()

@app.get("/health", summary="Verifica el estado del servicio")
def health_check():
    return {"status": "ok", "service": "Drive Service"}
