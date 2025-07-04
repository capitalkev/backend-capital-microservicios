import pika
import json
import os
import threading
import time
from fastapi import FastAPI
# from drive_adapter import GoogleDriveAdapter # Descomentar cuando integres tu lógica

# --- Configuración de la App ---
app = FastAPI(
    title="Drive Service",
    description="Microservicio para archivar archivos de operaciones en Google Drive.",
    version="1.0.0"
)

# --- Lógica del Worker (Consumidor de RabbitMQ) ---
def worker_logic():
    """Esta función corre en un hilo separado y se encarga de escuchar la cola."""
    RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    COMMAND_QUEUE = 'q.commands.archive.drive'
    EVENT_QUEUE = 'q.events.files.archived'

    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()

            channel.queue_declare(queue=COMMAND_QUEUE, durable=True)
            channel.queue_declare(queue=EVENT_QUEUE, durable=True)
            print('[Drive Service] Conectado a RabbitMQ. Esperando comandos...')

            def callback(ch, method, properties, body):
                command = json.loads(body)
                op_id = command.get("operation_id")
                all_filenames = command.get("all_filenames", [])
                local_folder_path = command.get("local_folder_path", "")
                
                print(f"[Drive Service] Recibido comando para archivar op: {op_id}")

                try:
                    # --- INICIO DE LA LÓGICA DE NEGOCIO ---
                    # drive_adapter = GoogleDriveAdapter()
                    # folder_url = drive_adapter.archive_operation_files(
                    #     operation_id=op_id,
                    #     local_folder_path=local_folder_path,
                    #     all_filenames=all_filenames
                    # )
                    # Simulación para desarrollo:
                    time.sleep(5) # Simula el tiempo de subida
                    folder_url = f"https://fake-drive-url.com/folder/{op_id}"
                    # --- FIN DE LA LÓGICA DE NEGOCIO ---
                    
                    result_event = {"operation_id": op_id, "status": "SUCCESS", "drive_folder_url": folder_url}
                    channel.basic_publish(exchange='', routing_key=EVENT_QUEUE, body=json.dumps(result_event))
                    print(f"[Drive Service] Éxito. URL: {folder_url}")

                except Exception as e:
                    print(f"[Drive Service] ERROR al archivar op {op_id}: {e}")
                    error_event = {"operation_id": op_id, "status": "ERROR", "source_service": "Drive Service", "error_message": str(e)}
                    channel.basic_publish(exchange='', routing_key=EVENT_QUEUE, body=json.dumps(error_event))
                
                finally:
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue=COMMAND_QUEUE, on_message_callback=callback)
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            print("[Drive Service] Conexión perdida con RabbitMQ. Reintentando en 5 segundos...")
            time.sleep(5)
        except Exception as e:
            print(f"[Drive Service] Error inesperado en el worker: {e}")
            break


# --- Endpoints de la API y arranque del worker ---
@app.on_event("startup")
def startup_event():
    """Al iniciar la app, corre el worker en un hilo en segundo plano."""
    thread = threading.Thread(target=worker_logic, daemon=True)
    thread.start()

@app.get("/health", summary="Verificar estado del servicio")
def health_check():
    """Endpoint para verificar que el servicio está activo."""
    return {"status": "ok", "service": "Drive Service"}
