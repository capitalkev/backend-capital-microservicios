import pika
import json
import os
import threading
import time
from fastapi import FastAPI
# from cavali_adapter import CavaliAdapter # Descomentar cuando integres tu lógica

# --- Configuración de la App ---
app = FastAPI(
    title="Cavali Service",
    description="Microservicio para validar facturas con la API de Cavali.",
    version="1.0.0"
)

# --- Lógica del Worker (Consumidor de RabbitMQ) ---
def worker_logic():
    """Escucha los comandos de validación y procesa los XML con Cavali."""
    RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    COMMAND_QUEUE = 'q.commands.validate.cavali'
    EVENT_QUEUE = 'q.events.invoices.validated'

    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()

            channel.queue_declare(queue=COMMAND_QUEUE, durable=True)
            channel.queue_declare(queue=EVENT_QUEUE, durable=True)
            print('[Cavali Service] Conectado a RabbitMQ. Esperando comandos...')

            def callback(ch, method, properties, body):
                command = json.loads(body)
                op_id = command.get("operation_id")
                print(f"[Cavali Service] Recibido comando para validar op: {op_id}")

                try:
                    # --- INICIO DE LA LÓGICA DE NEGOCIO ---
                    # cavali_adapter = CavaliAdapter()
                    # # Aquí necesitarías leer el contenido de los archivos XML
                    # xml_files_content = command.get("xml_files_content")
                    # cavali_responses = cavali_adapter.validate_invoices_in_batches(xml_files_content)
                    
                    # Simulación para desarrollo:
                    time.sleep(8) # Simula el tiempo de la API de Cavali
                    cavali_responses = [{"status": "SUCCESS", "message": "Factura validada correctamente."}]
                    # --- FIN DE LA LÓGICA DE NEGOCIO ---

                    result_event = {"operation_id": op_id, "status": "SUCCESS", "cavali_results": cavali_responses}
                    channel.basic_publish(exchange='', routing_key=EVENT_QUEUE, body=json.dumps(result_event))
                    print(f"[Cavali Service] Éxito al validar facturas para op: {op_id}")

                except Exception as e:
                    print(f"[Cavali Service] ERROR al validar op {op_id}: {e}")
                    error_event = {"operation_id": op_id, "status": "ERROR", "source_service": "Cavali Service", "error_message": str(e)}
                    channel.basic_publish(exchange='', routing_key=EVENT_QUEUE, body=json.dumps(error_event))
                
                finally:
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue=COMMAND_QUEUE, on_message_callback=callback)
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            print("[Cavali Service] Conexión perdida con RabbitMQ. Reintentando en 5 segundos...")
            time.sleep(5)
        except Exception as e:
            print(f"[Cavali Service] Error inesperado en el worker: {e}")
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
    return {"status": "ok", "service": "Cavali Service"}
