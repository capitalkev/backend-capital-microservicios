import pika
import json
import os
import threading
import time
from fastapi import FastAPI
# from gmail_adapter import GmailAdapter # Descomentar cuando integres tu lógica

# --- Configuración de la App ---
app = FastAPI(
    title="Gmail Service",
    description="Microservicio para enviar notificaciones por correo electrónico.",
    version="1.0.0"
)

# --- Lógica del Worker (Consumidor de RabbitMQ) ---
def worker_logic():
    """Escucha los comandos de envío de correo y los procesa."""
    RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    COMMAND_QUEUE = 'q.commands.send.gmail'
    EVENT_QUEUE = 'q.events.email.sent'

    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()

            channel.queue_declare(queue=COMMAND_QUEUE, durable=True)
            channel.queue_declare(queue=EVENT_QUEUE, durable=True)
            print('[Gmail Service] Conectado a RabbitMQ. Esperando comandos...')

            def callback(ch, method, properties, body):
                command = json.loads(body)
                op_id = command.get("operation_id")
                recipient = command.get("recipient")
                print(f"[Gmail Service] Recibido comando para enviar email a '{recipient}' para op: {op_id}")

                try:
                    # --- INICIO DE LA LÓGICA DE NEGOCIO ---
                    # gmail_adapter = GmailAdapter()
                    # sent_message = gmail_adapter.send_confirmation_email(
                    #     recipient=recipient,
                    #     operation_id=op_id,
                    #     invoices=command.get("invoices_data"),
                    #     attachments=command.get("attachments")
                    # )
                    
                    # Simulación para desarrollo:
                    time.sleep(3)
                    sent_message_id = f"gmail-msg-id-{op_id}"
                    # --- FIN DE LA LÓGICA DE NEGOCIO ---

                    result_event = {"operation_id": op_id, "status": "SUCCESS", "recipient": recipient, "message_id": sent_message_id}
                    channel.basic_publish(exchange='', routing_key=EVENT_QUEUE, body=json.dumps(result_event))
                    print(f"[Gmail Service] Éxito al enviar email para op: {op_id}")

                except Exception as e:
                    print(f"[Gmail Service] ERROR al enviar email para op {op_id}: {e}")
                    error_event = {"operation_id": op_id, "status": "ERROR", "source_service": "Gmail Service", "error_message": str(e)}
                    channel.basic_publish(exchange='', routing_key=EVENT_QUEUE, body=json.dumps(error_event))
                
                finally:
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue=COMMAND_QUEUE, on_message_callback=callback)
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            print("[Gmail Service] Conexión perdida con RabbitMQ. Reintentando en 5 segundos...")
            time.sleep(5)
        except Exception as e:
            print(f"[Gmail Service] Error inesperado en el worker: {e}")
            break

# --- Endpoints de la API y arranque del worker ---
@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=worker_logic, daemon=True)
    thread.start()

@app.get("/health", summary="Verificar estado del servicio")
def health_check():
    return {"status": "ok", "service": "Gmail Service"}
