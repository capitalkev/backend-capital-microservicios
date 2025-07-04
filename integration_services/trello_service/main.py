import pika
import json
import os
import threading
import time
from fastapi import FastAPI
# from trello_adapter import TrelloAdapter # Descomentar cuando integres tu lógica

app = FastAPI(
    title="Trello Service",
    description="Microservicio para gestionar tarjetas de operaciones en Trello.",
    version="1.0.0"
)

def worker_logic():
    RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    COMMAND_QUEUE = 'q.commands.create.trello'
    EVENT_QUEUE = 'q.events.card.created'
    
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()

            channel.queue_declare(queue=COMMAND_QUEUE, durable=True)
            channel.queue_declare(queue=EVENT_QUEUE, durable=True)
            print('[Trello Service] Conectado a RabbitMQ. Esperando comandos...')
            
            def callback(ch, method, properties, body):
                command = json.loads(body)
                op_id = command.get("operation_id")
                print(f"[Trello Service] Recibido comando para crear tarjeta para op: {op_id}")

                try:
                    # --- INICIO DE LA LÓGICA DE NEGOCIO ---
                    # trello_adapter = TrelloAdapter()
                    # card_url = trello_adapter.create_operation_card(**command['card_details'])
                    # Simulación para desarrollo:
                    time.sleep(2)
                    card_url = f"https://fake-trello.com/c/{op_id}"
                    # --- FIN DE LA LÓGICA DE NEGOCIO ---

                    result_event = {"operation_id": op_id, "status": "SUCCESS", "trello_card_url": card_url}
                    channel.basic_publish(exchange='', routing_key=EVENT_QUEUE, body=json.dumps(result_event))
                    print(f"[Trello Service] Éxito. URL: {card_url}")

                except Exception as e:
                    print(f"[Trello Service] ERROR al crear tarjeta para op {op_id}: {e}")
                    error_event = {"operation_id": op_id, "status": "ERROR", "source_service": "Trello Service", "error_message": str(e)}
                    channel.basic_publish(exchange='', routing_key=EVENT_QUEUE, body=json.dumps(error_event))
                
                finally:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            
            channel.basic_consume(queue=COMMAND_QUEUE, on_message_callback=callback)
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            print("[Trello Service] Conexión perdida con RabbitMQ. Reintentando en 5 segundos...")
            time.sleep(5)
        except Exception as e:
            print(f"[Trello Service] Error inesperado en el worker: {e}")
            break

@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=worker_logic, daemon=True)
    thread.start()

@app.get("/health", summary="Verificar estado del servicio")
def health_check():
    return {"status": "ok", "service": "Trello Service"}
