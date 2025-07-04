import pika
import json
import os
import threading
import time
from fastapi import FastAPI
# from lxml import etree # Descomentar cuando integres tu lógica

# --- Configuración de la App ---
app = FastAPI(
    title="Parser Service",
    description="Microservicio para extraer datos de facturas XML.",
    version="1.0.0"
)

# --- Lógica del Worker (Consumidor de RabbitMQ) ---
def worker_logic():
    """Escucha los comandos de parseo y extrae la información de los XML."""
    RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    COMMAND_QUEUE = 'q.commands.parse.xml'
    EVENT_QUEUE = 'q.events.invoices.parsed'

    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()

            channel.queue_declare(queue=COMMAND_QUEUE, durable=True)
            channel.queue_declare(queue=EVENT_QUEUE, durable=True)
            print('[Parser Service] Conectado a RabbitMQ. Esperando comandos...')

            def callback(ch, method, properties, body):
                command = json.loads(body)
                op_id = command.get("operation_id")
                xml_file_paths = command.get("xml_file_paths", [])
                print(f"[Parser Service] Recibido comando para parsear {len(xml_file_paths)} XMLs para op: {op_id}")

                try:
                    parsed_invoices = []
                    # --- INICIO DE LA LÓGICA DE NEGOCIO ---
                    # for xml_path in xml_file_paths:
                    #     # Aquí adaptas tu lógica de _parse_xml_files
                    #     with open(xml_path, 'rb') as f:
                    #         xml_content = f.read()
                    #     # ... lógica de parseo con lxml ...
                    #     parsed_invoices.append(invoice_data_dict)
                    
                    # Simulación para desarrollo:
                    time.sleep(1)
                    for i, path in enumerate(xml_file_paths):
                        parsed_invoices.append({"document_id": f"F001-SIM-{i}", "client_ruc": "20100066601", "total_amount": 1500.0})
                    # --- FIN DE LA LÓGICA DE NEGOCIO ---

                    result_event = {"operation_id": op_id, "status": "SUCCESS", "parsed_invoices": parsed_invoices}
                    channel.basic_publish(exchange='', routing_key=EVENT_QUEUE, body=json.dumps(result_event))
                    print(f"[Parser Service] Éxito al parsear facturas para op: {op_id}")

                except Exception as e:
                    print(f"[Parser Service] ERROR al parsear op {op_id}: {e}")
                    error_event = {"operation_id": op_id, "status": "ERROR", "source_service": "Parser Service", "error_message": str(e)}
                    channel.basic_publish(exchange='', routing_key=EVENT_QUEUE, body=json.dumps(error_event))
                
                finally:
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue=COMMAND_QUEUE, on_message_callback=callback)
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            print("[Parser Service] Conexión perdida con RabbitMQ. Reintentando en 5 segundos...")
            time.sleep(5)
        except Exception as e:
            print(f"[Parser Service] Error inesperado en el worker: {e}")
            break

# --- Endpoints de la API y arranque del worker ---
@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=worker_logic, daemon=True)
    thread.start()

@app.get("/health", summary="Verificar estado del servicio")
def health_check():
    return {"status": "ok", "service": "Parser Service"}
