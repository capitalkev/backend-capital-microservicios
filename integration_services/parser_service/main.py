import pika
import json
import os
from lxml import etree
from shared.event_models import ParseCommand

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
COMMAND_QUEUE = 'q.commands.parse'
EVENT_QUEUE = 'q.events.invoices.parsed' # Publica su resultado aquí

def publish_event(queue_name: str, event: dict):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(event))
    connection.close()

def parse_xml_file(file_path: str) -> dict:
    # Aquí adaptas tu lógica de _parse_xml_files de tu use case original
    # para que procese un solo archivo y devuelva un diccionario.
    # ... Lógica de parseo ...
    print(f"Parseando archivo: {file_path}")
    # Simulación
    return {"document_id": "F001-123", "total_amount": 1000.0}


def handle_parse_command(ch, method, properties, body):
    command_data = json.loads(body)
    command = ParseCommand(**command_data)
    
    print(f"[Parser] Recibido comando para parsear op: {command.operation_id}")
    
    parsed_invoices = []
    try:
        for xml_path in command.xml_file_paths:
            invoice_data = parse_xml_file(xml_path)
            parsed_invoices.append(invoice_data)
        
        # Publicar evento de éxito
        result_event = {
            "operation_id": command.operation_id,
            "status": "SUCCESS",
            "invoices": parsed_invoices
        }
        publish_event(EVENT_QUEUE, result_event)
        print(f"[Parser] Éxito al parsear {len(parsed_invoices)} facturas.")

    except Exception as e:
        # Publicar evento de error
        error_event = {
            "operation_id": command.operation_id,
            "status": "ERROR",
            "error_message": str(e)
        }
        publish_event(EVENT_QUEUE, error_event)
        print(f"[Parser] ERROR al parsear: {e}")
        
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)

def start_listening():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=COMMAND_QUEUE, durable=True)
    channel.queue_declare(queue=EVENT_QUEUE, durable=True)
    channel.basic_consume(queue=COMMAND_QUEUE, on_message_callback=handle_parse_command)
    
    print('[Parser Service] Esperando comandos...')
    channel.start_consuming()

if __name__ == '__main__':
    start_listening()