import pika
import json
import os
from repository import OperationRepository
from database import SessionLocal
from shared.event_models import OperationReceivedEvent, ParseCommand

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')

# Colas de entrada
OP_RECEIVED_QUEUE = 'q.operations.received'

# Colas de salida (comandos)
PARSE_COMMAND_QUEUE = 'q.commands.parse'
# ...otras colas de comandos

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def publish_command(queue_name: str, command: dict):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=json.dumps(command),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    connection.close()

def handle_operation_received(ch, method, properties, body):
    db = next(get_db())
    repo = OperationRepository(db)
    
    event_data = json.loads(body)
    event = OperationReceivedEvent(**event_data)
    
    print(f"[Orquestador] Recibida operación: {event.operation_id}")
    
    # 1. Crear registro en la BD
    repo.create_operation(
        operation_id=event.operation_id,
        status='RECEIVED',
        user_email=event.metadata.get('user_email', 'N/A')
    )
    
    # 2. Publicar comando para el siguiente paso: Parseo
    xml_files = [path for name, path in event.temp_file_paths.items() if name.lower().endswith('.xml')]
    
    parse_command = ParseCommand(
        operation_id=event.operation_id,
        xml_file_paths=xml_files
    )
    
    publish_command(PARSE_COMMAND_QUEUE, parse_command.dict())
    print(f"[Orquestador] Publicado comando para parsear XML de {event.operation_id}")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_listening():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    
    # Declarar colas
    channel.queue_declare(queue=OP_RECEIVED_QUEUE, durable=True)
    channel.queue_declare(queue=PARSE_COMMAND_QUEUE, durable=True)
    
    # Consumir de la cola de operaciones recibidas
    channel.basic_consume(queue=OP_RECEIVED_QUEUE, on_message_callback=handle_operation_received)

    print('[Orquestador] Esperando eventos...')
    channel.start_consuming()

if __name__ == '__main__':
    start_listening()