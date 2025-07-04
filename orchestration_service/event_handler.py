import pika
import json
import os
import time
from sqlalchemy.orm import Session
from .database import get_db
from . import state_machine

# Mapeo de colas a funciones de manejo
EVENT_HANDLERS = {
    'q.operations.received': state_machine.on_operation_received,
    'q.events.invoices.parsed': state_machine.on_invoices_parsed,
    # 'q.events.invoices.validated': state_machine.on_invoices_validated,
    # ... otros eventos
}

def start_event_listener():
    """Inicia el consumidor de RabbitMQ para todos los eventos."""
    RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            print('[Orquestador] Conectado a RabbitMQ. Escuchando eventos...')

            db_session = next(get_db())

            def callback(ch, method, properties, body):
                queue_name = method.routing_key
                handler_func = EVENT_HANDLERS.get(queue_name)
                
                if handler_func:
                    event_data = json.loads(body)
                    print(f" [Orquestador] Evento recibido de '{queue_name}' para op: {event_data.get('operation_id')}")
                    try:
                        handler_func(channel, db_session, event_data)
                    except Exception as e:
                        print(f"ERROR: Falla al procesar evento de '{queue_name}': {e}")
                        # Aquí podrías implementar lógica de reintento o mover a una cola de errores.
                
                ch.basic_ack(delivery_tag=method.delivery_tag)

            # Declarar y consumir de todas las colas de eventos
            for queue in EVENT_HANDLERS.keys():
                channel.queue_declare(queue=queue, durable=True)
                channel.basic_consume(queue=queue, on_message_callback=callback)

            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            print("[Orquestador] Conexión perdida. Reintentando en 5 segundos...")
            time.sleep(5)
        finally:
            if 'db_session' in locals() and db_session:
                db_session.close()
