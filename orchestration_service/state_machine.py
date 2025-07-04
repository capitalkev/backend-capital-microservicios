from sqlalchemy.orm import Session
from .repository import OperationRepository

def publish_command(channel, queue_name: str, command: dict):
    """Función helper para publicar un comando."""
    import json
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=json.dumps(command)
    )
    print(f" [Orquestador] Publicando comando en '{queue_name}' para op: {command['operation_id']}")

def on_operation_received(channel, db_session: Session, event_data: dict):
    """Acción cuando una nueva operación es recibida del API Gateway."""
    repo = OperationRepository(db_session)
    op_id = event_data['operation_id']
    
    # 1. Guardar el estado inicial en la BD
    repo.create_operation(
        operation_id=op_id,
        metadata=event_data['metadata'],
        file_paths=event_data['file_paths']
    )
    
    # 2. Decidir el primer paso: Parsear los XML
    repo.update_operation_status(op_id, "PARSING_XML")
    
    xml_paths = [path for name, path in event_data['file_paths'].items() if name.lower().endswith('.xml')]
    
    command = {
        "operation_id": op_id,
        "xml_file_paths": xml_paths
    }
    publish_command(channel, 'q.commands.parse.xml', command)


def on_invoices_parsed(channel, db_session: Session, event_data: dict):
    """Acción cuando el Parser Service termina su trabajo."""
    repo = OperationRepository(db_session)
    op_id = event_data['operation_id']
    
    if event_data['status'] == 'ERROR':
        repo.record_error(op_id, f"Parser Service: {event_data['error_message']}")
        return

    # 1. Guardar el resultado del parseo
    repo.record_step_result(op_id, "parsing", {"invoices": event_data['parsed_invoices']})
    
    # 2. Decidir el siguiente paso: Validar con Cavali
    repo.update_operation_status(op_id, "VALIDATING_CAVALI")
    
    operation = repo.get_operation(op_id)
    # Suponiendo que el servicio Cavali necesita el contenido de los archivos
    xml_content = []
    for path in operation.file_paths.values():
        if path.lower().endswith('.xml'):
            with open(path, 'rb') as f:
                # En un caso real, podrías necesitar codificar esto a base64
                xml_content.append({"filename": os.path.basename(path), "content_bytes": f.read().decode('utf-8', 'ignore')})

    command = {
        "operation_id": op_id,
        "xml_files_content": xml_content 
    }
    publish_command(channel, 'q.commands.validate.cavali', command)

# --- Define aquí las demás funciones de estado ---
# def on_invoices_validated(...)
# def on_files_archived(...)
# def on_card_created(...)
# def on_email_sent(...)

# Al final, una función podría verificar si todos los pasos están completos
# y marcar la operación como "COMPLETED".
