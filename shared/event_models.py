from pydantic import BaseModel
from typing import List, Dict, Any

class OperationReceivedEvent(BaseModel):
    operation_id: str
    metadata: Dict[str, Any]
    # En un sistema real, aquí irían las URLs a un bucket de S3/GCS
    temp_file_paths: Dict[str, str] 

class ParseCommand(BaseModel):
    operation_id: str
    xml_file_paths: List[str]

# Define aquí otros modelos para comandos y eventos
# Ej: FilesArchivedEvent, InvoicesValidatedEvent, etc.