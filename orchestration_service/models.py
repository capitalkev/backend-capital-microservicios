from sqlalchemy import Column, String, DateTime, Text, JSON
from sqlalchemy.sql import func
from .database import Base

class OperationState(Base):
    __tablename__ = "operations_log"

    # ID único de la operación, recibido del API Gateway
    operation_id = Column(String(255), primary_key=True, index=True)

    # Estado actual del flujo de trabajo
    status = Column(String(50), nullable=False, index=True)
    
    # Metadatos iniciales de la operación
    metadata = Column(JSON)
    
    # Almacena las rutas de los archivos en el volumen compartido
    file_paths = Column(JSON)
    
    # Almacena los resultados de cada paso
    results = Column(JSON, default={})

    # Mensajes de error si algo falla
    error_message = Column(Text)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
