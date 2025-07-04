from sqlalchemy import Column, String, DateTime, Text, JSON
from sqlalchemy.sql import func
from .database import Base

class OperationLog(Base):
    __tablename__ = "operations_log"
    operation_id = Column(String, primary_key=True)
    status = Column(String, nullable=False, default="RECEIVED")
    file_paths = Column(JSON) # Para guardar las rutas GCS de los archivos
    results = Column(JSON, default={})
    error_message = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())