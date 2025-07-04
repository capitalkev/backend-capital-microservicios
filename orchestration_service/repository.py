from sqlalchemy.orm import Session
from . import models

class OperationRepository:
    def __init__(self, db: Session):
        self.db = db

    def create_operation(self, operation_id: str, metadata: dict, file_paths: dict):
        """Crea un nuevo registro para una operación."""
        db_operation = models.OperationState(
            operation_id=operation_id,
            status="RECEIVED",
            metadata=metadata,
            file_paths=file_paths,
            results={}
        )
        self.db.add(db_operation)
        self.db.commit()
        self.db.refresh(db_operation)
        return db_operation

    def get_operation(self, operation_id: str):
        """Obtiene una operación por su ID."""
        return self.db.query(models.OperationState).filter(models.OperationState.operation_id == operation_id).first()

    def update_operation_status(self, operation_id: str, new_status: str):
        """Actualiza solo el estado de una operación."""
        db_operation = self.get_operation(operation_id)
        if db_operation:
            db_operation.status = new_status
            self.db.commit()
            self.db.refresh(db_operation)
        return db_operation

    def record_step_result(self, operation_id: str, step_name: str, result_data: dict):
        """Guarda el resultado de un paso (ej. la URL de Drive)."""
        db_operation = self.get_operation(operation_id)
        if db_operation:
            # Actualiza el diccionario de resultados de forma segura
            current_results = db_operation.results.copy() if db_operation.results else {}
            current_results[step_name] = result_data
            db_operation.results = current_results
            self.db.commit()
            self.db.refresh(db_operation)
        return db_operation

    def record_error(self, operation_id: str, error_message: str):
        """Registra un error y marca la operación como fallida."""
        db_operation = self.get_operation(operation_id)
        if db_operation:
            db_operation.status = "FAILED"
            db_operation.error_message = error_message
            self.db.commit()
            self.db.refresh(db_operation)
        return db_operation
