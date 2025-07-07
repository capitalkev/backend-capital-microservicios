from sqlalchemy.orm import Session
from . import models

class OperationRepository:
    def __init__(self, db: Session):
        self.db = db

    def create_log(self, operation_id: str, file_paths: dict):
        db_log = models.OperationLog(operation_id=operation_id, file_paths=file_paths)
        self.db.add(db_log)
        self.db.commit()

    def update_status(self, operation_id: str, new_status: str, error_msg: str = None):
        db_log = self.db.query(models.OperationLog).filter_by(operation_id=operation_id).first()
        if db_log:
            db_log.status = new_status
            if error_msg:
                db_log.error_message = error_msg
            self.db.commit()
    
    def get_operation(self, operation_id: str):
        return self.db.query(models.OperationLog).filter_by(operation_id=operation_id).first()