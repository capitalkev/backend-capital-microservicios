import threading
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from . import event_handler, repository
from .database import get_db, Base, engine

# Crear tablas en la base de datos al iniciar
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Orchestration Service",
    description="El cerebro del sistema de microservicios. Gestiona el estado de las operaciones.",
    version="1.0.0"
)

@app.on_event("startup")
def startup_event():
    """Inicia el consumidor de eventos de RabbitMQ en un hilo separado."""
    thread = threading.Thread(target=event_handler.start_event_listener, daemon=True)
    thread.start()

@app.get("/api/v1/operaciones/{operation_id}/status", summary="Obtener el estado de una operación")
def get_operation_status(operation_id: str, db: Session = Depends(get_db)):
    """
    Endpoint para que el frontend pueda consultar el estado actual
    y los resultados de una operación específica.
    """
    repo = repository.OperationRepository(db)
    operation = repo.get_operation(operation_id)
    if not operation:
        raise HTTPException(status_code=404, detail="Operación no encontrada")
    return {
        "operation_id": operation.operation_id,
        "status": operation.status,
        "results": operation.results,
        "error": operation.error_message,
        "created_at": operation.created_at,
        "last_updated": operation.updated_at
    }

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "Orchestration Service"}
