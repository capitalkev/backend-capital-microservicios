import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv

load_dotenv()

# Apunta a tu instancia de Cloud SQL o a una BD local para desarrollo
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/orchestration_db")

if not DATABASE_URL:
    raise ValueError("No se ha definido DATABASE_URL en el archivo .env")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    """Función de dependencia para obtener una sesión de BD."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
