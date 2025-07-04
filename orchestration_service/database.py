# orchestration_service/database.py (VERSIÓN FINAL Y CORRECTA)

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME", "postgres")
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")


if not DB_PASS or not INSTANCE_CONNECTION_NAME:
    raise ValueError("Las variables de entorno DB_PASSWORD e INSTANCE_CONNECTION_NAME deben estar definidas.")

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@/{DB_NAME}"
    f"?host=/cloudsql/{INSTANCE_CONNECTION_NAME}"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()