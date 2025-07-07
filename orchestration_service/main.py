from fastapi import FastAPI, UploadFile, Form, File
from google.cloud import pubsub_v1, storage
import os, uuid, json

app = FastAPI(title="Orquestador Capital Express")

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME", "capitalexpress-operations")
TOPIC_NAME = "parser-invoices-xml"

publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

@app.post("/submit-operation")
async def submit_operation(
    xml_file: UploadFile = File(...),
    pdf_file: UploadFile = File(...),
    respaldo_files: list[UploadFile] = File(...),
    tasa: float = Form(...),
    comision: float = Form(...),
    adelanto: float = Form(...),
    correos: str = Form(""),
    cuenta_bancaria: str = Form(...)
):
    operation_id = f"OP-{uuid.uuid4().hex[:8].upper()}"

    # 1. Subir archivos a Cloud Storage
    def upload_file(file: UploadFile, folder: str) -> str:
        blob_path = f"{operation_id}/{folder}/{file.filename}"
        blob = bucket.blob(blob_path)
        blob.upload_from_file(file.file)
        return f"gs://{BUCKET_NAME}/{blob_path}"

    xml_path = upload_file(xml_file, "xml")
    pdf_path = upload_file(pdf_file, "pdf")
    respaldo_paths = [upload_file(f, "respaldos") for f in respaldo_files]

    # 2. Publicar mensaje inicial a parser-invoices-xml
    msg = {
        "operation_id": operation_id,
        "xml_file_path": xml_path
    }

    topic_path = publisher.topic_path(GCP_PROJECT_ID, TOPIC_NAME)
    publisher.publish(topic_path, json.dumps(msg).encode("utf-8"))

    return {
        "status": "started",
        "operation_id": operation_id,
        "xml": xml_path,
        "pdf": pdf_path,
        "respaldos": respaldo_paths
    }