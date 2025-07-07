import base64
import json
import os
from fastapi import FastAPI, Request
from google.cloud import pubsub_v1, storage
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import pandas as pd

app = FastAPI(title="Gmail Service")

# --- Configuración ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "operaciones-peru")
publisher = pubsub_v1.PublisherClient()
EVENT_TOPIC_PATH = publisher.topic_path(PROJECT_ID, "events-gmail-sent")
storage_client = storage.Client()
CLIENT_SECRETS_JSON = os.getenv("CLIENT_SECRETS_FILE")
OAUTH_TOKEN_JSON = os.getenv("OAUTH_TOKEN_FILE")

def get_gmail_service():
    client_config = json.loads(CLIENT_SECRETS_JSON).get("installed")
    token_info = json.loads(OAUTH_TOKEN_JSON)
    creds = Credentials.from_authorized_user_info(info=token_info, client_config=client_config)
    return build('gmail', 'v1', credentials=creds)

# --- Lógica de Negocio (de gmail_adapter.py) ---

def create_html_body(invoice_data_list: list) -> str:
        """
        Crea el cuerpo HTML del correo a partir de una lista de datos de facturas.
        """
        if not invoice_data_list:
            return "<p>No se encontraron datos de facturas válidos para procesar en esta operación.</p>"

        cliente_nombre = invoice_data_list[0].client_name
        cliente_ruc = invoice_data_list[0].client_ruc

        data_for_df = [invoice.dict() for invoice in invoice_data_list]
        df = pd.DataFrame(data_for_df)

        df['total_amount'] = df.apply(
            lambda row: f"{row.get('currency', '')} {float(row.get('total_amount', 0)):,.2f}".strip(),
            axis=1
        )
        df['net_amount'] = df.apply(
            lambda row: f"{row.get('currency', '')} {float(row.get('net_amount', 0)):,.2f}".strip(),
            axis=1
        )
        df['due_date'] = pd.to_datetime(df['due_date']).dt.strftime('%d/%m/%Y')

        df_display = df.rename(columns={
            'total_amount': 'Monto Factura',
            'due_date': 'Fecha de Pago',
            'debtor_name': 'Empresa Emisora',
            'debtor_ruc': 'RUC Emisor',
            'document_id': 'Documento',
            'net_amount': 'Monto Neto'
        })
        
        display_columns = ['RUC Emisor', 'Empresa Emisora', 'Documento', 'Monto Factura', 'Monto Neto', 'Fecha de Pago']
        df_html = df_display[display_columns]

        tabla_html = df_html.to_html(index=False, border=0, justify='center', classes='invoices_table')

        mensaje_html = f"""
        <!DOCTYPE html>
        <html lang="es">
        <head>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; font-size: 14px; color: #333; line-height: 1.6; }}
                .email-container {{ max-width: 700px; margin: 20px auto; padding: 20px; border: 1px solid #ddd; border-radius: 8px; background-color: #f9f9f9; }}
                table.invoices_table {{ border-collapse: collapse; width: 100%; margin: 25px 0; }}
                th, td {{ text-align: left; padding: 12px; border-bottom: 1px solid #eee; }}
                th {{ background-color: #f2f2f2; font-weight: 600; color: #555; }}
                .highlight {{ font-weight: 600; color: #0056b3; }}
                .disclaimer {{ font-style: italic; color: #777; font-size: 11px; margin-top: 30px; border-top: 1px solid #eee; padding-top: 15px; }}
            </style>
        </head>
        <body>
            <div class="email-container">
                <p>Estimados señores,</p>
                <p>
                    Por medio de la presente, les informamos que los señores de 
                    <span class="highlight">{cliente_nombre}</span> (RUC: {cliente_ruc}) nos han transferido la(s) siguiente(s)
                    factura(s) negociable(s). Agradeceríamos su amable confirmación.
                </p>
                <h3>Detalle de las facturas:</h3>
                {tabla_html}
                <p class="disclaimer"><strong>Cláusula Legal:</strong> Sin perjuicio de lo anteriormente mencionado, nos permitimos recordarles que toda acción tendiente a simular 
                la emisión de la referida factura negociable para obtener un beneficio, teniendo pleno conocimiento de que la misma no proviene de una relación comercial verdadera, 
                se encuentra sancionada penalmente como delito de estafa en nuestro ordenamiento jurídico. Asimismo, en caso de que vuestra representada cometa un delito de forma 
                conjunta con el emitente de la factura, dicha acción podría tipificarse como delito de asociación ilícita para delinquir, según el artículo 317 del Código Penal, 
                por lo que nos reservamos el derecho de iniciar las acciones penales correspondientes.</p>
            </div>
        </body>
        </html>
        """
        return mensaje_html

def send_confirmation_email(gmail_service, recipient: str, subject: str, invoices: list, attachment_paths: list):
    html_body = create_html_body(invoices)
    message = MIMEMultipart()
    message['to'] = recipient
    message['subject'] = subject
    message.attach(MIMEText(html_body, 'html'))

    for gcs_path in attachment_paths:
        filename = os.path.basename(gcs_path)
        bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
        blob = storage_client.bucket(bucket_name).blob(blob_name)
        file_content = blob.download_as_bytes()
        
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(file_content)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f"attachment; filename=\"{filename}\"")
        message.attach(part)

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    gmail_service.users().messages().send(userId='me', body={'raw': raw_message}).execute()
    print(f"Correo enviado a {recipient}")

@app.post("/", status_code=204)
async def handle_pubsub_message(request: Request):
    envelope = await request.json()
    message = envelope.get("message")
    if not message: return ""

    command = json.loads(base64.b64decode(message["data"]).decode("utf-8"))
    op_id = command.get("operation_id")
    # El orquestador debe enviar todos estos datos
    recipient = command.get("recipient_email")
    subject = command.get("email_subject")
    invoices_data = command.get("invoices_data", [])
    attachment_paths = command.get("attachment_paths", [])

    result_event = {"operation_id": op_id}
    print(f"[Gmail Service] Comando recibido para op: {op_id}")
    try:
        service = get_gmail_service()
        send_confirmation_email(service, recipient, subject, invoices_data, attachment_paths)
        result_event.update({"status": "SUCCESS", "sent_to": recipient})
    except Exception as e:
        print(f"[Gmail Service] ERROR para op {op_id}: {e}")
        result_event.update({"status": "ERROR", "error_message": str(e)})

    publisher.publish(EVENT_TOPIC_PATH, json.dumps(result_event).encode("utf-8"))
    return ""