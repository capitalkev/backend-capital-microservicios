# Usa la imagen completa de Python 3.9, que es más robusta
FROM python:3.9

ENV PYTHONUNBUFFERED True
WORKDIR /app

# Actualiza pip primero para asegurar la compatibilidad de los paquetes
RUN python -m pip install --upgrade pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]