# Usa una imagen base de Python oficial y ligera
FROM python:3.10-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia primero el archivo de dependencias para aprovechar el cache de Docker
COPY requirements.txt requirements.txt

# Instala las dependencias
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Copia el resto del código de la aplicación
COPY . .

# Expone el puerto que usará FastAPI
EXPOSE 8080

# Comando para ejecutar la aplicación con Uvicorn
# Se usa 0.0.0.0 para que sea accesible desde fuera del contenedor
# y 8080 es el puerto estándar que espera Cloud Run
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]