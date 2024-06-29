# Usar una imagen base de Python oficial
FROM python:3.9-slim

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Copiar los archivos de requerimientos y el código fuente al contenedor
COPY requirements.txt ./
COPY . .

# Instalar las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Exponer el puerto que tu aplicación utiliza
EXPOSE 8000

# Comando para ejecutar la aplicación
CMD ["python", "API_Rest.py"]
