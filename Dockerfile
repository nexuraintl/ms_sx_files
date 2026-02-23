# Usamos una imagen base oficial de Python ligera
FROM python:3.10-slim

# Evita que Python genere archivos .pyc y permite ver logs en tiempo real en Cloud Run
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Establecemos el directorio de trabajo
WORKDIR /app

# Instalamos dependencias del sistema necesarias para compilar paquetes
# (gcc y libmysqlclient-dev son a veces necesarios para drivers de BD avanzados)
RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copiamos primero el archivo de requerimientos para aprovechar la caché de Docker
COPY requirements.txt .

# Instalamos las dependencias de Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiamos el resto del código de la aplicación
COPY . .

# Creamos el directorio base para el montaje, aunque Cloud Run lo sobreescribirá al montar el NFS.
# Esto es buena práctica para evitar errores si se corre en local sin montar nada.
RUN mkdir -p /app/media

# Creamos un usuario no-root por seguridad (Best Practice en Contenedores)
# Nota: Cloud Run ignora esto si no se configura, pero es bueno tenerlo.
RUN adduser --disabled-password --gecos "" appuser
USER appuser

# Exponemos el puerto (Cloud Run inyecta la variable PORT, por defecto 8080)
EXPOSE 8080

# Comando de inicio usando Uvicorn con workers gestionados para producción
# --proxy-headers es vital porque estás detrás de un Load Balancer
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--proxy-headers"]