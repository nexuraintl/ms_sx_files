import os
from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    # Configuración de Google Cloud
    PROJECT_ID: str = os.getenv("GCP_PROJECT_ID", "tu-proyecto-id")
    SECRET_ID: str = os.getenv("GCP_SECRET_ID", "MICROSERVICE_CONFIG")
    SECRET_VERSION: str = "latest"

    # Configuración del Microservicio
    APP_TITLE: str = "NFS Download Microservice"
    LOG_LEVEL: str = "INFO"
    
    # Parámetros de Performance
    # 1MB por chunk es ideal para no saturar la RAM en descargas grandes
    CHUNK_SIZE: int = 1024 * 1024 
    
    # Seguridad
    # Tiempo en segundos para el bloqueo anti-spam
    ANTI_SPAM_SECONDS: int = 10

    class Config:
        case_sensitive = True

# Instancia global para ser importada en otros archivos
settings = Settings()