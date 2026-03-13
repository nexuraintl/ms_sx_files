import os
from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):

    # Datos de la DB de Gestión (Maestra)
    DB_GESTION_HOST: str = os.getenv("DB_GESTION_HOST")
    DB_GESTION_USER: str = os.getenv("DB_GESTION_USER")
    DB_GESTION_PASS: str = os.getenv("DB_GESTION_PASS")
    DB_GESTION_NAME: str = os.getenv("DB_GESTION_NAME", "gestion_clientes")
    DB_GESTION_PORT: int = int(os.getenv("DB_GESTION_PORT", 3306))

    @property
    def DATABASE_URL_GESTION(self) -> str:
        return (
            f"mysql+aiomysql://{self.DB_GESTION_USER}:{self.DB_GESTION_PASS}@"
            f"{self.DB_GESTION_HOST}:{self.DB_GESTION_PORT}/{self.DB_GESTION_NAME}"
        )

    # Configuración del Microservicio
    APP_TITLE: str = "NFS Download Microservice"
    LOG_LEVEL: str = "INFO"
    
    # Parámetros de Performance
    # 1MB por chunk es ideal para no saturar la RAM en descargas grandes
    CHUNK_SIZE: int = 64 * 1024 #256KB
    
    # Seguridad
    # Tiempo en segundos para el bloqueo anti-spam
    ANTI_SPAM_SECONDS: int = 10

    class Config:
        case_sensitive = True

# Instancia global para ser importada en otros archivos
settings = Settings()