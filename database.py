from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine
from sqlalchemy.orm import sessionmaker
from core.config import settings
from schemas import ClientDBConfig
import logging

logger = logging.getLogger("NFS-Service")

# Motor para la base de datos de gestión (Maestra)
# Se construye usando las variables de entorno definidas en config.py
url_gestion = (
    f"mysql+aiomysql://{settings.DB_GESTION_USER}:{settings.DB_GESTION_PASS}@"
    f"{settings.DB_GESTION_HOST}:{settings.DB_GESTION_PORT}/{settings.DB_GESTION_NAME}"
)

# pool_pre_ping=True ayuda a Cloud Run a descartar conexiones muertas automáticamente
engine_gestion = create_async_engine(
    url_gestion, 
    pool_pre_ping=True,
    pool_recycle=3600
)

# Cache para motores de BD de clientes
_engines: dict[str, AsyncEngine] = {}

async def get_engine_for_client(client_id: str) -> AsyncEngine:
    """
    Consulta la tabla de gestión y retorna el motor de BD del cliente.
    Si no existe en caché, lo crea y lo guarda.
    """
    if client_id not in _engines:
        logger.info(f"Buscando configuración para el cliente: {client_id}")
        
        async with AsyncSession(engine_gestion) as session:
            # Consulta a la tabla maestra
            query = text("""
                SELECT nombreBaseDeDatos, usuario, contraseña, hosting, puerto 
                FROM tn_gestion_bdconex 
                WHERE idCliente = :c_id LIMIT 1
            """)
            
            result = await session.execute(query, {"c_id": client_id})
            row = result.fetchone()
            
            if not row:
                logger.error(f"No se encontró el cliente {client_id} en tn_gestion_bdconex")
                return None
            
            try:
                # Validamos y mapeamos los datos usando Pydantic
                # row._asdict() convierte el resultado de SQLAlchemy en un diccionario compatible
                config = ClientDBConfig.model_validate(row._asdict())
                
                # Construcción de la URL dinámica para el cliente
                url_cliente = (
                    f"mysql+aiomysql://{config.usuario}:{config.password}@"
                    f"{config.hosting}:{config.puerto}/{config.nombreBaseDeDatos}"
                )
                
                logger.info(f"Creando nuevo motor de BD para cliente: {client_id}")
                _engines[client_id] = create_async_engine(
                    url_cliente, 
                    pool_size=5, 
                    max_overflow=10, 
                    pool_recycle=3600,
                    pool_pre_ping=True
                )
            except Exception as e:
                logger.error(f"Error procesando credenciales para cliente {client_id}: {e}")
                return None
            
    return _engines[client_id]

async def get_db_session(client_id: str):
    """
    Generador de sesión dinámico (Dependency Injection para FastAPI).
    """
    engine = await get_engine_for_client(client_id)
    if not engine:
        raise ValueError(f"Configuración de base de datos no encontrada para el cliente: {client_id}")
        
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session() as session:
        yield session