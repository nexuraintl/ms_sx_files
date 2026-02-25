from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine
from sqlalchemy.orm import sessionmaker
from core.config import settings
from schemas import ClientDBConfig
import logging

logger = logging.getLogger("NFS-Service")

# Motor de gestión (Desactivado para pruebas, pero mantenemos la variable para evitar errores de importación)
engine_gestion = create_async_engine("mysql+aiomysql://user:pass@localhost/db")

# Cache para motores de BD de clientes
_engines: dict[str, AsyncEngine] = {}

async def get_engine_for_client(client_id: str) -> AsyncEngine:
    """
    Versión de PRUEBA: No consulta la BD maestra, usa datos quemados.
    """
    if client_id not in _engines:
        logger.info(f"MODO PRUEBA: Cargando configuración quemada para cliente: {client_id}")
        
        # --- DATOS QUEMADOS (Simula lo que traería la tabla tn_gestion_bdconex) ---
        # Puedes agregar más IDs al diccionario según necesites probar
        config_mock = {
            "20001": {
                "nombreBaseDeDatos": "portal_coomeva_import",
                "usuario": "portal_coomeva_import",
                "contraseña": "8wLVK3wi3l3x",
                "hosting": "10.142.0.7", # Asegúrate que esta IP sea accesible
                "puerto": 3306
            }
        }

        client_data = config_mock.get(client_id)

        if not client_data:
            logger.error(f"Cliente {client_id} no definido en el MOCK de pruebas")
            return None
        
        try:
            # Validamos con el esquema de Pydantic para asegurar que el formato es correcto
            config = ClientDBConfig.model_validate(client_data)
            
            url_cliente = (
                f"mysql+aiomysql://{config.usuario}:{config.password}@"
                f"{config.hosting}:{config.puerto}/{config.nombreBaseDeDatos}"
            )
            
            logger.info(f"Creando motor de BD (MOCK) para cliente: {client_id}")
            _engines[client_id] = create_async_engine(
                url_cliente, 
                pool_size=5, 
                max_overflow=10, 
                pool_recycle=3600,
                pool_pre_ping=True
            )
        except Exception as e:
            logger.error(f"Error en configuración MOCK para cliente {client_id}: {e}")
            return None
            
    return _engines[client_id]

async def get_db_session(client_id: str):
    engine = await get_engine_for_client(client_id)
    if not engine:
        raise ValueError(f"Configuración MOCK no encontrada para: {client_id}")
        
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session() as session:
        yield session