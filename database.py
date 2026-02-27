import logging
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine
from sqlalchemy.orm import sessionmaker
from core.config import settings
from schemas import ClientDBConfig

logger = logging.getLogger("NFS-Service")

# --- MOCK DE LA BASE DE DATOS MAESTRA ---
# Aquí quemamos los datos de la tabla 'tn_gestion_bdconex' para el cliente 20001
MOCK_CLIENTS_DB = {
    "20001": {
        "nombreBaseDeDatos": "portal_coomeva_import",
        "usuario": "portal_coomeva_import",
        "contraseña": "8wLVK3wi3l3x",
        "hosting": "10.142.0.7",  # IP interna del cliente
        "puerto": 3306
    }
}

# 1. MOTOR DE GESTIÓN (Temporalmente dummy para evitar el error 504/2003)
# Usamos una URL de sqlite en memoria o simplemente no lo inicializamos
engine_gestion = None 

# Cache para motores de BD de clientes
_engines: dict[str, AsyncEngine] = {}

async def get_engine_for_client(client_id: str) -> AsyncEngine:
    """
    Versión HÍBRIDA: Simula la consulta a la BD maestra usando el diccionario quemado.
    """
    if client_id in _engines:
        return _engines[client_id]

    logger.info(f"🛠️ MODO TEMPORAL: Obteniendo credenciales quemadas para cliente: {client_id}")

    # Simulamos la respuesta de la base de datos maestra
    row = MOCK_CLIENTS_DB.get(client_id)

    if not row:
        logger.error(f"❌ No se encontró configuración MOCK para el cliente: {client_id}")
        return None

    try:
        # Validamos con el esquema de Pydantic (usamos 'password' para que coincida con el esquema)
        config_data = row.copy()
        if "contraseña" in config_data:
            config_data["password"] = config_data.pop("contraseña")
            
        config = ClientDBConfig.model_validate(config_data)
        
        url_cliente = (
            f"mysql+aiomysql://{config.usuario}:{config.password}@"
            f"{config.hosting}:{config.puerto}/{config.nombreBaseDeDatos}"
        )
        
        logger.info(f"🏗️ Creando pool de conexiones para cliente: {client_id} (Hacia {config.hosting})")
        
        _engines[client_id] = create_async_engine(
            url_cliente, 
            pool_size=10,
            max_overflow=20, 
            pool_recycle=3600,
            pool_pre_ping=True,
            connect_args={"connect_timeout": 10} # Timeout para la DB del cliente
        )
        return _engines[client_id]

    except Exception as e:
        logger.error(f"🔥 Error al crear motor para cliente {client_id}: {str(e)}")
        return None

async def get_db_session(client_id: str):
    engine = await get_engine_for_client(client_id)
    if not engine:
        raise ValueError(f"No se pudo establecer conexión para el cliente: {client_id}")
        
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session() as session:
        yield session