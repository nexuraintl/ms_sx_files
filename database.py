import logging
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine
from sqlalchemy.orm import sessionmaker
from core.config import settings
from schemas import ClientDBConfig

logger = logging.getLogger("NFS-Service")

# 1. MOTOR DE GESTIÓN (Base de datos maestra donde están las credenciales de clientes)
# Asegúrate de que settings.DATABASE_URL_GESTION tenga el formato:
# mysql+aiomysql://usuario:password@IP_MAESTRA:3306/nombre_bd_gestion
engine_gestion = create_async_engine(
    settings.DATABASE_URL_GESTION,
    pool_size=5,
    max_overflow=2,
    pool_recycle=3600,
    pool_pre_ping=True,
    connect_args={"connect_timeout": 5}
)


# Cache para motores de BD de clientes (para no recrear la conexión en cada petición)
_engines: dict[str, AsyncEngine] = {}

async def get_engine_for_client(client_id: str) -> AsyncEngine:
    """
    Consulta la BD maestra para obtener las credenciales del cliente
    y retorna (o crea) su motor de conexión.
    """
    if client_id in _engines:
        return _engines[client_id]

    logger.info(f"🔍 Consultando credenciales maestras para cliente: {client_id}")

    async_session_gestion = sessionmaker(engine_gestion, expire_on_commit=False, class_=AsyncSession)
    
    async with async_session_gestion() as session:
        try:
            # Consulta a la tabla donde guardas las conexiones (ajusta los nombres de campos si varían)
            query = text("""
                SELECT 
                    nombreBaseDeDatos, 
                    usuario, 
                    contrasena as password, 
                    hosting, 
                    puerto 
                FROM tn_gestion_bdconex 
                WHERE id_cliente = :client_id 
                AND estado = 1 
                LIMIT 1
            """)
            
            result = await session.execute(query, {"client_id": client_id})
            row = result.mappings().first()

            if not row:
                logger.error(f"❌ No se encontró configuración activa para el cliente: {client_id}")
                return None

            # Validamos con el esquema de Pydantic
            config = ClientDBConfig.model_validate(dict(row))
            
            url_cliente = (
                f"mysql+aiomysql://{config.usuario}:{config.password}@"
                f"{config.hosting}:{config.puerto}/{config.nombreBaseDeDatos}"
            )
            
            logger.info(f"🏗️ Creando nuevo pool de conexiones para cliente: {client_id}")
            
            _engines[client_id] = create_async_engine(
                url_cliente, 
                pool_size=10,        # Aumentado para producción
                max_overflow=20, 
                pool_recycle=3600,
                pool_pre_ping=True
            )
            return _engines[client_id]

        except Exception as e:
            logger.error(f"🔥 Error al obtener motor para cliente {client_id}: {str(e)}")
            return None

async def get_db_session(client_id: str):
    """
    Generador de sesiones para el cliente solicitado.
    """
    engine = await get_engine_for_client(client_id)
    if not engine:
        raise ValueError(f"No se pudo establecer conexión para el cliente: {client_id}")
        
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session() as session:
        yield session