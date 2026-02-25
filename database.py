from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from core.config import settings

# Motor para la base de datos de gestión (Maestra)
# Se construye usando las variables de entorno definidas en config.py
url_gestion = (
    f"mysql+aiomysql://{settings.DB_GESTION_USER}:{settings.DB_GESTION_PASS}@"
    f"{settings.DB_GESTION_HOST}:{settings.DB_GESTION_PORT}/{settings.DB_GESTION_NAME}"
)
engine_gestion = create_async_engine(url_gestion, pool_pre_ping=True)

# Cache para motores de BD de clientes
_engines = {}

async def get_engine_for_client(client_id: str):
    """
    Consulta la tabla de gestión y retorna el motor de BD del cliente.
    """
    if client_id not in _engines:
        async with AsyncSession(engine_gestion) as session:
            # Consulta a la tabla maestra usando los nombres de campos que indicaste
            query = text("""
                SELECT nombreBaseDeDatos, usuario, contraseña, hosting, puerto 
                FROM tn_gestion_bdconex 
                WHERE idCliente = :c_id LIMIT 1
            """)
            result = await session.execute(query, {"c_id": client_id})
            row = result.fetchone()
            
            if not row:
                return None
            
            # Construcción de la URL dinámica para el cliente
            url_cliente = (
                f"mysql+aiomysql://{row.usuario}:{row.contraseña}@"
                f"{row.hosting}:{row.puerto}/{row.nombreBaseDeDatos}"
            )
            
            _engines[client_id] = create_async_engine(
                url_cliente, pool_size=5, max_overflow=10, pool_recycle=3600
            )
            
    return _engines[client_id]

async def get_db_session(client_id: str):
    """
    Generador de sesión dinámico.
    """
    engine = await get_engine_for_client(client_id)
    if not engine:
        raise ValueError(f"Configuración no encontrada para el cliente: {client_id}")
        
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session() as session:
        yield session