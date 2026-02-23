import json
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from google.cloud import secretmanager
from core.config import settings

client = secretmanager.SecretManagerServiceClient()

# Cache para no crear motores de BD en cada petición
_engines = {}

def get_config_from_secret():
    """Trae el JSON maestro de Secret Manager"""
    name = f"projects/{settings.PROJECT_ID}/secrets/{settings.SECRET_ID}/versions/{settings.SECRET_VERSION}"
    
    response = client.access_secret_version(request={"name": name})
    return json.loads(response.payload.data.decode("UTF-8"))

def get_engine_for_domain(domain: str, config: dict):

    domain_obj = config[domain]
    """Retorna o crea el motor de base de datos para un dominio específico"""
    if domain not in _engines:
        db_cfg = domain_obj.db_config
        url = f"mysql+aiomysql://{db_cfg.user}:{db_cfg.password}@{db_cfg.host}:{db_cfg.port}/{db_cfg.name}"
        
        _engines[domain] = create_async_engine(
            url, pool_size=5, max_overflow=10, pool_recycle=3600
        )
    return _engines[domain]

# Generador de sesión dinámico
async def get_db_session(domain: str, config: dict):
    engine = get_engine_for_domain(domain, config)
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session() as session:
        yield session