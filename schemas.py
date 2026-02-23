from pydantic import BaseModel, Field
from typing import Dict, Optional

class DBConfig(BaseModel):
    user: str
    # Usamos password como alias por si el JSON usa "pass"
    password: str = Field(..., alias="pass") 
    host: str
    name: str
    port: int = 3306

class DomainConfig(BaseModel):
    db_config: DBConfig
    nfs_mount_path: str

# El esquema maestro es un diccionario de dominios
class MasterConfig(BaseModel):
    # Ejemplo: {"midominio.com": DomainConfig, ...}
    domains: Dict[str, DomainConfig]