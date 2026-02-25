from pydantic import BaseModel, Field
from typing import Optional

class ClientDBConfig(BaseModel):
    """
    Valida los datos de conexión obtenidos de la tabla tn_gestion_bdconex.
    """
    nombreBaseDeDatos: str
    usuario: str
    # Mapeamos 'contraseña' a un nombre de atributo sin caracteres especiales para Python
    password: str = Field(..., alias="contraseña") 
    hosting: str
    puerto: int = 3306

    class Config:
        # Esto permite que Pydantic lea los datos tanto de diccionarios 
        # como de objetos de fila de SQLAlchemy
        from_attributes = True
        populate_by_name = True