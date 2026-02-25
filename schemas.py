from pydantic import BaseModel, Field
from typing import Dict, Optional

class DBConfig(BaseModel):
    user: str
    # Usamos password como alias por si el JSON usa "pass"
    password: str = Field(..., alias="pass") 
    host: str
    name: str
    port: int = 3306

