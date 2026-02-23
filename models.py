from sqlalchemy import Column, Integer, String, DateTime, BigInteger, Float
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime,timezone

Base = declarative_base()

class DescargaAuditoria(Base):
    __tablename__ = "tn_descargas_auditoria"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fecha = Column(DateTime, default=datetime.utcnow) # Campo 2 de tu imagen
    ip = Column(String(255))                          # Campo 3
    recurso = Column(String(500))                    # Campo 4
    mime = Column(String(50))                         # Campo 5
    usuario_id = Column(Integer, nullable=True)       # Campo 6
    origen = Column(String(50), nullable=False)       # Campo 7 
    user_agent = Column(String(350), nullable=True)   # Campo 8
    estado = Column(String(50), default="PENDING")    # Campo 9
    tamano_bytes = Column(BigInteger, default=0)      # Campo 11
    duracion_ms = Column(Integer, default=0)          # Campo 12
    fecha_actualizacion = Column(DateTime, default=datetime.utcnow, onupdate=datetime.now(timezone.utc))