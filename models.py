from sqlalchemy import Column, Integer, String, DateTime, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timezone

Base = declarative_base()

class DescargaAuditoria(Base):
    __tablename__ = "tn_descargas_auditoria"

    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String(255))    
    token = Column(String)                   
    fecha = Column(DateTime, default=datetime.utcnow) 
    ip = Column(String(255))                          
    recurso = Column(String(500))                    
    mime = Column(String(50))                         
    usuario_id = Column(Integer, nullable=True)       
    origen = Column(String(50), nullable=False)       
    user_agent = Column(String(350), nullable=True)   
    estado = Column(String(50), default="PENDING")  
    codigo_http = Column(Integer, nullable=True)  
    tamano_bytes = Column(BigInteger, default=0)      
    duracion_ms = Column(Integer, default=0)          
    fecha_actualizacion = Column(DateTime, 
                                 default=lambda: datetime.now(timezone.utc), 
                                 onupdate=lambda: datetime.now(timezone.utc))