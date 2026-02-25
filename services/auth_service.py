from datetime import datetime, timedelta, timezone
from fastapi import HTTPException, Request, status
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession
from models import DescargaAuditoria
from core.config import settings

import logging

logger = logging.getLogger("NFS-Service")

class AuthService:
    @staticmethod
    def get_client_ip(request: Request) -> str:
        """
        Extrae la IP real considerando que estamos detrás de 
        un Load Balancer y un API Gateway.
        """
        x_forwarded_for = request.headers.get("x-forwarded-for")
        if x_forwarded_for:
            # El primer elemento es la IP original del cliente
            return x_forwarded_for.split(",")[0].strip()
        return request.client.host

    @staticmethod
    def validar_permiso_descarga(registro: DescargaAuditoria, ip_cliente: str):
        """
        Regla Estricta 1: No permitir si ya fue completada.
        Regla Estricta 2: No permitir si la IP es diferente a la que originó 
        el registro (opcional, pero añade seguridad).
        """
        # Si el estado ya es COMPLETED, el link queda invalidado
        if registro.estado == "COMPLETED":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Este enlace de descarga ya ha sido utilizado y no es válido."
            )
        
        # Si quieres que solo la IP que generó el registro pueda descargarlo:
        if registro.ip and registro.ip != ip_cliente:
           raise HTTPException(status_code=403, detail="IP no autorizada para este ID.")

    @staticmethod
    async def check_anti_spam(
        db: AsyncSession, 
        ip: str, 
        recurso: str, 
        current_audit_id: int
    ):
        """
        Verifica si hay descargas activas para el mismo recurso e IP 
        en los últimos 10 segundos.
        """
        tiempo_bloqueo = datetime.now(timezone.utc) - timedelta(seconds=settings.ANTI_SPAM_SECONDS)
        
        query = select(func.count()).where(
            and_(
                DescargaAuditoria.ip == ip,
                DescargaAuditoria.recurso == recurso,
                # Consideramos PENDING y REDIRECTED como estados "en proceso"
                DescargaAuditoria.estado.in_(["PENDING", "REDIRECTED"]),
                DescargaAuditoria.fecha_actualizacion >= tiempo_bloqueo,
                DescargaAuditoria.id != current_audit_id
            )
        )
        
        result = await db.execute(query)
        if result.scalar() > 0:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Demasiadas peticiones. Ya existe una descarga en curso para este archivo."
            )
    
    @staticmethod
    def validar_token_auditoria(token_url: str, registro_auditoria):
        """
        Verifica que el token de la URL coincida con el token asignado al registro.
        """
        if not registro_auditoria.token:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="El registro de auditoría no tiene un token asignado."
            )
            
        if token_url != registro_auditoria.token:
            logger.warning(f"Intento de descarga con token inválido para ID {registro_auditoria.id}")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Token de descarga inválido o expirado."
            )