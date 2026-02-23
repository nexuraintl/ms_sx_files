from datetime import datetime, timedelta, timezone
from fastapi import HTTPException, Request, status
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession
from models import DescargaAuditoria
from core.config import settings

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