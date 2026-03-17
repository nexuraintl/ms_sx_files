from datetime import datetime, timedelta, timezone
from fastapi import HTTPException, Request, status
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession
from models import DescargaAuditoria
from core.config import settings
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests

import logging

logger = logging.getLogger("NFS-Service")

class AuthService:

    _request_adapter = google_requests.Request()

    @staticmethod
    def validar_access_token_google(token: str, client_id: str):
        """
        Valida criptográficamente un Google ID Token (Access Token).
        Verifica: Firma, Emisor (Google), Expiración y Audiencia.
        """
        try:
            # El campo 'aud' en tu JSON es el Client ID de la App de Google
            # Lo validamos para asegurar que el token fue emitido para tu proyecto
            expected_audience = "618104708054-9r9s1c4alg36erliucho9t52n32n6dgq.apps.googleusercontent.com"
            
            # verify_oauth2_token hace todo el trabajo pesado:
            # 1. Descarga certs de https://www.googleapis.com/oauth2/v3/certs
            # 2. Verifica firma RS256 con el 'kid' del header
            # 3. Verifica 'exp' (expiración) e 'iss' (accounts.google.com)
            id_info = id_token.verify_oauth2_token(
                token, 
                AuthService._request_adapter, 
                expected_audience
            )

            # Validación lógica adicional: El client_id de la URL 
            # podrías compararlo con algún claim interno si fuera necesario.
            # Por ahora, si llegamos aquí, el token es LEGÍTIMO de Google.
            
            logger.info(f"🔐 Token verificado exitosamente para sub: {id_info['sub']}")
            return id_info

        except ValueError as e:
            # Este error ocurre si la firma es falsa, el token expiró o la aud no coincide
            logger.error(f"❌ Fallo de seguridad en JWT: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Acceso denegado: Token de seguridad inválido o expirado."
            )
        except Exception as e:
            logger.error(f"🔥 Error inesperado validando identidad: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error al validar la identidad del solicitante."
            )

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