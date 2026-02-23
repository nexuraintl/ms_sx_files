import time
import os
import logging
from datetime import datetime, timezone
from typing import Dict

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

# Importaciones locales
from models import DescargaAuditoria
from services.file_service import FileService
from services.auth_service import AuthService
from schemas import MasterConfig # Para validación de la config del Secret Manager
from database import get_config_from_secret, get_db_session, get_engine_for_domain
from core.config import settings

# Configuración de Logging
logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger("NFS-Service")

app = FastAPI(title=settings.APP_TITLE)

# Cache global de configuración
CONFIG_CACHE: Dict = {}

def obtener_configuracion() -> Dict:
    """
    Obtiene y valida la configuración desde Secret Manager.
    """
    global CONFIG_CACHE
    if not CONFIG_CACHE:
        raw_data = get_config_from_secret()
        try:
            # Validamos que el JSON cumpla con el esquema definido
            validated_config = MasterConfig(domains=raw_data)
            CONFIG_CACHE = validated_config.domains
            logger.info("Configuración cargada y validada exitosamente.")
        except Exception as e:
            logger.error(f"Error de validación en el JSON de Secret Manager: {e}")
            # En caso de error, devolvemos el raw para no romper todo, 
            # pero lo ideal es corregir el JSON.
            CONFIG_CACHE = raw_data
    return CONFIG_CACHE

async def finalizar_auditoria(audit_id: int, estado: str, bytes_enviados: int, start_time: float, domain: str, config: dict):
    """
    Actualiza el resultado final en la base de datos del cliente correspondiente.
    """
    duracion = (time.time() - start_time) * 1000
    engine = get_engine_for_domain(domain, config)
    
    async with AsyncSession(engine) as session:
        try:
            stmt = (
                update(DescargaAuditoria)
                .where(DescargaAuditoria.id == audit_id)
                .values(
                    estado=estado,
                    duracion_ms=duracion,
                    tamano_bytes=bytes_enviados,
                    fecha_actualizacion=datetime.now(timezone.utc)
                )
            )
            await session.execute(stmt)
            await session.commit()
            logger.info(f"[{domain}] Auditoría {audit_id} cerrada como {estado}.")
        except Exception as e:
            logger.error(f"[{domain}] Error al actualizar auditoría final: {e}")

@app.get("/download")
async def download_file(audit_id: int, token: str, request: Request):
    start_time = time.time()
    
    # 1. Obtener Identidad y Configuración
    client_ip = AuthService.get_client_ip(request)
    domain = request.headers.get("x-original-host", request.headers.get("host"))
    
    full_config = obtener_configuracion()
    
    if domain not in full_config:
        logger.error(f"Acceso denegado. Dominio no configurado: {domain}")
        raise HTTPException(status_code=403, detail="Dominio no autorizado.")
    
    domain_cfg = full_config[domain]
    nfs_base_path = getattr(domain_cfg, "nfs_mount_path", None)

    # 2. Conexión dinámica a la BD
    async for db in get_db_session(domain, full_config):
        
        # 3. Buscar registro de auditoría
        result = await db.execute(select(DescargaAuditoria).where(DescargaAuditoria.id == audit_id))
        registro = result.scalars().first()

        if not registro:
            raise HTTPException(status_code=404, detail="ID de auditoría inválido.")

        # 4. Validación de Seguridad
        await AuthService.check_anti_spam(db, client_ip, registro.recurso, audit_id)

        # 5. Localizar archivo en NFS
        try:
            full_path = FileService.get_secure_path(nfs_base_path, registro.recurso)
        except HTTPException as e:
            await finalizar_auditoria(audit_id, "FAILED", 0, start_time, domain, full_config)
            raise e

        # 6. Lógica de Nombre y Extensión (NUEVA)
        # Usamos el campo 'mime' que viene de la tabla DescargaAuditoria
        friendly_name = FileService.generate_friendly_filename(registro.mime, audit_id)
        content_type = registro.mime if registro.mime else "application/octet-stream"

        # 7. Actualizar registro a estado intermedio
        registro.estado = "REDIRECTED"
        registro.ip = client_ip
        registro.fecha_actualizacion = datetime.now(timezone.utc)
        await db.commit()

        # 8. Streaming wrapper (sin cambios en lógica, solo contexto)
        async def stream_wrapper():
            bytes_totales = 0
            try:
                async for chunk in FileService.file_iterator(full_path):
                    bytes_totales += len(chunk)
                    yield chunk
                await finalizar_auditoria(audit_id, "COMPLETED", bytes_totales, start_time, domain, full_config)
            except Exception as e:
                logger.error(f"Error en streaming para auditoría {audit_id}: {e}")
                await finalizar_auditoria(audit_id, "FAILED", bytes_totales, start_time, domain, full_config)

        # 9. Retorno con Headers corregidos
        return StreamingResponse(
            stream_wrapper(),
            media_type=content_type,
            headers={
                # El parámetro 'filename' es el que verá el navegador al descargar
                "Content-Disposition": f'attachment; filename="{friendly_name}"',
                "X-Content-Type-Options": "nosniff"
            }
        )

@app.get("/health")
async def health_check():
    return {"status": "alive", "domains_loaded": list(CONFIG_CACHE.keys())}