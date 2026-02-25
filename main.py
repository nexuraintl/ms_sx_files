import time
import logging
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine
from sqlalchemy import select, update

# Importaciones locales
from models import DescargaAuditoria
from services.file_service import FileService
from services.auth_service import AuthService
from database import get_db_session, get_engine_for_client, _engines
from core.config import settings

# Configuración de Logging
logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger("NFS-Service")

app = FastAPI(title=settings.APP_TITLE)

async def finalizar_auditoria_dinamica(
    audit_id: int, 
    estado: str, 
    bytes_enviados: int, 
    start_time: float, 
    client_id: str, 
    engine: AsyncEngine
):
    """
    Actualiza el resultado final en la base de datos del cliente.
    """
    duracion = (time.time() - start_time) * 1000
    
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
            logger.info(f"[Cliente: {client_id}] Auditoría {audit_id} cerrada como {estado}.")
        except Exception as e:
            logger.error(f"[Cliente: {client_id}] Error al actualizar auditoría final: {e}")

@app.get("/core/files/download/{audit_id}")
async def download_file(audit_id: int, token: str, client_id: str, request: Request):
    start_time = time.time()
    client_ip = AuthService.get_client_ip(request)
    
    try:
        # 1. Obtener el motor del cliente (para auditoría posterior)
        engine_cliente = await get_engine_for_client(client_id)
        if not engine_cliente:
            raise HTTPException(status_code=404, detail="Configuración de cliente no encontrada.")

        # 2. Conexión dinámica a la BD del cliente
        async for db in get_db_session(client_id):
            
            # 3. Buscar registro de auditoría
            result = await db.execute(select(DescargaAuditoria).where(DescargaAuditoria.id == audit_id))
            registro = result.scalars().first()

            if not registro:
                raise HTTPException(status_code=404, detail="ID de auditoría inválido.")

            # 4. Validaciones de Seguridad
            AuthService.validar_permiso_descarga(registro, client_ip)
            await AuthService.check_anti_spam(db, client_ip, registro.recurso, audit_id)

            # 5. Localizar archivo en NFS
            nfs_base_path = "/app/media" 
            try:
                full_path = FileService.get_secure_path(nfs_base_path, registro.recurso)
            except HTTPException as e:
                await finalizar_auditoria_dinamica(audit_id, "FAILED", 0, start_time, client_id, engine_cliente)
                raise e

            # 6. Preparar Metadatos
            friendly_name = FileService.generate_friendly_filename(registro.mime, audit_id)
            content_type = registro.mime if registro.mime else "application/octet-stream"

            # 7. Marcar como REDIRECTED (Inicio de descarga)
            registro.estado = "REDIRECTED"
            registro.ip = client_ip
            registro.fecha_actualizacion = datetime.now(timezone.utc)
            await db.commit()

            # 8. Streaming wrapper
            async def stream_wrapper():
                bytes_totales = 0
                try:
                    async for chunk in FileService.file_iterator(full_path):
                        bytes_totales += len(chunk)
                        yield chunk
                    
                    await finalizar_auditoria_dinamica(
                        audit_id, "COMPLETED", bytes_totales, start_time, client_id, engine_cliente
                    )
                except Exception as e:
                    logger.error(f"Error en streaming para auditoría {audit_id}: {e}")
                    await finalizar_auditoria_dinamica(
                        audit_id, "FAILED", bytes_totales, start_time, client_id, engine_cliente
                    )

            return StreamingResponse(
                stream_wrapper(),
                media_type=content_type,
                headers={
                    "Content-Disposition": f'attachment; filename="{friendly_name}"',
                    "X-Content-Type-Options": "nosniff"
                }
            )
            
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

@app.get("/health")
async def health_check():
    return {
        "status": "alive", 
        "clients_active": list(_engines.keys()),
        "total_active_connections": len(_engines)
    }