import time
import os
import logging
from datetime import datetime, timezone
from typing import Dict

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine
from sqlalchemy import select, update

# Importaciones locales
from models import DescargaAuditoria
from services.file_service import FileService
from services.auth_service import AuthService
from schemas import MasterConfig # Para validación de la config del Secret Manager
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
    Actualiza el resultado final en la base de datos del cliente usando su motor específico.
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
    
    # 1. Obtener Identidad
    client_ip = AuthService.get_client_ip(request)
    
    # 2. Conexión dinámica a la BD del cliente usando client_id
    try:
        # Usamos el nuevo generador que busca en la BD de gestión
        async for db in get_db_session(client_id):
            
            # 3. Buscar registro de auditoría en la BD del cliente
            result = await db.execute(select(DescargaAuditoria).where(DescargaAuditoria.id == audit_id))
            registro = result.scalars().first()

            if not registro:
                raise HTTPException(status_code=404, detail="ID de auditoría inválido.")

            # 4. Validación de Seguridad y Token
            # Aquí podrías añadir: AuthService.validar_token(token, registro)
            AuthService.validar_permiso_descarga(registro, client_ip)
            await AuthService.check_anti_spam(db, client_ip, registro.recurso, audit_id)

            # 5. Localizar archivo en NFS
            # Importante: Aquí el nfs_base_path debería venir también de la BD de gestión 
            # si es diferente por cliente, o mantenerse fijo si es el mismo volumen.
            # Por ahora, usamos una ruta base genérica o la extraemos si añades el campo a la tabla.
            nfs_base_path = "/app/media" 
            
            try:
                full_path = FileService.get_secure_path(nfs_base_path, registro.recurso)
            except HTTPException as e:
                # Nota: finalizar_auditoria ahora debe recibir el engine del cliente
                engine_cliente = await get_engine_for_client(client_id)
                await finalizar_auditoria_dinamica(audit_id, "FAILED", 0, start_time, engine_cliente)
                raise e

            # 6. Lógica de Nombre y Extensión
            friendly_name = FileService.generate_friendly_filename(registro.mime, audit_id)
            content_type = registro.mime if registro.mime else "application/octet-stream"

            # 7. Actualizar registro a estado REDIRECTED
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
                    
                    engine_cliente = await get_engine_for_client(client_id)
                    await finalizar_auditoria_dinamica(audit_id, "COMPLETED", bytes_totales, start_time, engine_cliente)
                except Exception as e:
                    logger.error(f"Error en streaming para auditoría {audit_id}: {e}")
                    engine_cliente = await get_engine_for_client(client_id)
                    await finalizar_auditoria_dinamica(audit_id, "FAILED", bytes_totales, start_time, engine_cliente)

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
    """
    Verifica que el servicio esté vivo y muestra qué clientes 
    tienen conexiones activas en caché.
    """
    return {
        "status": "alive", 
        "clients_active": list(_engines.keys()),
        "total_active_connections": len(_engines)
    }