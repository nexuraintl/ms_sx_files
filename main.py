import time
import logging
import os
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine
from sqlalchemy import select, update
from starlette.requests import ClientDisconnect

# Importaciones locales
from models import DescargaAuditoria
from services.file_service import FileService
from services.auth_service import AuthService
from database import get_db_session, get_engine_for_client, _engines
from core.config import settings

logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger("NFS-Service")

app = FastAPI(title=settings.APP_TITLE)

async def finalizar_auditoria_dinamica(
    audit_id: int, 
    estado: str, 
    bytes_enviados: int, 
    start_time: float, 
    client_id: str, 
    codigo_http: int,
    engine: AsyncEngine
):
    """
    Actualiza el resultado final en la base de datos del cliente.
    """
    duracion = (time.time() - start_time) * 1000
    logger.info(f"Background Task: Iniciando actualización ID {audit_id} a {estado}")
    
    async with AsyncSession(engine) as session:
        try:
            stmt = (
                update(DescargaAuditoria)
                .where(DescargaAuditoria.id == audit_id)
                .values(
                    estado=estado,
                    duracion_ms=duracion,
                    tamano_bytes=bytes_enviados,
                    codigo_http=codigo_http,
                    fecha_actualizacion=datetime.now(timezone.utc)
                )
            )
            await session.execute(stmt)
            await session.commit()
            logger.info(f"[Cliente: {client_id}] Auditoría {audit_id} cerrada como {estado} con {bytes_enviados} bytes.")
        except Exception as e:
            logger.error(f"[Cliente: {client_id}] Error en finalizar_auditoria_dinamica: {e}")

@app.get("/core/files/download/{audit_id}")
async def download_file(
    audit_id: int, 
    token: str, 
    client_id: str, 
    request: Request,
    background_tasks: BackgroundTasks # Añadido BackgroundTasks
):
    start_time = time.time()
    client_ip = AuthService.get_client_ip(request)
    nfs_base_path = "/app/media" 
    
    # Variables de estado para el stream
    full_path = None
    friendly_name = None
    content_type = None
    file_size = 0
    engine_cliente = None

    try:
        engine_cliente = await get_engine_for_client(client_id)
        if not engine_cliente:
            raise HTTPException(status_code=404, detail="Configuración de cliente no encontrada.")

        # FASE 1: Operaciones de Base de Datos iniciales
        # Usamos el iterador pero salimos rápido para LIBERAR la conexión
        async for db in get_db_session(client_id):
            result = await db.execute(select(DescargaAuditoria).where(DescargaAuditoria.id == audit_id))
            registro = result.scalars().first()

            if not registro:
                raise HTTPException(status_code=404, detail="ID de auditoría inválido.")
            
            # Validaciones
            try:
                AuthService.validar_token_auditoria(token, registro)
            except HTTPException as e:
                background_tasks.add_task(finalizar_auditoria_dinamica, audit_id, "FAILED", 0, start_time, client_id, e.status_code, engine_cliente)
                raise e

            AuthService.validar_permiso_descarga(registro, client_ip)
            await AuthService.check_anti_spam(db, client_ip, registro.recurso, audit_id)

            # Preparar metadatos del archivo
            full_path = FileService.get_secure_path(nfs_base_path, registro.recurso)
            file_size = os.path.getsize(full_path)
            friendly_name = FileService.generate_friendly_filename(registro.nombre, registro.mime, audit_id)
            content_type = registro.mime if registro.mime else "application/octet-stream"

            # Marcar como REDIRECTED y COMMIT INMEDIATO para liberar el lock de la fila
            registro.estado = "REDIRECTED"
            registro.ip = client_ip
            registro.fecha_actualizacion = datetime.now(timezone.utc)
            await db.commit()
            break # SALIMOS DEL LOOP: Cerramos la sesión 'db' antes de enviar el archivo

        # FASE 2: Streaming Wrapper
        async def stream_wrapper():
            bytes_sent = 0
            success = False
            try:
                async for chunk in FileService.file_iterator(full_path):
                    if await request.is_disconnected():
                        raise ClientDisconnect("Cliente desconectado")
                    yield chunk
                    bytes_sent += len(chunk)
                
                success = True # El bucle terminó, el archivo se leyó todo
            except Exception as e:
                logger.warning(f"Error o desconexión en stream {audit_id}: {e}")
            finally:
                # FASE 3: Delegar la actualización a BackgroundTasks
                # Esto se ejecuta incluso si el generador es cancelado
                estado_final = "COMPLETED" if success else "FAILED"
                codigo_http = 200 if success else 499
                
                # Usar background_tasks asegura que la actualización ocurra fuera del ciclo de vida del stream
                background_tasks.add_task(
                    finalizar_auditoria_dinamica,
                    audit_id, estado_final, bytes_sent, start_time, client_id, codigo_http, engine_cliente
                )

        return StreamingResponse(
            stream_wrapper(),
            media_type=content_type,
            headers={
                "Content-Disposition": f'attachment; filename="{friendly_name}"',
                "Content-Length": str(file_size),
                "X-Accel-Buffering": "no",
                "Cache-Control": "no-cache",
                "Accept-Ranges": "bytes",
                "X-Content-Type-Options": "nosniff"
            }
        )
        
    except HTTPException as he:
        raise he
    except Exception as ge:
        logger.error(f"Error inesperado: {ge}")
        if engine_cliente:
            background_tasks.add_task(finalizar_auditoria_dinamica, audit_id, "ERROR", 0, start_time, client_id, 500, engine_cliente)
        raise HTTPException(status_code=500, detail="Error interno del servidor")