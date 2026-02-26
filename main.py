import time
import logging
import os
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
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
    codigo_http: int,
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
                    codigo_http=codigo_http,
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
    # Definimos la ruta base al inicio para evitar errores de NameError
    nfs_base_path = "/app/media" 
    
    try:
        # 1. Obtener el motor del cliente
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
            

            # 4. Validaciones de Seguridad (Permisos y Anti-Spam)

            try:
                AuthService.validar_token_auditoria(token, registro)
            except HTTPException as e:
                await finalizar_auditoria_dinamica(
                    audit_id, "FAILED", 0, start_time, client_id, e.status_code, engine_cliente
                )
                raise e
           

            AuthService.validar_permiso_descarga(registro, client_ip)
            await AuthService.check_anti_spam(db, client_ip, registro.recurso, audit_id)

            # 5. Localizar archivo en NFS
            try:
                full_path = FileService.get_secure_path(nfs_base_path, registro.recurso)
            except HTTPException as e:
                # Si el archivo no existe o no hay permisos en disco, cerramos auditoría aquí
                await finalizar_auditoria_dinamica(
                    audit_id, "FAILED", 0, start_time, client_id, e.status_code, engine_cliente
                )
                raise e

            # 6. Preparar Metadatos
            friendly_name = FileService.generate_friendly_filename(
                nombre_db=registro.nombre, 
                mime_type=registro.mime, 
                audit_id=audit_id
            )
            content_type = registro.mime if registro.mime else "application/octet-stream"

            # 7. Marcar como REDIRECTED (Inicio de descarga)
            registro.estado = "REDIRECTED"
            registro.ip = client_ip
            registro.fecha_actualizacion = datetime.now(timezone.utc)
            await db.commit()

            file_size = os.path.getsize(full_path) 
            # 8. Streaming wrapper
            async def stream_wrapper():
                bytes_sent = 0
                try:
                    # Usamos el iterador de archivos definido en FileService
                    async for chunk in FileService.file_iterator(full_path):
                        # 1. Verificación proactiva: si el usuario cancela, dejamos de leer el NFS
                        if await request.is_disconnected():
                            raise ClientDisconnect("El cliente cerró la conexión")
                        
                        yield chunk
                        bytes_sent += len(chunk)
                    
                    # 2. ÉXITO TOTAL: Si llegamos aquí, el bucle terminó sin excepciones.
                    # Ejecutamos la actualización ANTES de que el generador se cierre.
                    logger.info(f"Lectura completa para auditoría {audit_id}. Enviando actualización a COMPLETED.")
                    
                    await finalizar_auditoria_dinamica(
                        audit_id=audit_id,
                        estado="COMPLETED",
                        bytes_enviados=bytes_sent,
                        start_time=start_time,
                        client_id=client_id,
                        codigo_http=200,
                        engine=engine_cliente
                    )

                except ClientDisconnect:
                    # Caso: El usuario canceló la descarga manualmente
                    logger.warning(f"Descarga cancelada por el usuario (Audit: {audit_id}).")
                    await finalizar_auditoria_dinamica(
                        audit_id, "FAILED", bytes_sent, start_time, client_id, 499, engine_cliente
                    )
                except Exception as e:
                    # Caso: Error inesperado durante el streaming (ej. error de red o disco)
                    logger.error(f"Error crítico en streaming (Audit: {audit_id}): {str(e)}")
                    await finalizar_auditoria_dinamica(
                        audit_id, "FAILED", bytes_sent, start_time, client_id, 500, engine_cliente
                    )

            # 9. Retorno de la respuesta con Headers de rendimiento
            return StreamingResponse(
                stream_wrapper(),
                media_type=content_type,
                headers={
                    "Content-Disposition": f'attachment; filename="{friendly_name}"',
                    "Content-Length": str(file_size),  # Permite al navegador mostrar progreso real
                    "X-Accel-Buffering": "no",        # Desactiva el buffering de Google Cloud (VITAL)
                    "Cache-Control": "no-cache",      # Evita problemas de caché en el proxy
                    "Accept-Ranges": "bytes",         # Facilita la descarga de archivos grandes
                    "X-Content-Type-Options": "nosniff"
                }
            )
        
        
    except HTTPException as he:
        # Re-lanzamos errores controlados de FastAPI
        raise he
    except ValueError as ve:
        # Error de client_id no encontrado en database.py
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as ge:
        logger.error(f"Error inesperado: {ge}")
        # Si ya teníamos el motor, intentamos cerrar la auditoría como error 500
        if 'engine_cliente' in locals() and 'audit_id' in locals():
            await finalizar_auditoria_dinamica(audit_id, "ERROR", 0, start_time, client_id, 500, engine_cliente)
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    
@app.get("/health")
async def health_check():
    return {
        "status": "alive", 
        "clients_active": list(_engines.keys()),
        "total_active_connections": len(_engines)
    }