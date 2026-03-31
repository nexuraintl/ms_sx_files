import time
import logging
import os
from datetime import datetime, timezone
from urllib.parse import quote  # Necesario para codificar el nombre del archivo

from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine
from sqlalchemy import select, update, text
from starlette.requests import ClientDisconnect

# Importaciones locales
from models import DescargaAuditoria
from services.file_service import FileService
from services.auth_service import AuthService
from database import get_db_session, get_engine_for_client, engine_gestion
from core.config import settings

logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger("NFS-Service")

app = FastAPI(title=settings.APP_TITLE)

# @app.on_event("startup")
# async def startup_event():
#     logger.info("🔍 Verificando conectividad con la base de datos maestra...")
#     try:
#         # Importamos text para la consulta de prueba
#         from sqlalchemy import text
        
#         # Intentamos una operación mínima (SELECT 1) con un timeout corto
#         async with engine_gestion.connect() as conn:
#             await conn.execute(text("SELECT 1"))
#             logger.info("✅ CONEXIÓN EXITOSA: El microservicio llega a la DB de Gestión.")
#     except Exception as e:
#         logger.error(f"❌ ERROR DE CONEXIÓN INICIAL: No se pudo conectar a la DB maestra.")
#         logger.error(f"Detalle técnico: {str(e)}")
#         # No detenemos la app para permitir que Cloud Run termine de subir y ver los logs

async def finalizar_auditoria_dinamica(
    audit_id: int, 
    estado: str, 
    bytes_enviados: int, 
    start_time: float, 
    client_id: str, 
    codigo_http: int,
    engine: AsyncEngine,
    request_id: int = None
):
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

            if estado == "COMPLETED" and request_id:
                # Usamos text() para la tabla tn_docs_descargas ya que parece ser una tabla de métricas
                stmt_counter = text("""
                    UPDATE tn_docs_descargas 
                    SET descargas = COALESCE(descargas, 0) + 1 
                    WHERE id = :req_id
                """)
                await session.execute(stmt_counter, {"req_id": request_id})
                logger.info(f"📊 Contador incrementado para request_id: {request_id}")
                
            await session.commit()
            logger.info(f"[Cliente: {client_id}] Auditoría {audit_id} cerrada como {estado}.")
        except Exception as e:
            await session.rollback()
            logger.error(f"[Cliente: {client_id}] Error en finalizar_auditoria_dinamica: {e}")

@app.get("/core/files/download/{audit_id}")
async def download_file(
    audit_id: int, 
    token: str, 
    client_id: str, 
    request: Request,
    background_tasks: BackgroundTasks
):
    start_time = time.time()
    client_ip = AuthService.get_client_ip(request)
    nfs_base_path = "/app/media" 
    
    full_path = None
    friendly_name = None
    file_size = 0
    engine_cliente = None
    registro_id_for_stats = None

    try:
        engine_cliente = await get_engine_for_client(client_id)
        if not engine_cliente:
            raise HTTPException(status_code=404, detail="Configuración de cliente no encontrada.")

        async for db in get_db_session(client_id):
            result = await db.execute(select(DescargaAuditoria).where(DescargaAuditoria.id == audit_id))
            registro = result.scalars().first()

            if not registro:
                raise HTTPException(status_code=404, detail="ID de auditoría inválido.")
            
            registro_id_for_stats = registro.request_id

            try:
                AuthService.validar_access_token_google(token, client_id)
            except HTTPException as e:
                # Si el token es inválido, auditamos el fallo antes de lanzar el error
                background_tasks.add_task(
                    finalizar_auditoria_dinamica, 
                    audit_id, "FAILED", 0, start_time, client_id, e.status_code, engine_cliente
                )
                raise e

            try:
                AuthService.validar_token_auditoria(token, registro)
            except HTTPException as e:
                background_tasks.add_task(finalizar_auditoria_dinamica, audit_id, "FAILED", 0, start_time, client_id, e.status_code, engine_cliente)
                raise e

            AuthService.validar_permiso_descarga(registro, client_ip)
            await AuthService.check_anti_spam(db, client_ip, registro.recurso, audit_id)

            full_path = FileService.get_secure_path(nfs_base_path, registro.recurso)
            file_size = os.path.getsize(full_path)
            friendly_name = FileService.generate_friendly_filename(registro.nombre, registro.mime, audit_id)
            

            registro.estado = "REDIRECTED"
            registro.ip = client_ip
            registro.fecha_actualizacion = datetime.now(timezone.utc)
            await db.commit()
            break 

        async def stream_wrapper():
            bytes_sent = 0
            success = False
            #last_log_checkpoint = 0
            #chunk_count = 0
            try:
                logger.info(f"🚀 Iniciando stream para ID {audit_id}. Tamaño total: {file_size} bytes")

                async for chunk in FileService.file_iterator(full_path):
                    #chunk_count += 1
                    if await request.is_disconnected():
                        #logger.warning(f"❌ Cliente desconectado prematuramente en byte {bytes_sent}")
                        raise ClientDisconnect("Cliente desconectado")
                    yield chunk
                    bytes_sent += len(chunk)

                    # Loguear cada 5MB para no saturar los logs pero tener rastro
                    #if bytes_sent - last_log_checkpoint > 5 * 1024 * 1024:
                        #logger.info(f"📥 Progreso ID {audit_id}: {bytes_sent}/{file_size} bytes sent...")
                        #last_log_checkpoint = bytes_sent
                
                if bytes_sent >= file_size:
                    success = True 
                    logger.info(f"✅ Stream finalizado con éxito para ID {audit_id}. Total: {bytes_sent} bytes")

            except Exception as e:
                logger.warning(f"Error en stream {audit_id}: {e}")
                logger.error(f"🔥 Error crítico en el stream de ID {audit_id} (Byte {bytes_sent}): {str(e)}")
            finally:
                if bytes_sent >= file_size:
                    success = True
                
                estado_final = "COMPLETED" if success else "FAILED"
                codigo_http = 200 if success else 499
                background_tasks.add_task(
                    finalizar_auditoria_dinamica,
                    audit_id, estado_final, bytes_sent, start_time, client_id, codigo_http, engine_cliente, registro_id_for_stats
                )

        # --- CONFIGURACIÓN DE CABECERAS PARA DESCARGA FORZADA ---
        # 1. Codificar nombre para evitar errores en headers con caracteres especiales
        friendly_name_encoded = quote(friendly_name)
        friendly_name_ascii = friendly_name.encode('ascii', 'ignore').decode('ascii')

        return StreamingResponse(
            stream_wrapper(),
            # 2. Forzamos octet-stream para que el navegador no intente renderizar (PDF/JPG/etc)
            media_type="application/octet-stream", 
            headers={
                # 3. 'attachment' fuerza la descarga. filename* asegura compatibilidad UTF-8
                "Content-Disposition": f'attachment; filename="{friendly_name_ascii}"; filename*=UTF-8\'\'{friendly_name_encoded}',
                #"Content-Length": str(file_size),
                "X-Accel-Buffering": "no",
                # 4. Prohibimos al navegador "adivinar" el contenido
                "X-Content-Type-Options": "nosniff", 
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
                "Accept-Ranges": "bytes"
            }
        )
        
    except HTTPException as he:
        raise he
    except Exception as ge:
        logger.error(f"Error inesperado: {ge}")
        if engine_cliente:
            background_tasks.add_task(finalizar_auditoria_dinamica, audit_id, "ERROR", 0, start_time, client_id, 500, engine_cliente)
        raise HTTPException(status_code=500, detail="Error interno del servidor")