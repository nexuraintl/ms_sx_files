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
from database import get_db_session, get_engine_for_client, engine_gestion, get_client_folder
from core.config import settings

#importaciones adicionales para upload
import re
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

from fastapi import File, Form, UploadFile
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

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

@app.get("/download/{audit_id}")
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

    # --- CAPA 0: VALIDACIÓN DE IDENTIDAD CRIPTOGRÁFICA (EL PORTERO) ---
    # Validamos con Google antes de CUALQUIER otra operación.
    # Si falla aquí, lanzamos 401/403 de inmediato sin tocar la DB.
    AuthService.validar_access_token_google(token, client_id)

    try:
        # --- CAPA 1: CONECTIVIDAD ---
        engine_cliente = await get_engine_for_client(client_id)
        if not engine_cliente:
            raise HTTPException(status_code=404, detail="Configuración de cliente no encontrada.")

        async for db in get_db_session(client_id):
            # --- CAPA 2: EXISTENCIA DEL REGISTRO ---
            result = await db.execute(select(DescargaAuditoria).where(DescargaAuditoria.id == audit_id))
            registro = result.scalars().first()

            if not registro:
                raise HTTPException(status_code=404, detail="ID de auditoría inválido.")
            
            registro_id_for_stats = registro.request_id

            # --- CAPA 3: VINCULACIÓN TOKEN-REGISTRO ---
            try:
                AuthService.validar_token_auditoria(token, registro)
            except HTTPException as e:
                background_tasks.add_task(
                    finalizar_auditoria_dinamica, 
                    audit_id, "FAILED", 0, start_time, client_id, e.status_code, engine_cliente
                )
                raise e

            # --- CAPA 4: REGLAS DE NEGOCIO (IP Y SPAM) ---
            AuthService.validar_permiso_descarga(registro, client_ip)
            await AuthService.check_anti_spam(db, client_ip, registro.recurso, audit_id)

            # --- CAPA 5: PREPARACIÓN DE ARCHIVO ---
            full_path = FileService.get_secure_path(nfs_base_path, registro.recurso)
            if not os.path.exists(full_path):
                logger.error(f"Fisicamente no existe el archivo: {full_path}")
                raise HTTPException(status_code=404, detail="El archivo físico no se encuentra en el servidor.")
                
            file_size = os.path.getsize(full_path)
            friendly_name = FileService.generate_friendly_filename(registro.nombre, registro.mime, audit_id)

            # Actualizamos estado a REDIRECTED (en proceso)
            registro.estado = "REDIRECTED"
            registro.ip = client_ip
            registro.fecha_actualizacion = datetime.now(timezone.utc)
            await db.commit()
            break 

        async def stream_wrapper():
            # Usamos un diccionario para asegurar que el estado sea mutable y persistente
            stats = {"bytes_sent": 0, "success": False}
            
            try:
                logger.info(f"🚀 Iniciando stream para ID {audit_id}. Tamaño total: {file_size} bytes")

                async for chunk in FileService.file_iterator(full_path):
                    # Verificamos desconexión antes de enviar
                    if await request.is_disconnected():
                        logger.warning(f"❌ Cliente desconectado en byte {stats['bytes_sent']}")
                        raise ClientDisconnect("Cliente desconectado")
                    
                    yield chunk
                    stats["bytes_sent"] += len(chunk)
                
                # Verificación de integridad: Si enviamos todo, es éxito
                if stats["bytes_sent"] >= file_size:
                    stats["success"] = True 
                    logger.info(f"✅ Stream finalizado con éxito para ID {audit_id}. Total: {stats['bytes_sent']} bytes")
                else:
                    logger.warning(f"⚠️ Stream incompleto: {stats['bytes_sent']}/{file_size} bytes")

            except Exception as e:
                # Si es una desconexión normal del cliente, no lo logueamos como error crítico
                if isinstance(e, ClientDisconnect):
                    logger.info(f"ℹ️ Cliente cerró la conexión para ID {audit_id}")
                else:
                    logger.error(f"🔥 Error en stream ID {audit_id}: {str(e)}")
            
            finally:
                # Determinamos estado final basándonos en los bytes reales enviados
                # Un margen de error de 0 bytes (comparación exacta)
                final_success = stats["success"] or (stats["bytes_sent"] >= file_size and file_size > 0)
                
                estado_final = "COMPLETED" if final_success else "FAILED"
                codigo_http = 200 if final_success else 499
                
                background_tasks.add_task(
                    finalizar_auditoria_dinamica,
                    audit_id, 
                    estado_final, 
                    stats["bytes_sent"], 
                    start_time, 
                    client_id, 
                    codigo_http, 
                    engine_cliente, 
                    registro_id_for_stats
                )

        # Codificación de cabeceras
        friendly_name_encoded = quote(friendly_name)
        friendly_name_ascii = friendly_name.encode('ascii', 'ignore').decode('ascii')

        return StreamingResponse(
            stream_wrapper(),
            media_type="application/octet-stream", 
            headers={
                "Content-Disposition": f'attachment; filename="{friendly_name_ascii}"; filename*=UTF-8\'\'{friendly_name_encoded}',
                #"Content-Length": str(file_size),
                "X-Accel-Buffering": "no",
                "X-Content-Type-Options": "nosniff", 
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
                "Accept-Ranges": "bytes"
            }
        )
        
    except HTTPException as he:
        # Errores controlados (401, 403, 404, etc.)
        raise he
    except Exception as ge:
        logger.error(f"Error inesperado: {ge}")
        if engine_cliente:
            background_tasks.add_task(finalizar_auditoria_dinamica, audit_id, "ERROR", 0, start_time, client_id, 500, engine_cliente)
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# ============================================================
# UPLOAD FILES
# ============================================================

UPLOAD_BASE_PATH = Path("/app/media")
MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50 MB por archivo
MAX_FILES = 3


def sanitize_path_component(value: str) -> str:
    """
    Sanitiza un componente de ruta para evitar path traversal.
    """
    value = value.strip()

    if not value or value in {".", ".."}:
        raise HTTPException(
            status_code=400,
            detail="Componente de ruta inválido"
        )

    sanitized = re.sub(r"[^a-zA-Z0-9_-]", "", value)

    if not sanitized:
        raise HTTPException(
            status_code=400,
            detail="Componente de ruta inválido"
        )

    return sanitized


def normalize_module_name(modulo: str) -> str:
    """
    Equivalente aproximado al comportamiento ucfirst() de PHP.
    """
    modulo = sanitize_path_component(modulo)

    return modulo[:1].upper() + modulo[1:]


def parse_expedition_date(value: Optional[str]) -> Optional[int]:
    """
    Convierte una fecha de expedición a timestamp Unix.
    """
    if not value or not value.strip():
        return None

    value = value.strip()

    try:
        parsed_date = datetime.fromisoformat(value.replace("Z", "+00:00"))

        if parsed_date.tzinfo is None:
            parsed_date = parsed_date.replace(tzinfo=timezone.utc)

        return int(parsed_date.timestamp())

    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Fecha de expedición inválida: {value}"
        )


def parse_publication_date(value: Optional[str]) -> Optional[datetime]:
    """
    Convierte la fecha de publicación para MySQL.
    """
    if not value or not value.strip():
        return None

    value = value.strip()

    try:
        parsed_date = datetime.fromisoformat(value.replace("Z", "+00:00"))

        if parsed_date.tzinfo is not None:
            parsed_date = parsed_date.astimezone(timezone.utc)
            parsed_date = parsed_date.replace(tzinfo=None)

        return parsed_date

    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Fecha de publicación inválida: {value}"
        )


async def get_next_file_id(client_engine: AsyncEngine) -> int:
    """
    Incrementa el consecutivo consFiles en la base de datos
    del cliente.
    """

    async with AsyncSession(client_engine) as session:
        try:
            async with session.begin():

                result = await session.execute(
                    text("""
                        SELECT valor
                        FROM tn_sx_vars
                        WHERE modulo = :modulo
                          AND variable = :variable
                        FOR UPDATE
                    """),
                    {
                        "modulo": "Sx",
                        "variable": "consFiles"
                    }
                )

                row = result.first()

                if not row:
                    raise ValueError(
                        "No existe el consecutivo Sx / consFiles "
                        "en la base de datos del cliente"
                    )

                current_value = int(row[0])
                next_id = current_value + 1

                update_result = await session.execute(
                    text("""
                        UPDATE tn_sx_vars
                        SET valor = :next_id
                        WHERE modulo = :modulo
                          AND variable = :variable
                    """),
                    {
                        "next_id": next_id,
                        "modulo": "Sx",
                        "variable": "consFiles"
                    }
                )

                if update_result.rowcount != 1:
                    raise ValueError(
                        "No fue posible actualizar consFiles"
                    )

            logger.info(
                "Consecutivo generado en BD del cliente: %s",
                next_id
            )

            return next_id

        except Exception as exc:
            logger.exception(
                "Error generando consecutivo en BD del cliente"
            )

            raise HTTPException(
                status_code=500,
                detail="Error generando el consecutivo del archivo"
            ) from exc



async def save_upload_file(
    upload_file: UploadFile,
    destination: Path
) -> int:
    """
    Guarda un archivo de manera incremental evitando cargarlo
    completamente en memoria.

    Retorna el tamaño real guardado.
    """

    total_size = 0

    destination.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    try:
        with destination.open("wb") as destination_file:

            while True:
                chunk = await upload_file.read(1024 * 1024)

                if not chunk:
                    break

                total_size += len(chunk)

                if total_size > MAX_UPLOAD_SIZE:
                    raise HTTPException(
                        status_code=413,
                        detail="El archivo supera el tamaño máximo permitido"
                    )

                destination_file.write(chunk)

        return total_size

    except Exception:
        if destination.exists():
            destination.unlink(missing_ok=True)

        raise

    finally:
        await upload_file.close()


@app.post("/upload")
async def upload_files(
    request: Request,
    modulo: str = Form(...),
    opcion: str = Form(...),
    idRel: int = Form(...),
    archivos: list[UploadFile] = File(...),
    descripcion: list[str] = Form(default=[]),
    tipo: list[str] = Form(default=[]),
    expedicion: list[str] = Form(default=[]),
    fpublicacion: list[str] = Form(default=[]),
    seguro: list[str] = Form(default=[]),
    feedback: bool = Form(default=False),
    client_id: str = Form(...),
    token: str = Form(...)
):
    """
    Endpoint equivalente a regNuevos2() del PHP legado.
    """

    # --------------------------------------------------------
    # 1. Validación de autenticación
    # --------------------------------------------------------

    valid_token = AuthService.validar_access_token_google(
        token,
        client_id
    )

    if not valid_token:
        raise HTTPException(
            status_code=401,
            detail="Token de autenticación inválido"
        )

    # --------------------------------------------------------
    # 2. Validación de archivos
    # --------------------------------------------------------

    if not archivos:
        raise HTTPException(
            status_code=400,
            detail="No se recibieron archivos"
        )

    if len(archivos) > MAX_FILES:
        raise HTTPException(
            status_code=400,
            detail=f"Se permite cargar máximo {MAX_FILES} archivos"
        )

    # --------------------------------------------------------
    # 3. Obtener configuración del cliente
    # --------------------------------------------------------

    client_engine = await get_engine_for_client(client_id)

    if not client_engine:
        raise HTTPException(
            status_code=404,
            detail="No se encontró la configuración de base de datos del cliente"
        )

    cliente_folder = await get_client_folder(client_id)

    if not cliente_folder:
        raise HTTPException(
            status_code=400,
            detail="El cliente no tiene configurado clienteFolder"
        )

    cliente_folder = sanitize_path_component(cliente_folder)

    # --------------------------------------------------------
    # 4. Preparar estructura del NFS
    # --------------------------------------------------------

    modulo_folder = normalize_module_name(modulo)

    now = datetime.now(timezone.utc)

    year = now.strftime("%Y")
    month = now.strftime("%m")

    upload_directory = (
        UPLOAD_BASE_PATH
        / cliente_folder
        / "activo"
        / modulo_folder
        / year
        / month
    )

    # --------------------------------------------------------
    # 5. Procesar archivos
    # --------------------------------------------------------

    uploaded_files = []
    failed_files = []

    async for db in get_db_session(client_id):

        for index, upload_file in enumerate(archivos):

            file_id = None
            destination = None
            db_inserted = False

            try:

                if not upload_file.filename:
                    failed_files.append({
                        "filename": None,
                        "error": "Nombre de archivo inválido"
                    })
                    continue

                original_filename = Path(
                    upload_file.filename
                ).name

                file_extension = Path(
                    original_filename
                ).suffix.lower()

                mime_type = (
                    upload_file.content_type
                    or "application/octet-stream"
                )

                # ------------------------------------------------
                # Metadata individual del archivo
                # ------------------------------------------------

                file_description = (
                    descripcion[index].strip()
                    if index < len(descripcion)
                    else ""
                )

                file_type = (
                    tipo[index].strip()
                    if index < len(tipo)
                    else ""
                )

                file_expedition = (
                    expedicion[index]
                    if index < len(expedicion)
                    else ""
                )

                file_publication = (
                    fpublicacion[index]
                    if index < len(fpublicacion)
                    else ""
                )

                file_insurance = (
                    seguro[index]
                    if index < len(seguro)
                    else "0"
                )

                expedition_timestamp = parse_expedition_date(
                    file_expedition
                )

                publication_date = parse_publication_date(
                    file_publication
                )

                insurance_value = (
                    "1"
                    if str(file_insurance).strip().lower()
                    in {"1", "true", "si", "sí"}
                    else "0"
                )

                # --------------------------------------------
                # 5.1 Obtener consecutivo
                # --------------------------------------------

                file_id = await get_next_file_id(client_engine)

                # --------------------------------------------
                # 5.2 Nombre físico
                # --------------------------------------------

                physical_filename = f"sxFile{file_id}"

                if feedback and file_extension:
                    physical_filename += file_extension

                destination = (
                    upload_directory
                    / physical_filename
                )

                # --------------------------------------------
                # 5.3 Guardar archivo físico
                # --------------------------------------------

                file_size = await save_upload_file(
                    upload_file,
                    destination
                )

                # --------------------------------------------
                # 5.4 Insertar metadatos
                # --------------------------------------------

                current_timestamp = int(
                    datetime.now(timezone.utc).timestamp()
                )

                await db.execute(
                    text("""
                        INSERT INTO tn_sx_files (
                            id,
                            modulo,
                            opcion,
                            idRel,
                            nombre,
                            descripcion,
                            tipoDoc,
                            fExpedicion,
                            fPublicacion,
                            tipoMime,
                            size,
                            fecha,
                            estado,
                            seguro,
                            ordenamiento,
                            cliente,
                            is_gcs,
                            gcs_unique_filename,
                            gcs_url
                        )
                        VALUES (
                            :id,
                            :modulo,
                            :opcion,
                            :idRel,
                            :nombre,
                            :descripcion,
                            :tipoDoc,
                            :fExpedicion,
                            :fPublicacion,
                            :tipoMime,
                            :size,
                            :fecha,
                            'A',
                            :seguro,
                            0,
                            0,
                            0,
                            NULL,
                            NULL
                        )
                    """),
                    {
                        "id": file_id,
                        "modulo": modulo,
                        "opcion": opcion,
                        "idRel": idRel,
                        "nombre": original_filename,
                        "descripcion": file_description or None,
                        "tipoDoc": file_type or None,
                        "fExpedicion": expedition_timestamp,
                        "fPublicacion": publication_date,
                        "tipoMime": mime_type,
                        "size": file_size,
                        "fecha": current_timestamp,
                        "seguro": insurance_value
                    }
                )

                await db.commit()

                db_inserted = True

                uploaded_files.append({
                    "id": file_id,
                    "nombre": original_filename,
                    "nombreFisico": physical_filename,
                    "size": file_size,
                    "tipoMime": mime_type,
                    "modulo": modulo,
                    "opcion": opcion,
                    "idRel": idRel,
                    "estado": "A"
                })

            except HTTPException as exc:

                if destination and destination.exists():
                    destination.unlink(missing_ok=True)

                if db_inserted and file_id:
                    await db.execute(
                        text("""
                            DELETE FROM tn_sx_files
                            WHERE id = :id
                        """),
                        {"id": file_id}
                    )
                    await db.commit()

                failed_files.append({
                    "filename": upload_file.filename,
                    "error": exc.detail
                })

            except Exception as exc:

                await db.rollback()

                if destination and destination.exists():
                    destination.unlink(missing_ok=True)

                if file_id:
                    try:
                        await db.execute(
                            text("""
                                DELETE FROM tn_sx_files
                                WHERE id = :id
                            """),
                            {"id": file_id}
                        )
                        await db.commit()
                    except Exception:
                        await db.rollback()

                logger.exception(
                    "Error cargando archivo %s",
                    upload_file.filename
                )

                failed_files.append({
                    "filename": upload_file.filename,
                    "error": "Error interno procesando el archivo"
                })

        break

    # --------------------------------------------------------
    # 6. Respuesta
    # --------------------------------------------------------

    return {
        "success": len(uploaded_files) > 0,
        "total": len(archivos),
        "uploaded": len(uploaded_files),
        "failed": len(failed_files),
        "files": uploaded_files,
        "errors": failed_files,
        "lastId": (
            uploaded_files[-1]["id"]
            if uploaded_files
            else None
        )
    }