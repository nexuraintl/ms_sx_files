import os
import aiofiles
import mimetypes
import re
import asyncio
from datetime import datetime
from fastapi import HTTPException
from core.config import settings

# Registramos tipos MIME comunes que suelen no estar en la base estándar
mimetypes.add_type('application/x-zip-compressed', '.zip')
mimetypes.add_type('application/x-7z-compressed', '.7z')
mimetypes.add_type('application/vnd.rar', '.rar')

class FileService:
    @staticmethod
    def get_secure_path(base_nfs_path: str, relative_path: str) -> str:
        # Aseguramos que la base existe
        if not os.path.exists(base_nfs_path):
            raise HTTPException(status_code=500, detail="Error interno: Ruta NFS no montada.")

        # Construcción segura
        safe_path = os.path.normpath(os.path.join(base_nfs_path, relative_path.lstrip("/")))
        
        if not safe_path.startswith(os.path.abspath(base_nfs_path)):
            raise HTTPException(status_code=403, detail="Acceso no permitido.")
            
        if not os.path.isfile(safe_path):
            raise HTTPException(status_code=404, detail="Archivo no encontrado en el volumen.")
            
        return safe_path

    @staticmethod
    async def file_iterator(file_path: str):
        async with aiofiles.open(file_path, mode="rb") as f:
            while True:
                chunk = await f.read(settings.CHUNK_SIZE)
                if not chunk: break
                yield chunk
                await asyncio.sleep(0)

    @staticmethod
    def generate_friendly_filename(nombre_db: str, mime_type: str, audit_id: int) -> str:
        """
        Genera un nombre de archivo seguro, manejando extensiones conflictivas y 
        caracteres que rompen las cabeceras HTTP.
        """
        # 1. Determinar la extensión con máxima prioridad
        extension = None
        
        # Intento A: Por MIME type
        if mime_type:
            m_type = mime_type.strip().lower()
            extension = mimetypes.guess_extension(m_type)
            
            # Ajustes manuales para tipos comunes que mimetypes suele fallar
            if not extension or extension == ".bin":
                if 'pdf' in m_type: extension = '.pdf'
                elif 'word' in m_type or 'officedocument.word' in m_type: extension = '.docx'
                elif 'excel' in m_type or 'spreadsheet' in m_type: extension = '.xlsx'
                elif 'zip' in m_type: extension = '.zip'
                elif 'rar' in m_type: extension = '.rar'
                elif '7z' in m_type: extension = '.7z'
                elif 'jpeg' in m_type or 'jpg' in m_type: extension = '.jpg'

        # Intento B: Si el MIME falló, extraerla del nombre original en la DB
        if (not extension or extension == ".bin") and nombre_db and "." in nombre_db:
            ext_extraida = os.path.splitext(nombre_db)[1].lower()
            if len(ext_extraida) > 1: # Aseguramos que no sea solo un punto
                extension = ext_extraida

        # Fallback final
        if not extension:
            extension = ".bin"
        
        # Normalización de extensiones raras
        if extension == ".jpe": extension = ".jpg"

        # 2. Limpiar el cuerpo del nombre (Base Name)
        base_name = nombre_db if nombre_db else f"archivo_{audit_id}"
        
        # Quitamos la extensión del nombre base si ya la trae (para evitar archivo.pdf.pdf)
        if base_name.lower().endswith(extension.lower()):
            base_name = base_name[: -len(extension)]

        # LIMPIEZA DE CARACTERES CRÍTICOS
        # Eliminamos: , ; " \ / (rompen headers) y [ ] { } < > (conflictos de SO)
        # Mantenemos: letras, números, puntos internos, guiones y espacios.
        base_name = re.sub(r'[,;\"\\\/\[\]{}<>]', '', base_name)
        
        # IMPORTANTE: Quitamos puntos al final del nombre base para no confundir al navegador
        # Esto corrige el caso "2.4.3.2..pdf" -> "2.4.3.2.pdf"
        base_name = base_name.strip().rstrip('.')

        # Limpiar espacios múltiples y limitar longitud para estabilidad de cabeceras
        base_name = " ".join(base_name.split())[:150]
        
        if not base_name:
            base_name = f"archivo_{audit_id}"

        # 3. Retornar la unión perfecta
        return f"{base_name}{extension}"