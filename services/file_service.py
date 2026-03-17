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
        # Recomendación: CHUNK_SIZE = 64 * 1024 (64KB) en settings
        async with aiofiles.open(file_path, mode="rb") as f:
            while True:
                chunk = await f.read(settings.CHUNK_SIZE)
                if not chunk: 
                    break
                yield chunk
                # Un sleep de 1ms (0.001) es la diferencia entre colapsar el 
                # Gateway o mantener un flujo constante y saludable.
                await asyncio.sleep(0.001)

    @staticmethod
    def generate_friendly_filename(nombre_db: str, mime_type: str, audit_id: int) -> str:
        """
        Genera un nombre de archivo seguro, asegurando que la extensión sea correcta
        y que el nombre no rompa las cabeceras HTTP.
        """
        # 1. DETERMINAR LA EXTENSIÓN REAL
        extension = None
        
        # Prioridad A: Intentar extraerla del nombre original de la DB (lo más fiable para el usuario)
        if nombre_db and "." in nombre_db:
            ext_extraida = os.path.splitext(nombre_db)[1].lower().strip()
            # Validamos que sea una extensión razonable (ej: .pdf, no .2019)
            if 2 <= len(ext_extraida) <= 5: 
                extension = ext_extraida

        # Prioridad B: Si no hay extensión en el nombre, usar el MIME type
        if (not extension or extension == ".bin") and mime_type:
            m_type = mime_type.strip().lower()
            extension = mimetypes.guess_extension(m_type)
            
            # Ajustes manuales para tipos de Office y comprimidos
            if not extension or extension == ".bin":
                if 'pdf' in m_type: extension = '.pdf'
                elif 'word' in m_type: extension = '.docx'
                elif 'excel' in m_type or 'spreadsheet' in m_type: extension = '.xlsx'
                elif 'zip' in m_type: extension = '.zip'
                elif 'rar' in m_type: extension = '.rar'
                elif '7z' in m_type: extension = '.7z'
                elif 'jpeg' in m_type or 'jpg' in m_type: extension = '.jpg'

        # Fallback de seguridad
        if not extension:
            extension = ".bin"
        
        if extension == ".jpe": extension = ".jpg"

        # 2. LIMPIAR EL CUERPO DEL NOMBRE (BASE NAME)
        # Usamos el nombre de la DB o el fallback de ID
        raw_name = nombre_db if nombre_db else f"archivo_{audit_id}"
        
        # Quitamos la extensión del cuerpo para procesarlo limpio
        # Usamos os.path.splitext para que sea exacto y no falle con endswith
        base_name, _ = os.path.splitext(raw_name)

        # LIMPIEZA RADICAL:
        # Solo permitimos letras, números, espacios, puntos, guiones y paréntesis.
        # Esto elimina saltos de línea (\n), comas, comillas y barras que rompen el header.
        base_name = re.sub(r'[^\w\s\.\-\(\)]', '', base_name)
        
        # Eliminar puntos al final del nombre (ej: "2.4.3.2." -> "2.4.3.2")
        # Esto evita el error de doble punto antes de la extensión "..pdf"
        base_name = base_name.strip().rstrip('.')

        # Colapsar espacios múltiples y limitar longitud
        base_name = " ".join(base_name.split())[:150]
        
        # Si después de la limpieza no quedó nada, usamos el ID
        if not base_name or base_name.strip() == "":
            base_name = f"archivo_{audit_id}"

        # 3. RETORNO DE LA UNIÓN PERFECTA
        return f"{base_name}{extension}"