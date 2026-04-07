import os
from io import BytesIO
from minio import Minio

# ====================================================
# CONFIGURACIÓN DESDE VARIABLES DE ENTORNO (CAPROVER)
# ====================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "verde-minicloud-ficheros.verdesuite.sytes.net")
MINIO_ACCESS_KEY = os.getenv("df71c1471ebb08e1a71273a7")
MINIO_SECRET_KEY = os.getenv("141226bfa68433dd5bfc0b30dc1a425ef60e9a")
MINIO_USE_SSL = os.getenv("MINIO_USE_SSL", "true").lower() == "true"
MINIO_PUBLIC_URL = os.getenv("MINIO_PUBLIC_URL", "https://verde-minicloud-ficheros.verdesuite.sytes.net")

# Mapeo de tipos a buckets (puedes ajustarlo)
BUCKET_MAP = {
    "incidencia": "incidencias",
    "viabilidad": "viabilidades",
    "ticket": "tickets",
    "presupuesto": "presupuestos"
}

# Inicializar cliente de MinIO
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_USE_SSL
)


def upload_image_to_cloudinary(file, filename, folder=None, tipo="incidencia"):
    """
    Sube un archivo a MinIO en el bucket correspondiente según el tipo.

    Args:
        file: Objeto UploadedFile de Streamlit o bytes.
        filename: Nombre del archivo (ej: "12345.jpg").
        folder: Subcarpeta opcional DENTRO del bucket (ej: "2025/02").
        tipo: String que indica el tipo de archivo. Valores posibles:
              "incidencia", "viabilidad", "ticket", "presupuesto"

    Returns:
        URL pública del archivo subido.
    """
    # 1. Determinar bucket según tipo
    bucket_name = BUCKET_MAP.get(tipo, "incidencias")

    # 2. Asegurar que el bucket existe
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        # Política de lectura pública (opcional, comenta si prefieres hacerlo manual)
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket_name}/*"]
                }
            ]
        }
        client.set_bucket_policy(bucket_name, policy)

    # 3. Construir object_name
    object_name = filename
    if folder:
        object_name = f"{folder.rstrip('/')}/{filename}"

    # 4. Leer datos del archivo
    if hasattr(file, 'read'):
        file_data = file.read()
    else:
        file_data = file

    # 5. Determinar content_type según extensión
    ext = os.path.splitext(filename)[1].lower()
    content_type = {
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.png': 'image/png',
        '.gif': 'image/gif',
        '.pdf': 'application/pdf',
        '.doc': 'application/msword',
        '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    }.get(ext, 'application/octet-stream')

    # 6. Subir a MinIO
    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=BytesIO(file_data),
        length=len(file_data),
        content_type=content_type
    )

    # 7. Generar URL pública
    base_url = MINIO_PUBLIC_URL.rstrip('/')
    url = f"{base_url}/{bucket_name}/{object_name}"
    return url