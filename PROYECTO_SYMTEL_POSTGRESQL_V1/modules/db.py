# db.py — Modulo centralizado de conexion a PostgreSQL
# Todos los modulos importan la conexion desde aqui.
# Si cambias de servidor, solo tocas este archivo.

import psycopg
import os

# Configuracion de conexion
_DB_CONNINFO = "host={host} port={port} dbname={dbname} user={user} password={password}".format(
    host=os.getenv("DB_HOST", "pre-verde-suite.verdesuite.sytes.net"),
    port=os.getenv("DB_PORT", "5432"),
    dbname=os.getenv("DB_NAME", "verde_suite_pre"),
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD", "7ee2db054df467f5"),
)


def get_db_connection():
    """
    Crea y retorna una conexion a PostgreSQL.
    Uso:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT ...")
        conn.commit()
        conn.close()
    """
    return psycopg.connect(_DB_CONNINFO)
