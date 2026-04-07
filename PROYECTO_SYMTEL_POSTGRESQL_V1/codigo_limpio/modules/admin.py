from modules.notificaciones import correo_usuario, correo_nuevas_zonas_comercial, correo_excel_control, \
    correo_envio_presupuesto_manual, correo_nueva_version, correo_asignacion_puntos_existentes, \
    correo_viabilidad_comercial, notificar_asignacion_ticket, notificar_actualizacion_ticket, correo_respuesta_comercial,\
    notificar_resolucion_ticket, notificar_reasignacion_ticket
from datetime import datetime as dt  # Para evitar conflicto con datetime
from streamlit_option_menu import option_menu
from streamlit_cookies_controller import CookieController
from st_aggrid import AgGrid, GridOptionsBuilder, DataReturnMode, GridUpdateMode
from io import BytesIO
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import ftfy, folium, warnings, json, gspread, urllib, zipfile, sqlite3, datetime, bcrypt, os, sqlitecloud, io
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st
from folium.plugins import MarkerCluster, Geocoder, Fullscreen
from streamlit_folium import st_folium
from branca.element import Template, MacroElement
from modules.reportes_pdf import preparar_datos_para_pdf, generar_pdf_reportlab
from modules.cdr_kpis import mostrar_cdrs
from typing import List, Tuple, Dict

warnings.filterwarnings("ignore", category=UserWarning)

cookie_name = "my_app"

# Función para obtener conexión a la base de datos
def obtener_conexion():
    """Retorna una nueva conexión a la base de datos."""
    try:
        conn = sqlitecloud.connect(
            "sqlitecloud://ceafu04onz.g6.sqlite.cloud:8860/usuarios.db?apikey=Qo9m18B9ONpfEGYngUKm99QB5bgzUTGtK7iAcThmwvY")
        return conn
    except sqlite3.Error as e:
        print(f"Error al conectar con la base de datos: {e}")
        return None

#def obtener_conexion():
#    """Retorna una nueva conexión a la base de datos SQLite local."""
#    try:
#        # Ruta del archivo dentro del contenedor (puedes cambiarla)
#        db_path = "/data/usuarios.db"  # o usa variable de entorno
#        # Verifica si el archivo existe
#        if not os.path.exists(db_path):
#            raise FileNotFoundError(f"No se encuentra la base de datos en {db_path}")
#        conn = sqlite3.connect(db_path)
#        return conn
#    except (sqlite3.Error, FileNotFoundError) as e:
#        print(f"Error al conectar con la base de datos: {e}")
#        return None

def log_trazabilidad(usuario, accion, detalles):
    """Inserta un registro en la tabla de trazabilidad."""
    try:
        conn = obtener_conexion()
        cursor = conn.cursor()
        fecha = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("""
            INSERT INTO trazabilidad (usuario_id, accion, detalles, fecha)
            VALUES (?, ?, ?, ?)
        """, (usuario, accion, detalles, fecha))
        conn.commit()
        conn.close()
    except Exception as e:
        # En caso de error en la trazabilidad, se imprime en consola (no se interrumpe la app)
        print(f"Error registrando trazabilidad: {e}")

# Función para convertir a numérico y manejar excepciones
def safe_convert_to_numeric(col):
    try:
        return pd.to_numeric(col)
    except ValueError:
        return col  # Si ocurre un error, regresamos la columna original sin cambios

def actualizar_google_sheet_desde_db(sheet_id, sheet_name="Viabilidades"):
    try:
        # --- 1️⃣ Leer datos de la base de datos ---
        conn = obtener_conexion()
        df_db = pd.read_sql("SELECT * FROM viabilidades", conn)
        conn.close()

        if df_db.empty:
            st.warning("⚠️ No hay datos en la tabla 'viabilidades'.")
            return

        # --- 2️⃣ Expandir filas con múltiples apartment_id ---
        expanded_rows = []
        for _, row in df_db.iterrows():
            apartment_ids = str(row["apartment_id"]).split(",") if pd.notna(row["apartment_id"]) else [""]
            for apt in apartment_ids:
                new_row = row.copy()
                new_row["apartment_id"] = apt.strip()
                expanded_rows.append(new_row)
        df_db_expanded = pd.DataFrame(expanded_rows)

        # --- 3️⃣ Cargar credenciales ---
        posibles_rutas = [
            "modules/carga-contratos-verde-c5068516c7cf.json",
            "/etc/secrets/carga-contratos-verde-c5068516c7cf.json",
            os.path.join(os.path.dirname(__file__), "carga-contratos-verde-c5068516c7cf.json"),
        ]
        ruta_credenciales = next((r for r in posibles_rutas if os.path.exists(r)), None)

        if not ruta_credenciales and "GOOGLE_APPLICATION_CREDENTIALS_JSON" in os.environ:
            creds_dict = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
            creds = Credentials.from_service_account_info(
                creds_dict,
                scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            )
        elif ruta_credenciales:
            creds = Credentials.from_service_account_file(
                ruta_credenciales,
                scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            )
        else:
            raise ValueError("❌ No se encontraron credenciales de Google Service Account.")

        # --- 4️⃣ Conexión con Google Sheets ---
        service = build("sheets", "v4", credentials=creds)
        sheet_metadata = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        available_sheets = [s["properties"]["title"] for s in sheet_metadata.get("sheets", [])]

        if sheet_name not in available_sheets:
            st.warning(f"⚠️ La hoja '{sheet_name}' no existe. Se usará '{available_sheets[0]}' en su lugar.")
            sheet_name = available_sheets[0]

        sheet = service.spreadsheets()

        # --- 5️⃣ Leer encabezados y datos actuales ---
        result = sheet.values().get(spreadsheetId=sheet_id, range=f"{sheet_name}!1:1").execute()
        headers = result.get("values", [[]])[0]

        if not headers:
            st.toast("❌ No se encontraron encabezados en la hoja de Google Sheets.")
            return

        result_data = sheet.values().get(spreadsheetId=sheet_id, range=sheet_name).execute()
        values = result_data.get("values", [])
        df_sheet = pd.DataFrame(values[1:], columns=headers) if len(values) > 1 else pd.DataFrame(columns=headers)

        # --- 6️⃣ Mapear columnas Excel -> Base de datos ---
        excel_to_db_map = {
            "SOLICITANTE": "usuario",
            "FECHA DE ENTREGA": "fecha_entrega",
            "ESTADO OBRA": "estado_obra",
            "Nueva Promoción": "nuevapromocion",
            "RESULTADO": "resultado",
            "JUSTIFICACIÓN": "justificacion",
            "PRESUPUESTO": "coste",
            "UUII": "zona_estudio",
            "CONTRATOS": "contratos",
            "RESPUESTA COMERCIAL": "respuesta_comercial"
        }

        # --- 7️⃣ Actualizar o agregar filas por apartment_id y ticket ---
        updated = 0
        added = 0
        df_sheet = df_sheet.copy()

        # Normalizar columnas clave
        if "apartment_id" not in df_sheet.columns:
            st.toast("❌ La hoja no tiene columna 'apartment_id'.")
            return
        if "ticket" not in df_sheet.columns:
            df_sheet["ticket"] = ""

        df_sheet["apartment_id"] = df_sheet["apartment_id"].astype(str).str.strip().str.upper()
        df_sheet["ticket"] = df_sheet["ticket"].astype(str).str.strip()

        for _, row_db in df_db_expanded.iterrows():
            apt_db = str(row_db.get("apartment_id", "")).strip().upper()
            ticket_db = str(row_db.get("ticket", "")).strip()
            if not ticket_db:
                continue  # ignorar filas sin ticket

            # Buscar coincidencia exacta de ticket + apartment_id
            mask = (
                    (df_sheet["ticket"] == ticket_db) &
                    (df_sheet["apartment_id"] == apt_db)
            )

            # --- Si la fila ya existe en el Sheet ---
            if mask.any():
                idx = df_sheet[mask].index[0]
                cambios_realizados = False

                # 🔹 Actualizar todas las columnas mapeadas y coincidentes
                for col in headers:
                    db_col = excel_to_db_map.get(col, col)  # Usa el mapeo si existe, sino el mismo nombre
                    if db_col in df_db_expanded.columns:
                        nuevo_valor = "" if pd.isna(row_db[db_col]) else str(row_db[db_col])
                        actual_valor = "" if pd.isna(df_sheet.at[idx, col]) else str(df_sheet.at[idx, col])
                        # Compara sin espacios y sin distinción de mayúsculas
                        if nuevo_valor.strip() != actual_valor.strip():
                            df_sheet.at[idx, col] = nuevo_valor
                            cambios_realizados = True

                if cambios_realizados:
                    updated += 1

            # --- Si la fila no existe, crearla ---
            else:
                new_row = {col: "" for col in headers}
                for col in headers:
                    db_col = excel_to_db_map.get(col, col)
                    if db_col in df_db_expanded.columns:
                        new_row[col] = "" if pd.isna(row_db[db_col]) else str(row_db[db_col])
                new_row["ticket"] = ticket_db
                new_row["apartment_id"] = apt_db
                df_sheet = pd.concat([df_sheet, pd.DataFrame([new_row])], ignore_index=True)
                added += 1

        # --- 8️⃣ Escribir datos actualizados ---
        values_out = [headers] + df_sheet.fillna("").astype(str).values.tolist()
        sheet.values().clear(spreadsheetId=sheet_id, range=sheet_name).execute()
        sheet.values().update(
            spreadsheetId=sheet_id,
            range=sheet_name,
            valueInputOption="RAW",
            body={"values": values_out}
        ).execute()

        st.toast(
            f"✅ Google Sheet '{sheet_name}' actualizado correctamente.\n"
            f"🟢 {updated} filas actualizadas.\n"
            f"🆕 {added} filas nuevas añadidas."
        )

    except Exception as e:
        st.toast(f"❌ Error al actualizar la hoja de Google Sheets: {e}")

@st.cache_data(ttl=3600, max_entries=1, show_spinner="Cargando contratos desde Google Sheets...")
def cargar_contratos_google():
    try:
        # --- Detectar entorno y elegir archivo de credenciales ---
        posibles_rutas = [
            "modules/carga-contratos-verde-c5068516c7cf.json",  # Render: secret file
            "/etc/secrets/carga-contratos-verde-c5068516c7cf.json",  # Otra ruta posible en Render
            os.path.join(os.path.dirname(__file__), "carga-contratos-verde-c5068516c7cf.json"),  # Local
        ]

        ruta_credenciales = None
        for r in posibles_rutas:
            if os.path.exists(r):
                ruta_credenciales = r
                break

        if not ruta_credenciales and "GOOGLE_APPLICATION_CREDENTIALS_JSON" in os.environ:
            # Si no hay archivo pero sí variable de entorno
            creds_dict = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
            creds = Credentials.from_service_account_info(creds_dict, scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ])
        elif ruta_credenciales:
            print(f"🔑 Usando credenciales desde: {ruta_credenciales}")
            creds = Credentials.from_service_account_file(
                ruta_credenciales,
                scopes=[
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"
                ]
            )
        else:
            raise ValueError("❌ No se encontraron credenciales de Google Service Account.")

        # Crear cliente
        client = gspread.authorize(creds)

        # --- Abrir la hoja de Google Sheets ---
        spreadsheet = client.open("SEGUIMIENTO CLIENTES/CONTRATOS VERDE")
        sheet = spreadsheet.worksheet("LISTADO DE ESTADO DE CONTRATOS")

        # DIAGNÓSTICO: Ver estructura real de la hoja
        print(f"🔍 Hoja cargada. Título: {sheet.title}")
        print(f"🔍 Filas totales: {sheet.row_count}, Columnas: {sheet.col_count}")

        # OPCIÓN 1: Usar get_all_values() para ver TODO
        all_values = sheet.get_all_values()
        print(f"🔍 Valores obtenidos: {len(all_values)} filas")

        if len(all_values) <= 1:  # Solo encabezados o vacía
            st.error(f"⚠️ La hoja tiene {len(all_values)} filas. ¿Está vacía o mal formateada?")

            # Mostrar lo que hay (para diagnóstico)
            if all_values:
                st.write("Encabezados encontrados:", all_values[0])
            return pd.DataFrame()

        # Crear DataFrame manualmente
        # Primera fila = encabezados
        headers = [str(h).strip() for h in all_values[0]]

        # Filas siguientes = datos
        data_rows = all_values[1:] if len(all_values) > 1 else []

        print(f"✅ {len(headers)} encabezados: {headers}")
        print(f"✅ {len(data_rows)} filas de datos")

        # Mostrar algunas filas para diagnóstico
        if data_rows:
            print("📋 Primera fila de datos:", data_rows[0])
            print("📋 Segunda fila de datos:", data_rows[1] if len(data_rows) > 1 else "No hay")

        df = pd.DataFrame(data_rows, columns=headers)

        print(f"✅ DataFrame creado: {len(df)} filas x {len(df.columns)} columnas")

        # --- Mapeo de columnas ---
        # ACTUALIZADO: Incluir las nuevas columnas
        column_mapping = {
            'Nº CONTRATO': 'num_contrato',
            'APARTMENT ID': 'apartment_id',
            'CLIENTE': 'cliente',
            'COORDENADAS': 'coordenadas',
            'ESTADO': 'estado',
            'COMERCIAL': 'comercial',
            'FECHA INGRESO': 'fecha_ingreso',
            'FECHA INSTALACIÓN': 'fecha_instalacion',
            'FECHA FIN CONTRATO': 'fecha_fin_contrato',
            'FECHA INICIO CONTRATO': 'fecha_inicio_contrato',
            'COMENTARIOS': 'comentarios',
            'DIVISOR': 'divisor',
            'PUERTO': 'puerto',
            # NUEVAS COLUMNAS - mapear posibles nombres
            'SAT': 'SAT',
            'TIPO CLIENTE': 'Tipo_cliente',
            'TIPO CLIENTES': 'Tipo_cliente',  # Posible variación
            'TÉCNICO': 'tecnico',
            'TECNICO': 'tecnico',  # Sin tilde
            'MÉTODO ENTRADA': 'metodo_entrada',
            'METODO ENTRADA': 'metodo_entrada',  # Sin tilde
            'METODO DE ENTRADA': 'metodo_entrada',  # Otra variación
            'BILLING': 'billing',
            'BILL': 'billing',  # Posible variación corta
            'PERMANENCIA': 'permanencia'
        }

        # Primero, normalizar nombres de columnas del DataFrame (quitar espacios extra, convertir a mayúsculas)
        df.columns = df.columns.map(lambda x: str(x).strip().upper() if x is not None else "")

        # Aplicar el mapeo
        for sheet_col, db_col in column_mapping.items():
            if sheet_col in df.columns:
                # Si la columna existe en el sheet, mantenerla
                pass
            else:
                # Buscar variaciones (sin espacios, con/sin tildes, etc.)
                normalized_sheet_col = sheet_col.replace(' ', '').replace('Á', 'A').replace('É', 'E').replace('Í',
                                                                                                              'I').replace(
                    'Ó', 'O').replace('Ú', 'U')
                for actual_col in df.columns:
                    normalized_actual_col = actual_col.replace(' ', '').replace('Á', 'A').replace('É', 'E').replace('Í',
                                                                                                                    'I').replace(
                        'Ó', 'O').replace('Ú', 'U')
                    if normalized_sheet_col == normalized_actual_col:
                        # Renombrar la columna actual al nombre esperado
                        df.rename(columns={actual_col: sheet_col}, inplace=True)
                        print(f"✅ Renombrada columna '{actual_col}' -> '{sheet_col}'")
                        break

        # Ahora aplicar el mapeo de nombres
        df.rename(columns=column_mapping, inplace=True)

        # Verificar qué columnas se han mapeado correctamente
        print("🔍 Columnas después del mapeo:", df.columns.tolist())

        # --- Normalizar fechas ---
        for date_col in ['fecha_inicio_contrato', 'fecha_ingreso', 'fecha_instalacion', 'fecha_fin_contrato']:
            if date_col in df.columns:
                try:
                    df[date_col] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
                except Exception:
                    df[date_col] = df[date_col].astype(str)

        # --- Normalizar nuevas columnas (asegurar tipos de datos) ---
        new_columns_config = {
            'SAT': str,
            'Tipo_cliente': str,
            'tecnico': str,
            'metodo_entrada': str,
            'billing': str
        }

        for col, dtype in new_columns_config.items():
            if col in df.columns:
                df[col] = df[col].fillna('').astype(dtype)
                print(f"✅ Columna '{col}' normalizada: {len(df[df[col] != ''])} valores no vacíos")

        print("✅ Datos cargados. Columnas:", df.columns.tolist(), "Total filas:", len(df))

        # Mostrar conteo de valores por nueva columna
        new_cols = ['SAT', 'Tipo_cliente', 'tecnico', 'metodo_entrada', 'billing']
        for col in new_cols:
            if col in df.columns:
                non_empty = len(df[df[col] != ''])
                print(f"📊 Columna '{col}': {non_empty}/{len(df)} valores no vacíos")

        return df

    except Exception as e:
        print(f"❌ Error cargando contratos desde Google Sheets: {e}")
        import traceback
        print(traceback.format_exc())
        return pd.DataFrame()

    except Exception as e:
        print(f"❌ Error cargando contratos desde Google Sheets: {e}")
        return pd.DataFrame()

def cargar_usuarios():
    """Carga los usuarios desde la base de datos."""
    conn = obtener_conexion()
    if not conn:
        return []  # Salida temprana si la conexión falla

    try:
        with conn:  # `with` cierra automáticamente
            return conn.execute("SELECT id, username, role, email FROM usuarios").fetchall()
    except sqlite3.Error as e:
        print(f"Error al cargar los usuarios: {e}")
        return []

# Función para agregar un nuevo usuario
def agregar_usuario(username, rol, password, email):
    conn = obtener_conexion()
    cursor = conn.cursor()
    hashed_pw = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    try:
        cursor.execute("INSERT INTO usuarios (username, password, role, email) VALUES (?, ?, ?, ?)", (username, hashed_pw, rol, email))
        conn.commit()
        st.toast(f"Usuario '{username}' creado con éxito.")
        log_trazabilidad(st.session_state["username"], "Agregar Usuario",
                         f"El admin agregó al usuario '{username}' con rol '{rol}'.")

        # Enviar correo al usuario
        asunto = "🆕 ¡Nuevo Usuario Creado!"
        mensaje = (
            f"Estimado {username},<br><br>"
            f"Se ha creado una cuenta para ti en nuestro sistema con los siguientes detalles:<br><br>"
            f"📋 <strong>Nombre:</strong> {username}<br>"
            f"🛠 <strong>Rol:</strong> {rol}<br>"
            f"📧 <strong>Email:</strong> {email}<br><br>"
            f"🔑 <strong>Tu contraseña es:</strong> {password}<br><br>"
            f"Por favor, ingresa al sistema y comprueba que todo es correcto.<br><br>"
            f"⚠️ <strong>Por seguridad:</strong> No compartas esta información con nadie. "
            f"Si no has realizado esta solicitud o tienes alguna duda sobre la creación de tu cuenta, por favor contacta con nuestro equipo de soporte de inmediato.<br><br>"
            f"Si has recibido este correo por error, te recomendamos solicitar el cambio de tu contraseña tan pronto como puedas para garantizar la seguridad de tu cuenta.<br><br>"
            f"Gracias por ser parte de nuestro sistema.<br><br>"
        )
        correo_usuario(email, asunto, mensaje)  # Llamada a la función de correo

    except sqlite3.IntegrityError:
        st.toast(f"El usuario '{username}' ya existe.")
    finally:
        conn.close()

def editar_usuario(id, username, rol, password, email):
    conn = obtener_conexion()
    cursor = conn.cursor()

    # Obtenemos los datos actuales del usuario
    cursor.execute("SELECT username, role, email, password FROM usuarios WHERE id = ?", (id,))
    usuario_actual = cursor.fetchone()

    if usuario_actual:
        # Guardamos los valores actuales
        username_anterior, rol_anterior, email_anterior, password_anterior = usuario_actual

        # Realizamos las actualizaciones solo si hay cambios
        hashed_pw = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode() if password else None

        # Si la contraseña fue cambiada, realizamos la actualización correspondiente
        if password:
            cursor.execute("UPDATE usuarios SET username = ?, role = ?, password = ?, email = ? WHERE id = ?",
                           (username, rol, hashed_pw, email, id))
        else:
            cursor.execute("UPDATE usuarios SET username = ?, role = ?, email = ? WHERE id = ?",
                           (username, rol, email, id))

        conn.commit()
        conn.close()

        st.toast(f"Usuario con ID {id} actualizado correctamente.")
        log_trazabilidad(st.session_state["username"], "Editar Usuario", f"El admin editó al usuario con ID {id}.")

        # Ahora creamos el mensaje del correo, especificando qué ha cambiado
        cambios = []

        if username != username_anterior:
            cambios.append(f"📋 Nombre cambiado de <strong>{username_anterior}</strong> a <strong>{username}</strong>.")
        if rol != rol_anterior:
            cambios.append(f"🛠 Rol cambiado de <strong>{rol_anterior}</strong> a <strong>{rol}</strong>.")
        if email != email_anterior:
            cambios.append(f"📧 Email cambiado de <strong>{email_anterior}</strong> a <strong>{email}</strong>.")
        if password:  # Si la contraseña fue modificada
            cambios.append(f"🔑 Tu contraseña ha sido cambiada. Asegúrate de usar una nueva contraseña segura.")

        # Si no hay cambios, se indica en el correo
        if not cambios:
            cambios.append("❗ No se realizaron cambios en tu cuenta.")

        # Asunto y cuerpo del correo
        asunto = "¡Detalles de tu cuenta actualizados!"
        mensaje = (
            f"📢 Se han realizado cambios en tu cuenta con los siguientes detalles:<br><br>"
            f"{''.join([f'<strong>{cambio}</strong><br>' for cambio in cambios])}"  # Unimos los cambios en un formato adecuado
            f"<br>ℹ️ Si no realizaste estos cambios o tienes alguna duda, por favor contacta con el equipo de administración.<br><br>"
            f"⚠️ <strong>Por seguridad, te recordamos no compartir este correo con nadie. Si no reconoces los cambios, por favor contacta con el equipo de administración de inmediato.</strong><br><br>"
        )

        # Enviamos el correo
        correo_usuario(email, asunto, mensaje)  # Llamada a la función de correo
    else:
        conn.close()
        st.toast(f"Usuario con ID {id} no encontrado.")

# Función para eliminar un usuario
def eliminar_usuario(id):
    conn = obtener_conexion()
    cursor = conn.cursor()
    cursor.execute("SELECT username, email FROM usuarios WHERE id = ?", (id,))
    usuario = cursor.fetchone()

    if usuario:
        nombre_usuario = usuario[0]
        email_usuario = usuario[1]

        cursor.execute("DELETE FROM usuarios WHERE id = ?", (id,))
        conn.commit()
        conn.close()
        log_trazabilidad(st.session_state["username"], "Eliminar Usuario", f"El admin eliminó al usuario con ID {id}.")

        # Enviar correo de baja al usuario
        asunto = "❌ Tu cuenta ha sido desactivada"
        mensaje = (
            f"📢 Tu cuenta ha sido desactivada y eliminada de nuestro sistema. <br><br>"
            f"ℹ️ Si consideras que esto ha sido un error o necesitas más detalles, por favor, contacta con el equipo de administración.<br><br>"
            f"🔒 <strong>Por seguridad, no compartas este correo con nadie. Si no reconoces esta acción, contacta con el equipo de administración de inmediato.</strong><br><br>"
        )

        correo_usuario(email_usuario, asunto, mensaje)  # Llamada a la función de correo
    else:
        st.toast("Usuario no encontrado.")

def cargar_datos_uis():
    """Carga y cachea los datos de las tablas 'datos_uis', 'comercial_rafa'."""
    conn = obtener_conexion()

    # Consulta de datos_uis
    query_datos_uis = """
        SELECT apartment_id, latitud, longitud, provincia, municipio, poblacion, tipo_olt_rental, serviciable,
               vial, numero, letra, cp, cto_id, cto, site_operational_state, apartment_operational_state, zona
        FROM datos_uis
    """
    datos_uis = pd.read_sql(query_datos_uis, conn)

    # Consulta de comercial_rafa
    query_rafa = """
        SELECT apartment_id, serviciable, Contrato, provincia, municipio, poblacion,
               motivo_serviciable, incidencia, motivo_incidencia, nombre_cliente,
               telefono, direccion_alternativa, observaciones, comercial, comentarios
        FROM comercial_rafa
    """
    comercial_rafa_df = pd.read_sql(query_rafa, conn)

    conn.close()
    #return datos_uis, ofertas_df, comercial_rafa_df
    return datos_uis, comercial_rafa_df

#def limpiar_mapa():
#    """Evita errores de re-inicialización del mapa"""
#    st.write("### Mapa actualizado")  # Esto forzará un refresh

#def cargar_provincias():
#    conn = obtener_conexion()
#    query = "SELECT DISTINCT provincia FROM datos_uis"
#    df = pd.read_sql(query, conn)
#    conn.close()
#    return sorted(df['provincia'].dropna().unique())


def cargar_datos_por_provincia(provincia):
    conn = obtener_conexion()

    query_datos_uis = """
        SELECT * 
        FROM datos_uis
        WHERE provincia = ?
    """
    datos_uis = pd.read_sql(query_datos_uis, conn, params=(provincia,))

    query_comercial_rafa = """
        SELECT * 
        FROM comercial_rafa
        WHERE provincia = ?
    """
    comercial_rafa_df = pd.read_sql(query_comercial_rafa, conn, params=(provincia,))

    conn.close()
    return datos_uis, comercial_rafa_df

# ============================================
# FUNCIONES DE CARGUE OPTIMIZADAS
# ============================================

@st.cache_data(ttl=600, max_entries=20)
def cargar_provincias() -> List[str]:
    """Carga la lista de provincias disponibles (cache por 10 minutos)"""
    conn = obtener_conexion()
    try:
        query = "SELECT DISTINCT provincia FROM datos_uis WHERE provincia IS NOT NULL ORDER BY provincia"
        df = pd.read_sql(query, conn)
        return df['provincia'].tolist()
    finally:
        conn.close()


@st.cache_data(ttl=300, max_entries=50)
def cargar_datos_por_provincia(provincia: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Carga datos de una provincia específica con columnas esenciales"""
    conn = obtener_conexion()
    try:
        # Solo columnas necesarias para el mapa
        query_uis = f"""
            SELECT apartment_id, latitud, longitud, provincia, municipio, 
                   poblacion, vial, numero
            FROM datos_uis 
            WHERE provincia = ? 
            AND latitud IS NOT NULL 
            AND longitud IS NOT NULL
            AND latitud != 0 
            AND longitud != 0
            LIMIT 1000  -- Limitar para carga rápida
        """

        query_comercial = f"""
            SELECT apartment_id, comercial, serviciable, incidencia, contrato
            FROM comercial_rafa c
            WHERE EXISTS (
                SELECT 1 FROM datos_uis d 
                WHERE d.apartment_id = c.apartment_id 
                AND d.provincia = ?
            )
        """

        datos_uis = pd.read_sql(query_uis, conn, params=(provincia,))
        comercial_rafa = pd.read_sql(query_comercial, conn, params=(provincia,))

        # Optimizar tipos de datos
        if not datos_uis.empty and 'latitud' in datos_uis.columns and 'longitud' in datos_uis.columns:
            datos_uis[['latitud', 'longitud']] = datos_uis[['latitud', 'longitud']].astype(float)

        return datos_uis, comercial_rafa
    finally:
        conn.close()


@st.cache_data(ttl=300, max_entries=10)
def cargar_datos_limitados() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Carga datos limitados para vista inicial rápida"""
    conn = obtener_conexion()
    try:
        # Solo primeros 500 registros para carga rápida
        query_uis = """
            SELECT apartment_id, latitud, longitud, provincia, municipio, 
                   poblacion, vial, numero
            FROM datos_uis 
            WHERE latitud IS NOT NULL 
            AND longitud IS NOT NULL
            AND latitud != 0 
            AND longitud != 0
            LIMIT 500
        """

        query_comercial = """
            SELECT apartment_id, comercial, serviciable, incidencia, contrato
            FROM comercial_rafa
            LIMIT 1000
        """

        datos_uis = pd.read_sql(query_uis, conn)
        comercial_rafa = pd.read_sql(query_comercial, conn)

        if not datos_uis.empty and 'latitud' in datos_uis.columns and 'longitud' in datos_uis.columns:
            datos_uis[['latitud', 'longitud']] = datos_uis[['latitud', 'longitud']].astype(float)

        return datos_uis, comercial_rafa
    finally:
        conn.close()


@st.cache_data(ttl=300)
def buscar_por_id(apartment_id: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Búsqueda optimizada por ID de apartment"""
    conn = obtener_conexion()
    try:
        query_uis = f"""
            SELECT apartment_id, latitud, longitud, provincia, municipio, 
                   poblacion, vial, numero
            FROM datos_uis 
            WHERE apartment_id = ? 
            AND latitud IS NOT NULL 
            AND longitud IS NOT NULL
        """

        query_comercial = f"""
            SELECT apartment_id, comercial, serviciable, incidencia, contrato
            FROM comercial_rafa
            WHERE apartment_id = ?
        """

        datos_uis = pd.read_sql(query_uis, conn, params=(apartment_id,))
        comercial_rafa = pd.read_sql(query_comercial, conn, params=(apartment_id,))

        if not datos_uis.empty and 'latitud' in datos_uis.columns and 'longitud' in datos_uis.columns:
            datos_uis[['latitud', 'longitud']] = datos_uis[['latitud', 'longitud']].astype(float)

        return datos_uis, comercial_rafa
    finally:
        conn.close()


# ============================================
# FUNCIONES AUXILIARES OPTIMIZADAS
# ============================================
def crear_diccionarios_optimizados(comercial_df: pd.DataFrame) -> Dict:
    """Crea diccionarios optimizados para búsqueda rápida"""
    dicts = {
        'serviciable': {},
        'contrato': {},
        'incidencia': {},
        'comercial': {}
    }

    if comercial_df.empty:
        return dicts

    # Crear diccionarios solo para columnas que existen
    for columna in dicts.keys():
        if columna in comercial_df.columns:
            # Usar vectorización para mejor rendimiento
            mask = comercial_df[columna].notna()
            if mask.any():
                subset = comercial_df[mask]
                dicts[columna] = pd.Series(
                    subset[columna].astype(str).str.strip().str.lower().values,
                    index=subset['apartment_id']
                ).to_dict()

    return dicts


def determinar_color_marcador(apt_id: str, serv_uis: str, dicts: Dict) -> Tuple[str, str]:
    """Determina el color y categoría del marcador (función vectorizable)"""

    # Valores del diccionario
    incidencia = dicts['incidencia'].get(apt_id, '')
    serv_oferta = dicts['serviciable'].get(apt_id, '')
    contrato = dicts['contrato'].get(apt_id, '')

    # Lógica de decisión optimizada
    if incidencia == 'sí':
        return 'purple', 'incidencia'
    elif serv_oferta == 'no':
        return 'red', 'no_serviciable'
    elif serv_uis == 'sí':
        return 'green', 'serviciable'
    elif contrato == 'sí' and serv_uis != 'sí':
        return 'orange', 'contratado'
    elif contrato == 'no interesado' and serv_uis != 'sí':
        return 'gray', 'no_interesado'
    else:
        return 'blue', 'no_visitado'


# ============================================
# FUNCIÓN PRINCIPAL CON FILTROS EN ZONA PRINCIPAL
# ============================================
def agregar_leyenda_al_mapa(mapa):
    """Añade una leyenda como control HTML al mapa"""

    leyenda_html = '''
    <div style="position: fixed; 
                bottom: 50px; left: 50px; 
                background-color: white; 
                border: 2px solid grey; 
                z-index: 9999; 
                padding: 10px;
                border-radius: 5px;
                font-family: Arial;
                font-size: 12px;
                box-shadow: 0 0 10px rgba(0,0,0,0.2);">
        <h4 style="margin: 0 0 10px 0;">Leyenda</h4>
        <div style="display: flex; align-items: center; margin-bottom: 5px;">
            <div style="width: 15px; height: 15px; background-color: green; 
                        margin-right: 8px; border-radius: 50%;"></div>
            <span>Serviciable</span>
        </div>
        <div style="display: flex; align-items: center; margin-bottom: 5px;">
            <div style="width: 15px; height: 15px; background-color: red; 
                        margin-right: 8px; border-radius: 50%;"></div>
            <span>No serviciable</span>
        </div>
        <div style="display: flex; align-items: center; margin-bottom: 5px;">
            <div style="width: 15px; height: 15px; background-color: blue; 
                        margin-right: 8px; border-radius: 50%;"></div>
            <span>Contratado</span>
        </div>
        <div style="display: flex; align-items: center; margin-bottom: 5px;">
            <div style="width: 15px; height: 15px; background-color: orange; 
                        margin-right: 8px; border-radius: 50%;"></div>
            <span>Incidencia</span>
        </div>
        <div style="display: flex; align-items: center; margin-bottom: 5px;">
            <div style="width: 15px; height: 15px; background-color: gray; 
                        margin-right: 8px; border-radius: 50%;"></div>
            <span>No interesado</span>
        </div>
        <div style="display: flex; align-items: center;">
            <div style="width: 15px; height: 15px; background-color: black; 
                        margin-right: 8px; border-radius: 50%;"></div>
            <span>No visitado</span>
        </div>
    </div>
    '''

    mapa.get_root().html.add_child(folium.Element(leyenda_html))


def determinar_color_marcador(apartment_id: str, serv_uis: str, dicts: Dict) -> Tuple[str, str]:

    # Primero verificar si existe en datos comerciales
    if apartment_id in dicts.get('serviciable', {}):
        serv_comercial = dicts['serviciable'][apartment_id]

        # Verificar si hay incidencia
        if apartment_id in dicts.get('incidencia', {}) and dicts['incidencia'][apartment_id] == 'sí':
            return 'orange', 'incidencia'

        # Verificar si está contratado
        if apartment_id in dicts.get('contrato', {}) and dicts['contrato'][apartment_id] == 'sí':
            return 'blue', 'contratado'

        # Verificar estado serviciable
        if serv_comercial == 'sí':
            return 'green', 'serviciable'
        elif serv_comercial == 'no':
            return 'red', 'no_serviciable'
        elif serv_comercial == 'no interesado':
            return 'gray', 'no_interesado'

    # Si no hay datos comerciales, usar datos UIS
    if serv_uis and isinstance(serv_uis, str):
        serv_uis_lower = serv_uis.lower()
        if 'serviciable' in serv_uis_lower or 'sí' in serv_uis_lower:
            return 'green', 'serviciable'
        elif 'no serviciable' in serv_uis_lower or 'no' in serv_uis_lower:
            return 'red', 'no_serviciable'

    # Por defecto
    return 'black', 'no_visitado'


def mostrar_info_detallada(apartment_id: str, datos_filtrados: pd.DataFrame,
                           comercial_filtradas: pd.DataFrame, dicts: Dict):

    # Quitar el prefijo "🏠 " si existe
    apartment_id = apartment_id.replace("🏠 ", "")

    st.subheader(f"🏠 **Información del Apartment ID: {apartment_id}**")

    # Buscar datos en ambos dataframes
    datos_apt = datos_filtrados[datos_filtrados['apartment_id'] == apartment_id]
    comercial_apt = comercial_filtradas[comercial_filtradas['apartment_id'] == apartment_id]

    if datos_apt.empty:
        st.warning("No se encontraron datos para este apartamento")
        return

    datos_apt = datos_apt.iloc[0]

    # Crear columnas para la visualización
    col1, col2 = st.columns(2)

    # Columna 1: Datos generales
    with col1:
        st.markdown("##### 🔹 **Datos Generales**")

        datos_generales = {
            "ID Apartamento": datos_apt.get('apartment_id', 'N/A'),
            "Provincia": datos_apt.get('provincia', 'N/A'),
            "Municipio": datos_apt.get('municipio', 'N/A'),
            "Población": datos_apt.get('poblacion', 'N/A'),
            "Dirección": f"{datos_apt.get('vial', '')} {datos_apt.get('numero', '')} {datos_apt.get('letra', '')}",
            "Código Postal": datos_apt.get('cp', 'N/A'),
            "CTO ID": datos_apt.get('cto_id', 'N/A'),
            "Zona": datos_apt.get('zona', 'N/A')
        }

        for key, value in datos_generales.items():
            st.text(f"{key}: {value}")

    # Columna 2: Datos comerciales y estado
    with col2:
        st.markdown("##### 🔹 **Estado y Comercial**")

        # Determinar estado actual
        serv_uis = str(datos_apt.get('serviciable', '')).lower().strip()
        _, estado = determinar_color_marcador(apartment_id, serv_uis, dicts)

        st.metric("Estado", estado.replace('_', ' ').title())

        if apartment_id in dicts.get('comercial', {}):
            st.metric("Comercial", dicts['comercial'][apartment_id])

        if apartment_id in dicts.get('serviciable', {}):
            st.metric("Serviciable", dicts['serviciable'][apartment_id].title())

    # Sección de comentarios si hay datos comerciales
    if not comercial_apt.empty:
        st.markdown("##### 📝 **Información Comercial**")

        # Mostrar datos comerciales
        comercial_data = comercial_apt.iloc[0]
        cols_com = st.columns(2)

        with cols_com[0]:
            if 'motivo_serviciable' in comercial_data:
                st.text(f"Motivo: {comercial_data['motivo_serviciable']}")
            if 'nombre_cliente' in comercial_data:
                st.text(f"Cliente: {comercial_data['nombre_cliente']}")

        with cols_com[1]:
            if 'telefono' in comercial_data:
                st.text(f"Teléfono: {comercial_data['telefono']}")
            if 'observaciones' in comercial_data:
                st.text(f"Observaciones: {comercial_data['observaciones']}")

        # Campo para comentarios
        st.markdown("##### 💬 **Comentarios**")

        # Obtener comentario actual
        comentario_actual = comercial_data.get('comentarios', '')
        if pd.isna(comentario_actual):
            comentario_actual = ""

        nuevo_comentario = st.text_area(
            "Añadir o editar comentario:",
            value=comentario_actual,
            height=100,
            key=f"comentario_{apartment_id}"
        )

        if st.button("💾 Guardar Comentario", key=f"guardar_{apartment_id}"):
            try:
                # Actualizar el comentario en los datos
                if 'guardar_comentario' in globals():
                    resultado = guardar_comentario(apartment_id, nuevo_comentario, "comercial_rafa")
                    if resultado:
                        st.toast("✅ Comentario guardado exitosamente")
                        st.rerun()
                else:
                    st.info("⚠️ La función 'guardar_comentario' no está disponible")
            except Exception as e:
                st.toast(f"❌ Error al guardar: {str(e)}")


def mapa_seccion():
    """Muestra un mapa interactivo con filtros en zona principal"""

    # Fila 1: Búsqueda por ID y Provincia
    col1, col2, col3 = st.columns([2, 2, 1])

    with col1:
        apartment_search = st.text_input(
            "Buscar por Apartment ID",
            placeholder="Ej: APT123456",
            help="Busca un apartment específico por su ID",
            key="search_id_input"
        )

    with col2:
        # Cargar provincias
        with st.spinner("Cargando..."):
            provincias = cargar_provincias()

        provincia_sel = st.selectbox(
            "Provincia",
            ["Selecciona provincia"] + provincias,
            key="select_provincia_input"
        )

    with col3:
        modo_busqueda = st.radio(
            "Modo",
            ["Exacta", "Parcial"],
            horizontal=True,
            index=0,
            key="modo_busqueda_input",
            label_visibility="collapsed"
        )

    # Fila 2: Filtros avanzados en expander
    with st.expander("⚙️ Filtros Avanzados", expanded=False):
        col_a1, col_a2, col_a3, col_a4 = st.columns(4)

        with col_a1:
            estado_filtro = st.multiselect(
                "Filtrar por estado",
                ["Serviciable", "No serviciable", "Contratado", "Incidencia", "No interesado", "No visitado"],
                default=["Serviciable", "No serviciable", "Contratado", "Incidencia", "No interesado", "No visitado"],
                key="estado_filtro_input"
            )

        with col_a2:
            mostrar_clusters = st.checkbox("Mostrar clusters", value=True, key="mostrar_clusters_input")

        with col_a3:
            mostrar_leyenda = st.checkbox("Mostrar leyenda en mapa", value=True, key="mostrar_leyenda_input")

        with col_a4:
            zoom_inicial = st.slider("Zoom inicial", 8, 18, 12, key="zoom_inicial_input")

    # ===== LÓGICA DE CARGA DE DATOS =====

    # Inicializar variables
    datos_filtrados = pd.DataFrame()
    comercial_filtradas = pd.DataFrame()
    dicts = {}

    # Opción 1: Búsqueda por ID
    if apartment_search:
        with st.spinner("🔍 Buscando apartment..."):
            if modo_busqueda == "Exacta":
                datos_uis, comercial_rafa_df = buscar_por_id(apartment_search)
                if not datos_uis.empty:
                    datos_filtrados = datos_uis
                    comercial_filtradas = comercial_rafa_df
                    st.toast(f"✅ Encontrado: {apartment_search}")
                else:
                    st.toast(f"❌ No se encontró el Apartment ID: {apartment_search}")
                    return
            else:
                # Búsqueda parcial - cargar datos limitados primero
                datos_uis, comercial_rafa_df = cargar_datos_limitados()
                mask = datos_uis['apartment_id'].astype(str).str.contains(apartment_search, case=False, na=False)
                datos_filtrados = datos_uis[mask]
                comercial_filtradas = comercial_rafa_df[
                    comercial_rafa_df['apartment_id'].isin(datos_filtrados['apartment_id'])]

                if datos_filtrados.empty:
                    st.warning(f"⚠️ No se encontraron coincidencias para: {apartment_search}")
                    # Mostrar vista limitada por defecto
                    datos_filtrados, comercial_filtradas = cargar_datos_limitados()
                else:
                    st.toast(f"✅ Encontradas {len(datos_filtrados)} coincidencias")

    # Opción 2: Filtro por provincia
    elif provincia_sel != "Selecciona provincia":
        with st.spinner(f"⏳ Cargando datos de {provincia_sel}..."):
            datos_uis, comercial_rafa_df = cargar_datos_por_provincia(provincia_sel)

            if datos_uis.empty:
                st.warning(f"⚠️ No hay datos para {provincia_sel}")
                # Cargar vista limitada
                datos_filtrados, comercial_filtradas = cargar_datos_limitados()
            else:
                datos_filtrados = datos_uis
                comercial_filtradas = comercial_rafa_df

                # Filtros adicionales
                col_m1, col_m2 = st.columns(2)

                with col_m1:
                    if 'municipio' in datos_filtrados.columns:
                        municipios = ["Todos"] + sorted(datos_filtrados['municipio'].dropna().unique().tolist())
                        municipio_sel = st.selectbox("Municipio", municipios, key="select_municipio_input")

                        if municipio_sel and municipio_sel != "Todos":
                            datos_filtrados = datos_filtrados[datos_filtrados['municipio'] == municipio_sel]

                with col_m2:
                    if 'poblacion' in datos_filtrados.columns and 'municipio_sel' in locals() and municipio_sel != "Todos":
                        poblaciones = ["Todas"] + sorted(datos_filtrados['poblacion'].dropna().unique().tolist())
                        poblacion_sel = st.selectbox("Población", poblaciones, key="select_poblacion_input")

                        if poblacion_sel and poblacion_sel != "Todas":
                            datos_filtrados = datos_filtrados[datos_filtrados['poblacion'] == poblacion_sel]

    # Opción 3: Vista inicial (sin filtros)
    else:
        st.info("👆 Selecciona una provincia o busca por ID para cargar datos")

        # Cargar datos limitados para vista previa
        with st.spinner("⏳ Cargando vista previa..."):
            datos_filtrados, comercial_filtradas = cargar_datos_limitados()

            if not datos_filtrados.empty:
                st.toast(f"✅ Vista previa cargada: {len(datos_filtrados)} apartments")

    # ===== VERIFICACIÓN Y PROCESAMIENTO DE DATOS =====

    if datos_filtrados.empty:
        st.warning("⚠️ No hay datos para mostrar. Prueba con otros filtros.")
        return

    # Crear diccionarios optimizados
    dicts = crear_diccionarios_optimizados(comercial_filtradas)

    # Aplicar filtros de estado si están activos
    if estado_filtro and len(estado_filtro) < 6:
        estados_permitidos = [estado.lower().replace(" ", "_") for estado in estado_filtro]

        # Calcular estado para cada fila
        estados = []
        for _, row in datos_filtrados.iterrows():
            apt_id = row['apartment_id']
            serv_uis = str(row.get('serviciable', '')).lower().strip() if 'serviciable' in row else ''
            _, estado = determinar_color_marcador(apt_id, serv_uis, dicts)
            estados.append(estado)

        # Filtrar por estado
        mask = [estado in estados_permitidos for estado in estados]
        datos_filtrados = datos_filtrados[mask].copy()

        # Actualizar datos comerciales
        if not datos_filtrados.empty:
            apt_ids = datos_filtrados['apartment_id'].tolist()
            comercial_filtradas = comercial_filtradas[comercial_filtradas['apartment_id'].isin(apt_ids)]
            dicts = crear_diccionarios_optimizados(comercial_filtradas)

    # ===== ESTADÍSTICAS =====
    if not datos_filtrados.empty:
        col_s1, col_s2, col_s3, col_s4 = st.columns(4)

        with col_s1:
            st.metric("Total Apartments", f"{len(datos_filtrados):,}")

        with col_s2:
            if not comercial_filtradas.empty:
                comerciales = comercial_filtradas['comercial'].nunique()
                st.metric("Comerciales", comerciales)

        with col_s3:
            # Contar serviciables
            serviciables = sum(1 for apt_id in datos_filtrados['apartment_id']
                               if dicts.get('serviciable', {}).get(apt_id) == 'sí')
            st.metric("Serviciables", serviciables)

        with col_s4:
            # Contar incidencias
            incidencias = sum(1 for apt_id in datos_filtrados['apartment_id']
                              if dicts.get('incidencia', {}).get(apt_id) == 'sí')
            st.metric("Incidencias", incidencias)

    # ===== CREACIÓN DEL MAPA =====

    if datos_filtrados.empty:
        st.warning("⚠️ No hay datos que cumplan los filtros seleccionados")
        return

    # Calcular centro del mapa
    if len(datos_filtrados) == 1:
        center_lat = float(datos_filtrados.iloc[0]['latitud'])
        center_lon = float(datos_filtrados.iloc[0]['longitud'])
        zoom_start = 16
    elif len(datos_filtrados) <= 10:
        center_lat = float(datos_filtrados['latitud'].mean())
        center_lon = float(datos_filtrados['longitud'].mean())
        zoom_start = 14
    else:
        center_lat = float(datos_filtrados['latitud'].mean())
        center_lon = float(datos_filtrados['longitud'].mean())
        zoom_start = zoom_inicial

    # Crear mapa
    with st.spinner("🗺️ Generando mapa..."):
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=zoom_start,
            max_zoom=21,
            tiles="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}",
            attr="Google Satellite",
            control_scale=True
        )

        # Añadir plugins
        if mostrar_clusters and len(datos_filtrados) > 10:
            cluster_layer = MarkerCluster(
                max_cluster_radius=80,
                min_cluster_size=2,
                disable_clustering_at_zoom=16
            ).add_to(m)
            layer = cluster_layer
        else:
            layer = m

        Geocoder(collapsed=True, position='topright').add_to(m)
        Fullscreen(position='topright').add_to(m)

        # Añadir leyenda al mapa si está activado
        if mostrar_leyenda:
            agregar_leyenda_al_mapa(m)

        # Manejar coordenadas duplicadas
        coord_counts = {}
        for _, row in datos_filtrados.iterrows():
            coord = (round(row['latitud'], 6), round(row['longitud'], 6))
            coord_counts[coord] = coord_counts.get(coord, 0) + 1

        # Añadir marcadores
        markers_added = 0
        for _, row in datos_filtrados.iterrows():
            apt_id = row['apartment_id']
            lat = float(row['latitud'])
            lon = float(row['longitud'])

            # Aplicar offset si hay duplicados
            coord_key = (round(lat, 6), round(lon, 6))
            if coord_counts.get(coord_key, 0) > 1:
                offset = coord_counts[coord_key] * 0.00002
                lat += offset
                lon -= offset
                coord_counts[coord_key] -= 1

            # Determinar color
            serv_uis = str(row.get('serviciable', '')).lower().strip() if 'serviciable' in row else ''
            color, estado = determinar_color_marcador(apt_id, serv_uis, dicts)

            # Crear popup
            popup_html = f"""
            <div style="font-family: Arial; max-width: 250px;">
                <div style="background: #2c3e50; color: white; padding: 8px; border-radius: 5px 5px 0 0;">
                    <strong>🏠 {apt_id}</strong>
                </div>
                <div style="padding: 10px;">
                    <div><strong>📍 Ubicación:</strong></div>
                    <div>{row.get('provincia', '')}</div>
                    <div>{row.get('municipio', '')} - {row.get('poblacion', '')}</div>
                    <div style="margin-top: 5px;">{row.get('vial', '')} {row.get('numero', '')}</div>
                    <div style="color: #666; font-size: 11px; margin-top: 5px;">
                        📍 {lat:.6f}, {lon:.6f}
                    </div>
            """

            # Añadir info comercial si existe
            if apt_id in dicts.get('comercial', {}) or apt_id in dicts.get('serviciable', {}):
                popup_html += '<hr style="margin: 10px 0;"><div><strong>👤 Datos:</strong></div>'

                if apt_id in dicts.get('comercial', {}):
                    popup_html += f"<div>Comercial: {dicts['comercial'][apt_id]}</div>"

                if apt_id in dicts.get('serviciable', {}):
                    serv_value = dicts['serviciable'][apt_id].title()
                    popup_html += f"<div>Serviciable: {serv_value}</div>"

            popup_html += "</div></div>"

            # Crear marcador
            folium.Marker(
                location=[lat, lon],
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"🏠 {apt_id}",
                icon=folium.Icon(color=color, icon="home", prefix="fa")
            ).add_to(layer)

            markers_added += 1

            # Límite de rendimiento
            if markers_added >= 1000:
                st.warning("⚠️ Mostrando primeros 1000 puntos por rendimiento")
                break

        # Renderizar mapa
        map_data = st_folium(
            m,
            height=600,
            width='stretch',
            returned_objects=["last_object_clicked_tooltip", "bounds", "zoom"]
        )

        # Manejar clic en marcador
        if map_data and map_data.get("last_object_clicked_tooltip"):
            mostrar_info_detallada(
                map_data["last_object_clicked_tooltip"],
                datos_filtrados,
                comercial_filtradas,
                dicts
            )

    # ===== ACCIONES RÁPIDAS =====
    col_a1, col_a2, col_a3 = st.columns(3)

    with col_a1:
        if st.button("🔄 Actualizar Vista", width='stretch'):
            st.cache_data.clear()
            st.rerun()

    with col_a2:
        if st.button("📍 Ver Todos", width='stretch', key="ver_todos_btn"):
            # Limpiar caché y recargar para mostrar todos
            st.cache_data.clear()
            st.rerun()

    with col_a3:
        # Exportar datos
        if not datos_filtrados.empty:
            csv = datos_filtrados.to_csv(index=False, sep=';')
            st.download_button(
                label="📥 Exportar CSV",
                data=csv,
                file_name=f"mapa_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                width='stretch',
                key="exportar_csv_btn"
            )


# Funciones de compatibilidad
#def limpiar_mapa():
#    """Función placeholder para mantener compatibilidad"""
#    pass


#def cargar_datos_uis():
#    """Función original para mantener compatibilidad"""
#    return cargar_datos_limitados()


#def mostrar_info_rapida(apartment_id: str, datos_filtrados: pd.DataFrame,
#                        comercial_filtradas: pd.DataFrame, dicts: Dict):
#    """Función original para mantener compatibilidad - usar mostrar_info_detallada en su lugar"""
#    mostrar_info_detallada(apartment_id, datos_filtrados, comercial_filtradas, dicts)


#def mostrar_info_apartamento(apartment_id, datos_df, comercial_rafa_df):
#    """Función original para mantener compatibilidad - usar mostrar_info_detallada en su lugar"""
#    dicts = crear_diccionarios_optimizados(comercial_rafa_df)
#    mostrar_info_detallada(apartment_id, datos_df, comercial_rafa_df, dicts)


def guardar_comentario(apartment_id, comentario, tabla):
    try:
        # Conexión a la base de datos (cambia la ruta o la conexión según corresponda)
        conn = obtener_conexion()
        cursor = conn.cursor()

        # Actualizar el comentario para el registro con el apartment_id dado
        query = f"UPDATE {tabla} SET comentarios = ? WHERE apartment_id = ?"
        cursor.execute(query, (comentario, apartment_id))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.toast(f"Error al actualizar la base de datos: {str(e)}")
        return False


def upload_file_to_minio(file, filename, folder=None, tipo="presupuesto"):
    """
    Sube un archivo genérico (PDF, Excel, etc.) a MinIO en el bucket correspondiente.

    Args:
        file: Objeto UploadedFile de Streamlit o bytes.
        filename: Nombre del archivo (ej. "presupuesto_123.pdf").
        folder: Subcarpeta opcional dentro del bucket (ej. "2025/02" o el ticket).
        tipo: Tipo de archivo para determinar el bucket. Por defecto "presupuesto".

    Returns:
        URL pública del archivo subido.
    """
    from modules.minIO import upload_image_to_cloudinary  # Reutilizamos la función genérica de MinIO
    # La función upload_image_to_cloudinary ya maneja MinIO y acepta el parámetro 'tipo'
    return upload_image_to_cloudinary(file, filename, folder=folder, tipo=tipo)


# Para mantener compatibilidad con las llamadas existentes, podemos renombrar la función
# o simplemente asignar upload_file_to_cloudinary = upload_file_to_minio
upload_file_to_cloudinary = upload_file_to_minio

def viabilidades_seccion():
    # 🟩 Submenú horizontal
    sub_seccion = option_menu(
        menu_title=None,
        options=["Ver Viabilidades", "Crear Viabilidades"],
        icons=["map", "plus-circle"],
        default_index=0,
        orientation="horizontal",
        styles={
            "container": {
                "padding": "0!important",
                "margin": "0px",
                "background-color": "#F0F7F2",
                "border-radius": "0px",
                "max-width": "none"
            },
            "icon": {
                "color": "#2C5A2E",
                "font-size": "25px"
            },
            "nav-link": {
                "color": "#2C5A2E",
                "font-size": "18px",
                "text-align": "center",
                "margin": "0px",
                "--hover-color": "#66B032",
                "border-radius": "0px",
            },
            "nav-link-selected": {
                "background-color": "#66B032",
                "color": "white",
                "font-weight": "bold"
            }
        }
    )
    # 🧩 Sección 1: Ver Viabilidades (tu código actual)
    if sub_seccion == "Ver Viabilidades":
        log_trazabilidad("Administrador", "Visualización de Viabilidades",
                         "El administrador visualizó la sección de viabilidades.")

        # Inicializamos el estado si no existe
        if "map_center" not in st.session_state:
            st.session_state["map_center"] = [43.463444, -3.790476]
        if "map_zoom" not in st.session_state:
            st.session_state["map_zoom"] = 12
        if "selected_ticket" not in st.session_state:
            st.session_state["selected_ticket"] = None

        # Cargar datos
        with st.spinner("⏳ Cargando los datos de viabilidades..."):
            try:
                conn = obtener_conexion()
                tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
                if 'viabilidades' not in tables['name'].values:
                    st.toast("❌ La tabla 'viabilidades' no se encuentra en la base de datos.")
                    conn.close()
                    return

                viabilidades_df = pd.read_sql("SELECT * FROM viabilidades", conn)
                conn.close()

                if viabilidades_df.empty:
                    st.warning("⚠️ No hay viabilidades disponibles.")
                    return

            except Exception as e:
                st.toast(f"❌ Error al cargar los datos de la base de datos: {e}")
                return

        # Verificamos columnas necesarias
        for col in ['latitud', 'longitud', 'ticket']:
            if col not in viabilidades_df.columns:
                st.toast(f"❌ Falta la columna '{col}'.")
                return

        # Agregamos columna de duplicados
        viabilidades_df.loc[:, 'is_duplicate'] = viabilidades_df['apartment_id'].duplicated(keep=False)

        # ✅ CORRECCIÓN 2: Agregar columna que indica si tiene presupuesto asociado
        try:
            conn = obtener_conexion()
            presupuestos_df = pd.read_sql("SELECT DISTINCT ticket FROM presupuestos_viabilidades", conn)
            conn.close()
            # Usar .loc para una asignación segura
            viabilidades_df.loc[:, 'tiene_presupuesto'] = viabilidades_df['ticket'].isin(presupuestos_df['ticket'])
        except Exception as e:
            viabilidades_df.loc[:, 'tiene_presupuesto'] = False

        def highlight_duplicates(val):
            if isinstance(val, str) and val in viabilidades_df[viabilidades_df['is_duplicate']]['apartment_id'].values:
                return 'background-color: yellow'
            return ''

        # Interfaz: columnas para mapa y tabla
        col1, col2 = st.columns([3, 3])

        with col2:
            # Reordenamos para que 'ticket' quede primero
            cols = viabilidades_df.columns.tolist()
            if 'ticket' in cols:
                cols.remove('ticket')
                cols = ['ticket'] + cols
            df_reordered = viabilidades_df[cols].copy()

            # Preparamos la configuración con filtros y anchos
            gb = GridOptionsBuilder.from_dataframe(df_reordered)
            gb.configure_default_column(
                filter=True,
                floatingFilter=True,
                sortable=True,
                resizable=True,
                minWidth=100,
                flex=1
            )

            # Resaltado de duplicados
            dup_ids = viabilidades_df.loc[viabilidades_df['is_duplicate'], 'apartment_id'].copy().unique().tolist()

            gb.configure_column(
                'apartment_id',
                cellStyle={
                    'function': f"""
                        function(params) {{
                            if (params.value && {dup_ids}.includes(params.value)) {{
                                return {{'backgroundColor': 'yellow', 'cursor': 'pointer'}};
                            }}
                            return {{'cursor': 'pointer'}};
                        }}
                    """
                },
                cellRenderer='''function(params) {
                    return `<a href="#" style="color:#00bfff;text-decoration:underline;">${params.value}</a>`;
                }'''
            )

            # Selección de fila única
            gb.configure_selection(selection_mode="single", use_checkbox=False)

            gridOptions = gb.build()

            # Fila en rojo si resultado = NO
            for col_def in gridOptions['columnDefs']:
                if col_def['field'] != 'apartment_id':
                    col_def['cellStyle'] = {
                        'function': """
                            function(params) {
                                if (params.data.resultado && params.data.resultado.toUpperCase() === 'NO') {
                                    return {'backgroundColor': 'red'};
                                }
                            }
                        """
                    }

            grid_response = AgGrid(
                df_reordered,
                gridOptions=gridOptions,
                enable_enterprise_modules=True,
                update_mode=GridUpdateMode.SELECTION_CHANGED,
                data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                fit_columns_on_grid_load=False,
                height=400,
                theme='alpine-dark'
            )

            # ==============================
            # 🔍 Manejo robusto de selección
            # ==============================
            selected_rows = grid_response.get("selected_data", [])
            if isinstance(selected_rows, pd.DataFrame):
                selected_rows = selected_rows.to_dict(orient="records")

            if not isinstance(selected_rows, list):
                selected_rows = grid_response.get("selected_rows", [])

            if isinstance(selected_rows, pd.DataFrame):
                selected_rows = selected_rows.to_dict(orient="records")

            if selected_rows is None:
                selected_rows = []

            if isinstance(selected_rows, list) and len(selected_rows) > 0:
                row = selected_rows[0]
                ticket_key = next((k for k in row.keys() if k.lower().strip() == "ticket"), None)
                clicked_ticket = str(row.get(ticket_key, "")).strip() if ticket_key else ""

                if clicked_ticket and clicked_ticket != st.session_state.get("selected_ticket"):
                    st.session_state["selected_ticket"] = clicked_ticket
                    st.session_state["reload_form"] = True
                    st.rerun()

            # ==============================
            # Mostrar detalles del ticket
            # ==============================
            selected_viabilidad = None
            if st.session_state.get("selected_ticket"):
                ticket_str = str(st.session_state["selected_ticket"]).strip()
                mask = viabilidades_df["ticket"].astype(str).str.strip() == ticket_str
                filtered = viabilidades_df.loc[mask].copy()
                if not filtered.empty:
                    selected_viabilidad = filtered.iloc[0].copy()

            # ==============================
            # Exportar a Excel
            # ==============================
            df_export = viabilidades_df.copy()

            def expand_apartments(df):
                rows = []
                for _, row in df.iterrows():
                    ids = str(row.get("apartment_id", "")).split(",")
                    for apt in ids:
                        new_row = row.copy()
                        new_row["apartment_id"] = apt.strip()
                        rows.append(new_row)
                return pd.DataFrame(rows)

            df_export = expand_apartments(viabilidades_df)

            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df_export.to_excel(writer, index=False, sheet_name="Viabilidades")
            output.seek(0)

            col_b1, _, col_b2 = st.columns([1, 2.3, 1])

            with col_b1:
                if st.button("🔄 Actualizar"):
                    with st.spinner("🔄 Actualizando hoja de Google Sheets..."):
                        actualizar_google_sheet_desde_db(
                            sheet_id="14nC88hQoCdh6B6pTq7Ktu2k8HWOyS2BaTqcUOIhXuZY",
                            sheet_name="viabilidades_verde"
                        )

            with col_b2:
                st.download_button(
                    label="📥 Descargar Excel",
                    data=output,
                    file_name="viabilidades_export.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

        with col1:

            # ==============================
            # Función para dibujar el mapa
            # ==============================
            def draw_map(df, center, zoom, selected_ticket=None):
                m = folium.Map(location=center, zoom_start=zoom,
                               tiles="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}",
                               attr="Google", min_zoom=4, max_zoom=20)
                marker_cluster = MarkerCluster().add_to(m)

                for _, row in df.iterrows():
                    ticket = str(row['ticket']).strip()
                    lat, lon = row['latitud'], row['longitud']

                    popup = f"""
                        <b>🏠 Ticket:</b> {ticket}<br>
                        📍 {lat:.6f}, {lon:.6f}<br>
                        <b>Cliente:</b> {row.get('nombre_cliente', 'N/D')}<br>
                        <b>Serviciable:</b> {row.get('serviciable', 'N/D')}<br>
                    """

                    serviciable = str(row.get('serviciable', '')).strip()
                    apartment_id = str(row.get('apartment_id', '')).strip()
                    tiene_presupuesto = row.get('tiene_presupuesto', False)

                    # Color por estado
                    if tiene_presupuesto:
                        marker_color = 'orange'
                    elif row.get('estado') == "No interesado":
                        marker_color = 'black'
                    elif row.get('estado') == "Incidencia":
                        marker_color = 'purple'
                    elif serviciable == "No":
                        marker_color = 'red'
                    elif serviciable == "Sí" and apartment_id not in ["", "N/D"]:
                        marker_color = 'green'
                    else:
                        marker_color = 'blue'

                    # Si es el ticket seleccionado, resaltamos en dorado
                    if selected_ticket and ticket == str(selected_ticket).strip():
                        folium.Marker(
                            location=[lat, lon],
                            popup=popup + "<b>🎯 Ticket seleccionado</b>",
                            icon=folium.Icon(icon='star')
                        ).add_to(m)
                    else:
                        folium.Marker(
                            location=[lat, lon],
                            popup=popup,
                            tooltip=f"{ticket}",
                            icon=folium.Icon(color=marker_color, icon='info-sign')
                        ).add_to(marker_cluster)

                return m

            # ==============================
            # Determinar centro y zoom
            # ==============================
            if st.session_state.get("selected_ticket"):
                ticket_str = str(st.session_state["selected_ticket"]).strip()
                df_sel = viabilidades_df.loc[viabilidades_df["ticket"].astype(str).str.strip() == ticket_str]
                if not df_sel.empty:
                    center = [df_sel.iloc[0]["latitud"], df_sel.iloc[0]["longitud"]]
                    zoom = 16
                else:
                    center = st.session_state.get("map_center", [40.0, -3.7])
                    zoom = st.session_state.get("map_zoom", 6)
            else:
                center = st.session_state.get("map_center", [40.0, -3.7])
                zoom = st.session_state.get("map_zoom", 6)

            # ==============================
            # Dibujar mapa
            # ==============================
            m_to_show = draw_map(
                viabilidades_df,
                center=center,
                zoom=zoom,
                selected_ticket=st.session_state.get("selected_ticket")
            )

            # ==============================
            # Leyenda
            # ==============================
            legend = """
            {% macro html(this, kwargs) %}
            <div style="
                position: fixed; 
                bottom: 0px; left: 0px; width: 170px; 
                z-index:9999; 
                font-size:14px;
                background-color: white;
                color: black;
                border:2px solid grey;
                border-radius:8px;
                padding: 10px;
                box-shadow: 2px 2px 6px rgba(0,0,0,0.3);
            ">
            <b>Leyenda</b><br>
            <i style="color:green;">●</i> Serviciado<br>
            <i style="color:red;">●</i> No serviciable<br>
            <i style="color:orange;">●</i> Presupuesto Sí<br>
            <i style="color:black;">●</i> No interesado<br>
            <i style="color:purple;">●</i> Incidencia<br>
            <i style="color:blue;">●</i> Sin estudio<br>
            <i style="color:gold;">★</i> Ticket seleccionado<br>
            </div>
            {% endmacro %}
            """
            macro = MacroElement()
            macro._template = Template(legend)
            m_to_show.get_root().add_child(macro)
            Geocoder().add_to(m_to_show)

            # ==============================
            # Mostrar mapa y detectar clic
            # ==============================
            map_output = st_folium(
                m_to_show,
                height=500,
                width=700,
                key="main_map",
                returned_objects=["last_object_clicked"]
            )

            # ==============================
            # Detectar clic del mapa
            # ==============================
            if map_output and map_output.get("last_object_clicked"):
                clicked_lat = map_output["last_object_clicked"]["lat"]
                clicked_lng = map_output["last_object_clicked"]["lng"]

                tolerance = 0.0001  # ~11 m
                match = viabilidades_df[
                    (viabilidades_df["latitud"].between(clicked_lat - tolerance, clicked_lat + tolerance)) &
                    (viabilidades_df["longitud"].between(clicked_lng - tolerance, clicked_lng + tolerance))
                    ]

                if not match.empty:
                    clicked_ticket = str(match.iloc[0]["ticket"]).strip()

                    if clicked_ticket != st.session_state.get("selected_ticket"):
                        st.session_state["selected_ticket"] = clicked_ticket
                        st.session_state["map_center"] = [clicked_lat, clicked_lng]
                        st.session_state["map_zoom"] = 16
                        st.session_state["selection_source"] = "map"

                        st.toast(f"📍 Ticket {clicked_ticket} seleccionado desde el mapa")

                        # Forzar recarga
                        st.rerun()

            # ==============================
            # Si venimos del mapa, limpiamos selección de tabla
            # ==============================
            if st.session_state.get("selection_source") == "map":
                st.session_state["selection_source"] = None
                st.session_state["last_table_selection"] = None

        # Mostrar formulario debajo
        if st.session_state["selected_ticket"]:
            mostrar_formulario(selected_viabilidad)

            if st.session_state.get("selected_ticket"):
                archivo = st.file_uploader(
                    f"📁 Sube el archivo PDF del presupuesto para Ticket {st.session_state['selected_ticket']}",
                    type=["pdf"]
                )

                if archivo:
                    st.toast("✅ Archivo PDF cargado correctamente.")

                    proyecto = st.text_input(
                        "🔖 Proyecto / Nombre del presupuesto",
                        value=f"Ticket {st.session_state['selected_ticket']}"
                    )
                    mensaje = st.text_area(
                        "📝 Mensaje para los destinatarios",
                        value="Adjunto presupuesto en formato PDF para su revisión."
                    )

                    # Define los destinatarios disponibles
                    destinatarios_posibles = {
                        "Rafa Sanz": "rafasanz9@gmail.com",
                        "Juan AsturPhone": "admin@asturphone.com",
                        "Correo para pruebas": "patricia@verdetuoperador.com",
                        "Juan Pablo": "jpterrel@verdetuoperador.com"
                    }

                    seleccionados = st.multiselect("👥 Selecciona destinatarios", list(destinatarios_posibles.keys()))

                    if seleccionados and st.button("🚀 Enviar presupuesto en PDF por correo"):
                        try:
                            nombre_archivo = archivo.name
                            archivo_bytes = archivo.getvalue()  # Leer bytes del PDF

                            # 📂 Subir a la carpeta "PRESUPUESTOS" en Cloudinary

                            # 🔹 Subir PDF a Cloudinary (como tipo raw)
                            st.toast("📤 Subiendo PDF a Cloudinary...")
                            cloudinary_url = upload_file_to_cloudinary(
                                archivo_bytes,  # Pasamos los bytes directamente
                                filename=nombre_archivo,  # Nombre del archivo
                                folder=st.session_state["selected_ticket"],  # Organiza por ticket dentro del bucket
                                tipo="presupuesto"
                            )

                            if not cloudinary_url:
                                st.toast("❌ Error al subir el archivo a Cloudinary. No se puede continuar.")
                                st.stop()

                            # 🔹 Enviar correo a los seleccionados
                            for nombre in seleccionados:
                                correo = destinatarios_posibles[nombre]

                                correo_envio_presupuesto_manual(
                                    destinatario=correo,
                                    proyecto=proyecto,
                                    mensaje_usuario=mensaje,
                                    archivo_bytes=archivo_bytes,
                                    nombre_archivo=nombre_archivo
                                )

                                # 🔹 Registrar el envío en la base de datos con URL
                                try:
                                    conn = obtener_conexion()
                                    cursor = conn.cursor()
                                    cursor.execute("""
                                        INSERT INTO envios_presupuesto_viabilidad 
                                        (ticket, destinatario, proyecto, fecha_envio, archivo_nombre, archivo_url)
                                        VALUES (?, ?, ?, ?, ?, ?)
                                    """, (
                                        st.session_state["selected_ticket"],
                                        correo,
                                        proyecto,
                                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                        nombre_archivo,
                                        cloudinary_url
                                    ))
                                    conn.commit()
                                    conn.close()
                                except Exception as db_error:
                                    st.toast(
                                        f"⚠️ Correo enviado a {correo}, pero no se pudo registrar en la BBDD: {db_error}"
                                    )

                            # 🔹 Marcar en la tabla viabilidades que se ha enviado
                            try:
                                conn = obtener_conexion()
                                cursor = conn.cursor()
                                cursor.execute("""
                                    UPDATE viabilidades
                                    SET presupuesto_enviado = 1
                                    WHERE ticket = ?
                                """, (st.session_state["selected_ticket"],))
                                conn.commit()
                                conn.close()
                                st.toast("🗂️ Se ha registrado en la BBDD que el presupuesto en PDF ha sido enviado.")
                            except Exception as db_error:
                                st.toast(
                                    f"⚠️ El correo fue enviado, pero hubo un error al actualizar la BBDD: {db_error}"
                                )

                            st.toast("✅ Presupuesto en PDF enviado y guardado correctamente en Cloudinary.")
                        except Exception as e:
                            st.toast(f"❌ Error al enviar o guardar el presupuesto PDF: {e}")

        with st.expander("📜 Historial de Envíos de Presupuesto"):
            try:
                conn = obtener_conexion()
                df_historial = pd.read_sql_query("""
                    SELECT fecha_envio, destinatario, proyecto, archivo_nombre
                    FROM envios_presupuesto_viabilidad
                    WHERE ticket = ?
                    ORDER BY datetime(fecha_envio) DESC
                """, conn, params=(st.session_state["selected_ticket"],))
                conn.close()

                if df_historial.empty:
                    st.info("No se han registrado envíos de presupuesto aún.")
                else:
                    df_historial["fecha_envio"] = pd.to_datetime(df_historial["fecha_envio"]).dt.strftime("%d/%m/%Y %H:%M")
                    st.dataframe(df_historial, width='stretch')

            except Exception as e:
                st.toast(f"❌ Error al cargar el historial de envíos: {e}")

        # 🧩 Sección 2: Crear Viabilidades (vacía por ahora)
    elif sub_seccion == "Crear Viabilidades":
        # Inicializar estados de sesión si no existen
        if "viabilidad_marker" not in st.session_state:
            st.session_state.viabilidad_marker = None
        if "map_center" not in st.session_state:
            st.session_state.map_center = (43.463444, -3.790476)  # Ubicación inicial predeterminada
        if "map_zoom" not in st.session_state:
            st.session_state.map_zoom = 12  # Zoom inicial

        # Crear el mapa centrado en la última ubicación guardada
        m = folium.Map(
            location=st.session_state.map_center,
            zoom_start=st.session_state.map_zoom,
            tiles="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}",
            attr="Google"
        )

        viabilidades = obtener_viabilidades()
        for v in viabilidades:
            lat, lon, ticket, serviciable, apartment_id, direccion_id = v

            # Determinar el color del marcador según las condiciones
            if serviciable is not None and str(serviciable).strip() != "":
                serv = str(serviciable).strip()
                apt = str(apartment_id).strip() if apartment_id is not None else ""
                if serv == "No":
                    marker_color = "red"
                elif serv == "Sí" and apt not in ["", "N/D"]:
                    marker_color = "green"
                else:
                    marker_color = "black"
            else:
                marker_color = "black"

            folium.Marker(
                [lat, lon],
                icon=folium.Icon(color=marker_color),
                popup=f"Ticket: {ticket}"
            ).add_to(m)

        # Si hay un marcador nuevo, agregarlo al mapa en azul
        if st.session_state.viabilidad_marker:
            lat = st.session_state.viabilidad_marker["lat"]
            lon = st.session_state.viabilidad_marker["lon"]
            folium.Marker(
                [lat, lon],
                icon=folium.Icon(color="blue")
            ).add_to(m)

        # 🔹 Añadir la leyenda flotante
        # Crear un figure para que FloatImage funcione bien
        legend = """
        {% macro html(this, kwargs) %}
        <div style="
            position: fixed; 
            bottom: 0px; left: 0px; width: 150px; 
            z-index:9999; 
            font-size:14px;
            background-color: white;
            color: black;
            border:2px solid grey;
            border-radius:8px;
            padding: 10px;
            box-shadow: 2px 2px 6px rgba(0,0,0,0.3);
        ">
        <b>Leyenda</b><br>
        <i style="color:green;">●</i> Serviciado<br>
        <i style="color:red;">●</i> No serviciable<br>
        <i style="color:orange;">●</i> Presupuesto Sí<br>
        <i style="color:black;">●</i> No interesado<br>
        <i style="color:purple;">●</i> Incidencia<br>
        <i style="color:blue;">●</i> Sin estudio<br>
        </div>
        {% endmacro %}
        """
        macro = MacroElement()
        macro._template = Template(legend)
        m.get_root().add_child(macro)

        # Mostrar el mapa y capturar clics
        Geocoder().add_to(m)
        map_data = st_folium(m, height=680, width="100%")

        # Detectar el clic para agregar el marcador nuevo
        if map_data and "last_clicked" in map_data and map_data["last_clicked"]:
            click = map_data["last_clicked"]
            st.session_state.viabilidad_marker = {"lat": click["lat"], "lon": click["lng"]}
            st.session_state.map_center = (click["lat"], click["lng"])  # Guardar la nueva vista
            st.session_state.map_zoom = map_data["zoom"]  # Actualizar el zoom también
            st.rerun()  # Actualizamos cuando se coloca un marcador

        # Botón para eliminar el marcador y crear uno nuevo
        if st.session_state.viabilidad_marker:
            if st.button("Eliminar marcador y crear uno nuevo"):
                st.session_state.viabilidad_marker = None
                st.session_state.map_center = (43.463444, -3.790476)  # Vuelve a la ubicación inicial
                st.rerun()

        # Mostrar el formulario si hay un marcador nuevo
        if st.session_state.viabilidad_marker:
            lat = st.session_state.viabilidad_marker["lat"]
            lon = st.session_state.viabilidad_marker["lon"]

            st.subheader("Completa los datos del punto de viabilidad")
            with st.form("viabilidad_form"):
                col1, col2 = st.columns(2)
                with col1:
                    st.text_input("📍 Latitud", value=str(lat), disabled=True)
                with col2:
                    st.text_input("📍 Longitud", value=str(lon), disabled=True)

                col3, col4, col5 = st.columns(3)
                with col3:
                    provincia = st.text_input("🏞️ Provincia")
                with col4:
                    municipio = st.text_input("🏘️ Municipio")
                with col5:
                    poblacion = st.text_input("👥 Población")

                col6, col7, col8, col9 = st.columns([3, 1, 1, 2])
                with col6:
                    vial = st.text_input("🛣️ Vial")
                with col7:
                    numero = st.text_input("🔢 Número")
                with col8:
                    letra = st.text_input("🔤 Letra")
                with col9:
                    cp = st.text_input("📮 Código Postal")

                col10, col11 = st.columns(2)
                with col10:
                    nombre_cliente = st.text_input("👤 Nombre Cliente")
                with col11:
                    telefono = st.text_input("📞 Teléfono")
                col12, col13 = st.columns(2)
                # Conexión para cargar los OLT desde la tabla
                conn = obtener_conexion()
                cursor = conn.cursor()
                cursor.execute("SELECT id_olt, nombre_olt FROM olt ORDER BY nombre_olt")
                olts = cursor.fetchall()
                conn.close()

                # Diccionario con clave 'id. nombre' y valor (id, nombre)
                opciones_olt = {f"{fila[0]}. {fila[1]}": fila for fila in olts}

                with col12:
                    opcion_olt = st.selectbox("🏢 OLT", options=list(opciones_olt.keys()))
                    id_olt, nombre_olt = opciones_olt[opcion_olt]
                with col13:
                    apartment_id = st.text_input("🏘️ Apartment ID")

                # 🔹 NUEVOS CAMPOS OPCIONALES
                col14, col15 = st.columns(2)
                with col14:
                    fecha_entrega = st.text_input(
                        "📅 Fecha de entrega (opcional)",
                        placeholder="DD/MM/AAAA",
                        help="Fecha estimada de entrega del proyecto (opcional)"
                    )
                with col15:
                    estado_obra = st.text_input(
                        "🏗️ Estado de la obra (opcional)",
                        placeholder="Ej: En progreso, Finalizada, Pendiente...",
                        help="Estado actual de la obra (opcional)"
                    )
                comentario = st.text_area("📝 Comentario")

                # ✅ Campo para seleccionar el comercial
                conn = obtener_conexion()
                cursor = conn.cursor()
                cursor.execute("SELECT username FROM usuarios ORDER BY username")
                lista_usuarios = [fila[0] for fila in cursor.fetchall()]
                conn.close()

                # Lista de usuarios a excluir
                excluir = ["roberto", "nestor", "rafaela"]

                # Filtrar la lista
                usuarios_filtrados = [u for u in lista_usuarios if u.lower() not in excluir]

                # Agregar opción vacía al inicio y usar index=0 para selección por defecto
                usuarios_con_opcion_vacia = [""] + usuarios_filtrados
                comercial = st.selectbox("🧑‍💼 Comercial responsable *",
                                         options=usuarios_con_opcion_vacia,
                                         placeholder="Selecciona un comercial...",
                                         index=None,
                                         help="Selecciona un comercial responsable. Este campo es obligatorio.")

                submit = st.form_submit_button("Enviar Formulario")

                if submit:
                    # Validar que se haya seleccionado un comercial
                    if not comercial or comercial == "":
                        st.toast("❌ Por favor, selecciona un comercial responsable. Este campo es obligatorio.")
                        st.stop()  # Detiene la ejecución para evitar guardar datos incompletos

                    # Generar ticket único
                    ticket = generar_ticket()

                    guardar_viabilidad((
                        lat,
                        lon,
                        provincia,
                        municipio,
                        poblacion,
                        vial,
                        numero,
                        letra,
                        cp,
                        comentario,
                        ticket,
                        nombre_cliente,
                        telefono,
                        comercial,
                        f"{id_olt}. {nombre_olt}",  # nuevo campo
                        apartment_id,  # nuevo campo
                        fecha_entrega,  # 🔹 NUEVO: Fecha de entrega (opcional)
                        estado_obra  # 🔹 NUEVO: Estado de la obra (opcional)
                    ))

                    st.toast(f"✅ Viabilidad guardada correctamente.\n\n📌 **Ticket:** `{ticket}`")

                    # Resetear marcador para permitir nuevas viabilidades
                    st.session_state.viabilidad_marker = None
                    st.session_state.map_center = (43.463444, -3.790476)  # Vuelve a la ubicación inicial
                    st.rerun()

# Función para obtener conexión a la base de datos (SQLite Cloud)
#def obtener_conexion():
#    return sqlitecloud.connect(
#        "sqlitecloud://ceafu04onz.g6.sqlite.cloud:8860/usuarios.db?apikey=Qo9m18B9ONpfEGYngUKm99QB5bgzUTGtK7iAcThmwvY"
#    )

#def obtener_conexion():
#    """Retorna una nueva conexión a la base de datos SQLite local."""
#    try:
#        # Ruta del archivo dentro del contenedor (puedes cambiarla)
#        db_path = "/data/usuarios.db"  # o usa variable de entorno
#        # Verifica si el archivo existe
#        if not os.path.exists(db_path):
#            raise FileNotFoundError(f"No se encuentra la base de datos en {db_path}")
#        conn = sqlite3.connect(db_path)
#        return conn
#    except (sqlite3.Error, FileNotFoundError) as e:
#        print(f"Error al conectar con la base de datos: {e}")
#        return None

def generar_ticket():
    """Genera un ticket único con formato: añomesdia(numero_consecutivo)"""
    conn = obtener_conexion()
    cursor = conn.cursor()
    fecha_actual = datetime.now().strftime("%Y%m%d")

    # Buscar el mayor número consecutivo para la fecha actual
    cursor.execute("SELECT MAX(CAST(SUBSTR(ticket, 9, 3) AS INTEGER)) FROM viabilidades WHERE ticket LIKE ?",
                   (f"{fecha_actual}%",))
    max_consecutivo = cursor.fetchone()[0]

    # Si no hay tickets previos, empezar desde 1
    if max_consecutivo is None:
        max_consecutivo = 0

    # Generar el nuevo ticket con el siguiente consecutivo
    ticket = f"{fecha_actual}{max_consecutivo + 1:03d}"
    conn.close()
    return ticket

def guardar_viabilidad(datos):
    """
    Inserta los datos en la tabla Viabilidades.
    Se espera que 'datos' sea una tupla con el siguiente orden:
    (latitud, longitud, provincia, municipio, poblacion, vial, numero, letra, cp, comentario, ticket, nombre_cliente, telefono, usuario)
    """
    # Guardar los datos en la base de datos
    conn = obtener_conexion()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO viabilidades (
            latitud, 
            longitud, 
            provincia, 
            municipio, 
            poblacion, 
            vial, 
            numero, 
            letra, 
            cp, 
            comentario, 
            fecha_viabilidad, 
            ticket, 
            nombre_cliente, 
            telefono, 
            usuario,
            olt,
            apartment_id,
            fecha_entrega,  -- 🔹 NUEVO CAMPO
            estado_obra     -- 🔹 NUEVO CAMPO
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)
    """, datos)
    conn.commit()

    # Obtener los emails de todos los administradores
    cursor.execute("SELECT email FROM usuarios WHERE role = 'admin'")
    resultados = cursor.fetchall()
    emails_admin = [fila[0] for fila in resultados]

    # Obtener email del comercial seleccionado
    comercial_email = None
    cursor.execute("SELECT email FROM usuarios WHERE username = ?", (datos[13],))
    fila = cursor.fetchone()
    if fila:
        comercial_email = fila[0]

    conn.close()

    # Información de la viabilidad
    ticket_id = datos[10]  # 'ticket'
    nombre_comercial = datos[13]  # 👈 el comercial elegido en el formulario
    descripcion_viabilidad = (
        f"📝 Viabilidad para el ticket {ticket_id}:<br><br>"
        f"🧑‍💼 Comercial: {nombre_comercial}<br><br>"
        f"📍 Latitud: {datos[0]}<br>"
        f"📍 Longitud: {datos[1]}<br>"
        f"🏞️ Provincia: {datos[2]}<br>"
        f"🏙️ Municipio: {datos[3]}<br>"
        f"🏘️ Población: {datos[4]}<br>"
        f"🛣️ Vial: {datos[5]}<br>"
        f"🔢 Número: {datos[6]}<br>"
        f"🔤 Letra: {datos[7]}<br>"
        f"🏷️ Código Postal (CP): {datos[8]}<br>"
        f"💬 Comentario: {datos[9]}<br>"
        f"👥 Nombre Cliente: {datos[11]}<br>"
        f"📞 Teléfono: {datos[12]}<br><br>"
        f"🏢 OLT: {datos[14]}<br>"
        f"🏘️ Apartment ID: {datos[15]}<br><br>"
    )
    # 🔹 Agregar los nuevos campos si tienen valor
    if datos[16]:  # fecha_entrega
        descripcion_viabilidad += f"📅 Fecha de entrega: {datos[16]}<br>"

    if datos[17]:  # estado_obra
        descripcion_viabilidad += f"🏗️ Estado de la obra: {datos[17]}<br>"

    descripcion_viabilidad += (
        f"<br>"
        f"ℹ️ Por favor, revise todos los detalles de la viabilidad para asegurar que toda la información esté correcta. "
        f"Si tiene alguna pregunta o necesita más detalles, no dude en ponerse en contacto con el comercial {nombre_comercial} o con el equipo responsable."
    )

    # Enviar la notificación por correo a cada administrador
    if emails_admin:
        for email in emails_admin:
            correo_viabilidad_comercial(email, ticket_id, descripcion_viabilidad)
        st.toast(
            f"📧 Se ha enviado una notificación a los administradores: {', '.join(emails_admin)} sobre la viabilidad completada."
        )
    else:
        st.toast("⚠️ No se encontró ningún email de administrador, no se pudo enviar la notificación.")

    # Enviar notificación al comercial seleccionado
    if comercial_email:
        correo_viabilidad_comercial(comercial_email, ticket_id, descripcion_viabilidad)
        st.toast(
            f"📧 Se ha enviado una notificación al comercial responsable: {nombre_comercial} ({comercial_email})")
    else:
        st.toast(f"⚠️ No se pudo encontrar el email del comercial {nombre_comercial}.")

    # Mostrar mensaje de éxito en Streamlit
    st.toast("✅ Los cambios para la viabilidad han sido guardados correctamente")

# Función para obtener viabilidades guardadas en la base de datos
def obtener_viabilidades():
    conn = obtener_conexion()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT latitud, longitud, ticket, serviciable, apartment_id, direccion_id 
        FROM viabilidades
    """)
    viabilidades = cursor.fetchall()
    conn.close()
    return viabilidades


def mostrar_formulario(click_data):
    """Muestra el formulario para editar los datos de la viabilidad y guarda los cambios en la base de datos."""

    # DEBUG: Verificar qué datos estamos recibiendo
    st.sidebar.write("🔍 DATOS RECIBIDOS:")
    st.sidebar.write(f"Ticket: {click_data.get('ticket', 'NO ENCONTRADO')}")
    st.sidebar.write(f"Municipio: {click_data.get('municipio', 'NO ENCONTRADO')}")
    st.sidebar.write(f"OLT: {click_data.get('olt', 'NO ENCONTRADO')}")

    # Obtener valores de la tabla OLT
    conn = obtener_conexion()
    cursor = conn.cursor()
    cursor.execute("SELECT id_olt, nombre_olt FROM olt ORDER BY id_olt ASC")
    olts = cursor.fetchall()
    conn.close()

    # Preparar opciones del selectbox: se mostrará "id_olt - nombre_olt"
    opciones_olt = [f"{olt[0]} - {olt[1]}" for olt in olts]

    # Extraer los datos del registro seleccionado
    ticket = click_data["ticket"]

    # Inicializar session_state para este ticket si no existe
    if f"form_data_{ticket}" not in st.session_state:
        st.session_state[f"form_data_{ticket}"] = {
            "latitud": click_data.get("latitud", ""),
            "longitud": click_data.get("longitud", ""),
            "provincia": click_data.get("provincia", ""),
            "municipio": click_data.get("municipio", ""),
            "poblacion": click_data.get("poblacion", ""),
            "vial": click_data.get("vial", ""),
            "numero": click_data.get("numero", ""),
            "letra": click_data.get("letra", ""),
            "cp": click_data.get("cp", ""),
            "comentario": click_data.get("comentario", ""),
            "cto_cercana": click_data.get("cto_cercana", ""),
            "olt": click_data.get("olt", ""),
            "cto_admin": click_data.get("cto_admin", ""),
            "id_cto": click_data.get("id_cto", ""),
            "municipio_admin": click_data.get("municipio_admin", ""),
            "serviciable": click_data.get("serviciable", "Sí"),
            "coste": float(click_data.get("coste", 0.0)),
            "comentarios_comercial": click_data.get("comentarios_comercial", ""),
            "comentarios_internos": click_data.get("comentarios_internos", ""),
            "fecha_viabilidad": click_data.get("fecha_viabilidad", ""),
            "apartment_id": click_data.get("apartment_id", ""),
            "nombre_cliente": click_data.get("nombre_cliente", ""),
            "telefono": click_data.get("telefono", ""),
            "usuario": click_data.get("usuario", ""),
            "direccion_id": click_data.get("direccion_id", ""),
            "confirmacion_rafa": click_data.get("confirmacion_rafa", ""),
            "zona_estudio": click_data.get("zona_estudio", ""),
            "estado": click_data.get("estado", "Sin estado"),
            "presupuesto_enviado": click_data.get("presupuesto_enviado", ""),
            "nuevapromocion": click_data.get("nuevapromocion", "NO"),
            "resultado": click_data.get("resultado", "NO"),
            "justificacion": click_data.get("justificacion", "SIN JUSTIFICACIÓN"),
            "contratos": click_data.get("contratos", ""),
            "respuesta_comercial": click_data.get("respuesta_comercial", ""),
            "comentarios_gestor": click_data.get("comentarios_gestor", ""),
            "fecha_entrega": click_data.get("fecha_entrega", ""),
            "estado_obra": click_data.get("estado_obra", "")
        }

    # Obtener datos actuales del formulario
    form_data = st.session_state[f"form_data_{ticket}"]

    # Función para actualizar valores en session_state
    def update_form_data(field, value):
        st.session_state[f"form_data_{ticket}"][field] = value

    with st.form(key=f"form_viabilidad_{ticket}"):
        st.subheader(f"✏️ Editar Viabilidad - Ticket {ticket}")

        # --- UBICACIÓN ---
        col1, col2, col3 = st.columns(3)
        with col1:
            st.text_input("🎟️ Ticket", value=ticket, disabled=True, key=f"ticket_{ticket}")
        with col2:
            latitud = st.text_input("📍 Latitud", value=form_data["latitud"],
                                    key=f"latitud_{ticket}")
            if latitud != form_data["latitud"]:
                update_form_data("latitud", latitud)
        with col3:
            longitud = st.text_input("📍 Longitud", value=form_data["longitud"],
                                     key=f"longitud_{ticket}")
            if longitud != form_data["longitud"]:
                update_form_data("longitud", longitud)

        col4, col5, col6 = st.columns(3)
        with col4:
            provincia = st.text_input("🏠 Provincia", value=form_data["provincia"],
                                      key=f"provincia_{ticket}")
            if provincia != form_data["provincia"]:
                update_form_data("provincia", provincia)
        with col5:
            municipio = st.text_input("🏙️ Municipio", value=form_data["municipio"],
                                      key=f"municipio_{ticket}")
            if municipio != form_data["municipio"]:
                update_form_data("municipio", municipio)
        with col6:
            poblacion = st.text_input("👥 Población", value=form_data["poblacion"],
                                      key=f"poblacion_{ticket}")
            if poblacion != form_data["poblacion"]:
                update_form_data("poblacion", poblacion)

        col7, col8, col9, col10 = st.columns([2, 1, 1, 1])
        with col7:
            vial = st.text_input("🚦 Vial", value=form_data["vial"],
                                 key=f"vial_{ticket}")
            if vial != form_data["vial"]:
                update_form_data("vial", vial)
        with col8:
            numero = st.text_input("🔢 Número", value=form_data["numero"],
                                   key=f"numero_{ticket}")
            if numero != form_data["numero"]:
                update_form_data("numero", numero)
        with col9:
            letra = st.text_input("🔠 Letra", value=form_data["letra"],
                                  key=f"letra_{ticket}")
            if letra != form_data["letra"]:
                update_form_data("letra", letra)
        with col10:
            cp = st.text_input("📮 Código Postal", value=form_data["cp"],
                               key=f"cp_{ticket}")
            if cp != form_data["cp"]:
                update_form_data("cp", cp)

        comentario = st.text_area("💬 Comentarios", value=form_data["comentario"],
                                  key=f"comentario_{ticket}")
        if comentario != form_data["comentario"]:
            update_form_data("comentario", comentario)

        # --- CONTACTO ---
        colc1, colc2, colc3 = st.columns(3)
        with colc1:
            nombre_cliente = st.text_input("👤 Nombre Cliente", value=form_data["nombre_cliente"],
                                           key=f"nombre_cliente_{ticket}")
            if nombre_cliente != form_data["nombre_cliente"]:
                update_form_data("nombre_cliente", nombre_cliente)
        with colc2:
            telefono = st.text_input("📞 Teléfono", value=form_data["telefono"],
                                     key=f"telefono_{ticket}")
            if telefono != form_data["telefono"]:
                update_form_data("telefono", telefono)
        with colc3:
            # --- Obtener lista de comerciales desde la base de datos ---
            try:
                conn = obtener_conexion()
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT usuario FROM viabilidades WHERE usuario IS NOT NULL AND usuario != ''")
                comerciales = [row[0] for row in cursor.fetchall()]
                conn.close()
            except Exception as e:
                st.toast(f"Error al cargar comerciales: {e}")
                comerciales = []

            # Añadir el valor actual si no está en la lista
            if form_data["usuario"] and form_data["usuario"] not in comerciales:
                comerciales.append(form_data["usuario"])

            comerciales = sorted(comerciales)  # ordenar alfabéticamente

            # --- Mostrar selectbox con comercial actual seleccionado ---
            if comerciales:
                index_actual = comerciales.index(form_data["usuario"]) if form_data["usuario"] in comerciales else 0
                usuario = st.selectbox("👤 Comercial", comerciales, index=index_actual, key=f"usuario_{ticket}")
            else:
                usuario = st.text_input("👤 Comercial", value=form_data["usuario"], key=f"usuario_{ticket}")

            if usuario != form_data["usuario"]:
                update_form_data("usuario", usuario)
######################
        col_nueva1, col_nueva2 = st.columns(2)
        with col_nueva1:
            fecha_entrega = st.text_input(
                "📅 Fecha de entrega",
                value=form_data.get("fecha_entrega", ""),
                placeholder="DD/MM/AAAA",
                key=f"fecha_entrega_{ticket}"
            )
            if fecha_entrega != form_data.get("fecha_entrega", ""):
                update_form_data("fecha_entrega", fecha_entrega)

        with col_nueva2:
            estado_obra = st.text_input(
                "🏗️ Estado de la obra",
                value=form_data.get("estado_obra", ""),
                placeholder="Ej: En progreso, Finalizada...",
                key=f"estado_obra_{ticket}"
            )
            if estado_obra != form_data.get("estado_obra", ""):
                update_form_data("estado_obra", estado_obra)

        ######################
        # --- FECHAS Y CTO ---
        colf1, colf2 = st.columns(2)
        with colf1:
            st.text_input("📅 Fecha Viabilidad", value=form_data["fecha_viabilidad"],
                          disabled=True, key=f"fecha_viabilidad_{ticket}")
        with colf2:
            cto_cercana = st.text_input("🔌 CTO Cercana", value=form_data["cto_cercana"],
                                        key=f"cto_cercana_{ticket}")
            if cto_cercana != form_data["cto_cercana"]:
                update_form_data("cto_cercana", cto_cercana)

        # --- APARTAMENTO / DIRECCIÓN / OLT ---
        col11, col12, col13 = st.columns(3)
        with col11:
            apartment_id = st.text_area("🏠 Apartment IDs", value=form_data["apartment_id"],
                                        key=f"apartment_id_{ticket}")
            if apartment_id != form_data["apartment_id"]:
                update_form_data("apartment_id", apartment_id)
        with col12:
            direccion_id = st.text_input("📍 Dirección ID", value=form_data["direccion_id"],
                                         key=f"direccion_id_{ticket}")
            if direccion_id != form_data["direccion_id"]:
                update_form_data("direccion_id", direccion_id)
        with col13:
            olt_guardado = str(form_data["olt"]) if form_data["olt"] else ""
            indice_default = 0

            def normalizar_id(olt_value):
                # Toma solo la parte antes de "-" o "."
                return str(olt_value).split("-")[0].split(".")[0].strip().upper()

            olt_guardado_norm = normalizar_id(olt_guardado)

            # Buscar cuál de las opciones tiene ese mismo ID
            for i, opcion in enumerate(opciones_olt):
                id_opcion = normalizar_id(opcion)
                if id_opcion == olt_guardado_norm:
                    indice_default = i
                    break

            # Selectbox para seleccionar la OLT
            olt_seleccionado = st.selectbox("⚡ OLT", opciones_olt, index=indice_default, key=f"olt_{ticket}")

            # 🟢 Guardar el texto completo (id - nombre)
            update_form_data("olt", olt_seleccionado)

        # --- ADMINISTRACIÓN CTO ---
        col14, col15, col16 = st.columns(3)
        with col14:
            cto_admin = st.text_input("⚙️ CTO Admin", value=form_data["cto_admin"],
                                      key=f"cto_admin_{ticket}")
            if cto_admin != form_data["cto_admin"]:
                update_form_data("cto_admin", cto_admin)
        with col15:
            municipio_admin = st.text_input("🌍 Municipio Admin", value=form_data["municipio_admin"],
                                            key=f"municipio_admin_{ticket}")
            if municipio_admin != form_data["municipio_admin"]:
                update_form_data("municipio_admin", municipio_admin)
        with col16:
            id_cto = st.text_input("🔧 ID CTO", value=form_data["id_cto"],
                                   key=f"id_cto_{ticket}")
            if id_cto != form_data["id_cto"]:
                update_form_data("id_cto", id_cto)

        # --- ESTADO Y VIABILIDAD ---
        col17, col18, col19, col20 = st.columns([1, 1, 1, 1])
        with col17:
            serviciable_index = 0 if str(form_data["serviciable"]).upper() in ["SÍ", "SI", "S", "YES", "TRUE",
                                                                               "1"] else 1
            serviciable = st.selectbox("🔍 Serviciable", ["Sí", "No"],
                                       index=serviciable_index,
                                       key=f"serviciable_{ticket}")
            if serviciable != form_data["serviciable"]:
                update_form_data("serviciable", serviciable)
        with col18:
            coste = st.number_input(
                "💰 Coste (sin IVA)",
                value=float(form_data["coste"]),
                step=0.01,
                key=f"coste_{ticket}"
            )
            if coste != form_data["coste"]:
                update_form_data("coste", coste)
        with col19:
            coste_con_iva = round(float(form_data["coste"]) * 1.21, 2)
            st.text_input("💰 Coste con IVA 21%", value=f"{coste_con_iva:.2f}",
                          disabled=True, key=f"coste_iva_{ticket}")
        with col20:
            presupuesto_enviado = st.text_input("📤 Presupuesto Enviado",
                                                value=form_data["presupuesto_enviado"],
                                                key=f"presupuesto_enviado_{ticket}")
            if presupuesto_enviado != form_data["presupuesto_enviado"]:
                update_form_data("presupuesto_enviado", presupuesto_enviado)

        # --- COMENTARIOS ---
        comentarios_comercial = st.text_area("📝 Comentarios Comerciales",
                                             value=form_data["comentarios_comercial"],
                                             key=f"comentarios_comercial_{ticket}")
        if comentarios_comercial != form_data["comentarios_comercial"]:
            update_form_data("comentarios_comercial", comentarios_comercial)

        comentarios_internos = st.text_area("📄 Comentarios Internos",
                                            value=form_data["comentarios_internos"],
                                            key=f"comentarios_internos_{ticket}")
        if comentarios_internos != form_data["comentarios_internos"]:
            update_form_data("comentarios_internos", comentarios_internos)

        comentarios_gestor = st.text_area("🗒️ Comentarios Gestor",
                                          value=form_data["comentarios_gestor"],
                                          key=f"comentarios_gestor_{ticket}")
        if comentarios_gestor != form_data["comentarios_gestor"]:
            update_form_data("comentarios_gestor", comentarios_gestor)

        # --- OTROS CAMPOS ---
        col20, col21, col22 = st.columns(3)
        with col20:
            confirmacion_rafa = st.text_input("📍 Confirmación Rafa",
                                              value=form_data["confirmacion_rafa"],
                                              key=f"confirmacion_rafa_{ticket}")
            if confirmacion_rafa != form_data["confirmacion_rafa"]:
                update_form_data("confirmacion_rafa", confirmacion_rafa)
        with col21:
            zona_estudio = st.text_input("🗺️ Zona de Estudio",
                                         value=form_data["zona_estudio"],
                                         key=f"zona_estudio_{ticket}")
            if zona_estudio != form_data["zona_estudio"]:
                update_form_data("zona_estudio", zona_estudio)
        with col22:
            estado = st.text_input("📌 Estado", value=form_data["estado"],
                                   key=f"estado_{ticket}")
            if estado != form_data["estado"]:
                update_form_data("estado", estado)

        col23, col24, col25 = st.columns(3)
        with col23:
            nueva_promocion_index = 0 if str(form_data["nuevapromocion"]).upper() == "SI" else 1
            nueva_promocion = st.selectbox("🏗️ Nueva Promoción", ["SI", "NO"],
                                           index=nueva_promocion_index,
                                           key=f"nueva_promocion_{ticket}")
            if nueva_promocion != form_data["nuevapromocion"]:
                update_form_data("nuevapromocion", nueva_promocion)
        with col24:
            opciones_resultado = ["NO", "OK", "PDTE. INFORMACION", "SERVICIADO", "SOBRECOSTE"]
            resultado_index = opciones_resultado.index(form_data["resultado"]) if form_data[
                                                                                      "resultado"] in opciones_resultado else 0
            resultado = st.selectbox("✅ Resultado", opciones_resultado,
                                     index=resultado_index,
                                     key=f"resultado_{ticket}")
            if resultado != form_data["resultado"]:
                update_form_data("resultado", resultado)
        with col25:
            opciones_justificacion = ["SIN JUSTIFICACIÓN", "ZONA SUBVENCIONADA", "INVIABLE", "MAS PREVENTA",
                                      "RESERVADA WHL", "PDTE. FIN DE OBRA", "NO ES UNA VIABILIDAD"]
            justificacion_index = opciones_justificacion.index(form_data["justificacion"]) if form_data[
                                                                                                  "justificacion"] in opciones_justificacion else 0
            justificacion = st.selectbox("📌 Justificación", opciones_justificacion,
                                         index=justificacion_index,
                                         key=f"justificacion_{ticket}")
            if justificacion != form_data["justificacion"]:
                update_form_data("justificacion", justificacion)

        contratos = st.text_input("📑 Contratos", value=form_data["contratos"],
                                  key=f"contratos_{ticket}")
        if contratos != form_data["contratos"]:
            update_form_data("contratos", contratos)

        respuesta_comercial = st.text_input("📨 Respuesta Comercial",
                                            value=form_data["respuesta_comercial"],
                                            key=f"respuesta_comercial_{ticket}")
        if respuesta_comercial != form_data["respuesta_comercial"]:
            update_form_data("respuesta_comercial", respuesta_comercial)

        submit = st.form_submit_button("💾 Guardar cambios")

    if submit:
        try:
            # ============================================
            # 1. VALIDACIÓN DE CAMPOS OBLIGATORIOS
            # ============================================
            campos_obligatorios = [
                #("cto_admin", "CTO Admin"),
                #("id_cto", "ID CTO"),
                ("serviciable", "Serviciable"),
                ("resultado", "Resultado"),
                ("justificacion", "Justificación")
            ]

            campos_faltantes = []
            current_data = st.session_state[f"form_data_{ticket}"]

            for campo_key, campo_nombre in campos_obligatorios:
                if not current_data.get(campo_key) or str(current_data[campo_key]).strip() == "":
                    campos_faltantes.append(campo_nombre)

            if campos_faltantes:
                st.toast(f"❌ Campos obligatorios faltantes: {', '.join(campos_faltantes)}")
                st.stop()

            # ============================================
            # 2. CONEXIÓN A BASE DE DATOS Y ACTUALIZACIÓN
            # ============================================
            conn = obtener_conexion()
            cursor = conn.cursor()

            # Limpiar apartment_id
            apartment_id_clean = ",".join(
                [aid.strip() for aid in (current_data["apartment_id"] or "").split(",") if aid.strip()]
            )

            # Actualización completa de la viabilidad
            cursor.execute("""
                UPDATE viabilidades SET
                    latitud=?, longitud=?, provincia=?, municipio=?, poblacion=?, vial=?, numero=?, letra=?, cp=?, comentario=?,
                    cto_cercana=?, olt=?, cto_admin=?, id_cto=?, municipio_admin=?, serviciable=?, coste=?, comentarios_comercial=?, 
                    comentarios_internos=?, fecha_viabilidad=?, apartment_id=?, nombre_cliente=?, telefono=?, usuario=?, 
                    direccion_id=?, confirmacion_rafa=?, zona_estudio=?, estado=?, presupuesto_enviado=?, nuevapromocion=?, 
                    resultado=?, justificacion=?, contratos=?, respuesta_comercial=?, comentarios_gestor=?, fecha_entrega=?, estado_obra=?
                WHERE ticket=?
            """, (
                current_data["latitud"],
                current_data["longitud"],
                current_data["provincia"],
                current_data["municipio"],
                current_data["poblacion"],
                current_data["vial"],
                current_data["numero"],
                current_data["letra"],
                current_data["cp"],
                current_data["comentario"],
                current_data["cto_cercana"],
                current_data["olt"],
                current_data["cto_admin"],
                current_data["id_cto"],
                current_data["municipio_admin"],
                current_data["serviciable"],
                current_data["coste"],
                current_data["comentarios_comercial"],
                current_data["comentarios_internos"],
                current_data["fecha_viabilidad"],
                apartment_id_clean,
                current_data["nombre_cliente"],
                current_data["telefono"],
                current_data["usuario"],
                current_data["direccion_id"],
                current_data["confirmacion_rafa"],
                current_data["zona_estudio"],
                current_data["estado"],
                current_data["presupuesto_enviado"],
                current_data["nuevapromocion"],
                current_data["resultado"],
                current_data["justificacion"],
                current_data["contratos"],
                current_data["respuesta_comercial"],
                current_data["comentarios_gestor"],
                current_data.get("fecha_entrega", ""),  # 🔹 NUEVO CAMPO
                current_data.get("estado_obra", ""),  # 🔹 NUEVO CAMPO
                ticket
            ))

            conn.commit()

            # ============================================
            # 3. ENVIAR NOTIFICACIÓN AL COMERCIAL ASIGNADO (SIN REGISTRO EN BD)
            # ============================================
            try:
                # Verificar si hay un comercial asignado
                comercial_asignado = current_data["usuario"]

                if comercial_asignado and comercial_asignado.strip():
                    # Obtener el email del comercial desde la tabla usuarios
                    cursor.execute("SELECT email FROM usuarios WHERE username = ?", (comercial_asignado,))
                    row = cursor.fetchone()
                    correo_comercial = row[0] if row else None

                    if correo_comercial:
                        # Importar la función de notificaciones
                        try:
                            #from modules.notificaciones import correo_respuesta_comercial

                            # Preparar el comentario para la notificación
                            comentario_notificacion = (
                                    current_data.get("respuesta_comercial") or
                                    current_data.get("comentarios_comercial") or
                                    f"""
                                <strong>Actualización de viabilidad - Ticket {ticket}</strong><br><br>
                                <strong>Resultado:</strong> {current_data.get('resultado', 'N/A')}<br>
                                <strong>Serviciable:</strong> {current_data.get('serviciable', 'N/A')}<br>
                                <strong>Estado:</strong> {current_data.get('estado', 'N/A')}<br>
                                <strong>Comentarios:</strong> {current_data.get('comentarios_comercial', 'Sin comentarios')}
                                """
                            )

                            # Enviar correo de notificación al comercial
                            correo_respuesta_comercial(
                                destinatario=correo_comercial,
                                ticket_id=ticket,
                                nombre_comercial=comercial_asignado,
                                comentario=comentario_notificacion
                            )

                            st.toast(f"📧 Notificación enviada al comercial {comercial_asignado}")

                        except ImportError:
                            st.toast("⚠️ Módulo 'notificaciones' no encontrado. La notificación no se envió.")
                    else:
                        st.toast(f"⚠️ No se encontró el correo del comercial {comercial_asignado}")
                else:
                    st.toast("ℹ️ No hay comercial asignado para notificar")
            except Exception as e:
                st.toast(f"⚠️ Error al enviar notificación: {str(e)}")
                # Continuar con el flujo aunque falle la notificación

            conn.close()

            # ============================================
            # 4. MENSAJE DE CONFIRMACIÓN Y LIMPIEZA
            # ============================================
            st.toast(f"✅ Cambios guardados correctamente para el ticket {ticket}")

            # Limpiar el session_state para forzar recarga de datos
            if f"form_data_{ticket}" in st.session_state:
                del st.session_state[f"form_data_{ticket}"]

            # Añadir pequeño delay visual antes del rerun
            import time
            time.sleep(1.5)
            st.rerun()

        except Exception as e:
            st.toast(f"❌ Error al guardar los cambios: {str(e)}")
            st.toast(f"❌ Error detallado: {str(e)}")

def obtener_apartment_ids_existentes(cursor):
    cursor.execute("SELECT apartment_id FROM datos_uis")
    return {row[0] for row in cursor.fetchall()}


def mostrar_ofertas_comerciales():
    """Función optimizada para mostrar y gestionar ofertas comerciales"""
    st.info("ℹ️ En esta sección puedes visualizar las ofertas registradas por los comerciales.")

    # Limpiar sesión si existe
    st.session_state.pop("df", None)

    # Cargar datos
    with st.spinner("⏳ Cargando ofertas comerciales..."):
        try:
            conn = obtener_conexion()
            query = "SELECT * FROM comercial_rafa WHERE serviciable IS NOT NULL"
            df_ofertas = pd.read_sql(query, conn)
            conn.close()

            if df_ofertas.empty:
                st.toast("❌ No se encontraron ofertas realizadas por los comerciales.")
                return

        except Exception as e:
            st.toast(f"❌ Error al cargar datos de la base de datos: {e}")
            return

    # Guardar en sesión
    st.session_state["df"] = df_ofertas

    # Configurar y mostrar AgGrid
    gb = GridOptionsBuilder.from_dataframe(df_ofertas)
    gb.configure_default_column(
        filter=True,
        floatingFilter=True,
        sortable=True,
        resizable=True,
        minWidth=120,
        flex=1
    )
    grid_options = gb.build()

    AgGrid(
        df_ofertas,
        gridOptions=grid_options,
        enable_enterprise_modules=True,
        update_mode=GridUpdateMode.NO_UPDATE,
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        fit_columns_on_grid_load=False,
        height=500,
        theme='alpine-dark',
        reload_data=False
    )

    # Sección de visualización de imagen
    mostrar_imagen_oferta(df_ofertas)

    # Sección de descarga de Excel
    descargar_excel_ofertas(df_ofertas)

    # Sección de eliminación de oferta
    eliminar_oferta_comercial(df_ofertas)

    # Sección de descarga de imágenes
    descargar_imagenes_ofertas(df_ofertas)


def mostrar_imagen_oferta(df_ofertas):
    """Muestra imagen de una oferta seleccionada"""
    st.subheader("🖼️ Visualizar Imagen de Oferta")

    # Filtrar solo ofertas con imágenes válidas
    ofertas_con_imagen = df_ofertas[
        df_ofertas["fichero_imagen"].notna() &
        (df_ofertas["fichero_imagen"].str.strip() != "")
        ]

    if ofertas_con_imagen.empty:
        st.warning("No hay ofertas con imágenes disponibles.")
        return

    seleccion_id = st.selectbox(
        "Selecciona un Apartment ID para ver su imagen:",
        ofertas_con_imagen["apartment_id"].unique(),
        key="select_imagen_oferta"
    )

    if seleccion_id:
        imagen_url = ofertas_con_imagen[
            ofertas_con_imagen["apartment_id"] == seleccion_id
            ].iloc[0]["fichero_imagen"]

        try:
            st.image(
                imagen_url,
                caption=f"Imagen de la oferta {seleccion_id}",
                width='stretch'
            )
        except Exception:
            st.warning(f"❌ No se pudo cargar la imagen para {seleccion_id}")


def descargar_excel_ofertas(df_ofertas):
    """Genera y permite descargar Excel con las ofertas"""
    st.markdown("---")
    st.subheader("📊 Descargar Datos")

    towrite = io.BytesIO()
    with pd.ExcelWriter(towrite, engine='xlsxwriter') as writer:
        df_ofertas.to_excel(writer, index=False, sheet_name='Ofertas')

    st.download_button(
        label="📥 Descargar todas las ofertas (Excel)",
        data=towrite.getvalue(),
        file_name="ofertas_comerciales.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        help="Descarga todas las ofertas en formato Excel"
    )


def eliminar_oferta_comercial(df_ofertas):
    """Elimina una oferta comercial seleccionada"""
    st.markdown("---")
    st.subheader("🗑️ Eliminar Oferta Comercial")

    # Usar un formulario para la eliminación
    with st.form("form_eliminar_oferta"):
        selected_apartment_id = st.selectbox(
            "Selecciona el Apartment ID de la oferta a eliminar:",
            ["-- Seleccione --"] + sorted(df_ofertas['apartment_id'].unique().tolist()),
            key="select_eliminar_oferta"
        )

        submitted = st.form_submit_button("Eliminar Oferta",
                                          type="primary",
                                          width='stretch')

        if submitted and selected_apartment_id != "-- Seleccione --":
            try:
                conn = obtener_conexion()
                cursor = conn.cursor()

                # Usar parámetros para prevenir SQL injection
                cursor.execute(
                    "DELETE FROM comercial_rafa WHERE apartment_id = ?",
                    (selected_apartment_id,)
                )

                conn.commit()
                conn.close()

                st.toast(f"✅ Oferta {selected_apartment_id} eliminada exitosamente.")
                st.toast(f"Oferta {selected_apartment_id} eliminada", icon="✅")

                # Forzar recarga de la página
                st.rerun()

            except Exception as e:
                st.toast(f"❌ Error al eliminar la oferta: {e}")


def descargar_imagenes_ofertas(df_ofertas):
    """Gestiona la descarga de imágenes de ofertas"""
    st.markdown("---")
    st.subheader("🖼️ Descargar Imágenes")

    # Filtrar ofertas con imágenes existentes
    ofertas_con_imagen = []
    for _, row in df_ofertas.iterrows():
        img_path = row.get("fichero_imagen")
        if (isinstance(img_path, str) and
                img_path.strip() != "" and
                os.path.exists(img_path)):
            ofertas_con_imagen.append({
                "apartment_id": row["apartment_id"],
                "path": img_path,
                "filename": os.path.basename(img_path)
            })

    if not ofertas_con_imagen:
        st.info("No hay ofertas con imágenes disponibles para descargar.")
        return

    # Descarga individual
    st.markdown("##### Descargar imagen individual")

    selected_offer = st.selectbox(
        "Selecciona una oferta:",
        ["-- Seleccione --"] + [f"{o['apartment_id']} - {o['filename']}"
                                for o in ofertas_con_imagen],
        key="select_descarga_imagen"
    )

    if selected_offer != "-- Seleccione --":
        # Extraer apartment_id de la selección
        apt_id = selected_offer.split(" - ")[0]
        oferta = next(o for o in ofertas_con_imagen if o["apartment_id"] == apt_id)

        col1, col2 = st.columns([1, 2])
        with col1:
            try:
                st.image(oferta["path"], width='stretch')
            except Exception:
                st.warning("No se pudo cargar la vista previa")

        with col2:
            # Determinar MIME type
            ext = os.path.splitext(oferta["path"].lower())[1]
            mime_types = {
                '.png': 'image/png',
                '.jpg': 'image/jpeg',
                '.jpeg': 'image/jpeg',
                '.gif': 'image/gif',
                '.bmp': 'image/bmp'
            }
            mime = mime_types.get(ext, 'application/octet-stream')

            with open(oferta["path"], "rb") as f:
                st.download_button(
                    label=f"Descargar {oferta['filename']}",
                    data=f.read(),
                    file_name=oferta['filename'],
                    mime=mime,
                    width='stretch'
                )

    # Descarga múltiple
    st.markdown("##### Descargar todas las imágenes")

    # Opción para seleccionar qué imágenes descargar
    imagenes_seleccionadas = st.multiselect(
        "Selecciona las imágenes a descargar:",
        [f"{o['apartment_id']} - {o['filename']}" for o in ofertas_con_imagen],
        default=[f"{o['apartment_id']} - {o['filename']}" for o in ofertas_con_imagen],
        key="multiselect_imagenes"
    )

    if imagenes_seleccionadas and st.button("📦 Descargar selección como ZIP"):
        with st.spinner("Creando archivo ZIP..."):
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                for item in imagenes_seleccionadas:
                    apt_id = item.split(" - ")[0]
                    oferta = next(o for o in ofertas_con_imagen
                                  if o["apartment_id"] == apt_id)
                    zip_file.write(oferta["path"], oferta["filename"])

            st.download_button(
                label=f"📥 Descargar {len(imagenes_seleccionadas)} imágenes",
                data=zip_buffer.getvalue(),
                file_name="imagenes_ofertas.zip",
                mime="application/zip",
                width='stretch'
            )


def admin_ticketing_panel():
    """Panel principal de administración del sistema de tickets."""
    # Submenú horizontal (similar al de "Ver Datos")
    sub_seccion = option_menu(
        menu_title=None,
        options=["Todos los Tickets", "Tickets Abiertos", "Tickets Asignados",
                 "Métricas", "Mis Tickets"],  # Añadido "Mis Tickets"
        icons=["list", "exclamation-circle", "person-check",
               "bar-chart", "person"],  # Añadido ícono "person"
        default_index=0,
        orientation="horizontal",
        styles={
            "container": {
                "padding": "0!important",
                "margin": "0px",
                "background-color": "#F0F7F2",
                "border-radius": "0px",
                "max-width": "none"
            },
            "icon": {
                "color": "#2C5A2E",
                "font-size": "25px"
            },
            "nav-link": {
                "color": "#2C5A2E",
                "font-size": "18px",
                "text-align": "center",
                "margin": "0px",
                "--hover-color": "#66B032",
                "border-radius": "0px",
            },
            "nav-link-selected": {
                "background-color": "#66B032",
                "color": "white",
                "font-weight": "bold"
            }
        }
    )

    # Registrar en trazabilidad
    rol_actual = st.session_state.get("role", "admin")
    log_trazabilidad(
        st.session_state["username"],
        f"Acceso a tickets ({rol_actual})",
        f"Seleccionó la sección: {sub_seccion}"
    )

    # Contenido dinámico según la subsección seleccionada
    if sub_seccion == "Mis Tickets":
        mostrar_mis_tickets()
    elif sub_seccion == "Todos los Tickets":
        mostrar_todos_tickets()

    elif sub_seccion == "Tickets Abiertos":
        mostrar_tickets_abiertos()

    elif sub_seccion == "Tickets Asignados":
        mostrar_tickets_asignados()

    elif sub_seccion == "Métricas":
        mostrar_metricas_tickets()


def mostrar_metricas_tickets():
    """Muestra métricas y estadísticas del sistema de tickets."""

    try:
        conn = obtener_conexion()

        # --- MÉTRICAS PRINCIPALES ---
        # Consultas para métricas
        metricas = {}

        # Total tickets
        total = pd.read_sql("SELECT COUNT(*) as total FROM tickets", conn)['total'].iloc[0]
        metricas['total'] = total

        # Tickets por estado
        estados = pd.read_sql("""
            SELECT estado, COUNT(*) as cantidad 
            FROM tickets 
            GROUP BY estado
        """, conn)

        # Tickets por prioridad
        prioridades = pd.read_sql("""
            SELECT prioridad, COUNT(*) as cantidad 
            FROM tickets 
            GROUP BY prioridad
        """, conn)

        # Tickets últimos 7 días
        ultimos_7d = pd.read_sql("""
            SELECT DATE(fecha_creacion) as fecha, COUNT(*) as cantidad
            FROM tickets 
            WHERE fecha_creacion >= DATE('now', '-7 days')
            GROUP BY DATE(fecha_creacion)
            ORDER BY fecha
        """, conn)

        # Tiempo promedio de resolución (tickets cerrados) - VERSIÓN SEGURA
        # Primero verificamos si existe el campo fecha_cierre
        try:
            # Intentamos una consulta que funcione con o sin fecha_cierre
            tiempo_resolucion = pd.read_sql("""
                SELECT 
                    AVG(
                        (JULIANDAY(COALESCE(fecha_cierre, fecha_creacion)) - JULIANDAY(fecha_creacion)) * 24
                    ) as horas_promedio
                FROM tickets 
                WHERE estado IN ('Resuelto', 'Cancelado')
            """, conn)
        except:
            # Si falla, creamos un DataFrame vacío
            tiempo_resolucion = pd.DataFrame(columns=['horas_promedio'])

        conn.close()

        # Mostrar métricas en tarjetas
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Tickets", total)

        with col2:
            abiertos = estados[estados['estado'] == 'Abierto']['cantidad'].sum() if not estados.empty else 0
            st.metric("Tickets Abiertos", abiertos, delta_color="inverse")

        with col3:
            en_progreso = estados[estados['estado'] == 'En Progreso']['cantidad'].sum() if not estados.empty else 0
            st.metric("En Progreso", en_progreso)

        with col4:
            resueltos = estados[estados['estado'].isin(['Resuelto', 'Cancelado'])][
                'cantidad'].sum() if not estados.empty else 0
            tasa_resolucion = (resueltos / total * 100) if total > 0 else 0
            st.metric("Tasa de Resolución", f"{tasa_resolucion:.1f}%")

        # --- GRÁFICOS ---
        # Gráfico 1: Distribución por estado
        if not estados.empty:
            col_graf1, col_graf2 = st.columns(2)

            with col_graf1:
                st.markdown("#### 📊 Distribución por Estado")
                # Crear gráfico de pastel
                fig_estado = px.pie(
                    estados,
                    values='cantidad',
                    names='estado',
                    color='estado',
                    color_discrete_map={
                        'Abierto': '#FF6B6B',
                        'En Progreso': '#FFD166',
                        'Resuelto': '#4ECDC4',
                        'Cancelado': '#06D6A0'
                    }
                )
                fig_estado.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_estado, use_container_width=True)

            with col_graf2:
                st.markdown("#### 🚨 Distribución por Prioridad")
                if not prioridades.empty:
                    fig_prioridad = px.bar(
                        prioridades,
                        x='prioridad',
                        y='cantidad',
                        color='prioridad',
                        color_discrete_map={
                            'Alta': '#FF6B6B',
                            'Media': '#FFD166',
                            'Baja': '#4ECDC4'
                        }
                    )
                    fig_prioridad.update_layout(showlegend=False)
                    st.plotly_chart(fig_prioridad, use_container_width=True)

        # Gráfico 2: Tendencia últimos 7 días
        if not ultimos_7d.empty:
            st.markdown("#### 📈 Tendencia (Últimos 7 días)")

            fig_tendencia = px.line(
                ultimos_7d,
                x='fecha',
                y='cantidad',
                markers=True,
                line_shape='spline'
            )
            fig_tendencia.update_layout(
                xaxis_title="Fecha",
                yaxis_title="Nuevos Tickets"
            )
            st.plotly_chart(fig_tendencia, use_container_width=True)

        # --- TABLAS DETALLADAS ---
        tab_cat, tab_user, tab_time = st.tabs(["🏷️ Por Categoría", "👥 Por Usuario", "⏱️ Tiempos"])

        with tab_cat:
            conn = obtener_conexion()
            por_categoria = pd.read_sql("""
                SELECT 
                    categoria,
                    COUNT(*) as total,
                    SUM(CASE WHEN estado = 'Abierto' THEN 1 ELSE 0 END) as abiertos,
                    SUM(CASE WHEN estado = 'En Progreso' THEN 1 ELSE 0 END) as en_progreso,
                    SUM(CASE WHEN estado IN ('Resuelto', 'Cancelado') THEN 1 ELSE 0 END) as resueltos
                FROM tickets
                GROUP BY categoria
                ORDER BY total DESC
            """, conn)
            conn.close()

            if not por_categoria.empty:
                st.dataframe(por_categoria, use_container_width=True)

        with tab_user:
            conn = obtener_conexion()
            # CONSULTA CORREGIDA - usando la tabla 'usuarios' correctamente
            por_usuario = pd.read_sql("""
                SELECT 
                    u.username as usuario,
                    COUNT(DISTINCT t.ticket_id) as tickets_creados,
                    COUNT(DISTINCT CASE WHEN t.estado = 'Abierto' THEN t.ticket_id END) as abiertos,
                    COUNT(DISTINCT ta.ticket_id) as asignados
                FROM usuarios u
                LEFT JOIN tickets t ON u.id = t.usuario_id
                LEFT JOIN tickets ta ON u.id = ta.asignado_a
                GROUP BY u.id, u.username
                ORDER BY tickets_creados DESC
            """, conn)
            conn.close()

            if not por_usuario.empty:
                st.dataframe(por_usuario, use_container_width=True)

        with tab_time:
            st.info("⏱️ **Estadísticas de Tiempo**")

            col_t1, col_t2, col_t3 = st.columns(3)
            with col_t1:
                # Mostrar tiempo promedio solo si hay datos
                if not tiempo_resolucion.empty and 'horas_promedio' in tiempo_resolucion.columns:
                    horas = tiempo_resolucion['horas_promedio'].iloc[0]
                    if horas and not pd.isna(horas):
                        st.metric("Tiempo Promedio Resolución", f"{horas:.1f} horas")
                    else:
                        st.metric("Tiempo Promedio Resolución", "Sin datos")
                else:
                    st.metric("Tiempo Promedio Resolución", "N/A")

            with col_t2:
                # Tickets antiguos (> 7 días)
                conn = obtener_conexion()
                antiguos = pd.read_sql("""
                    SELECT COUNT(*) as cantidad
                    FROM tickets
                    WHERE estado IN ('Abierto', 'En Progreso')
                    AND fecha_creacion < DATE('now', '-7 days')
                """, conn)['cantidad'].iloc[0]
                conn.close()
                st.metric("Tickets > 7 días", antiguos, delta_color="inverse")

            with col_t3:
                # Tickets sin asignar
                conn = obtener_conexion()
                sin_asignar = pd.read_sql("""
                    SELECT COUNT(*) as cantidad
                    FROM tickets
                    WHERE estado = 'Abierto' 
                    AND (asignado_a IS NULL OR asignado_a = '')
                """, conn)['cantidad'].iloc[0]
                conn.close()
                st.metric("Sin asignar", sin_asignar, delta_color="inverse")

        # --- REPORTE DESCARGABLE ---
        if st.button("📊 Generar Reporte Completo", type="primary", use_container_width=True):
            # Crear reporte en Excel
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                # Hoja 1: Resumen
                resumen_data = {
                    'Métrica': ['Total Tickets', 'Abiertos', 'En Progreso', 'Resueltos', 'Tasa Resolución'],
                    'Valor': [total, abiertos, en_progreso, resueltos, f"{tasa_resolucion:.1f}%"]
                }
                pd.DataFrame(resumen_data).to_excel(writer, sheet_name='Resumen', index=False)

                # Hoja 2: Tickets detallados
                conn = obtener_conexion()
                tickets_detalle = pd.read_sql("""
                    SELECT 
                        t.*,
                        u.username as nombre_usuario,
                        a.username as nombre_asignado
                    FROM tickets t
                    LEFT JOIN usuarios u ON t.usuario_id = u.id
                    LEFT JOIN usuarios a ON t.asignado_a = a.id
                    ORDER BY t.fecha_creacion DESC
                """, conn)
                conn.close()
                tickets_detalle.to_excel(writer, sheet_name='Tickets', index=False)

            output.seek(0)

            st.download_button(
                label="⬇️ Descargar Reporte Completo (.xlsx)",
                data=output,
                file_name=f"reporte_tickets_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

    except Exception as e:
        st.toast(f"⚠️ Error al cargar métricas: {str(e)[:200]}")
        st.info("""
        **Posibles soluciones:**
        1. Ejecuta este SQL para añadir el campo `fecha_cierre`:
           ```sql
           ALTER TABLE tickets ADD COLUMN fecha_cierre DATETIME;
           ```
        2. Verifica que la tabla `tickets` existe
        3. Comprueba la conexión a la base de datos
        """)


def actualizar_estado_ticket(ticket_id, nuevo_estado):
    """Actualiza el estado de un ticket y registra la acción como comentario."""
    try:
        user_id = st.session_state.get("user_id", 1)
        username = st.session_state.get("username", "Usuario")

        conn = obtener_conexion()
        cursor = conn.cursor()

        # Obtener estado anterior
        cursor.execute("SELECT estado, titulo FROM tickets WHERE ticket_id = ?", (ticket_id,))
        ticket_info = cursor.fetchone()
        estado_anterior = ticket_info[0] if ticket_info else "Desconocido"
        titulo_ticket = ticket_info[1] if ticket_info else f"#{ticket_id}"

        # Verificar si existe el campo fecha_cierre
        cursor.execute("PRAGMA table_info(tickets)")
        columnas = cursor.fetchall()
        tiene_fecha_cierre = any(col[1] == 'fecha_cierre' for col in columnas)

        # Actualizar estado del ticket
        if nuevo_estado in ['Resuelto', 'Cancelado'] and tiene_fecha_cierre:
            cursor.execute("""
                UPDATE tickets 
                SET estado = ?, fecha_cierre = CURRENT_TIMESTAMP 
                WHERE ticket_id = ?
            """, (nuevo_estado, ticket_id))
        else:
            cursor.execute("""
                UPDATE tickets 
                SET estado = ? 
                WHERE ticket_id = ?
            """, (nuevo_estado, ticket_id))

        # Registrar el cambio de estado como comentario
        cursor.execute("""
            INSERT INTO comentarios_tickets 
            (ticket_id, usuario_id, tipo, contenido)
            VALUES (?, ?, ?, ?)
        """, (
            ticket_id,
            user_id,
            'actualizacion',
            f"Estado cambiado de '{estado_anterior}' a '{nuevo_estado}' por {username}"
        ))

        conn.commit()
        conn.close()

        # Registrar en trazabilidad
        log_trazabilidad(
            username,
            "Actualización de ticket",
            f"Cambió estado del ticket #{ticket_id} ('{titulo_ticket}') de '{estado_anterior}' a '{nuevo_estado}'"
        )

        st.toast(f"✅ Ticket #{ticket_id} actualizado a '{nuevo_estado}'")
        return True

    except Exception as e:
        st.toast(f"⚠️ Error al actualizar ticket: {str(e)[:100]}")
        return False


def generar_reporte_actividad(user_id):
    """Genera un reporte de actividad del usuario."""
    try:
        conn = obtener_conexion()

        # Obtener información del usuario
        cursor = conn.cursor()
        cursor.execute("SELECT username FROM usuarios WHERE id = ?", (user_id,))
        user_info = cursor.fetchone()
        username = user_info[0] if user_info else f"Usuario #{user_id}"

        # Obtener tickets creados por el usuario
        tickets_creados = pd.read_sql("""
            SELECT 
                ticket_id,
                fecha_creacion,
                categoria,
                prioridad,
                estado,
                asignado_a,
                titulo
            FROM tickets
            WHERE usuario_id = ?
            ORDER BY fecha_creacion DESC
        """, conn, params=(user_id,))

        # Obtener tickets asignados al usuario
        tickets_asignados = pd.read_sql("""
            SELECT 
                t.ticket_id,
                t.fecha_creacion,
                u.username as reportado_por,
                t.categoria,
                t.prioridad,
                t.estado,
                t.titulo
            FROM tickets t
            LEFT JOIN usuarios u ON t.usuario_id = u.id
            WHERE t.asignado_a = ?
            ORDER BY t.fecha_creacion DESC
        """, conn, params=(user_id,))

        conn.close()

        # Mostrar el reporte
        st.subheader(f"📊 Reporte de Actividad: {username}")

        # Resumen estadístico
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Tickets Creados", len(tickets_creados))
        with col2:
            tickets_abiertos = len(tickets_creados[tickets_creados['estado'] == 'Abierto'])
            st.metric("Creados Abiertos", tickets_abiertos)
        with col3:
            st.metric("Tickets Asignados", len(tickets_asignados))
        with col4:
            asignados_activos = len(tickets_asignados[tickets_asignados['estado'].isin(['Abierto', 'En Progreso'])])
            st.metric("Asignados Activos", asignados_activos)

        # Pestañas para diferentes secciones del reporte
        tab1, tab2, tab3 = st.tabs(["📝 Tickets Creados", "👤 Tickets Asignados", "📈 Estadísticas"])

        with tab1:
            if not tickets_creados.empty:
                st.markdown(f"### 📝 Tickets Creados por Ti ({len(tickets_creados)})")

                # Formatear para mejor visualización
                tickets_creados_display = tickets_creados.copy()
                tickets_creados_display['fecha_creacion'] = pd.to_datetime(
                    tickets_creados_display['fecha_creacion']).dt.strftime('%d/%m/%Y %H:%M')

                st.dataframe(
                    tickets_creados_display.rename(columns={
                        'ticket_id': 'ID',
                        'fecha_creacion': 'Creado',
                        'categoria': 'Categoría',
                        'prioridad': 'Prioridad',
                        'estado': 'Estado',
                        'titulo': 'Título'
                    }),
                    use_container_width=True,
                    hide_index=True
                )

                # Distribución por estado
                if len(tickets_creados) > 0:
                    st.markdown("#### 📊 Distribución por Estado")
                    distribucion = tickets_creados['estado'].value_counts()
                    st.bar_chart(distribucion)
            else:
                st.info("No has creado ningún ticket todavía.")

        with tab2:
            if not tickets_asignados.empty:

                tickets_asignados_display = tickets_asignados.copy()
                tickets_asignados_display['fecha_creacion'] = pd.to_datetime(
                    tickets_asignados_display['fecha_creacion']).dt.strftime('%d/%m/%Y %H:%M')

                st.dataframe(
                    tickets_asignados_display.rename(columns={
                        'ticket_id': 'ID',
                        'fecha_creacion': 'Creado',
                        'reportado_por': 'Reportado por',
                        'categoria': 'Categoría',
                        'prioridad': 'Prioridad',
                        'estado': 'Estado',
                        'titulo': 'Título'
                    }),
                    use_container_width=True,
                    hide_index=True
                )

                # Métricas de desempeño
                st.markdown("#### 🎯 Métricas de Desempeño")

                col_perf1, col_perf2, col_perf3 = st.columns(3)
                with col_perf1:
                    resueltos = len(tickets_asignados[tickets_asignados['estado'].isin(['Resuelto', 'Cancelado'])])
                    porcentaje_resueltos = (resueltos / len(tickets_asignados) * 100) if len(
                        tickets_asignados) > 0 else 0
                    st.metric("Tasa de Resolución", f"{porcentaje_resueltos:.1f}%")

                with col_perf2:
                    alta_prioridad = len(tickets_asignados[tickets_asignados['prioridad'] == 'Alta'])
                    st.metric("Alta Prioridad", alta_prioridad)

                with col_perf3:
                    # Calcular tiempo promedio de resolución (si hay tickets resueltos)
                    if resueltos > 0:
                        st.metric("Tickets Resueltos", resueltos)
                    else:
                        st.metric("En Progreso", len(tickets_asignados[tickets_asignados['estado'] == 'En Progreso']))
            else:
                st.info("No tienes tickets asignados actualmente.")

        with tab3:
            st.markdown("### 📈 Estadísticas Detalladas")

            # Estadísticas por categoría
            if not tickets_creados.empty:
                st.markdown("#### 🏷️ Tickets Creados por Categoría")
                cat_stats = tickets_creados.groupby('categoria').agg({
                    'ticket_id': 'count',
                    'estado': lambda x: (x == 'Abierto').sum()
                }).rename(columns={'ticket_id': 'Total', 'estado': 'Abiertos'})

                st.dataframe(cat_stats, use_container_width=True)

            # Tendencia temporal (últimos 30 días)
            st.markdown("#### 📅 Actividad Reciente (Últimos 30 días)")

            try:
                conn = obtener_conexion()

                # Tickets creados en los últimos 30 días
                creados_30d = pd.read_sql("""
                    SELECT 
                        DATE(fecha_creacion) as fecha,
                        COUNT(*) as cantidad
                    FROM tickets
                    WHERE usuario_id = ? 
                        AND fecha_creacion >= DATE('now', '-30 days')
                    GROUP BY DATE(fecha_creacion)
                    ORDER BY fecha
                """, conn, params=(user_id,))

                if not creados_30d.empty:
                    # Crear gráfico de línea
                    fig = px.line(
                        creados_30d,
                        x='fecha',
                        y='cantidad',
                        markers=True,
                        title='Tickets Creados por Día (Últimos 30 días)'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No hay actividad en los últimos 30 días.")

                conn.close()
            except Exception as e:
                st.warning(f"No se pudo generar la tendencia temporal: {str(e)[:100]}")

        # Opción para exportar el reporte

        if st.button("💾 Descargar Reporte Completo", type="primary", use_container_width=True):
            # Crear un archivo Excel con el reporte
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                # Hoja 1: Resumen
                resumen_data = {
                    'Métrica': ['Usuario', 'Tickets Creados', 'Tickets Asignados',
                                'Tickets Abiertos', 'Tickets Resueltos', 'Fecha Reporte'],
                    'Valor': [username, len(tickets_creados), len(tickets_asignados),
                              tickets_abiertos, resueltos, datetime.now().strftime('%Y-%m-%d %H:%M')]
                }
                pd.DataFrame(resumen_data).to_excel(writer, sheet_name='Resumen', index=False)

                # Hoja 2: Tickets creados
                if not tickets_creados.empty:
                    tickets_creados.to_excel(writer, sheet_name='Tickets_Creados', index=False)

                # Hoja 3: Tickets asignados
                if not tickets_asignados.empty:
                    tickets_asignados.to_excel(writer, sheet_name='Tickets_Asignados', index=False)

            output.seek(0)

            st.download_button(
                label="⬇️ Descargar Reporte (.xlsx)",
                data=output,
                file_name=f"reporte_actividad_{username}_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

        return True

    except Exception as e:
        st.toast(f"⚠️ Error al generar reporte: {str(e)[:100]}")
        return False


def mostrar_tickets_asignados():
    """Muestra tickets asignados al administrador actual."""

    # Obtener ID del usuario actual
    user_id = st.session_state.get("user_id", 1)

    try:
        conn = obtener_conexion()

        # Consulta para tickets asignados al usuario actual
        query = """
        SELECT 
            t.ticket_id,
            t.fecha_creacion,
            u.username as usuario,
            t.categoria,
            t.prioridad,
            t.estado,
            t.titulo,
            t.descripcion,
            t.comentarios
        FROM tickets t
        LEFT JOIN usuarios u ON t.usuario_id = u.id
        WHERE t.asignado_a = ? 
            AND t.estado IN ('Abierto', 'En Progreso')
        ORDER BY 
            CASE t.prioridad 
                WHEN 'Alta' THEN 1
                WHEN 'Media' THEN 2
                WHEN 'Baja' THEN 3
            END,
            t.fecha_creacion DESC
        """

        df_tickets = pd.read_sql(query, conn, params=(user_id,))
        conn.close()

        if df_tickets.empty:
            st.toast("🎉 ¡Excelente! No tienes tickets asignados pendientes.")
            return

        # Convertir la columna fecha_creacion a datetime
        df_tickets['fecha_creacion'] = pd.to_datetime(df_tickets['fecha_creacion'], errors='coerce')
        df_tickets = df_tickets.dropna(subset=['fecha_creacion'])

        # --- RESUMEN ---
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Asignados", len(df_tickets))
        with col2:
            alta = len(df_tickets[df_tickets['prioridad'] == 'Alta'])
            st.metric("Alta Prioridad", alta, delta_color="inverse")
        with col3:
            tres_dias_atras = datetime.now() - timedelta(days=3)
            vencimiento = len(df_tickets[df_tickets['fecha_creacion'] < tres_dias_atras])
            st.metric("> 3 días", vencimiento)

        for _, ticket in df_tickets.iterrows():
            fecha_creacion = ticket['fecha_creacion']
            dias_transcurridos = (datetime.now() - fecha_creacion).days

            # Determinar color según antigüedad
            color_borde = "#FF6B6B" if dias_transcurridos > 3 else "#FFD166" if dias_transcurridos > 1 else "#4ECDC4"

            with st.container():
                # Usar columnas para el borde izquierdo
                left_border, content = st.columns([0.01, 0.99])

                with left_border:
                    st.markdown(f'<div style="background-color: {color_borde}; height: 100%; width: 100%;"></div>',
                                unsafe_allow_html=True)

                with content:
                    st.markdown(f"**🎫 Ticket #{ticket['ticket_id']}: {ticket['titulo']}**")

                    color_prioridad = '#FF6B6B' if ticket['prioridad'] == 'Alta' else '#FFD166' if ticket[
                                                                                                       'prioridad'] == 'Media' else '#4ECDC4'
                    st.markdown(
                        f'<span style="background-color: {color_prioridad}; color: white; padding: 0.2rem 0.5rem; border-radius: 10px; font-size: 0.8rem;">{ticket["prioridad"]}</span>',
                        unsafe_allow_html=True)

                    st.markdown(f"""
                        👤 **Reportado por:** {ticket['usuario']}  
                        📅 **Creado:** {fecha_creacion.strftime('%d/%m/%Y %H:%M')}  
                        ⏳ **Hace:** {dias_transcurridos} días
                        """)

                # Botones de acción en línea
                col_acc1, col_acc2, col_acc3, col_acc4 = st.columns([2, 2, 2, 2])

                with col_acc1:
                    if st.button(f"👁️ Ver Detalles #{ticket['ticket_id']}",
                                 key=f"ver_{ticket['ticket_id']}",
                                 use_container_width=True):
                        st.session_state[f"ver_ticket_{ticket['ticket_id']}"] = True

                with col_acc2:
                    if st.button(f"💬 Comentar #{ticket['ticket_id']}",
                                 key=f"com_{ticket['ticket_id']}",
                                 use_container_width=True):
                        st.session_state[f"comentar_ticket_{ticket['ticket_id']}"] = True

                with col_acc3:
                    if st.button(f"✅ Resolver #{ticket['ticket_id']}",
                                 key=f"res_{ticket['ticket_id']}",
                                 type="primary",
                                 use_container_width=True):
                        actualizar_estado_ticket(ticket['ticket_id'], 'Resuelto')
                        st.rerun()

                with col_acc4:
                    if st.button(f"🔄 Reasignar #{ticket['ticket_id']}",
                                 key=f"reas_{ticket['ticket_id']}",
                                 use_container_width=True):
                        st.session_state["ticket_a_asignar"] = ticket['ticket_id']
                        st.rerun()

                # Mostrar formulario de comentario si se activó
                if st.session_state.get(f"comentar_ticket_{ticket['ticket_id']}"):
                    st.markdown("---")
                    st.markdown(f"#### 💬 Añadir Comentario al Ticket #{ticket['ticket_id']}")

                    with st.form(key=f"form_comentario_{ticket['ticket_id']}", clear_on_submit=True):
                        nuevo_comentario = st.text_area(
                            "Escribe tu comentario:",
                            placeholder="Añade información adicional, preguntas o actualizaciones sobre este ticket...",
                            height=100,
                            key=f"comentario_text_{ticket['ticket_id']}"
                        )

                        tipo_comentario = st.selectbox(
                            "Tipo de comentario:",
                            ["Actualización", "Pregunta", "Solución", "Información adicional"],
                            key=f"tipo_comentario_{ticket['ticket_id']}"
                        )

                        es_interno = st.checkbox(
                            "Comentario interno (solo visible para el equipo)",
                            key=f"interno_{ticket['ticket_id']}"
                        )

                        col_submit1, col_submit2 = st.columns([1, 1])
                        with col_submit1:
                            submit_comentario = st.form_submit_button("💬 Enviar comentario", use_container_width=True)
                        with col_submit2:
                            cancelar_comentario = st.form_submit_button("❌ Cancelar", use_container_width=True)

                        if submit_comentario and nuevo_comentario.strip():
                            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
                            usuario = st.session_state.get("username", "Usuario")
                            tipo = f"[{tipo_comentario}]" if not es_interno else f"[{tipo_comentario} - INTERNO]"

                            nuevo_comentario_formateado = f"\n\n[{timestamp}] {usuario} {tipo}:\n{nuevo_comentario.strip()}"

                            conn = obtener_conexion()
                            cursor = conn.cursor()
                            cursor.execute("""
                                UPDATE tickets 
                                SET comentarios = COALESCE(comentarios || ?, ?)
                                WHERE ticket_id = ?
                            """, (
                                nuevo_comentario_formateado,
                                f"[{timestamp}] {usuario} {tipo}:\n{nuevo_comentario.strip()}",
                                ticket['ticket_id']
                            ))
                            conn.commit()
                            conn.close()

                            # Enviar notificación por correo
                            try:
                                # Obtener información del ticket para notificación
                                conn = obtener_conexion()
                                cursor = conn.cursor()
                                cursor.execute("""
                                    SELECT t.titulo, u.email as creador_email, u2.email as asignado_email
                                    FROM tickets t
                                    LEFT JOIN usuarios u ON t.usuario_id = u.id
                                    LEFT JOIN usuarios u2 ON t.asignado_a = u2.id
                                    WHERE t.ticket_id = ?
                                """, (ticket['ticket_id'],))

                                ticket_data = cursor.fetchone()
                                conn.close()

                                if ticket_data:
                                    ticket_info = {
                                        'ticket_id': ticket['ticket_id'],
                                        'titulo': ticket_data[0],
                                        'actualizado_por': usuario,
                                        'tipo_actualizacion': 'comentario',
                                        'descripcion_cambio': nuevo_comentario.strip(),
                                        'enlace': f"https://tu-dominio.com/ticket/{ticket['ticket_id']}"
                                    }

                                    # Notificar al creador del ticket
                                    if ticket_data[1]:
                                        notificar_actualizacion_ticket(ticket_data[1], ticket_info)

                                    # Notificar al asignado si no es el que comenta
                                    if ticket_data[2] and ticket_data[2] != st.session_state.get('email', ''):
                                        notificar_actualizacion_ticket(ticket_data[2], ticket_info)

                            except Exception as e:
                                st.warning(f"No se pudieron enviar notificaciones: {str(e)[:100]}")

                            log_trazabilidad(
                                st.session_state["username"],
                                "Comentario en ticket",
                                f"Añadió comentario al ticket #{ticket['ticket_id']}"
                            )

                            st.toast("✅ Comentario añadido")
                            st.session_state.pop(f"comentar_ticket_{ticket['ticket_id']}", None)
                            st.rerun()

                        if cancelar_comentario:
                            st.session_state.pop(f"comentar_ticket_{ticket['ticket_id']}", None)
                            st.rerun()

                # Mostrar detalles si se solicita
                if st.session_state.get(f"ver_ticket_{ticket['ticket_id']}"):
                    st.markdown("---")
                    st.markdown(f"#### 📄 Detalles del Ticket #{ticket['ticket_id']}")

                    col_det1, col_det2 = st.columns(2)
                    with col_det1:
                        st.markdown(f"**🏷️ Categoría:** {ticket['categoria']}")
                        st.markdown(f"**📊 Estado:** {ticket['estado']}")
                    with col_det2:
                        st.markdown(f"**🚨 Prioridad:** {ticket['prioridad']}")
                        st.markdown(f"**👤 Reportado por:** {ticket['usuario']}")

                    st.markdown("**📝 Descripción:**")
                    st.info(ticket['descripcion'])

                    if ticket['comentarios']:
                        st.markdown("**💬 Comentarios:**")
                        st.warning(ticket['comentarios'])

                    if st.button(f"❌ Cerrar Detalles #{ticket['ticket_id']}"):
                        st.session_state.pop(f"ver_ticket_{ticket['ticket_id']}", None)
                        st.rerun()

                    st.markdown("---")

        # --- SECCIÓN DE REASIGNACIÓN (se activa con el botón Reasignar) ---
        if st.session_state.get("ticket_a_asignar"):
            ticket_id = st.session_state["ticket_a_asignar"]
            st.markdown("---")
            st.markdown(f"### 👤 Reasignar Ticket #{ticket_id}")

            # Obtener lista de agentes
            conn = obtener_conexion()
            agentes = pd.read_sql("SELECT id, username, email FROM usuarios WHERE role IN ('admin', 'tecnico')", conn)
            conn.close()

            if not agentes.empty:
                agentes['id'] = agentes['id'].astype(int)
                agente_seleccionado = st.selectbox(
                    "Seleccionar nuevo agente:",
                    options=agentes['username'].tolist(),
                    key=f"reasignar_select_{ticket_id}"
                )

                col_btn1, col_btn2 = st.columns(2)
                with col_btn1:
                    if st.button("✅ Confirmar Reasignación", key=f"confirmar_reas_{ticket_id}"):
                        id_agente = agentes[agentes['username'] == agente_seleccionado]['id'].iloc[0]
                        email_agente = agentes[agentes['username'] == agente_seleccionado]['email'].iloc[0]
                        id_agente = int(id_agente)

                        conn = obtener_conexion()
                        cursor = conn.cursor()

                        # Obtener información del ticket
                        cursor.execute("""
                            SELECT t.titulo, t.prioridad, t.categoria, t.usuario_id, 
                                   u.email as creador_email, u.username as creador,
                                   u2.username as anterior_asignado
                            FROM tickets t
                            LEFT JOIN usuarios u ON t.usuario_id = u.id
                            LEFT JOIN usuarios u2 ON t.asignado_a = u2.id
                            WHERE t.ticket_id = ?
                        """, (ticket_id,))

                        ticket_data = cursor.fetchone()

                        # Actualizar asignación
                        cursor.execute(
                            "UPDATE tickets SET asignado_a = ?, estado = 'En Progreso' WHERE ticket_id = ?",
                            (id_agente, ticket_id)
                        )

                        # Añadir comentario sobre la reasignación
                        cursor.execute("""
                            UPDATE tickets 
                            SET comentarios = COALESCE(comentarios || '\n\n', '') || ?
                            WHERE ticket_id = ?
                        """, (
                            f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] {st.session_state['username']} reasignó el ticket a {agente_seleccionado}.",
                            ticket_id
                        ))

                        conn.commit()
                        conn.close()

                        # Enviar notificaciones
                        try:
                            ticket_info = {
                                'ticket_id': ticket_id,
                                'titulo': ticket_data[0],
                                'reasignado_por': st.session_state['username'],
                                'anterior_asignado': ticket_data[6] if ticket_data[6] else 'Nadie',
                                'nuevo_asignado': agente_seleccionado,
                                'motivo': 'Reasignación manual',
                                'enlace': f"https://tu-dominio.com/ticket/{ticket_id}"
                            }

                            # Notificar al nuevo asignado
                            if email_agente:
                                notificar_reasignacion_ticket(email_agente, ticket_info)

                            # Notificar al creador del ticket
                            if ticket_data[4]:
                                notificar_actualizacion_ticket(ticket_data[4], {
                                    'ticket_id': ticket_id,
                                    'titulo': ticket_data[0],
                                    'actualizado_por': st.session_state['username'],
                                    'tipo_actualizacion': 'reasignacion',
                                    'descripcion_cambio': f"Ticket reasignado de {ticket_data[6] if ticket_data[6] else 'Nadie'} a {agente_seleccionado}",
                                    'enlace': f"https://tu-dominio.com/ticket/{ticket_id}"
                                })

                            st.toast(f"📧 Notificaciones enviadas")

                        except Exception as e:
                            st.warning(f"No se pudieron enviar notificaciones: {str(e)[:100]}")

                        log_trazabilidad(
                            st.session_state["username"],
                            "Reasignación de ticket",
                            f"Reasignó el ticket #{ticket_id} a {agente_seleccionado}"
                        )

                        st.toast(f"✅ Ticket #{ticket_id} reasignado a {agente_seleccionado}")
                        st.session_state.pop("ticket_a_asignar", None)
                        st.rerun()

                with col_btn2:
                    if st.button("❌ Cancelar Reasignación", key=f"cancelar_reas_{ticket_id}"):
                        st.session_state.pop("ticket_a_asignar", None)
                        st.rerun()
            else:
                st.warning("No hay agentes disponibles para reasignar")
                if st.button("❌ Cancelar"):
                    st.session_state.pop("ticket_a_asignar", None)
                    st.rerun()

        # --- ACCIONES GLOBALES ---
        col_glob1, col_glob2 = st.columns(2)
        with col_glob1:
            if st.button("✅ Marcar Todos como Resueltos", use_container_width=True):
                conn = obtener_conexion()
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE tickets SET estado = 'Resuelto' WHERE asignado_a = ? AND estado IN ('Abierto', 'En Progreso')",
                    (user_id,)
                )
                conn.commit()
                conn.close()
                st.toast("✅ Todos los tickets marcados como resueltos")
                st.rerun()

        with col_glob2:
            if st.button("📊 Generar Reporte de Actividad", use_container_width=True):
                generar_reporte_actividad(user_id)

    except Exception as e:
        st.toast(f"⚠️ Error al cargar tickets asignados: {str(e)[:200]}")


def mostrar_tickets_abiertos():
    """Muestra tickets con estado 'Abierto' o 'En Progreso'."""

    try:
        conn = obtener_conexion()

        # Consulta para tickets abiertos/en progreso
        query = """
        SELECT 
            t.ticket_id,
            t.fecha_creacion,
            u.username as usuario,
            t.categoria,
            t.prioridad,
            t.estado,
            a.username as asignado_a,
            t.titulo,
            t.descripcion,
            t.comentarios
        FROM tickets t
        LEFT JOIN usuarios u ON t.usuario_id = u.id
        LEFT JOIN usuarios a ON t.asignado_a = a.id
        WHERE t.estado IN ('Abierto', 'En Progreso')
        ORDER BY 
            CASE t.prioridad 
                WHEN 'Alta' THEN 1
                WHEN 'Media' THEN 2
                WHEN 'Baja' THEN 3
            END,
            t.fecha_creacion DESC
        """

        df_tickets = pd.read_sql(query, conn)
        conn.close()

        if df_tickets.empty:
            st.toast("✅ ¡Genial! No hay tickets pendientes.")
            return

        # Crear pestañas para diferentes vistas
        tab1, tab2 = st.tabs(["📄 Vista Tabla", "📋 Vista Detallada"])

        with tab1:
            # Vista tabla compacta - VERSIÓN SIMPLIFICADA SIN SELECCIÓN
            df_display = df_tickets.copy()
            df_display = df_display.rename(columns={
                'ticket_id': 'ID',
                'fecha_creacion': 'Creado',
                'prioridad': 'Prioridad',
                'estado': 'Estado',
                'asignado_a': 'Asignado a',
                'titulo': 'Título'
            })

            # Mostrar solo la tabla sin funcionalidad de selección
            st.dataframe(
                df_display[['ID', 'Creado', 'Prioridad', 'Estado', 'Asignado a', 'Título']],
                use_container_width=True,
                hide_index=True
            )

            # Botones de acción generales (sin selección específica)
            col_acc1, col_acc2 = st.columns(2)
            with col_acc1:
                if st.button("📥 Exportar lista", use_container_width=True):
                    # Crear archivo Excel
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        df_display.to_excel(writer, sheet_name='Tickets_Abiertos', index=False)
                    output.seek(0)

                    st.download_button(
                        label="⬇️ Descargar Excel",
                        data=output,
                        file_name=f"tickets_abiertos_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )

            with col_acc2:
                if st.button("🔄 Actualizar vista", use_container_width=True):
                    st.rerun()

        with tab2:
            # Vista detallada con expanders
            for _, ticket in df_tickets.iterrows():
                with st.expander(f"🎫 #{ticket['ticket_id']} - {ticket['titulo']} ({ticket['prioridad']})"):
                    col1, col2 = st.columns(2)

                    with col1:
                        st.markdown(f"**📅 Creado:** {ticket['fecha_creacion']}")
                        st.markdown(f"**👤 Usuario:** {ticket['usuario']}")
                        st.markdown(f"**🏷️ Categoría:** {ticket['categoria']}")

                    with col2:
                        # Mostrar prioridad con color
                        color_prioridad = {
                            'Alta': '🔴',
                            'Media': '🟡',
                            'Baja': '🟢'
                        }.get(ticket['prioridad'], '⚪')

                        st.markdown(f"**🚨 Prioridad:** {color_prioridad} `{ticket['prioridad']}`")
                        st.markdown(f"**📊 Estado:** `{ticket['estado']}`")
                        st.markdown(f"**👥 Asignado a:** {ticket['asignado_a'] or 'Sin asignar'}")

                    st.markdown("---")
                    st.markdown(f"**📄 Descripción:**")
                    st.info(ticket['descripcion'])

                    if ticket['comentarios']:
                        st.markdown(f"**💬 Comentarios:**")
                        st.warning(ticket['comentarios'])

                    # --- NUEVO: SISTEMA DE COMENTARIOS EN TIEMPO REAL ---
                    st.markdown("---")
                    st.markdown("**💬 Añadir nuevo comentario:**")

                    # Formulario para nuevo comentario
                    with st.form(key=f"form_comentario_{ticket['ticket_id']}", clear_on_submit=True):
                        nuevo_comentario = st.text_area(
                            "Escribe tu comentario:",
                            placeholder="Añade información adicional, preguntas o actualizaciones sobre este ticket...",
                            height=100,
                            key=f"comentario_text_{ticket['ticket_id']}"
                        )

                        tipo_comentario = st.selectbox(
                            "Tipo de comentario:",
                            ["Actualización", "Pregunta", "Solución", "Información adicional"],
                            key=f"tipo_comentario_{ticket['ticket_id']}"
                        )

                        es_interno = st.checkbox(
                            "Comentario interno (solo visible para el equipo)",
                            key=f"interno_{ticket['ticket_id']}"
                        )

                        # Botones en una misma fila - 3 columnas
                        col_btn1, col_btn2, col_btn3 = st.columns(3)
                        with col_btn1:
                            enviar_comentario = st.form_submit_button(
                                "💬 Enviar comentario",
                                use_container_width=True
                            )

                        with col_btn2:
                            marcar_resuelto = st.form_submit_button(
                                "✅ Marcar como Resuelto",
                                use_container_width=True
                            )

                        with col_btn3:
                            asignar_otro = st.form_submit_button(
                                "👤 Asignar a otro",
                                use_container_width=True
                            )

                        # Lógica para cada botón
                        if enviar_comentario and nuevo_comentario.strip():
                            # Añadir el nuevo comentario a los comentarios existentes
                            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
                            usuario = st.session_state.get("username", "Usuario")
                            tipo = f"[{tipo_comentario}]" if not es_interno else f"[{tipo_comentario} - INTERNO]"

                            nuevo_comentario_formateado = f"\n\n[{timestamp}] {usuario} {tipo}:\n{nuevo_comentario.strip()}"

                            conn = obtener_conexion()
                            cursor = conn.cursor()

                            # Obtener información del ticket y emails para notificaciones
                            cursor.execute("""
                                SELECT t.titulo, t.prioridad, t.categoria, t.estado, 
                                       u.email as creador_email, u2.email as asignado_email,
                                       u.username as creador, u2.username as asignado
                                FROM tickets t
                                LEFT JOIN usuarios u ON t.usuario_id = u.id
                                LEFT JOIN usuarios u2 ON t.asignado_a = u2.id
                                WHERE t.ticket_id = ?
                            """, (ticket['ticket_id'],))

                            ticket_data = cursor.fetchone()

                            # Actualizar comentarios
                            cursor.execute("""
                                UPDATE tickets 
                                SET comentarios = COALESCE(comentarios || ?, ?)
                                WHERE ticket_id = ?
                            """, (
                                nuevo_comentario_formateado,
                                f"[{timestamp}] {usuario} {tipo}:\n{nuevo_comentario.strip()}",
                                ticket['ticket_id']
                            ))
                            conn.commit()
                            conn.close()

                            # Enviar notificación de comentario
                            if ticket_data and ticket_data[4]:  # Si hay email del creador
                                try:
                                    ticket_info = {
                                        'ticket_id': ticket['ticket_id'],
                                        'titulo': ticket_data[0],
                                        'actualizado_por': usuario,
                                        'tipo_actualizacion': 'comentario',
                                        'descripcion_cambio': nuevo_comentario.strip(),
                                        'enlace': f"https://tu-dominio.com/ticket/{ticket['ticket_id']}"
                                    }

                                    # Notificar al creador del ticket (si no es el mismo que comenta)
                                    if ticket_data[6] != usuario:  # creador != usuario actual
                                        notificar_actualizacion_ticket(ticket_data[4], ticket_info)

                                    # Notificar al asignado (si existe y no es el mismo que comenta)
                                    if ticket_data[5] and ticket_data[
                                        7] != usuario:  # asignado existe y no es usuario actual
                                        notificar_actualizacion_ticket(ticket_data[5], ticket_info)

                                    st.toast(f"📧 Notificaciones enviadas a los involucrados")

                                except Exception as e:
                                    st.warning(f"No se pudieron enviar notificaciones: {str(e)[:100]}")

                            log_trazabilidad(
                                st.session_state["username"],
                                "Comentario en ticket",
                                f"Añadió comentario al ticket #{ticket['ticket_id']}"
                            )

                            st.toast("✅ Comentario añadido")
                            st.rerun()

                        elif marcar_resuelto:
                            # Obtener información del ticket antes de actualizar
                            conn = obtener_conexion()
                            cursor = conn.cursor()

                            cursor.execute("""
                                SELECT t.titulo, t.prioridad, t.categoria, 
                                       u.email as creador_email, u.username as creador,
                                       u2.email as asignado_email, u2.username as asignado
                                FROM tickets t
                                LEFT JOIN usuarios u ON t.usuario_id = u.id
                                LEFT JOIN usuarios u2 ON t.asignado_a = u2.id
                                WHERE t.ticket_id = ?
                            """, (ticket['ticket_id'],))

                            ticket_data = cursor.fetchone()

                            # Actualizar estado a Resuelto
                            actualizar_estado_ticket(ticket['ticket_id'], 'Resuelto')

                            # Añadir comentario automático
                            cursor.execute("""
                                UPDATE tickets 
                                SET comentarios = COALESCE(comentarios || '\n\n', '') || ?
                                WHERE ticket_id = ?
                            """, (
                                f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] {st.session_state['username']} marcó el ticket como RESUELTO.",
                                ticket['ticket_id']
                            ))

                            conn.commit()
                            conn.close()

                            # Enviar notificación de resolución
                            if ticket_data:
                                try:
                                    ticket_info = {
                                        'ticket_id': ticket['ticket_id'],
                                        'titulo': ticket_data[0],
                                        'resuelto_por': st.session_state['username'],
                                        'fecha_resolucion': datetime.now().strftime('%d/%m/%Y %H:%M'),
                                        'comentario_final': f"Ticket resuelto por {st.session_state['username']}",
                                        'enlace': f"https://tu-dominio.com/ticket/{ticket['ticket_id']}"
                                    }

                                    # Notificar al creador del ticket
                                    if ticket_data[3]:  # Si hay email del creador
                                        notificar_resolucion_ticket(ticket_data[3], ticket_info)

                                    # Notificar al asignado (si existe y no es el mismo que resuelve)
                                    if ticket_data[5] and ticket_data[6] != st.session_state['username']:
                                        notificar_resolucion_ticket(ticket_data[5], ticket_info)

                                    st.toast(f"📧 Notificaciones de resolución enviadas")

                                except Exception as e:
                                    st.warning(f"No se pudieron enviar notificaciones: {str(e)[:100]}")

                            st.rerun()

                        elif asignar_otro:
                            st.session_state["ticket_a_asignar"] = ticket['ticket_id']
                            st.rerun()

        # --- ASIGNACIÓN DE TICKETS ---
        if st.session_state.get("ticket_a_asignar"):
            ticket_id = st.session_state["ticket_a_asignar"]
            st.markdown(f"### 👤 Asignar Ticket #{ticket_id}")

            # Obtener lista de agentes (usuarios con rol de agente)
            conn = obtener_conexion()
            agentes = pd.read_sql("SELECT id, username, email FROM usuarios WHERE role IN ('admin', 'tecnico')", conn)
            conn.close()

            # Convertir IDs de numpy a int nativo de Python
            if not agentes.empty:
                # Convertir toda la columna 'id' a int nativo
                agentes['id'] = agentes['id'].astype(int)

                agente_seleccionado = st.selectbox(
                    "Seleccionar agente:",
                    options=agentes['username'].tolist()
                )

                col_btn1, col_btn2 = st.columns(2)
                with col_btn1:
                    if st.button("✅ Confirmar Asignación"):
                        id_agente = agentes[agentes['username'] == agente_seleccionado]['id'].iloc[0]
                        email_agente = agentes[agentes['username'] == agente_seleccionado]['email'].iloc[0]

                        # Asegurarse de que id_agente sea int nativo
                        id_agente = int(id_agente)

                        conn = obtener_conexion()
                        cursor = conn.cursor()

                        # Obtener información del ticket para la notificación
                        cursor.execute("""
                            SELECT t.titulo, t.prioridad, t.categoria, t.usuario_id, 
                                   u.email as creador_email, u.username as creador
                            FROM tickets t
                            LEFT JOIN usuarios u ON t.usuario_id = u.id
                            WHERE t.ticket_id = ?
                        """, (ticket_id,))

                        ticket_data = cursor.fetchone()

                        # Actualizar el ticket
                        cursor.execute(
                            "UPDATE tickets SET asignado_a = ?, estado = 'En Progreso' WHERE ticket_id = ?",
                            (id_agente, ticket_id)
                        )

                        # Añadir comentario sobre la asignación
                        cursor.execute("""
                                        UPDATE tickets 
                                        SET comentarios = COALESCE(comentarios || '\n\n', '') || ?
                                        WHERE ticket_id = ?
                                    """, (
                            f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] {st.session_state['username']} asignó el ticket a {agente_seleccionado}.",
                            ticket_id
                        ))

                        conn.commit()
                        conn.close()

                        # Enviar notificación por correo
                        try:
                            ticket_info = {
                                'ticket_id': ticket_id,
                                'titulo': ticket_data[0],
                                'asignado_por': st.session_state['username'],
                                'prioridad': ticket_data[1],
                                'categoria': ticket_data[2],
                                'enlace': f"https://tu-dominio.com/ticket/{ticket_id}"
                            }

                            # Notificar al agente asignado
                            if email_agente:
                                notificar_asignacion_ticket(email_agente, ticket_info)

                            # Notificar al creador del ticket sobre la asignación
                            if ticket_data[4]:  # email del creador
                                notificar_actualizacion_ticket(ticket_data[4], {
                                    'ticket_id': ticket_id,
                                    'titulo': ticket_data[0],
                                    'actualizado_por': st.session_state['username'],
                                    'tipo_actualizacion': 'cambio_asignacion',
                                    'descripcion_cambio': f"Ticket asignado a {agente_seleccionado}",
                                    'enlace': f"https://tu-dominio.com/ticket/{ticket_id}"
                                })

                            st.toast(f"📧 Notificaciones enviadas a {agente_seleccionado} y al creador")

                        except Exception as e:
                            st.warning(f"No se pudo enviar la notificación por correo: {str(e)[:100]}")
                            # Continuar con el flujo aunque falle la notificación

                        log_trazabilidad(
                            st.session_state["username"],
                            "Asignación de ticket",
                            f"Asignó el ticket #{ticket_id} a {agente_seleccionado}"
                        )

                        st.toast(f"✅ Ticket #{ticket_id} asignado a {agente_seleccionado}")
                        st.session_state.pop("ticket_a_asignar", None)
                        st.rerun()

                with col_btn2:
                    if st.button("❌ Cancelar"):
                        st.session_state.pop("ticket_a_asignar", None)
                        st.rerun()
            else:
                st.warning("No hay agentes disponibles para asignar")
                if st.button("❌ Cancelar"):
                    st.session_state.pop("ticket_a_asignar", None)
                    st.rerun()

    except Exception as e:
        st.toast(f"⚠️ Error al cargar tickets abiertos: {str(e)[:200]}")
        st.info("""
        **Posibles soluciones:**
        1. Verifica que las tablas `tickets` y `usuarios` existen
        2. Comprueba la conexión a la base de datos
        3. Asegúrate de que los campos de la consulta coinciden con tu estructura de tabla
        """)


def mostrar_todos_tickets():
    """Muestra todos los tickets del sistema con filtros avanzados y vista detallada."""

    try:
        conn = obtener_conexion()

        # Consulta completa con información de usuarios
        query = """
        SELECT 
            t.ticket_id,
            t.fecha_creacion,
            u.username as usuario,
            t.categoria,
            t.prioridad,
            t.estado,
            a.username as asignado_a,
            t.asignado_a as asignado_id,
            t.titulo,
            t.descripcion,
            t.comentarios
        FROM tickets t
        LEFT JOIN usuarios u ON t.usuario_id = u.id
        LEFT JOIN usuarios a ON t.asignado_a = a.id
        ORDER BY 
            CASE t.prioridad 
                WHEN 'Alta' THEN 1
                WHEN 'Media' THEN 2
                WHEN 'Baja' THEN 3
            END,
            t.fecha_creacion DESC
        """

        df_tickets = pd.read_sql(query, conn)
        conn.close()

        if df_tickets.empty:
            st.info("🎉 No hay tickets en el sistema.")
            return

        # --- FILTROS ---

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            estados_filtro = st.multiselect(
                "Estado",
                options=df_tickets['estado'].unique(),
                default=df_tickets['estado'].unique()
            )
        with col2:
            prioridades_filtro = st.multiselect(
                "Prioridad",
                options=df_tickets['prioridad'].unique(),
                default=df_tickets['prioridad'].unique()
            )
        with col3:
            categorias_filtro = st.multiselect(
                "Categoría",
                options=df_tickets['categoria'].unique(),
                default=df_tickets['categoria'].unique()
            )
        with col4:
            # Filtro por asignado
            asignados = ["Todos"] + df_tickets['asignado_a'].dropna().unique().tolist()
            asignado_filtro = st.selectbox(
                "Asignado a",
                options=asignados
            )

        # Aplicar filtros
        mask = (
                df_tickets['estado'].isin(estados_filtro) &
                df_tickets['prioridad'].isin(prioridades_filtro) &
                df_tickets['categoria'].isin(categorias_filtro)
        )

        if asignado_filtro != "Todos":
            if pd.isna(asignado_filtro):
                mask = mask & df_tickets['asignado_a'].isna()
            else:
                mask = mask & (df_tickets['asignado_a'] == asignado_filtro)

        df_filtrado = df_tickets[mask]

        # --- MÉTRICAS RÁPIDAS ---

        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Total", len(df_filtrado))
        with col2:
            abiertos = len(df_filtrado[df_filtrado['estado'] == 'Abierto'])
            st.metric("Abiertos", abiertos, delta_color="inverse")
        with col3:
            en_progreso = len(df_filtrado[df_filtrado['estado'] == 'En Progreso'])
            st.metric("En Progreso", en_progreso)
        with col4:
            resueltos = len(df_filtrado[df_filtrado['estado'] == 'Resuelto'])
            st.metric("Resueltos", resueltos)
        with col5:
            cerrados = len(df_filtrado[df_filtrado['estado'] == 'Cerrado'])
            st.metric("Cerrados", cerrados)


        # --- PESTAÑAS PARA DIFERENTES VISTAS ---
        tab1, tab2 = st.tabs(["📋 Vista Tabla", "📄 Vista Detallada"])

        with tab1:

            # Formatear datos para visualización
            df_display = df_filtrado.copy()
            df_display = df_display.rename(columns={
                'ticket_id': 'ID',
                'fecha_creacion': 'Creado',
                'usuario': 'Usuario',
                'categoria': 'Categoría',
                'prioridad': 'Prioridad',
                'estado': 'Estado',
                'asignado_a': 'Asignado a',
                'titulo': 'Título'
            })

            # Mostrar tabla
            st.dataframe(
                df_display[['ID', 'Creado', 'Usuario', 'Categoría', 'Prioridad', 'Estado', 'Asignado a', 'Título']],
                column_config={
                    'ID': st.column_config.NumberColumn(width='small'),
                    'Creado': st.column_config.DatetimeColumn(format='DD/MM/YY HH:mm'),
                    'Prioridad': st.column_config.TextColumn(
                        help="🚨 Alta - 🔸 Media - 🔹 Baja"
                    ),
                    'Estado': st.column_config.TextColumn(
                        help="🟢 Abierto - 🟡 En Progreso - 🔵 Resuelto - ⚫ Cerrado"
                    )
                },
                use_container_width=True,
                hide_index=True
            )

        with tab2:
            # VISTA DETALLADA CON EXPANDERS
            st.markdown(f"### 📄 Vista Detallada ({len(df_filtrado)} tickets)")

            if len(df_filtrado) == 0:
                st.info("No hay tickets que coincidan con los filtros seleccionados.")

            for _, ticket in df_filtrado.iterrows():
                # Calcular días desde creación
                fecha_creacion = pd.to_datetime(ticket['fecha_creacion'])
                dias_transcurridos = (datetime.now() - fecha_creacion).days

                # Determinar color según antigüedad
                if dias_transcurridos > 7:
                    color_borde = "#FF0000"  # Rojo: muy antiguo
                    antiguedad_icono = "⏰"
                elif dias_transcurridos > 3:
                    color_borde = "#FF9900"  # Naranja: moderadamente antiguo
                    antiguedad_icono = "📅"
                else:
                    color_borde = "#4CAF50"  # Verde: reciente
                    antiguedad_icono = "🆕"

                # Determinar color según prioridad
                color_prioridad = {
                    'Alta': '#FF6B6B',
                    'Media': '#FFD166',
                    'Baja': '#4ECDC4'
                }.get(ticket['prioridad'], '#CCCCCC')

                # Determinar icono según estado
                icono_estado = {
                    'Abierto': '📥',
                    'En Progreso': '⚙️',
                    'Resuelto': '✅',
                    'Cerrado': '🔒'
                }.get(ticket['estado'], '📋')

                # Crear expander con información resumida en el título
                with st.expander(
                        f"{icono_estado} #{ticket['ticket_id']}: {ticket['titulo']} | "
                        f"👤 {ticket['usuario']} | 🏷️ {ticket['categoria']} | "
                        f"🚨 {ticket['prioridad']} | {antiguedad_icono} {dias_transcurridos}d"
                ):
                    # Contenido del expander con borde izquierdo
                    left_border, content = st.columns([0.02, 0.98])

                    with left_border:
                        st.markdown(
                            f'<div style="background-color: {color_borde}; height: 100%; width: 100%; border-radius: 5px;"></div>',
                            unsafe_allow_html=True
                        )

                    with content:
                        # Información principal en columnas
                        col_info1, col_info2 = st.columns(2)

                        with col_info1:
                            st.markdown(f"**📅 Fecha creación:** {fecha_creacion.strftime('%d/%m/%Y %H:%M')}")
                            st.markdown(f"**👤 Creado por:** {ticket['usuario']}")
                            st.markdown(f"**🏷️ Categoría:** {ticket['categoria']}")

                            # Prioridad con badge de color
                            st.markdown(f"**🚨 Prioridad:**")
                            st.markdown(
                                f'<span style="background-color: {color_prioridad}; color: white; padding: 4px 12px; border-radius: 15px; font-weight: bold;">{ticket["prioridad"]}</span>',
                                unsafe_allow_html=True
                            )

                        with col_info2:
                            st.markdown(f"**📊 Estado:** {ticket['estado']}")
                            st.markdown(f"**👥 Asignado a:** {ticket['asignado_a'] or 'Sin asignar'}")
                            st.markdown(f"**🎫 ID Ticket:** #{ticket['ticket_id']}")
                            st.markdown(f"**⏳ Antigüedad:** {dias_transcurridos} días")

                        # Pestañas para descripción y comentarios
                        tab_desc, tab_com, tab_acc = st.tabs(["📄 Descripción", "💬 Comentarios", "🔧 Acciones"])

                        with tab_desc:
                            st.markdown("**Descripción original:**")
                            st.info(ticket['descripcion'])

                        with tab_com:
                            if ticket['comentarios'] and str(ticket['comentarios']).strip():
                                st.markdown("**Historial de comentarios:**")
                                # Dividir comentarios por saltos de línea dobles
                                comentarios = str(ticket['comentarios']).split('\n\n')
                                for comentario in comentarios:
                                    if comentario.strip():
                                        # Formatear cada comentario
                                        lines = comentario.strip().split('\n')
                                        if len(lines) >= 2:
                                            fecha_line = lines[0]
                                            contenido = '\n'.join(lines[1:])

                                            # Detectar tipo de comentario por formato
                                            if 'INTERNO' in fecha_line:
                                                st.warning(f"**{fecha_line}**\n{contenido}")
                                            elif 'cliente' in fecha_line.lower():
                                                st.info(f"**{fecha_line}**\n{contenido}")
                                            else:
                                                st.success(f"**{fecha_line}**\n{contenido}")
                            else:
                                st.info("No hay comentarios aún.")

                        with tab_acc:
                            st.markdown("**Acciones disponibles:**")

                            # Cambiar estado
                            col_acc1, col_acc2 = st.columns(2)
                            with col_acc1:
                                nuevo_estado = st.selectbox(
                                    "Cambiar estado:",
                                    ["Abierto", "En Progreso", "Resuelto", "Cerrado"],
                                    key=f"estado_{ticket['ticket_id']}",
                                    index=0 if ticket['estado'] == 'Abierto' else
                                    1 if ticket['estado'] == 'En Progreso' else
                                    2 if ticket['estado'] == 'Resuelto' else 3
                                )

                            with col_acc2:
                                if st.button("🔄 Actualizar estado",
                                             key=f"btn_estado_{ticket['ticket_id']}",
                                             use_container_width=True):
                                    if nuevo_estado != ticket['estado']:
                                        actualizar_estado_ticket(ticket['ticket_id'], nuevo_estado)
                                        st.success(f"✅ Estado cambiado a '{nuevo_estado}'")
                                        st.rerun()

                            st.markdown("---")

                            # Asignar a técnico
                            col_asig1, col_asig2 = st.columns(2)
                            with col_asig1:
                                try:
                                    conn = obtener_conexion()
                                    tecnicos = pd.read_sql("""
                                        SELECT id, username FROM usuarios 
                                        WHERE role IN ('admin', 'tecnico', 'agent', 'soporte')
                                        ORDER BY username
                                    """, conn)
                                    conn.close()

                                    if not tecnicos.empty:
                                        opciones_tecnicos = ["Seleccionar..."] + tecnicos['username'].tolist()
                                        tecnico_seleccionado = st.selectbox(
                                            "Asignar a técnico:",
                                            options=opciones_tecnicos,
                                            key=f"asignar_{ticket['ticket_id']}"
                                        )
                                except:
                                    st.warning("No se pudo cargar lista de técnicos")
                                    tecnico_seleccionado = "Seleccionar..."

                            with col_asig2:
                                if st.button("👤 Asignar ticket",
                                             key=f"btn_asignar_{ticket['ticket_id']}",
                                             use_container_width=True,
                                             disabled=tecnico_seleccionado == "Seleccionar..."):
                                    try:
                                        id_tecnico = tecnicos[tecnicos['username'] == tecnico_seleccionado]['id'].iloc[
                                            0]
                                        conn = obtener_conexion()
                                        cursor = conn.cursor()
                                        cursor.execute("""
                                            UPDATE tickets 
                                            SET asignado_a = ?, estado = 'En Progreso' 
                                            WHERE ticket_id = ?
                                        """, (id_tecnico, ticket['ticket_id']))

                                        # Añadir comentario
                                        cursor.execute("""
                                            UPDATE tickets 
                                            SET comentarios = COALESCE(comentarios || '\n\n', '') || ?
                                            WHERE ticket_id = ?
                                        """, (
                                            f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] {st.session_state['username']} asignó el ticket a {tecnico_seleccionado}.",
                                            ticket['ticket_id']
                                        ))

                                        conn.commit()
                                        conn.close()

                                        log_trazabilidad(
                                            st.session_state["username"],
                                            "Asignación de ticket",
                                            f"Asignó ticket #{ticket['ticket_id']} a {tecnico_seleccionado}"
                                        )

                                        st.success(f"✅ Ticket asignado a {tecnico_seleccionado}")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Error al asignar: {str(e)[:100]}")

                            st.markdown("---")

                            # Comentario rápido
                            st.markdown("**💬 Añadir comentario rápido:**")
                            comentario_rapido = st.text_area(
                                "Comentario:",
                                placeholder="Escribe un comentario...",
                                height=80,
                                key=f"com_rap_{ticket['ticket_id']}"
                            )

                            if st.button("📝 Enviar comentario",
                                         key=f"btn_com_{ticket['ticket_id']}",
                                         use_container_width=True):
                                if comentario_rapido.strip():
                                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
                                    usuario = st.session_state.get("username", "Administrador")

                                    comentario_formateado = f"\n\n[{timestamp}] {usuario} [Comentario]:\n{comentario_rapido.strip()}"

                                    conn = obtener_conexion()
                                    cursor = conn.cursor()
                                    cursor.execute("""
                                        UPDATE tickets 
                                        SET comentarios = COALESCE(comentarios || ?, ?)
                                        WHERE ticket_id = ?
                                    """, (
                                        comentario_formateado,
                                        f"[{timestamp}] {usuario} [Comentario]:\n{comentario_rapido.strip()}",
                                        ticket['ticket_id']
                                    ))
                                    conn.commit()
                                    conn.close()

                                    log_trazabilidad(
                                        usuario,
                                        "Comentario en ticket",
                                        f"Añadió comentario al ticket #{ticket['ticket_id']}"
                                    )

                                    st.success("✅ Comentario añadido")
                                    st.rerun()

        # --- ACCIONES GENERALES ---

        col_acc1, col_acc2, col_acc3 = st.columns(3)
        with col_acc1:
            if st.button("📥 Exportar a Excel", use_container_width=True):
                # Crear archivo Excel
                output = BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_filtrado.to_excel(writer, sheet_name='Tickets', index=False)
                output.seek(0)

                st.download_button(
                    label="⬇️ Descargar archivo Excel",
                    data=output,
                    file_name=f"tickets_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )

        with col_acc2:
            if st.button("🔄 Actualizar Vista", use_container_width=True):
                st.rerun()

        with col_acc3:
            if st.button("➕ Crear Nuevo Ticket", type="primary", use_container_width=True):
                # Redirigir a función de creación de tickets
                # Dependiendo de tu implementación, podrías:
                # 1. Cambiar a otra sección
                # 2. Mostrar un formulario modal
                # 3. Redirigir a la función de creación
                st.session_state["mostrar_crear_ticket"] = True
                st.rerun()

    except Exception as e:
        st.error(f"⚠️ Error al cargar tickets: {str(e)[:200]}")
        st.info("""
        **Solución:** 
        1. Verifica que la tabla 'tickets' existe en la base de datos
        2. Asegúrate de que la función 'obtener_conexion()' funciona correctamente
        3. Comprueba que la tabla 'usuarios' existe y tiene los campos 'id' y 'username'
        """)

    except Exception as e:
        st.toast(f"⚠️ Error al cargar tickets: {str(e)[:200]}")
        st.info("""
        **Solución:** 
        1. Verifica que la tabla 'tickets' existe en la base de datos
        2. Asegúrate de que la función 'obtener_conexion()' funciona correctamente
        """)

def mostrar_mis_tickets():
    """Muestra los tickets del usuario actual."""

    # Obtener user_id del usuario actual (ajusta según tu sistema)
    user_id = st.session_state.get("user_id", 1)  # Temporal: ajusta con tu variable real

    # Cabecera específica para Mis Tickets
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("➕ Nuevo Ticket", type="primary", use_container_width=True):
            st.session_state["crear_nuevo_ticket"] = True

    # Si el usuario quiere crear un nuevo ticket
    if st.session_state.get("crear_nuevo_ticket", False):
        crear_nuevo_ticket_form(user_id)
        return

    # Mostrar tabla de tickets del usuario
    try:
        conn = obtener_conexion()

        # Consulta para obtener tickets del usuario
        query = """
        SELECT 
            ticket_id,
            fecha_creacion,
            categoria,
            prioridad,
            estado,
            asignado_a,
            titulo,
            descripcion
        FROM tickets 
        WHERE usuario_id = ?
        ORDER BY fecha_creacion DESC
        """

        df_tickets = pd.read_sql(query, conn, params=(user_id,))
        conn.close()

        if df_tickets.empty:
            st.toast("🎉 No tienes tickets reportados. ¡Todo está en orden!")
            st.info("Usa el botón '➕ Nuevo Ticket' para reportar un problema o solicitar ayuda.")
            return

        # Mostrar métricas rápidas
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Tickets", len(df_tickets))
        with col2:
            abiertos = len(df_tickets[df_tickets['estado'] == 'Abierto'])
            st.metric("Abiertos", abiertos, delta_color="inverse")
        with col3:
            en_progreso = len(df_tickets[df_tickets['estado'] == 'En Progreso'])
            st.metric("En Progreso", en_progreso)
        with col4:
            resueltos = len(df_tickets[df_tickets['estado'].isin(['Resuelto', 'Cancelado'])])
            st.metric("Resueltos", resueltos)

        # Mostrar tabla de tickets
        st.dataframe(
            df_tickets.rename(columns={
                'ticket_id': 'ID',
                'fecha_creacion': 'Creado',
                'categoria': 'Categoría',
                'prioridad': 'Prioridad',
                'estado': 'Estado',
                'titulo': 'Título'
            }),
            column_config={
                'ID': st.column_config.NumberColumn(width='small'),
                'Creado': st.column_config.DatetimeColumn(format='DD/MM/YY HH:mm'),
                'Prioridad': st.column_config.TextColumn(
                    help="🚨 Alta - 🔸 Media - 🔹 Baja"
                ),
                'Estado': st.column_config.TextColumn(
                    help="🟢 Abierto - 🟡 En Progreso - 🔴 Cancelado"
                )
            },
            use_container_width=True,
            hide_index=True
        )

    except Exception as e:
        st.toast(f"⚠️ Error al cargar tickets: {str(e)[:200]}")
        st.info("""
        **Posibles soluciones:**
        1. La tabla 'tickets' no existe en la base de datos
        2. El user_id no está correctamente configurado
        3. Problema de conexión con la base de datos
        """)

        # Mostrar datos de ejemplo para visualización
        with st.expander("📊 Ver datos de ejemplo"):
            datos_ejemplo = pd.DataFrame({
                "ID": [1001, 1002, 1003],
                "Título": ["Error en gráfico", "Consulta de datos", "Solicitud de acceso"],
                "Estado": ["En Progreso", "Abierto", "Resuelto"],
                "Prioridad": ["Alta", "Media", "Baja"],
                "Creado": ["2025-01-15", "2025-01-14", "2025-01-10"],
                "Asignado a": ["Soporte Técnico", "Pendiente", "Administrador"]
            })
            st.dataframe(datos_ejemplo, use_container_width=True)


def crear_nuevo_ticket_form(user_id):
    """Formulario para crear un nuevo ticket con opción de asignación."""

    st.subheader("✨ Crear Nuevo Ticket")

    # Obtener lista de agentes para asignación (solo si el usuario es admin)
    es_admin = st.session_state.get("role") == "admin"
    lista_agentes = []

    if es_admin:
        try:
            conn = obtener_conexion()
            # Obtener usuarios con roles de administrador o agente
            agentes_df = pd.read_sql("""
                SELECT id, username, role 
                FROM usuarios 
                WHERE role IN ('admin', 'agent', 'soporte', 'tecnico', 'support')
                OR role LIKE '%admin%' 
                OR role LIKE '%soporte%'
                ORDER BY username
            """, conn)
            conn.close()

            if not agentes_df.empty:
                lista_agentes = agentes_df[['id', 'username']].to_dict('records')
        except Exception as e:
            st.warning(f"No se pudieron cargar los agentes: {str(e)[:100]}")

    with st.form("nuevo_ticket_form", clear_on_submit=True):
        titulo = st.text_input(
            "📝 **Título del ticket** *",
            placeholder="Ej: Error al exportar reporte PDF",
            help="Describe brevemente el problema o solicitud"
        )

        col_cat, col_pri = st.columns(2)
        with col_cat:
            categoria = st.selectbox(
                "🏷️ **Categoría** *",
                ["Soporte Técnico", "Error/Bug", "Consulta",
                 "Solicitud de acceso", "Mejora", "Otro"]
            )

        with col_pri:
            prioridad = st.selectbox(
                "🚨 **Prioridad** *",
                ["Alta", "Media", "Baja"],
                index=1,
                help="Alta = Urgente, Media = Normal, Baja = Baja urgencia"
            )

        # Opción de asignación solo para administradores
        asignado_id = None
        if es_admin and lista_agentes:
            opciones_asignacion = ["Sin asignar"] + [f"{agente['username']} (ID: {agente['id']})" for agente in
                                                     lista_agentes]
            asignacion_seleccionada = st.selectbox(
                "👤 **Asignar a (opcional):**",
                options=opciones_asignacion,
                index=0,
                help="Selecciona un agente para asignar el ticket inmediatamente"
            )

            if asignacion_seleccionada != "Sin asignar":
                # Extraer el ID del agente seleccionado
                try:
                    asignado_id = int(asignacion_seleccionada.split("ID: ")[1].replace(")", ""))
                except:
                    asignado_id = None

        descripcion = st.text_area(
            "📄 **Descripción detallada** *",
            placeholder="""Describe el problema o solicitud con el mayor detalle posible:

• ¿Qué pasó exactamente?
• ¿Cuándo ocurrió? (Fecha y hora aproximada)
• ¿Qué esperabas que sucediera?
• ¿Qué pasó en su lugar?
• Pasos para reproducir el problema (si aplica):
  1. 
  2. 
  3. 

Información adicional (sistema operativo, navegador, versión de la app, etc.):""",
            height=250
        )

        # Mostrar información del usuario que crea el ticket
        with st.expander("ℹ️ Información del ticket"):
            usuario_actual = st.session_state.get("username", "Usuario")
            st.write(f"**Creado por:** {usuario_actual}")
            st.write(f"**ID de usuario:** {user_id}")
            st.write(f"**Fecha creación:** {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
            if asignado_id:
                agente_nombre = next((a['username'] for a in lista_agentes if a['id'] == asignado_id), "Desconocido")
                st.write(f"**Asignado a:** {agente_nombre}")

        st.markdown("**Campos obligatorios**")

        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            enviar = st.form_submit_button("✅ **Crear Ticket**", type="primary", use_container_width=True)
        with col2:
            cancelar = st.form_submit_button("❌ **Cancelar**", use_container_width=True)

        if cancelar:
            st.session_state["crear_nuevo_ticket"] = False
            st.rerun()

        if enviar:
            if not titulo or not descripcion:
                st.toast("⚠️ Por favor, completa todos los campos obligatorios (*)")
            else:
                try:
                    conn = obtener_conexion()
                    cursor = conn.cursor()

                    # Si hay asignación, determinar el estado
                    estado_inicial = "En Progreso" if asignado_id else "Abierto"

                    if asignado_id:
                        # Crear ticket con asignación
                        cursor.execute("""
                            INSERT INTO tickets 
                            (usuario_id, categoria, prioridad, estado, asignado_a, titulo, descripcion)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (
                            user_id,
                            categoria,
                            prioridad,
                            estado_inicial,
                            asignado_id,
                            titulo,
                            descripcion
                        ))
                    else:
                        # Crear ticket sin asignación
                        cursor.execute("""
                            INSERT INTO tickets 
                            (usuario_id, categoria, prioridad, estado, titulo, descripcion)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            user_id,
                            categoria,
                            prioridad,
                            estado_inicial,
                            titulo,
                            descripcion
                        ))

                    conn.commit()
                    ticket_id = cursor.lastrowid

                    # Si se asignó, actualizar para registrar quién asignó
                    if asignado_id:
                        cursor.execute("""
                            UPDATE tickets 
                            SET comentarios = ? 
                            WHERE ticket_id = ?
                        """, (
                            f"Asignado automáticamente a ID {asignado_id} por {st.session_state['username']} al crear el ticket.",
                            ticket_id
                        ))
                        conn.commit()

                    conn.close()

                    # Registrar en trazabilidad
                    log_trazabilidad(
                        st.session_state["username"],
                        "Creación de ticket",
                        f"Creó el ticket #{ticket_id}: '{titulo}' (Prioridad: {prioridad}, Estado: {estado_inicial})"
                    )

                    # Mostrar mensaje de éxito con detalles
                    st.toast(f"✅ **Ticket #{ticket_id} creado correctamente**")

                    # Mostrar resumen
                    with st.expander("📋 Ver resumen del ticket creado", expanded=True):
                        col_res1, col_res2 = st.columns(2)
                        with col_res1:
                            st.write(f"**ID:** #{ticket_id}")
                            st.write(f"**Título:** {titulo}")
                            st.write(f"**Categoría:** {categoria}")
                            st.write(f"**Prioridad:** {prioridad}")

                        with col_res2:
                            st.write(f"**Estado:** {estado_inicial}")
                            st.write(f"**Creado por:** {st.session_state.get('username')}")
                            if asignado_id:
                                agente_nombre = next((a['username'] for a in lista_agentes if a['id'] == asignado_id),
                                                     "Desconocido")
                                st.write(f"**Asignado a:** {agente_nombre}")
                            st.write(f"**Fecha:** {datetime.now().strftime('%d/%m/%Y %H:%M')}")

                except Exception as e:
                    error_msg = str(e)
                    st.toast(f"⚠️ Error al crear ticket: {error_msg[:200]}")

                    # Diagnóstico del error
                    with st.expander("🔍 Ver detalles del error"):
                        st.code(error_msg, language='python')

                        if "no such table" in error_msg.lower():
                            st.toast("""
                            **ERROR CRÍTICO: La tabla 'tickets' no existe.**

                            **Solución:**
                            1. Ejecuta este SQL en tu base de datos:
                            ```sql
                            CREATE TABLE IF NOT EXISTS tickets (
                                ticket_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                                usuario_id INTEGER NOT NULL,
                                categoria TEXT NOT NULL,
                                prioridad TEXT CHECK(prioridad IN ('Alta', 'Media', 'Baja')) DEFAULT 'Media',
                                estado TEXT CHECK(estado IN ('Abierto', 'En Progreso', 'Resuelto', 'Cancelado')) DEFAULT 'Abierto',
                                asignado_a INTEGER,
                                titulo TEXT NOT NULL,
                                descripcion TEXT NOT NULL,
                                comentarios TEXT,
                                FOREIGN KEY (usuario_id) REFERENCES usuarios(id),
                                FOREIGN KEY (asignado_a) REFERENCES usuarios(id)
                            );
                            ```

                            2. O usa el botón de creación de tabla en la sección de administración.
                            """)


# Mantén estas funciones sin cambios (las que ya tenías):
def user_ticketing_panel():
    """Panel simplificado para que los usuarios vean/creen sus tickets."""
    st.title("🎫 Mis Tickets")
    st.info("""
    **Vista de usuario final:**
    - ✨ **Crear nuevo ticket**: Formulario simple para reportar problemas
    - 📋 **Ver mis tickets**: Solo los tickets creados por el usuario actual
    - 🔄 **Seguimiento**: Ver estado y comentarios de mis tickets
    """)
    st.warning("⏳ Funcionalidad en desarrollo. Próximamente disponible.")


def crear_ticket_ejemplo():
    """Función temporal para crear un ticket de prueba."""
    try:
        conn = obtener_conexion()
        cursor = conn.cursor()

        # Insertar ticket de ejemplo
        cursor.execute("""
            INSERT INTO tickets 
            (usuario_id, categoria, prioridad, estado, titulo, descripcion)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            1,  # ID de usuario de ejemplo
            "Soporte Técnico",
            "Media",
            "Abierto",
            "Problema de integración con Streamlit",
            "Necesito ayuda para integrar el sistema de ticketing en el dashboard existente."
        ))

        conn.commit()
        conn.close()

        st.toast("✅ Ticket de ejemplo creado correctamente")

    except Exception as e:
        st.toast(f"⚠️ Error al crear ticket de ejemplo: {str(e)[:100]}")

# Función principal de la app (Dashboard de administración)
def admin_dashboard():
    """Panel del administrador."""
    controller = CookieController(key="cookies")
    st.markdown(
        """
        <style>
        .footer {
            position: fixed;
            left: 0;
            bottom: 0;
            width: 100%;
            background-color: #F7FBF9;
            color: black;
            text-align: center;
            padding: 8px 0;
            font-size: 14px;
            font-family: 'Segoe UI', sans-serif;
            z-index: 999;
        }
        </style>
        <div class="footer">
            <p>© 2025 Verde tu operador · Desarrollado para uso interno</p>
        </div>
        """,
        unsafe_allow_html=True
    )

    # Sidebar con opción de menú más moderno
    with st.sidebar:
        st.sidebar.markdown("""
            <style>
                .user-circle {
                    width: 100px;
                    height: 100px;
                    border-radius: 50%;
                    background-color: #0073e6;
                    color: white;
                    font-size: 50px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    margin: 0 auto 10px auto;
                    text-align: center;
                }
                .user-info {
                    text-align: center;
                    font-size: 16px;
                    color: #333;
                    margin-bottom: 10px;
                }
                .welcome-msg {
                    text-align: center;
                    font-weight: bold;
                    font-size: 18px;
                    margin-top: 0;
                }
            </style>

            <div class="user-circle">👤</div>
            <div class="user-info">Rol: Administrador</div>
            <div class="welcome-msg">¡Bienvenido, <strong>{username}</strong>!</div>
            <hr>
            """.replace("{username}", st.session_state['username']), unsafe_allow_html=True)

        opcion = option_menu(
            menu_title=None,
            options=[
                "Home", "Ver Datos", "Ofertas Comerciales", "Viabilidades",
                "Mapa UUIIs", "Cargar Nuevos Datos", "Generar Informe", "CDRs",
                "Trazabilidad y logs", "Gestionar Usuarios", "Anuncios",
                "Control de versiones", "Sistema de Ticketing"  # Nuevas opciones
            ],
            icons=[
                "house", "graph-up", "bar-chart", "check-circle", "globe", "upload",
                "file-earmark-text", "journal-text", "journal-text", "people", "megaphone",
                "arrow-clockwise", "ticket"  # Nuevos iconos
            ],
            menu_icon="list",
            default_index=0,
            styles={
                "container": {
                    "padding": "0px",
                    "background-color": "#F0F7F2",  # Coincide con secondaryBackgroundColor
                    "border-radius": "0px",
                },
                "icon": {
                    "color": "#2C5A2E",  # Verde oscuro
                    "font-size": "18px"
                },
                "nav-link": {
                    "color": "#2C5A2E",
                    "font-size": "16px",
                    "text-align": "left",
                    "margin": "0px",
                    "--hover-color": "#66B032",
                    "border-radius": "0px",
                },
                "nav-link-selected": {
                    "background-color": "#66B032",  # Verde principal de marca
                    "color": "white",  # Contraste
                    "font-weight": "bold"
                }
            }
        )

        # Registrar la selección de la opción en trazabilidad
        log_trazabilidad(st.session_state["username"], "Selección de opción", f"El admin seleccionó la opción '{opcion}'.")

        # Botón de Cerrar sesión en la barra lateral
        with st.sidebar:
            if st.button("Cerrar sesión"):
                detalles = f"El administrador {st.session_state.get('username', 'N/A')} cerró sesión."
                log_trazabilidad(st.session_state.get("username", "N/A"), "Cierre sesión", detalles)

                # Establecer la expiración de las cookies en el pasado para forzar su eliminación
                controller.set(f'{cookie_name}_session_id', '', max_age=0, expires=datetime(1970, 1, 1))
                controller.set(f'{cookie_name}_username', '', max_age=0, expires=datetime(1970, 1, 1))
                controller.set(f'{cookie_name}_role', '', max_age=0, expires=datetime(1970, 1, 1))

                # Reiniciar el estado de sesión
                st.session_state["login_ok"] = False
                st.session_state["username"] = ""
                st.session_state["role"] = ""
                st.session_state["session_id"] = ""

                st.toast("✅ Has cerrado sesión correctamente. Redirigiendo al login...")
                # Limpiar parámetros de la URL
                st.query_params.clear()  # Limpiamos la URL (opcional, si hay parámetros en la URL)
                st.rerun()

    # Opción: Visualizar datos de la tabla datos_uis
    if opcion == "Home":
        home_page()
    # AÑADE ESTAS DOS NUEVAS OPCIONES:
    elif opcion == "Sistema de Ticketing":
        admin_ticketing_panel()
    elif opcion == "Ver Datos":

        sub_seccion = option_menu(
            menu_title=None,  # Sin título encima del menú
            options=["Visualizar Datos UIS", "Seguimiento de Contratos", "Precontratos","TIRC"],
            icons=["table", "file-earmark-spreadsheet", "file-text", "puzzle"],  # Puedes cambiar iconos
            default_index=0,
            orientation="horizontal",  # horizontal para que quede tipo pestañas arriba
            styles={
                "container": {
                    "padding": "0!important",
                    "margin": "0px",
                    "background-color": "#F0F7F2",
                    "border-radius": "0px",
                    "max-width":"none"
                },
                "icon": {
                    "color": "#2C5A2E",  # Íconos en verde oscuro
                    "font-size": "25px"
                },
                "nav-link": {
                    "color": "#2C5A2E",
                    "font-size": "18px",
                    "text-align": "center",
                    "margin": "0px",
                    "--hover-color": "#66B032",
                    "border-radius": "0px",
                },
                "nav-link-selected": {
                    "background-color": "#66B032",  # Verde principal corporativo
                    "color": "white",
                    "font-weight": "bold"
                }
            }
        )
        if sub_seccion == "Visualizar Datos UIS":
            st.info(
                "ℹ️ Aquí puedes visualizar, filtrar y descargar los datos UIS, Viabilidades y Contratos en formato Excel.")

            if "df" in st.session_state:
                del st.session_state["df"]

            # FUNCIÓN DE NORMALIZACIÓN SIMPLIFICADA
            def normalizar_apartment_id(valor):
                """Convierte cualquier valor a formato P + 10 dígitos"""
                if pd.isna(valor) or valor is None or valor == "":
                    return None

                # Convertir a string y limpiar
                str_valor = str(valor).strip().upper()

                # Extraer solo números
                numeros = ''.join(filter(str.isdigit, str_valor))

                if not numeros:
                    return str_valor

                # Asegurar 10 dígitos (rellenar con ceros a la izquierda)
                numeros_10 = numeros.zfill(10)

                # Retornar en formato P + 10 dígitos
                return f"P{numeros_10}"

            @st.cache_data(ttl=300)
            def cargar_datos():
                """Carga todos los datos de la base de datos (solo columnas necesarias)"""
                try:
                    conn = obtener_conexion()

                    # datos_uis: columnas usadas
                    df_uis = pd.read_sql("""
                        SELECT apartment_id, provincia, municipio, poblacion, id_ams, address_id, olt, cto, latitud, longitud
                        FROM datos_uis
                    """, conn)
                    df_uis["apartment_id_normalizado"] = df_uis["apartment_id"].apply(normalizar_apartment_id)
                    df_uis["fuente"] = "UIS"

                    # viabilidades: columnas necesarias (incluye ticket, usuario, serviciable, coste, lat, lon)
                    df_via = pd.read_sql("""
                        SELECT apartment_id, ticket, provincia, municipio, poblacion, usuario, serviciable, coste, latitud, longitud
                        FROM viabilidades
                    """, conn)
                    # Expandir múltiples IDs
                    df_via_exp = df_via.assign(
                        apartment_id=df_via['apartment_id'].str.split(',')
                    ).explode('apartment_id')
                    df_via_exp['apartment_id'] = df_via_exp['apartment_id'].str.strip()
                    df_via = df_via_exp[df_via_exp['apartment_id'] != ''].copy()
                    df_via["apartment_id_normalizado"] = df_via["apartment_id"].apply(normalizar_apartment_id)
                    df_via["fuente"] = "Viabilidad"

                    # seguimiento_contratos: columnas necesarias
                    df_contratos = pd.read_sql("""
                        SELECT apartment_id, num_contrato, cliente, estado, tecnico, comercial, divisor, puerto
                        FROM seguimiento_contratos
                    """, conn)
                    df_contratos["apartment_id_normalizado"] = df_contratos["apartment_id"].apply(
                        normalizar_apartment_id)
                    df_contratos["fuente"] = "Contrato"

                    # TIRC: columnas necesarias
                    df_tirc = pd.read_sql("""
                        SELECT apartment_id, provincia, municipio, poblacion, address_id, OLT, CTO
                        FROM TIRC
                    """, conn)
                    df_tirc["apartment_id_normalizado"] = df_tirc["apartment_id"].apply(normalizar_apartment_id)
                    df_tirc["fuente"] = "TIRC"

                    conn.close()

                    return {
                        "uis": df_uis,
                        "via": df_via,
                        "contratos": df_contratos,
                        "tirc": df_tirc
                    }

                except Exception as e:
                    st.toast(f"❌ Error al cargar datos: {str(e)[:200]}")
                    return None

            # CARGAR DATOS
            with st.spinner("🔄 Cargando datos..."):
                datos = cargar_datos()
                if not datos:
                    st.stop()

            # CREAR TABLA MAESTRA SIMPLE
            st.toast("🔄 Creando tabla maestra...")

            # Recolectar todos los IDs únicos
            todos_ids = set()
            for nombre, df in datos.items():
                if 'apartment_id_normalizado' in df.columns:
                    ids_validos = df['apartment_id_normalizado'].dropna().unique()
                    todos_ids.update(ids_validos)

            # Crear tabla maestra
            tabla_maestra = pd.DataFrame({'apartment_id_normalizado': list(todos_ids)})

            # FUNCIÓN PARA UNIR DATOS DE FORMA SEGURA
            def agregar_datos(tabla_base, df, prefijo=""):
                """Agrega datos de una fuente a la tabla base"""
                if df.empty:
                    return tabla_base

                # Seleccionar columnas relevantes (excluyendo las que no necesitamos)
                columnas_excluir = ['id', 'created_at', 'updated_at', 'apartment_id']
                columnas_usar = [c for c in df.columns if c not in columnas_excluir and c != 'apartment_id_normalizado']

                # Agrupar por ID normalizado
                df_agrupado = df.groupby('apartment_id_normalizado')[columnas_usar].first().reset_index()

                # Renombrar columnas con prefijo
                if prefijo:
                    rename_dict = {col: f"{prefijo}_{col}" for col in columnas_usar}
                    df_agrupado = df_agrupado.rename(columns=rename_dict)

                # Unir
                return pd.merge(tabla_base, df_agrupado, on='apartment_id_normalizado', how='left')

            # AGREGAR DATOS DE CADA FUENTE
            st.toast("🔄 Integrando datos...")

            # Agregar cada fuente con prefijo único
            tabla_maestra = agregar_datos(tabla_maestra, datos['uis'], "uis")
            tabla_maestra = agregar_datos(tabla_maestra, datos['via'], "via")
            tabla_maestra = agregar_datos(tabla_maestra, datos['tirc'], "tirc")
            tabla_maestra = agregar_datos(tabla_maestra, datos['contratos'], "cto")

            # --- ANÁLISIS DETALLADO DE CRUCES ---
            st.toast("🔍 Analizando cruces con precisión...")

            # Análisis detallado de contratos
            df_contratos = datos['contratos'].copy()
            total_contratos = len(df_contratos)

            # 1. Contratos con apartment_id válido
            contratos_con_id = df_contratos['apartment_id_normalizado'].notna().sum()
            contratos_sin_id = df_contratos['apartment_id_normalizado'].isna().sum()

            # 2. IDs únicos de contratos (sin duplicados)
            ids_contratos_unicos = set(df_contratos['apartment_id_normalizado'].dropna().unique())
            num_ids_unicos = len(ids_contratos_unicos)

            # 3. IDs únicos en la tabla maestra
            ids_maestra_unicos = set(tabla_maestra['apartment_id_normalizado'].unique())

            # 4. Contratos cuyos IDs están en la tabla maestra
            df_contratos['en_maestra'] = df_contratos['apartment_id_normalizado'].isin(ids_maestra_unicos)
            contratos_en_maestra = df_contratos['en_maestra'].sum()

            # 5. IDs de contratos que NO están en la tabla maestra
            ids_contratos_no_en_maestra = ids_contratos_unicos - ids_maestra_unicos
            num_ids_no_en_maestra = len(ids_contratos_no_en_maestra)

            # 6. Contratos que tienen IDs que NO están en la tabla maestra
            contratos_con_id_no_en_maestra = df_contratos[
                df_contratos['apartment_id_normalizado'].isin(ids_contratos_no_en_maestra)
            ].shape[0]

            # 7. Calcular eficiencia real
            contratos_cruzados_real = contratos_en_maestra
            contratos_no_cruzados_real = total_contratos - contratos_cruzados_real
            eficiencia_real = (contratos_cruzados_real / total_contratos * 100) if total_contratos > 0 else 0

            # MOSTRAR MÉTRICAS DETALLADAS
            col1, col2, col3, col4, col5, col6, col7, col8 = st.columns(8)
            col1.metric("📋 Total contratos", total_contratos)
            col2.metric("✅ Con ID válido", contratos_con_id)
            col3.metric("⚠️ Sin ID válido", contratos_sin_id)
            col4.metric("🔢 IDs únicos", num_ids_unicos)
            col5.metric("🎯 Contratos en maestra", contratos_en_maestra)
            col6.metric("❌ Contratos NO en maestra", contratos_no_cruzados_real)
            col7.metric("📊 IDs únicos NO en maestra", num_ids_no_en_maestra)
            col8.metric("📈 Eficiencia real", f"{eficiencia_real:.1f}%")

            # ANÁLISIS DETALLADO DE CONTRATOS NO CRUZADOS

            # Mostrar contratos sin ID
            if contratos_sin_id > 0:
                st.warning(f"⚠️ {contratos_sin_id} contratos no tienen apartment_id válido")

                # Incluir las nuevas columnas en la visualización
                columnas_mostrar = ['apartment_id', 'num_contrato', 'cliente', 'estado',
                                    'TECNICO', 'COMERCIAL', 'DIVISOR', 'PUERTO']
                columnas_disponibles = [col for col in columnas_mostrar if col in df_contratos.columns]

                df_sin_id = df_contratos[df_contratos['apartment_id_normalizado'].isna()][columnas_disponibles].copy()

                with st.expander("📋 Ver contratos sin apartment_id", expanded=True):
                    st.dataframe(df_sin_id, width='stretch', height=300)

            # Mostrar contratos con ID pero no en maestra
            if num_ids_no_en_maestra > 0:
                st.toast(f"🚨 {num_ids_no_en_maestra} IDs únicos de contratos no están en la tabla maestra")
                st.toast(f"🚨 Esto afecta a {contratos_con_id_no_en_maestra} contratos")

                # Incluir las nuevas columnas
                columnas_mostrar = ['apartment_id', 'apartment_id_normalizado', 'num_contrato',
                                    'cliente', 'estado', 'TECNICO', 'COMERCIAL', 'DIVISOR', 'PUERTO']
                columnas_disponibles = [col for col in columnas_mostrar if col in df_contratos.columns]

                df_no_en_maestra = df_contratos[
                    df_contratos['apartment_id_normalizado'].isin(ids_contratos_no_en_maestra)
                ][columnas_disponibles].copy()

                with st.expander("🔍 Ver contratos con ID pero no en maestra", expanded=True):
                    st.write(f"**IDs únicos no encontrados:** {len(ids_contratos_no_en_maestra)}")
                    st.write(f"**Total contratos afectados:** {len(df_no_en_maestra)}")

                    # Mostrar los IDs únicos no encontrados (filtrar valores None)
                    st.write("**Lista de IDs no encontrados:**")
                    ids_lista = [id for id in list(ids_contratos_no_en_maestra) if id is not None]
                    if ids_lista:
                        ids_lista_sorted = sorted(ids_lista)
                        st.write(", ".join(ids_lista_sorted[:50]))  # Mostrar primeros 50

                        if len(ids_lista_sorted) > 50:
                            st.write(f"... y {len(ids_lista_sorted) - 50} más")
                    else:
                        st.write("No hay IDs válidos para mostrar")

                    # Mostrar tabla de contratos
                    st.dataframe(df_no_en_maestra, width='stretch', height=400)

                    # Descargar análisis
                    csv = df_no_en_maestra.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Descargar contratos problemáticos",
                        data=csv,
                        file_name="contratos_sin_cruce.csv",
                        mime="text/csv"
                    )

            # ANÁLISIS DE DUPLICADOS
            # Contar duplicados en apartment_id_normalizado (excluyendo None)
            duplicados_mask = df_contratos['apartment_id_normalizado'].duplicated(keep=False)
            duplicados_ids = df_contratos[duplicados_mask].copy()

            # Filtrar solo los que tienen apartment_id_normalizado no nulo
            duplicados_con_id = duplicados_ids[duplicados_ids['apartment_id_normalizado'].notna()]
            duplicados_count = len(duplicados_con_id)

            if duplicados_count > 0:
                st.warning(f"⚠️ Hay {duplicados_count} contratos con IDs duplicados")

                with st.expander("📋 Ver contratos duplicados", expanded=False):
                    # Mostrar IDs que tienen duplicados (excluir None)
                    ids_duplicados = [id for id in duplicados_con_id['apartment_id_normalizado'].unique() if
                                      id is not None]
                    st.write(f"**IDs con duplicados:** {len(ids_duplicados)}")

                    if ids_duplicados:
                        # Ordenar solo si hay elementos no nulos
                        ids_duplicados_sorted = sorted(ids_duplicados)
                        for apt_id in ids_duplicados_sorted[:20]:  # Mostrar primeros 20
                            count = df_contratos[df_contratos['apartment_id_normalizado'] == apt_id].shape[0]
                            st.write(f"- {apt_id}: {count} contratos")

                        if len(ids_duplicados_sorted) > 20:
                            st.write(f"... y {len(ids_duplicados_sorted) - 20} más")

                        # Incluir las nuevas columnas
                        columnas_mostrar = ['apartment_id', 'apartment_id_normalizado', 'num_contrato',
                                            'cliente', 'estado', 'TECNICO', 'COMERCIAL', 'DIVISOR', 'PUERTO']
                        columnas_disponibles = [col for col in columnas_mostrar if col in duplicados_con_id.columns]

                        # Mostrar tabla de duplicados
                        st.dataframe(
                            duplicados_con_id[columnas_disponibles].sort_values('apartment_id_normalizado'),
                            width='stretch',
                            height=300
                        )
                    else:
                        st.write("No hay IDs duplicados válidos para mostrar")
            else:
                st.toast("✅ No hay contratos con IDs duplicados")

            # RESUMEN FINAL
            resumen_data = {
                "Métrica": [
                    "Total de contratos",
                    "Contratos con apartment_id válido",
                    "Contratos sin apartment_id válido",
                    "IDs únicos de contratos",
                    "Contratos cuyo ID está en tabla maestra",
                    "Contratos cuyo ID NO está en tabla maestra",
                    "IDs únicos NO encontrados en tabla maestra",
                    "Contratos con IDs duplicados",
                    "Eficiencia de cruce"
                ],
                "Valor": [
                    str(total_contratos),
                    str(contratos_con_id),
                    str(contratos_sin_id),
                    str(num_ids_unicos),
                    str(contratos_en_maestra),
                    str(contratos_no_cruzados_real),
                    str(num_ids_no_en_maestra),
                    str(duplicados_count),
                    f"{eficiencia_real:.1f}%"
                ]
            }

            df_resumen = pd.DataFrame(resumen_data)
            st.dataframe(df_resumen, width='stretch', hide_index=True)

            # PREPARAR DATOS FINALES - VERSIÓN SIMPLIFICADA
            st.toast("🔄 Preparando datos finales...")

            # Crear DataFrame final
            df_final = tabla_maestra.copy()

            # Consolidar columnas comunes (priorizar UIS, luego viabilidades, luego contratos)
            columnas_consolidar = {
                'provincia': ['uis_provincia', 'via_provincia', 'tirc_provincia'],
                'municipio': ['uis_municipio', 'via_municipio', 'tirc_municipio'],
                'poblacion': ['uis_poblacion', 'via_poblacion', 'tirc_poblacion'],
                'id_ams': ['uis_id_ams', 'via_ticket'],
                'address_id': ['uis_address_id', 'tirc_address_id'],
                'olt': ['uis_olt', 'tirc_OLT'],
                'cto': ['uis_cto', 'tirc_CTO'],
                'latitud': ['uis_latitud', 'via_latitud'],
                'longitud': ['uis_longitud', 'via_longitud'],
                'solicitante': ['via_usuario'],
                'num_contrato': ['cto_num_contrato'],
                'cliente': ['cto_cliente'],
                'estado': ['cto_estado'],
                'serviciable': ['via_serviciable'],
                'coste': ['via_coste'],
                # NUEVAS COLUMNAS AÑADIDAS
                'tecnico': ['cto_TECNICO'],
                'comercial': ['cto_COMERCIAL'],
                'divisor': ['cto_DIVISOR'],
                'puerto': ['cto_PUERTO']
            }

            for col_final, fuentes in columnas_consolidar.items():
                # Filtrar fuentes que existen
                fuentes_existentes = [f for f in fuentes if f in df_final.columns]

                if fuentes_existentes:
                    # Comenzar con la primera fuente
                    df_final[col_final] = df_final[fuentes_existentes[0]]

                    # Rellenar con las demás fuentes
                    for fuente in fuentes_existentes[1:]:
                        df_final[col_final] = df_final[col_final].fillna(df_final[fuente])

            # Determinar origen
            def determinar_fuente(fila):
                fuentes = []
                if pd.notna(fila.get('uis_fuente')):
                    fuentes.append('UIS')
                if pd.notna(fila.get('via_fuente')):
                    fuentes.append('Viabilidad')
                if pd.notna(fila.get('tirc_fuente')):
                    fuentes.append('TIRC')
                if pd.notna(fila.get('cto_fuente')):
                    fuentes.append('Contrato')
                return ' + '.join(fuentes) if fuentes else 'Desconocido'

            if 'uis_fuente' in df_final.columns:
                df_final['fuente'] = df_final.apply(determinar_fuente, axis=1)

            # Seleccionar columnas para mostrar - Asegurar que las nuevas columnas estén incluidas
            columnas_base = ['apartment_id_normalizado', 'fuente']
            # Definir un orden preferido para las columnas
            columnas_preferidas = [
                'provincia', 'municipio', 'poblacion', 'id_ams', 'address_id',
                'olt', 'cto', 'latitud', 'longitud', 'solicitante',
                'num_contrato', 'cliente', 'estado', 'tecnico', 'comercial',
                'divisor', 'puerto', 'serviciable', 'coste'
            ]

            # Ordenar columnas: primero las base, luego las preferidas, luego el resto
            columnas_extra = [col for col in columnas_preferidas if col in df_final.columns]
            columnas_resto = [col for col in df_final.columns if col not in columnas_base + columnas_preferidas]

            df_final = df_final[columnas_base + columnas_extra + sorted(columnas_resto)]

            # Almacenar en session state
            st.session_state["df"] = df_final

            # Configurar AgGrid
            gb = GridOptionsBuilder.from_dataframe(df_final)
            gb.configure_default_column(
                filter=True,
                floatingFilter=True,
                sortable=True,
                resizable=True,
                minWidth=100,
                flex=1
            )

            # Ocultar columnas internas - Actualizado para incluir las nuevas
            columnas_ocultar = [
                'uis_fuente', 'via_fuente', 'tirc_fuente', 'cto_fuente',
                'cto_TECNICO', 'cto_COMERCIAL', 'cto_DIVISOR', 'cto_PUERTO'
            ]

            for col in columnas_ocultar:
                if col in df_final.columns:
                    gb.configure_column(col, hide=True)

            gridOptions = gb.build()

            # Mostrar tabla
            AgGrid(
                df_final,
                gridOptions=gridOptions,
                enable_enterprise_modules=True,
                update_mode=GridUpdateMode.NO_UPDATE,
                height=600,
                theme='alpine-dark'
            )

            # BOTONES DE EXPORTACIÓN
            # Preparar Excel para descarga
            towrite = io.BytesIO()
            with pd.ExcelWriter(towrite, engine='xlsxwriter') as writer:
                # Hoja 1: Datos consolidados
                df_final.to_excel(writer, index=False, sheet_name='Datos_Consolidados')

                # Hoja 2: Contratos sin ID (si existen)
                if contratos_sin_id > 0:
                    df_sin_id.to_excel(writer, index=False, sheet_name='Contratos_Sin_ID')

                # Hoja 3: Contratos con ID pero no en maestra (si existen)
                if num_ids_no_en_maestra > 0:
                    df_no_en_maestra.to_excel(writer, index=False, sheet_name='Contratos_Sin_Cruce')

                # Hoja 4: Resumen
                df_resumen.to_excel(writer, index=False, sheet_name='Resumen_Analisis')

            towrite.seek(0)

            col1, col2 = st.columns(2)

            with col1:
                st.download_button(
                    label="📥 Descargar Excel completo",
                    data=towrite,
                    file_name=f"datos_completos_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    width='stretch',
                    help="Incluye datos consolidados, análisis y resumen"
                )

            with col2:
                if st.button("📧 Enviar por correo", width='stretch', type="secondary"):
                    with st.spinner("Enviando Excel por correo..."):
                        try:
                            correo_excel_control(
                                destinatario="aarozamena@symtel.es",
                                bytes_excel=towrite.getvalue()
                            )
                            st.toast("✅ Correo enviado correctamente a aarozamena@symtel.es")
                        except Exception as e:
                            st.toast(f"❌ Error al enviar el correo: {str(e)}")

            st.toast("✅ Análisis completado y datos listos para exportación")



        elif sub_seccion == "Seguimiento de Contratos":
            st.info("ℹ️ Aquí puedes cargar contratos, mapear columnas, guardar en BD y sincronizar con datos UIS.")
            if st.button("🔄 Actualizar contratos"):
                with st.spinner("Cargando y guardando contratos desde Google Sheets..."):
                    try:
                        df = cargar_contratos_google()
                        df.columns = df.columns.map(lambda x: str(x).strip().lower() if x is not None else "")
                        conn = obtener_conexion()
                        cur = conn.cursor()
                        cur.execute("DELETE FROM seguimiento_contratos")
                        conn.commit()

                        total = len(df)
                        progress = st.progress(0)

                        insert_sql = '''INSERT INTO seguimiento_contratos (
                            num_contrato, cliente, coordenadas, estado, fecha_inicio_contrato, fecha_ingreso,
                            comercial, fecha_instalacion, apartment_id, fecha_fin_contrato, divisor, puerto, comentarios,
                            SAT, Tipo_cliente, tecnico, metodo_entrada, billing, permanencia
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''


                        inserted_divisor = 0
                        inserted_puerto = 0
                        inserted_fecha_fin = 0
                        inserted_sat = 0
                        inserted_tipo_cliente = 0
                        inserted_tecnico = 0
                        inserted_metodo_entrada = 0
                        inserted_billing = 0
                        inserted_permanencia = 0

                        for i, row in df.iterrows():
                            ap_id = row.get('apartment_id')
                            try:
                                padded_id = 'P' + str(int(ap_id)).zfill(10)
                            except:
                                padded_id = None
                            fecha_instalacion = row.get('fecha_instalacion')
                            fecha_fin_contrato = row.get('fecha_fin_contrato')
                            divisor = row.get('divisor')
                            puerto = row.get('puerto')
                            sat = row.get('sat')
                            tipo_cliente = row.get('tipo_cliente')
                            tecnico = row.get('tecnico')
                            metodo_entrada = row.get('metodo_entrada')
                            billing = row.get('billing')
                            permanencia = row.get('permanencia')


                            if divisor not in (None, ''): inserted_divisor += 1
                            if puerto not in (None, ''): inserted_puerto += 1
                            if fecha_fin_contrato not in (None, ''): inserted_fecha_fin += 1
                            if sat not in (None, ''): inserted_sat += 1
                            if tipo_cliente not in (None, ''): inserted_tipo_cliente += 1
                            if tecnico not in (None, ''): inserted_tecnico += 1
                            if metodo_entrada not in (None, ''): inserted_metodo_entrada += 1
                            if billing not in (None, ''): inserted_billing += 1
                            if permanencia not in (None, ''): inserted_permanencia += 1


                            try:
                                cur.execute(insert_sql, (
                                    row.get('num_contrato'),
                                    row.get('cliente'),
                                    row.get('coordenadas'),
                                    row.get('estado'),
                                    row.get('fecha_inicio_contrato'),
                                    row.get('fecha_ingreso'),
                                    row.get('comercial'),
                                    fecha_instalacion,
                                    padded_id,
                                    fecha_fin_contrato,
                                    divisor,
                                    puerto,
                                    row.get('comentarios'),
                                    sat,
                                    tipo_cliente,
                                    tecnico,
                                    metodo_entrada,
                                    billing,
                                    permanencia
                                ))

                            except Exception as e:
                                st.toast(f"⚠️ Error al insertar fila {i}: {e}")
                            progress.progress((i + 1) / total)
                        conn.commit()


                        st.info(f"📊 Divisores: {inserted_divisor}/{total}")
                        st.info(f"📊 Puertos: {inserted_puerto}/{total}")
                        st.info(f"📊 Fecha fin: {inserted_fecha_fin}/{total}")
                        st.info(f"📊 SAT: {inserted_sat}/{total}")
                        st.info(f"📊 Tipo cliente: {inserted_tipo_cliente}/{total}")
                        st.info(f"📊 Técnico: {inserted_tecnico}/{total}")
                        st.info(f"📊 Método entrada: {inserted_metodo_entrada}/{total}")
                        st.info(f"📊 Billing: {inserted_billing}/{total}")
                        st.info(f"📊 🔥 Permanencia: {inserted_permanencia}/{total}")


                        cur.execute("""
                            SELECT COUNT(*),
                                   COUNT(divisor),
                                   COUNT(puerto),
                                   COUNT(fecha_fin_contrato),
                                   COUNT(SAT),
                                   COUNT(Tipo_cliente),
                                   COUNT(tecnico),
                                   COUNT(metodo_entrada),
                                   COUNT(billing),
                                   COUNT(permanencia)
                            FROM seguimiento_contratos
                        """)

                        stats = cur.fetchone()
                        st.toast(
                            f"BD → Total:{stats[0]} | Div:{stats[1]} | Pue:{stats[2]} | Fin:{stats[3]} | SAT:{stats[4]} | Tipo:{stats[5]} | Tec:{stats[6]} | Met:{stats[7]} | Bill:{stats[8]} | Perm:{stats[9]}"
                        )

                        cur.execute("""
                            SELECT apartment_id, fecha_fin_contrato, divisor, puerto, SAT, Tipo_cliente, tecnico, permanencia
                            FROM seguimiento_contratos
                            WHERE permanencia IS NOT NULL
                            LIMIT 5
                        """)

                        for r in cur.fetchall():
                            st.write(r)


                        with obtener_conexion() as conn:
                            cur = conn.cursor()
                            # divisor
                            cur.execute("""
                                   UPDATE datos_uis
                                   SET divisor = (
                                       SELECT sc.divisor
                                       FROM seguimiento_contratos sc
                                       WHERE sc.apartment_id = datos_uis.apartment_id
                                       AND sc.divisor IS NOT NULL
                                       LIMIT 1
                                   )
                                   WHERE apartment_id IN (
                                       SELECT apartment_id FROM seguimiento_contratos
                                       WHERE divisor IS NOT NULL
                                   )
                               """)

                            # puerto
                            cur.execute("""
                                   UPDATE datos_uis
                                   SET puerto = (
                                       SELECT sc.puerto
                                       FROM seguimiento_contratos sc
                                       WHERE sc.apartment_id = datos_uis.apartment_id
                                       AND sc.puerto IS NOT NULL
                                       LIMIT 1
                                   )
                                   WHERE apartment_id IN (
                                       SELECT apartment_id FROM seguimiento_contratos
                                       WHERE puerto IS NOT NULL
                                   )
                               """)

                            # fecha fin
                            cur.execute("""
                                   UPDATE datos_uis
                                   SET fecha_fin_contrato = (
                                       SELECT sc.fecha_fin_contrato
                                       FROM seguimiento_contratos sc
                                       WHERE sc.apartment_id = datos_uis.apartment_id
                                       AND sc.fecha_fin_contrato IS NOT NULL
                                       LIMIT 1
                                   )
                                   WHERE apartment_id IN (
                                       SELECT apartment_id FROM seguimiento_contratos
                                       WHERE fecha_fin_contrato IS NOT NULL
                                   )
                               """)

                            conn.commit()
                        st.toast("✅ Proceso completado correctamente.")

                    except Exception as e:
                        st.toast(f"❌ Error en el proceso: {e}")
                        import traceback
                        st.code(traceback.format_exc())

            # ✅ CHECKBOX RESTAURADO - Mostrar registros existentes
            if st.checkbox("Mostrar registros existentes en la base de datos", key="view_existing_contracts_contratos"):
                with st.spinner("Cargando registros de contratos..."):
                    try:
                        conn = obtener_conexion()
                        existing = pd.read_sql("SELECT * FROM seguimiento_contratos", conn)
                        conn.close()

                        if existing.empty:
                            st.warning("⚠️ No hay registros en 'seguimiento_contratos'.")

                        else:
                            cols = st.multiselect("Filtra columnas a mostrar", existing.columns,
                                                  default=existing.columns,
                                                  key="cols_existing")
                            st.dataframe(existing[cols], width='stretch')

                    except Exception as e:
                        st.toast(f"❌ Error al cargar registros existentes: {e}")

        if sub_seccion == "Precontratos":
            # Conexión a la base de datos para mostrar precontratos existentes
            conn = obtener_conexion()
            cursor = conn.cursor()

            # Obtener precontratos (los más recientes primero) - CON NUEVOS CAMPOS
            cursor.execute("""
                            SELECT p.id, p.precontrato_id, p.apartment_id, p.nombre, p.tarifas, p.precio, 
                                   p.fecha, p.comercial, pl.usado, p.mail, p.permanencia, p.telefono1, p.telefono2
                            FROM precontratos p
                            LEFT JOIN precontrato_links pl ON p.id = pl.precontrato_id
                            ORDER BY p.fecha DESC
                            LIMIT 50
                        """)
            precontratos = cursor.fetchall()
            conn.close()

            if precontratos:
                st.write(f"**Últimos {len(precontratos)} precontratos:**")
                for precontrato in precontratos:
                    with st.expander(f"📄 {precontrato[1]} - {precontrato[3] or 'Sin nombre'} - {precontrato[4]}",
                                     expanded=False):

                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.write(f"**ID:** {precontrato[1]}")
                            st.write(f"**Apartment ID:** {precontrato[2] or 'No asignado'}")
                            st.write(f"**Tarifa:** {precontrato[4]}")
                            st.write(f"**Precio:** {precontrato[5]}€")

                        with col2:
                            st.write(f"**Fecha:** {precontrato[6]}")
                            st.write(f"**Comercial:** {precontrato[7]}")
                            st.write(f"**Permanencia:** {precontrato[10] or 'No especificada'}")

                        with col3:
                            estado = "✅ Usado" if precontrato[8] else "🟢 Activo"
                            st.write(f"**Estado:** {estado}")
                            st.write(f"**Email:** {precontrato[9] or 'No especificado'}")
                            st.write(f"**Teléfono 1:** {precontrato[11] or 'No especificado'}")
                            if precontrato[12]:  # Si hay teléfono 2
                                st.write(f"**Teléfono 2:** {precontrato[12]}")

                        # Botón para regenerar enlace si está usado o expirado
                        if precontrato[8]:  # Si está usado
                            if st.button(f"🔄 Regenerar enlace para {precontrato[1]}", key=f"regen_{precontrato[0]}"):
                                try:
                                    conn = obtener_conexion()
                                    cursor = conn.cursor()
                                    # Generar nuevo token
                                    token_valido = False
                                    max_intentos = 5
                                    intentos = 0

                                    while not token_valido and intentos < max_intentos:
                                        token = st.secrets.token_urlsafe(16)
                                        cursor.execute("SELECT id FROM precontrato_links WHERE token = ?", (token,))
                                        if cursor.fetchone() is None:
                                            token_valido = True
                                        intentos += 1

                                    if token_valido:
                                        expiracion = datetime.now() + datetime.timedelta(hours=24)

                                        # Actualizar el token existente
                                        cursor.execute("""
                                                        UPDATE precontrato_links 
                                                        SET token = ?, expiracion = ?, usado = 0
                                                        WHERE precontrato_id = ?
                                                    """, (token, expiracion, precontrato[0]))
                                        conn.commit()
                                        conn.close()
                                        #base_url = "https://one7022025.onrender.com"
                                        base_url = "https://verde-suite.verdesuite.sytes.net/"
                                        link_cliente = f"{base_url}?precontrato_id={precontrato[0]}&token={urllib.parse.quote(token)}"
                                        st.toast("✅ Nuevo enlace generado correctamente.")
                                        st.code(link_cliente, language="text")
                                        st.info("💡 Copia este nuevo enlace y envíalo al cliente.")
                                except Exception as e:
                                    st.toast(f"❌ Error al regenerar enlace: {e}")

            else:
                st.toast(
                    "📝 No hay precontratos registrados aún. Crea el primero en la pestaña 'Crear Nuevo Precontrato'.")

        if sub_seccion == "TIRC":
            st.info(
                "ℹ️ Aquí puedes visualizar, filtrar y descargar los datos TIRC junto con información de viabilidades relacionadas.")

            # --- 1️⃣ Leer datos de la base de datos ---
            try:
                conn = obtener_conexion()
                df_tirc = pd.read_sql("SELECT * FROM TIRC", conn)
                df_viabilidades = pd.read_sql("SELECT * FROM viabilidades", conn)
                conn.close()
            except Exception as e:
                st.toast(f"❌ Error al cargar datos: {e}")
                df_tirc = pd.DataFrame()
                df_viabilidades = pd.DataFrame()

            if not df_tirc.empty:
                # --- 2️⃣ PROCESAR Y ENRIQUECER DATOS TIRC ---

                # Función para normalizar apartment_id (la misma que usamos antes)
                def normalizar_apartment_id(apartment_id):
                    if pd.isna(apartment_id) or apartment_id is None or apartment_id == "":
                        return None
                    str_id = str(apartment_id).strip().upper()
                    if str_id.startswith('P00'):
                        numeros = ''.join(filter(str.isdigit, str_id[3:]))
                        return f"P00{numeros}" if numeros else str_id
                    if str_id.isdigit() and 1 <= len(str_id) <= 10:
                        return f"P00{str_id}"
                    numeros = ''.join(filter(str.isdigit, str_id))
                    if numeros and 1 <= len(numeros) <= 10:
                        return f"P00{numeros}"
                    return str_id

                # Aplicar normalización a TIRC
                df_tirc["apartment_id_normalizado"] = df_tirc["apartment_id"].apply(normalizar_apartment_id)

                # Preparar viabilidades para el cruce (expandir múltiples apartment_id)
                df_via_expandido = df_viabilidades.assign(
                    apartment_id=df_viabilidades['apartment_id'].str.split(',')
                ).explode('apartment_id')
                df_via_expandido['apartment_id'] = df_via_expandido['apartment_id'].str.strip()
                df_via_expandido = df_via_expandido[df_via_expandido['apartment_id'] != ''].copy()
                df_via_expandido["apartment_id_normalizado"] = df_via_expandido["apartment_id"].apply(
                    normalizar_apartment_id)

                # --- 3️⃣ CREAR DATASET ENRIQUECIDO ---

                # Agrupar viabilidades para evitar duplicados
                via_agrupada = df_via_expandido.groupby('apartment_id_normalizado').agg({
                    'ticket': 'first',
                    'estado': 'first',
                    'serviciable': 'first',
                    'coste': 'first',
                    'fecha_viabilidad': 'first',
                    'usuario': 'first',
                    'nombre_cliente': 'first',
                    'telefono': 'first',
                    'id': 'count'  # Contar cuántas viabilidades tiene este apartment_id
                }).reset_index()

                via_agrupada = via_agrupada.rename(columns={
                    'id': 'cantidad_viabilidades',
                    'usuario': 'comercial_viabilidad'
                })

                # Unir TIRC con viabilidades
                df_tirc_enriquecido = pd.merge(
                    df_tirc,
                    via_agrupada,
                    on='apartment_id_normalizado',
                    how='left',
                    suffixes=('', '_via')
                )

                # Crear columna de relación
                df_tirc_enriquecido['relacion_viabilidad'] = df_tirc_enriquecido['ticket'].apply(
                    lambda x: '✅ Con viabilidad' if pd.notna(x) else '❌ Sin viabilidad'
                )

                # --- 4️⃣ ESTADÍSTICAS ---
                total_tirc = len(df_tirc_enriquecido)
                tirc_con_viabilidad = len(
                    df_tirc_enriquecido[df_tirc_enriquecido['relacion_viabilidad'] == '✅ Con viabilidad'])
                porcentaje_con_viabilidad = (tirc_con_viabilidad / total_tirc) * 100 if total_tirc > 0 else 0

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total TIRC", total_tirc)
                with col2:
                    st.metric("TIRC con viabilidad", tirc_con_viabilidad)
                with col3:
                    st.metric("Cobertura", f"{porcentaje_con_viabilidad:.1f}%")

                # --- 5️⃣ FILTROS ---
                col1, col2, col3 = st.columns(3)
                with col1:
                    filtro_relacion = st.selectbox(
                        "Relación con viabilidad:",
                        ["Todos", "✅ Con viabilidad", "❌ Sin viabilidad"]
                    )
                with col2:
                    filtro_estado = st.selectbox(
                        "Estado viabilidad:",
                        ["Todos"] + list(df_tirc_enriquecido['estado'].dropna().unique())
                    )
                with col3:
                    filtro_serviciable = st.selectbox(
                        "Serviciable:",
                        ["Todos"] + list(df_tirc_enriquecido['serviciable'].dropna().unique())
                    )

                # Aplicar filtros
                df_filtrado = df_tirc_enriquecido.copy()
                if filtro_relacion != "Todos":
                    df_filtrado = df_filtrado[df_filtrado['relacion_viabilidad'] == filtro_relacion]
                if filtro_estado != "Todos":
                    df_filtrado = df_filtrado[df_filtrado['estado'] == filtro_estado]
                if filtro_serviciable != "Todos":
                    df_filtrado = df_filtrado[df_filtrado['serviciable'] == filtro_serviciable]

                # --- 6️⃣ COLUMNAS PARA MOSTRAR ---
                columnas_base = [
                    'apartment_id', 'provincia', 'municipio', 'poblacion',
                    'ESTADO', 'SINCRONISMO', 'TIPO CTO', 'CTO', 'OLT'
                ]

                columnas_viabilidad = [
                    'relacion_viabilidad', 'ticket', 'estado', 'serviciable',
                    'coste', 'fecha_viabilidad', 'comercial_viabilidad',
                    'nombre_cliente', 'telefono', 'cantidad_viabilidades'
                ]

                # Seleccionar solo columnas que existen
                columnas_a_mostrar = []
                for col in columnas_base + columnas_viabilidad:
                    if col in df_filtrado.columns:
                        columnas_a_mostrar.append(col)

                # --- 7️⃣ CONFIGURAR AgGrid ---
                gb = GridOptionsBuilder.from_dataframe(df_filtrado[columnas_a_mostrar])
                gb.configure_pagination(paginationAutoPageSize=True)
                gb.configure_default_column(
                    editable=False,
                    filter=True,
                    sortable=True,
                    minWidth=120,
                    flex=1
                )

                # Configurar columnas específicas
                gb.configure_column("relacion_viabilidad", headerName="📋 Relación", width=150)
                gb.configure_column("ticket", headerName="🎫 Ticket Viabilidad", width=150)
                gb.configure_column("coste", headerName="💰 Coste", width=100)
                gb.configure_column("fecha_viabilidad", headerName="📅 Fecha Viab.", width=120)
                gb.configure_column("estado", headerName="📊 Estado Viab.", width=120)
                gb.configure_column("serviciable", headerName="✅ Serviciable", width=120)

                grid_options = gb.build()

                # --- 8️⃣ MOSTRAR TABLA ---
                AgGrid(
                    df_filtrado[columnas_a_mostrar],
                    gridOptions=grid_options,
                    enable_enterprise_modules=True,
                    update_mode="MODEL_CHANGED",
                    height=500,
                    fit_columns_on_grid_load=False,
                    theme='alpine-dark'
                )

                # --- 9️⃣ OPCIONES DE DESCARGA ---
                col1, col2, col3 = st.columns(3)

                with col1:
                    # Descargar CSV filtrado
                    csv = df_filtrado[columnas_a_mostrar].to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Descargar CSV (filtrado)",
                        data=csv,
                        file_name="tirc_filtrado.csv",
                        mime="text/csv",
                        width='stretch'
                    )

                with col2:
                    # Descargar Excel completo
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        df_tirc_enriquecido.to_excel(writer, sheet_name='TIRC Completo', index=False)
                        df_filtrado.to_excel(writer, sheet_name='TIRC Filtrado', index=False)
                    output.seek(0)

                    st.download_button(
                        label="📥 Descargar Excel (completo)",
                        data=output,
                        file_name="tirc_completo.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        width='stretch'
                    )

                with col3:
                    # Descargar solo TIRC sin viabilidad
                    tirc_sin_viabilidad = df_tirc_enriquecido[
                        df_tirc_enriquecido['relacion_viabilidad'] == '❌ Sin viabilidad']
                    if not tirc_sin_viabilidad.empty:
                        csv_sin_viab = tirc_sin_viabilidad[columnas_base].to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 TIRC sin viabilidad",
                            data=csv_sin_viab,
                            file_name="tirc_sin_viabilidad.csv",
                            mime="text/csv",
                            width='stretch'
                        )

                # --- 🔟 INFORMACIÓN ADICIONAL ---
                with st.expander("📈 Información detallada de la relación TIRC-Viabilidades"):
                    col1, col2 = st.columns(2)

                    with col1:
                        st.write("**Distribución por estado de viabilidad:**")
                        if 'estado' in df_tirc_enriquecido.columns:
                            estado_counts = df_tirc_enriquecido['estado'].value_counts()
                            st.dataframe(estado_counts, width='stretch')

                    with col2:
                        st.write("**Distribución por serviciable:**")
                        if 'serviciable' in df_tirc_enriquecido.columns:
                            serviciable_counts = df_tirc_enriquecido['serviciable'].value_counts()
                            st.dataframe(serviciable_counts, width='stretch')

                    # Mostrar algunos ejemplos de TIRC sin viabilidad
                    tirc_sin_viab_ejemplos = df_tirc_enriquecido[
                        df_tirc_enriquecido['relacion_viabilidad'] == '❌ Sin viabilidad'
                        ].head(10)

                    if not tirc_sin_viab_ejemplos.empty:
                        st.write("**Ejemplos de TIRC sin viabilidad:**")
                        st.dataframe(tirc_sin_viab_ejemplos[['apartment_id', 'provincia', 'municipio', 'poblacion']])

            else:
                st.warning("⚠️ No hay datos en la tabla TIRC.")

    # Opción: Visualizar datos de la tabla comercial_rafa
    elif opcion == "Ofertas Comerciales":
        sub_seccion = option_menu(
            menu_title=None,
            options=["Ver Ofertas", "Certificación Visitas", "Certificación Contratos"],
            icons=["table", "file-earmark-check", "file-earmark-check"],
            orientation="horizontal",
            styles={
                "container": {
                    "padding": "0px",
                    "margin": "0px",
                    "max-width":"none",
                    "background-color": "#F0F7F2",
                    "border-radius": "0px"
                },
                "icon": {
                    "color": "#2C5A2E",  # Íconos en verde oscuro
                    "font-size": "25px"
                },
                "nav-link": {
                    "color": "#2C5A2E",
                    "font-size": "18px",
                    "text-align": "center",
                    "margin": "0px",
                    "--hover-color": "#66B032",
                    "border-radius": "0px",
                },
                "nav-link-selected": {
                    "background-color": "#66B032",  # Verde principal corporativo
                    "color": "white",
                    "font-weight": "bold",
                }
            })
        # Uso en el código principal
        if sub_seccion == "Ver Ofertas":
            mostrar_ofertas_comerciales()

        elif sub_seccion == "Certificación Visitas":
            mostrar_certificacion()
        elif sub_seccion == "Certificación Contratos":
            mostrar_kpis_seguimiento_contratos()

    elif opcion == "Viabilidades":
        st.header("Viabilidades")

        with st.expander("🧭 Guía de uso del panel de viabilidades", expanded=False):
            st.info("""
            ℹ️ En esta sección puedes **consultar y completar los tickets de viabilidades** según el comercial, filtrar los datos por etiquetas o columnas, buscar elementos concretos (lupa de la tabla)  
            y **descargar los resultados filtrados en Excel o CSV**.

            🔹 **Organización:**  
            Usa las etiquetas rojas para personalizar cómo deseas visualizar la información en la tabla.  

            🔹 **Edición:**  
            Selecciona la viabilidad que quieras estudiar en el plano y completa los datos en el formulario que se despliega en la parte inferior.  
            Una vez guardadas tus modificaciones, podrás refrescar la tabla para ver los cambios reflejados.  

            🔹 **Creación:**  
            Al pulsar **“Crear Viabilidades”**, haz clic en el mapa para agregar un marcador que represente el punto de viabilidad.  
            También puedes actualizar las tablas internas y el Excel externo desde **“Actualizar tablas”**.  
            
            🔹 **Presupuestos:**  
            Al subir un presupuesto, no te olvides de elegir un remitente y darle a **"Enviar"**. Si no quieres que lo reciba nadie, usa el correo de prueba. 

            🔹 **Importante:**  
            Si una viabilidad requiere **más de una CTO o varios Apartment ID por CTO**, debes crear una viabilidad nueva por cada una.  
            Esto asegura que todos los elementos queden correctamente asignados a su caja específica, generando así dos o más tickets separados.
            """)
        viabilidades_seccion()

    elif opcion == "Mapa UUIIs":
        with st.expander("📊 Guía de uso del panel de datos cruzados AMS / Ofertas", expanded=False):
            st.info("""
                ℹ️ En esta sección puedes **visualizar geográficamente todos los datos cruzados entre AMS y las ofertas de los comerciales**, mostrando su estado actual en el mapa interactivo.

                🔍 **Modos de búsqueda disponibles:**  
                - **Búsqueda por Apartment ID:** Filtra por identificador específico (modo exacto o parcial)  
                - **Búsqueda por ubicación:** Filtra progresivamente por **Provincia → Municipio → Población**  

                ⚙️ **Configuración adicional en "Filtros Avanzados":**
                - **Filtrar por estado:** Serviciable, No serviciable, Contratado, Incidencia, No interesado, No visitado
                - **Personalizar mapa:** Activar/desactivar clusters, leyenda y ajustar zoom inicial

                📊 **Funcionalidades del mapa:**
                - Vista satélite de Google Maps con zoom completo
                - Información detallada al hacer clic en cualquier punto
                - Exportación de los datos filtrados
                - Estadísticas en tiempo real

                ⚠️ **Nota importante:**  
                Los filtros de **ubicación (Provincia, Municipio, Población) solo están activos cuando NO se ha ingresado un Apartment ID**.  
                Para usar filtros geográficos, asegúrate de que el campo de ID esté vacío.
                """)
        mapa_seccion()

    # Opción: Generar Informes
    elif opcion == "Generar Informe":
        st.header("Generar Informe")
        st.info("ℹ️ Aquí puedes generar informes basados en los datos disponibles.")
        log_trazabilidad(st.session_state["username"], "Generar Informe", "El admin accedió al generador de informes.")

        # Selección del periodo de tiempo en columnas
        col1, col2 = st.columns(2)
        with col1:
            fecha_inicio = st.date_input("Fecha de inicio")
        with col2:
            fecha_fin = st.date_input("Fecha de fin")
        if st.button("Generar Informe"):
            informe = generar_informe(str(fecha_inicio), str(fecha_fin))
            st.dataframe(informe)

    elif opcion == "CDRs":
        st.info("ℹ️ Aquí puedes generar informes basados en los datos disponibles.")
        mostrar_cdrs()

    elif opcion == "Gestionar Usuarios":
        sub_seccion = option_menu(
            menu_title=None,
            options=["Listado de usuarios", "Agregar usuarios", "Editar/eliminar usuarios"],
            icons=["people", "person-plus", "pencil-square"],
            default_index=0,
            orientation="horizontal",
            styles={
                "container": {
                    "padding": "0!important",
                    "margin": "0px",
                    "background-color": "#F0F7F2",
                    "border-radius": "0px",
                    "max-width": "none"
                },
                "icon": {
                    "color": "#2C5A2E",
                    "font-size": "25px"
                },
                "nav-link": {
                    "color": "#2C5A2E",
                    "font-size": "18px",
                    "text-align": "center",
                    "margin": "0px",
                    "--hover-color": "#66B032",
                    "border-radius": "0px",
                },
                "nav-link-selected": {
                    "background-color": "#66B032",
                    "color": "white",
                    "font-weight": "bold"
                }
            }
        )

        log_trazabilidad(st.session_state["username"], "Gestionar Usuarios", "Accedió a la gestión de usuarios.")

        # Cargar usuarios para todas las subsecciones
        usuarios = cargar_usuarios()
        df_usuarios = pd.DataFrame(usuarios, columns=["ID", "Nombre", "Rol", "Email"]) if usuarios else pd.DataFrame()

        # 🧾 SUBSECCIÓN: Listado de usuarios
        if sub_seccion == "Listado de usuarios":
            st.info("ℹ️ Desde esta sección puedes consultar usuarios registrados en el sistema.")
            if not df_usuarios.empty:
                st.dataframe(df_usuarios, width='stretch', height=540)
            else:
                st.warning("No hay usuarios registrados.")

        # ➕ SUBSECCIÓN: Agregar usuarios
        elif sub_seccion == "Agregar usuarios":
            st.info("ℹ️ Desde esta sección puedes agregar nuevos usuarios al sistema.")
            nombre = st.text_input("Nombre del Usuario")
            rol = st.selectbox("Rol", ["admin", "comercial", "comercial_jefe", "comercial_rafa", "comercial_vip","demo", "marketing","viabilidad","auditor"])
            email = st.text_input("Email del Usuario")
            password = st.text_input("Contraseña", type="password")

            if st.button("Agregar Usuario"):
                if nombre and password and email:
                    agregar_usuario(nombre, rol, password, email)
                    st.toast("✅ Usuario agregado correctamente.")
                else:
                    st.toast("❌ Por favor, completa todos los campos.")

        # ✏️ SUBSECCIÓN: Editar/Eliminar usuarios
        elif sub_seccion == "Editar/eliminar usuarios":
            st.info("ℹ️ Edita el usuario que quieras del sistema.")
            usuario_id = st.number_input("ID del Usuario a Editar", min_value=1, step=1)

            if usuario_id:
                conn = obtener_conexion()
                cursor = conn.cursor()
                cursor.execute("SELECT username, role, email FROM usuarios WHERE id = ?", (usuario_id,))
                usuario = cursor.fetchone()
                conn.close()

                if usuario:
                    nuevo_nombre = st.text_input("Nuevo Nombre", value=usuario[0])
                    nuevo_rol = st.selectbox("Nuevo Rol",
                                             ["admin", "comercial", "comercial_jefe", "comercial_rafa","comercial_vip","demo", "marketing","viabilidad","auditor"],
                                             index=["admin", "comercial", "comercial_jefe",
                                                    "comercial_rafa","comercial_vip","demo", "marketing","viabilidad","auditor"].index(usuario[1]))
                    nuevo_email = st.text_input("Nuevo Email", value=usuario[2])
                    nueva_contraseña = st.text_input("Nueva Contraseña", type="password")

                    if st.button("Guardar Cambios"):
                        editar_usuario(usuario_id, nuevo_nombre, nuevo_rol, nueva_contraseña, nuevo_email)
                        st.toast("✅ Usuario editado correctamente.")
                else:
                    st.toast("❌ Usuario no encontrado.")

            st.info("ℹ️ Elimina el usuario que quieras del sistema.")
            eliminar_id = st.number_input("ID del Usuario a Eliminar", min_value=1, step=1)
            if eliminar_id and st.button("Eliminar Usuario"):
                eliminar_usuario(eliminar_id)
                st.toast("✅ Usuario eliminado correctamente.")


    elif opcion == "Cargar Nuevos Datos":
        st.header("Cargar Nuevos Datos")
        with st.expander("⚠️ Carga y reemplazo de base de datos", expanded=False):
            st.info("""
            ℹ️ Aquí puedes **cargar un archivo Excel o CSV** para reemplazar los datos existentes en la base de datos por una versión más reciente.  

            ⚠️ **ATENCIÓN:**  
            - Esta acción **eliminará todos los datos actuales** de la base de datos.  
            - Cualquier actualización realizada dentro de la aplicación también se perderá.  
            - Antes de continuar, asegúrate de que el nuevo archivo contenga **todas las columnas actualizadas** necesarias.  

            🗂️ **Recomendación:**  
            Si el archivo que cargas no tiene la información completa, **recarga el Excel de seguimiento de contratos** para mantener la integridad de los datos.  

            📥 **Nota:** Es posible cargar tanto **nuevos puntos** como **nuevas TIRC**.
            """)

        log_trazabilidad(
            st.session_state["username"],
            "Cargar Nuevos Datos",
            "El admin accedió a la sección de carga de nuevos datos y se procederá a reemplazar el contenido de la tabla."
        )
        col1, col2 = st.columns(2)
        # ===================== 📁 TARJETA PARA CARGAR TIRC =====================
        with col1:
            st.markdown("""
                <div style='
                    background-color:#F0F7F2;
                    padding:25px;
                    margin-top:10px;
                    text-align:center;
                    border-radius:0px;
                '>
                    <h4 style='color:#1e3d59;'>🧩 Cargar Archivos TIRC</h4>
                    <p style='color:#333;'>
                        Arrastra o selecciona uno o varios archivos <b>Excel (.xlsx o .xls)</b> con los datos TIRC actualizados.
                    </p>
                </div>
            """, unsafe_allow_html=True)

            uploaded_tirc_files = st.file_uploader(
                "Selecciona uno o varios Excel para la tabla TIRC",
                type=["xlsx", "xls"],
                key="upload_tirc",
                accept_multiple_files=True,
                label_visibility="collapsed"
            )

            if uploaded_tirc_files:
                conn = obtener_conexion()
                cursor = conn.cursor()

                columnas_tirc = [
                    "id", "apartment_id", "address_id", "provincia", "municipio", "poblacion", "street_id",
                    "tipo_vial", "vial", "parcela_catastral", "tipo", "numero", "bis", "bloque", "portal_puerta",
                    "letra", "cp", "site_dummy", "site_operational_state", "subvention_code", "nodo", "sales_area",
                    "electronica", "red", "competencia", "descripcion", "nota_interna", "lng", "lat", "gis_status",
                    "created_at", "homes", "site_activation_date", "escalera", "piso", "mano1", "mano2",
                    "apartment_sales_area", "apartment_dummy", "apartment_operational_state",
                    "apartment_created_at", "apartment_activation_date", "cto_id", "OLT", "CTO",
                    "FECHA PRIMERA ACTIVACION", "ESTADO", "SINCRONISMO", "TIPO CTO", "CT", "ID TIRC",
                    "FECHA REVISION", "PROYECTO", "POBLACION CORREGIDA"
                ]

                for uploaded_tirc in uploaded_tirc_files:
                    try:
                        with st.spinner(f"⏳ Procesando {uploaded_tirc.name}..."):
                            # Leer Excel con pandas
                            df_tirc = pd.read_excel(uploaded_tirc, dtype=str)

                            # Normalizar encabezados (quitar espacios, mayúsculas, etc.)
                            df_tirc.columns = [c.strip() for c in df_tirc.columns]

                            # Verificar columnas faltantes
                            faltantes = [c for c in columnas_tirc if c not in df_tirc.columns]
                            if faltantes:
                                st.toast(f"❌ {uploaded_tirc.name}: faltan columnas: {', '.join(faltantes)}")
                                continue

                            # Ordenar columnas según estructura esperada
                            df_tirc = df_tirc[columnas_tirc].fillna("")

                            data_values = df_tirc.values.tolist()

                            insert_query = f"""
                                INSERT INTO TIRC ({', '.join([f'"{c}"' for c in columnas_tirc])})
                                VALUES ({', '.join(['?'] * len(columnas_tirc))})
                                ON CONFLICT(id) DO UPDATE SET
                                {', '.join([f'"{c}"=excluded."{c}"' for c in columnas_tirc if c != "id"])}
                            """

                            cursor.executemany(insert_query, data_values)
                            conn.commit()

                            st.toast(f"✅ {uploaded_tirc.name}: {len(df_tirc)} registros insertados/actualizados.")

                            log_trazabilidad(
                                st.session_state["username"],
                                "Carga TIRC incremental",
                                f"Archivo {uploaded_tirc.name} con {len(df_tirc)} registros procesados."
                            )

                    except Exception as e:
                        st.toast(f"❌ Error en {uploaded_tirc.name}: {e}")

                conn.close()

            # ===================== 🧱 TARJETA PARA CARGAR UUII =====================
            with col2:
                st.markdown("""
                <div style='background-color:#F0F7F2; padding:25px; margin-top:10px; text-align:center;'>
                    <h4 style='color:#1e3d59;'>🏢 Cargar Archivo UUII</h4>
                    <p>Arrastra o selecciona el archivo <b>Excel (.xlsx)</b> o <b>CSV</b> con los datos actualizados de puntos comerciales.</p>
                </div>
                """, unsafe_allow_html=True)

                uploaded_file = st.file_uploader(
                    "Selecciona un archivo Excel o CSV para subir nuevos puntos comerciales visitables",
                    type=["xlsx", "csv"],
                    key="upload_uu",
                    label_visibility="collapsed"
                )

                if uploaded_file is not None:
                    try:
                        with st.spinner("⏳ Cargando archivo..."):
                            # Intentar importar ftfy
                            try:
                                USE_FTFY = True
                            except ImportError:
                                USE_FTFY = False
                                st.toast("⚠️ Para mejor corrección de caracteres, instala: pip install ftfy")

                            # Función para limpiar texto usando ftfy si está disponible
                            def limpiar_texto(texto):
                                if texto is None or not isinstance(texto, str):
                                    return texto
                                if USE_FTFY:
                                    return ftfy.fix_text(texto)
                                return texto

                            # Función para limpiar nombres de columnas
                            def limpiar_nombre_columna(nombre):
                                if not isinstance(nombre, str):
                                    return nombre
                                nombre = limpiar_texto(nombre)
                                # Simplificar: convertir a minúsculas y reemplazar espacios/guiones
                                nombre = nombre.strip().lower()
                                nombre = nombre.replace(' ', '_').replace('-', '_')
                                # Eliminar caracteres especiales excepto guión bajo
                                nombre = ''.join(c for c in nombre if c.isalnum() or c == '_')
                                return nombre

                            if uploaded_file.name.endswith(".xlsx"):
                                # Para Excel, usar engine openpyxl
                                data = pd.read_excel(uploaded_file, engine='openpyxl')
                                # Limpiar nombres de columnas
                                data.columns = [limpiar_nombre_columna(col) for col in data.columns]

                            elif uploaded_file.name.endswith(".csv"):
                                # Para CSV, probar diferentes encodings
                                encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 'utf-8-sig']

                                for encoding in encodings_to_try:
                                    try:
                                        uploaded_file.seek(0)  # Resetear puntero
                                        data = pd.read_csv(uploaded_file, encoding=encoding, on_bad_lines='warn')
                                        st.toast(f"✅ Archivo leído con encoding: {encoding}")
                                        break
                                    except Exception:
                                        continue
                                else:
                                    # Si fallan todos, intentar con reemplazo de errores
                                    uploaded_file.seek(0)
                                    data = pd.read_csv(uploaded_file, encoding='utf-8', on_bad_lines='warn',
                                                       errors='replace')
                                    st.toast("⚠️ Se usó reemplazo de caracteres para leer el archivo")

                                # Limpiar nombres de columnas
                                data.columns = [limpiar_nombre_columna(col) for col in data.columns]

                            # Diccionario para mapear columnas (con variantes permitidas)
                            mapeo_columnas_base = {
                                "id_ams": "id_ams",
                                "apartment_id": "apartment_id",
                                "address_id": "address_id",
                                "provincia": "provincia",
                                "municipio": "municipio",
                                "poblacion": "poblacion",
                                "vial": "vial",
                                "numero": "numero",
                                "parcela_catastral": "parcela_catastral",
                                "letra": "letra",
                                "cp": "cp",
                                "site_operational_state": "site_operational_state",
                                "apartment_operational_state": "apartment_operational_state",
                                "cto_id": "cto_id",
                                "olt": "olt",
                                "cto": "cto",
                                "lat": "latitud",
                                "lng": "longitud",
                                "tipo_olt_rental": "tipo_olt_rental",
                                #"certificable": "CERTIFICABLE",
                                "comercial": "comercial",
                                "zona": "zona",
                                "fecha": "fecha",
                                "serviciable": "serviciable",
                                "motivo": "motivo",
                                "contrato_uis": "contrato_uis"
                            }

                            # Crear mapeo con variantes comunes
                            mapeo_columnas = {}
                            variantes_comunes = {
                                "poblacion": ["poblacion", "población", "localidad"],
                                "provincia": ["provincia", "provincia", "prov"],
                                "municipio": ["municipio", "municipio", "ciudad"],
                                "cp": ["cp", "codigo_postal", "codigopostal", "código_postal"],
                                "lat": ["lat", "latitud", "latitude"],
                                "lng": ["lng", "longitud", "longitude", "long"],
                                "fecha": ["fecha", "date", "fecha_actualizacion"],
                                "comercial": ["comercial", "vendedor", "agente"]
                            }

                            # Buscar columnas que coincidan
                            columnas_encontradas = {}
                            columnas_faltantes = []

                            for col_db, col_excel in mapeo_columnas_base.items():
                                encontrada = False

                                # Buscar la columna exacta o variantes
                                posibles_nombres = [col_excel]
                                if col_excel in variantes_comunes:
                                    posibles_nombres.extend(variantes_comunes[col_excel])

                                for posible_nombre in posibles_nombres:
                                    if posible_nombre in data.columns:
                                        columnas_encontradas[col_db] = posible_nombre
                                        encontrada = True
                                        break

                                if not encontrada:
                                    columnas_faltantes.append(col_excel)

                            if columnas_faltantes:
                                st.toast(f"❌ Columnas faltantes: {', '.join(columnas_faltantes)}")
                                st.toast(f"📋 Columnas encontradas en el archivo: {', '.join(data.columns[:10])}")
                                return

                            # Renombrar columnas y limpiar datos
                            data = data.rename(columns={v: k for k, v in columnas_encontradas.items()})

                            # Limpiar contenido de columnas de texto
                            columnas_texto = [
                                'provincia', 'municipio', 'poblacion', 'vial', 'letra',
                                'site_operational_state', 'apartment_operational_state',
                                'olt', 'cto', 'tipo_olt_rental', 'comercial', 'zona',
                                'motivo', 'contrato_uis'
                            ]

                            for col in columnas_texto:
                                if col in data.columns:
                                    data[col] = data[col].astype(str).apply(limpiar_texto)

                            # Seleccionar solo las columnas necesarias
                            columnas_necesarias = list(mapeo_columnas_base.keys())
                            columnas_disponibles = [col for col in columnas_necesarias if col in data.columns]

                            data_filtrada = data[columnas_disponibles].copy()

                            # Renombrar columnas según el esquema final
                            data_filtrada.rename(columns={
                                'lat': 'latitud',
                                'lng': 'longitud',
                                #'certificable': 'CERTIFICABLE'
                            }, inplace=True)

                            # Asegurar que tenemos todas las columnas finales
                            columnas_finales = [
                                "id_ams", "apartment_id", "address_id", "provincia", "municipio", "poblacion",
                                "vial", "numero", "parcela_catastral", "letra", "cp", "site_operational_state",
                                "apartment_operational_state", "cto_id", "olt", "cto", "latitud", "longitud",
                                "tipo_olt_rental", "comercial", "zona", "fecha",
                                "serviciable", "motivo", "contrato_uis"
                            ]

                            # Añadir columnas faltantes con valores por defecto
                            for col in columnas_finales:
                                if col not in data_filtrada.columns:
                                    if col in ['serviciable']:
                                        data_filtrada[col] = None
                                    elif col == 'fecha':
                                        data_filtrada[col] = pd.Timestamp.now().strftime("%Y-%m-%d")
                                    else:
                                        data_filtrada[col] = ''

                            # Ordenar columnas
                            data_filtrada = data_filtrada[columnas_finales]

                            # Convertir coordenadas
                            data_filtrada["latitud"] = pd.to_numeric(
                                data_filtrada["latitud"].astype(str).str.replace(",", "."), errors="coerce"
                            ).round(7)

                            data_filtrada["longitud"] = pd.to_numeric(
                                data_filtrada["longitud"].astype(str).str.replace(",", "."), errors="coerce"
                            ).round(7)

                            # Procesar fecha
                            if "fecha" in data_filtrada.columns:
                                data_filtrada["fecha"] = pd.to_datetime(data_filtrada["fecha"], errors="coerce")
                                data_filtrada["fecha"] = data_filtrada["fecha"].dt.strftime("%Y-%m-%d")
                                data_filtrada["fecha"] = data_filtrada["fecha"].where(
                                    pd.notnull(data_filtrada["fecha"]), None)

                            # Leer datos anteriores
                            conn = obtener_conexion()
                            df_antiguos = pd.read_sql("SELECT * FROM datos_uis", conn)
                            st.write(
                                "✅ Datos filtrados correctamente. Procediendo a reemplazar los datos en la base de datos...")

                            cursor = conn.cursor()
                            cursor.execute("DELETE FROM datos_uis")
                            cursor.execute("DELETE FROM sqlite_sequence WHERE name='datos_uis'")
                            conn.commit()

                            total_registros = len(data_filtrada)
                            insert_values = data_filtrada.values.tolist()
                            chunk_size = 500
                            num_chunks = (total_registros + chunk_size - 1) // chunk_size

                            query = """
                                INSERT INTO datos_uis (
                                    id_ams, apartment_id, address_id, provincia, municipio, poblacion, vial, numero,
                                    parcela_catastral, letra, cp, site_operational_state, apartment_operational_state,
                                    cto_id, olt, cto, latitud, longitud, tipo_olt_rental, comercial,
                                    zona, fecha, serviciable, motivo, contrato_uis
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """

                            progress_bar = st.progress(0)
                            for i in range(num_chunks):
                                chunk = insert_values[i * chunk_size: (i + 1) * chunk_size]
                                cursor.executemany(query, chunk)
                                conn.commit()
                                progress_bar.progress(min((i + 1) / num_chunks, 1.0))

                            # -------------------------------------------------------
                            # 🔄 Asignación automática de nuevos puntos en zonas ya asignadas
                            # -------------------------------------------------------

                            # Buscar zonas ya asignadas en comercial_rafa
                            cursor.execute("""
                                SELECT DISTINCT provincia, municipio, poblacion, comercial
                                FROM comercial_rafa
                            """)
                            zonas_asignadas = cursor.fetchall()

                            for zona in zonas_asignadas:
                                provincia, municipio, poblacion, comercial = zona

                                # Puntos ya asignados en esa zona
                                cursor.execute("""
                                    SELECT apartment_id
                                    FROM comercial_rafa
                                    WHERE provincia = ? AND municipio = ? AND poblacion = ? AND comercial = ?
                                """, (provincia, municipio, poblacion, comercial))
                                asignados_ids = {fila[0] for fila in cursor.fetchall()}

                                # Puntos disponibles en datos_uis para esa zona
                                cursor.execute("""
                                    SELECT apartment_id, provincia, municipio, poblacion, vial, numero, letra, cp, latitud, longitud
                                    FROM datos_uis
                                    WHERE provincia = ? AND municipio = ? AND poblacion = ?
                                """, (provincia, municipio, poblacion))
                                puntos_zona = cursor.fetchall()

                                # 🔹 Obtener todos los apartment_id ya existentes en comercial_rafa
                                cursor.execute("SELECT apartment_id FROM comercial_rafa")
                                todos_asignados = {fila[0] for fila in cursor.fetchall()}

                                # 🔹 Filtrar los nuevos para no insertar duplicados
                                nuevos_para_asignar = [p for p in puntos_zona if p[0] not in todos_asignados]

                                # Insertarlos asignados al mismo comercial
                                for p in nuevos_para_asignar:
                                    cursor.execute("""
                                        INSERT INTO comercial_rafa
                                        (apartment_id, provincia, municipio, poblacion, vial, numero, letra, cp, latitud, longitud, comercial, Contrato)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    """, (p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], comercial,
                                          'Pendiente'))

                                if nuevos_para_asignar:
                                    st.toast(
                                        f"📌 Se asignaron {len(nuevos_para_asignar)} nuevos puntos a {comercial} en la zona {poblacion} ({municipio}, {provincia})"
                                    )

                                    # 🔹 Notificación al comercial
                                    cursor.execute("SELECT email FROM usuarios WHERE LOWER(username) = ?",
                                                   (comercial.lower(),))
                                    resultado = cursor.fetchone()
                                    if resultado:
                                        email = resultado[0]
                                        try:
                                            correo_asignacion_puntos_existentes(
                                                destinatario=email,
                                                nombre_comercial=comercial,
                                                provincia=provincia,
                                                municipio=municipio,
                                                poblacion=poblacion,
                                                nuevos_puntos=len(nuevos_para_asignar)
                                            )
                                            st.write(
                                                f"📧 Notificación enviada a {comercial} ({email}) por nuevos puntos en zona existente")
                                        except Exception as e:
                                            st.toast(f"❌ Error enviando correo a {comercial} ({email}): {e}")
                                    else:
                                        st.toast(f"⚠️ No se encontró email para el comercial: {comercial}")

                                    # 🔹 Notificación a administradores
                                    cursor.execute("SELECT email FROM usuarios WHERE role = 'admin'")
                                    admins = [fila[0] for fila in cursor.fetchall()]
                                    for email_admin in admins:
                                        try:
                                            correo_asignacion_puntos_existentes(
                                                destinatario=email_admin,
                                                nombre_comercial=comercial,
                                                provincia=provincia,
                                                municipio=municipio,
                                                poblacion=poblacion,
                                                nuevos_puntos=len(nuevos_para_asignar)
                                            )
                                            st.toast(
                                                f"📧 Notificación enviada a administrador ({email_admin}) por nuevos puntos en zona existente")
                                        except Exception as e:
                                            st.toast(f"❌ Error enviando correo a admin ({email_admin}): {e}")

                            conn.commit()

                            # Comparar apartment_id nuevos
                            apt_antiguos = set(df_antiguos['apartment_id'].unique())
                            apt_nuevos = set(data_filtrada['apartment_id'].unique())
                            nuevos_apartment_id = apt_nuevos - apt_antiguos
                            df_nuevos_filtrados = data_filtrada[data_filtrada['apartment_id'].isin(nuevos_apartment_id)]

                            try:
                                df_nuevos_filtrados["comercial"] = df_nuevos_filtrados["comercial"].astype(str)
                                df_nuevos_filtrados["poblacion"] = df_nuevos_filtrados["poblacion"].astype(str)

                                resumen = df_nuevos_filtrados.groupby('comercial').agg(
                                    total_nuevos=('apartment_id', 'count'),
                                    poblaciones_nuevas=('poblacion', lambda x: ', '.join(sorted(x.dropna().unique())))
                                ).reset_index()
                            except Exception as e:
                                st.warning(f"⚠️ Error generando resumen de nuevos datos: {e}")
                                resumen = pd.DataFrame()

                            for _, row in resumen.iterrows():
                                comercial = str(row["comercial"]).strip()
                                total_nuevos = row["total_nuevos"]
                                poblaciones_nuevas = row["poblaciones_nuevas"]

                                comercial_normalizado = comercial.lower()
                                cursor.execute("SELECT email FROM usuarios WHERE LOWER(username) = ?",
                                               (comercial_normalizado,))
                                resultado = cursor.fetchone()

                                if resultado:
                                    email = resultado[0]
                                    try:
                                        correo_nuevas_zonas_comercial(
                                            destinatario=email,
                                            nombre_comercial=comercial,
                                            total_nuevos=total_nuevos,
                                            poblaciones_nuevas=poblaciones_nuevas
                                        )
                                        st.write(f"📧 Notificación enviada a {comercial} ({email})")
                                    except Exception as e:
                                        st.toast(f"❌ Error enviando correo a {comercial} ({email}): {e}")
                                else:
                                    st.toast(f"⚠️ No se encontró email para el comercial: {comercial}")

                            # 🔹 Notificar también a los administradores
                            cursor.execute("SELECT email FROM usuarios WHERE role = 'admin'")
                            admins = [fila[0] for fila in cursor.fetchall()]

                            for email_admin in admins:
                                try:
                                    correo_nuevas_zonas_comercial(
                                        destinatario=email_admin,
                                        nombre_comercial="ADMINISTRACIÓN",
                                        total_nuevos=total_registros,
                                        poblaciones_nuevas="Se han cargado nuevos datos en el sistema."
                                    )
                                    st.write(f"📧 Notificación enviada a administrador ({email_admin})")
                                except Exception as e:
                                    st.toast(f"❌ Error enviando correo a admin ({email_admin}): {e}")

                            st.toast("✅ Archivo procesado correctamente y datos actualizados en la base de datos")

                    except Exception as e:
                        st.toast(f"❌ Error al cargar el archivo: {e}")
                        import traceback
                        st.toast(f"Detalles: {traceback.format_exc()}")


    # Opción: Trazabilidad y logs
    elif opcion == "Trazabilidad y logs":
        st.header("Trazabilidad y logs")
        st.info(
            "ℹ️ Aquí se pueden visualizar los logs y la trazabilidad de las acciones realizadas. Puedes utilizar las etiquetas rojas para filtrar la tabla y "
            "descargar los datos relevantes en formato excel y csv.")
        log_trazabilidad(st.session_state["username"], "Visualización de Trazabilidad",
                         "El admin visualizó la sección de trazabilidad y logs.")

        # Botón para vaciar la tabla
        if st.button("🗑️ Vaciar tabla y resetear IDs"):
            with st.spinner("Eliminando registros..."):
                try:
                    # Conectar a la base de datos
                    conn = obtener_conexion()
                    cursor = conn.cursor()

                    # Eliminar todos los registros de la tabla
                    cursor.execute("DELETE FROM trazabilidad")
                    # Resetear los IDs de la tabla
                    cursor.execute("VACUUM")  # Esto optimiza la base de datos y resetea los IDs autoincrementables
                    conn.commit()
                    conn.close()
                    st.toast("✔️ Tabla vaciada y IDs reseteados con éxito.")
                except Exception as e:
                    st.toast(f"❌ Error al vaciar la tabla: {e}")

        with st.spinner("Cargando trazabilidad..."):
            try:
                conn = obtener_conexion()
                query = "SELECT usuario_id, accion, detalles, fecha FROM trazabilidad"
                trazabilidad_data = pd.read_sql(query, conn)
                conn.close()

                if trazabilidad_data.empty:
                    st.info("No hay registros de trazabilidad para mostrar.")
                else:
                    if trazabilidad_data.columns.duplicated().any():
                        st.warning("¡Se encontraron columnas duplicadas! Se eliminarán las duplicadas.")
                        trazabilidad_data = trazabilidad_data.loc[:, ~trazabilidad_data.columns.duplicated()]

                    columnas = st.multiselect("Selecciona las columnas a mostrar", trazabilidad_data.columns.tolist(),
                                              default=trazabilidad_data.columns.tolist())
                    st.dataframe(trazabilidad_data[columnas], width='stretch')

                    # ✅ Solo botón de descarga Excel
                    towrite = io.BytesIO()
                    with pd.ExcelWriter(towrite, engine='xlsxwriter') as writer:
                        trazabilidad_data[columnas].to_excel(writer, index=False, sheet_name='Trazabilidad')
                    towrite.seek(0)

                    with st.spinner("Preparando archivo Excel..."):
                        st.download_button(
                            label="📥 Descargar Excel",
                            data=towrite,
                            file_name="trazabilidad.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
            except Exception as e:
                st.toast(f"❌ Error al cargar la trazabilidad: {e}")

    elif opcion == "Anuncios":
        st.info(f"📢 Panel de Anuncios Internos")
        conn = obtener_conexion()
        cursor = conn.cursor()

        # Crear tabla si no existe (sin columna autor)
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS anuncios (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    titulo TEXT NOT NULL,
                    descripcion TEXT NOT NULL,
                    fecha TEXT NOT NULL
                )
            """)
        conn.commit()

        # Obtener rol del usuario actual
        rol = st.session_state.get("role", "user")

        # ───────────────────────────────────────
        # 📝 Formulario solo para admin o jefe
        # ───────────────────────────────────────
        if rol in ["admin", "comercial_jefe"]:
            with st.form("form_anuncio"):
                titulo = st.text_input("📰 Título del anuncio", placeholder="Ej: Nueva carga de datos completada")
                descripcion = st.text_area(
                    "📋 Descripción o comentarios",
                    placeholder="Ej: Se han actualizado los datos de asignaciones del 10 al 15 de octubre..."
                )
                enviar = st.form_submit_button("📢 Publicar anuncio")

                if enviar:
                    if not titulo or not descripcion:
                        st.toast("❌ Debes completar todos los campos.")
                    else:
                        fecha_actual = pd.Timestamp.now(tz="Europe/Madrid").strftime("%Y-%m-%d %H:%M:%S")
                        cursor.execute("""
                                INSERT INTO anuncios (titulo, descripcion, fecha)
                                VALUES (?, ?, ?)
                            """, (titulo, descripcion, fecha_actual))
                        conn.commit()
                        st.toast("✅ Anuncio publicado correctamente.")

        # ───────────────────────────────────────
        # 🗂️ Listado de anuncios
        # ───────────────────────────────────────
        df_anuncios = pd.read_sql_query("SELECT * FROM anuncios ORDER BY id DESC", conn)
        conn.close()

        if df_anuncios.empty:
            st.info("ℹ️ No hay anuncios publicados todavía.")
        else:
            for _, row in df_anuncios.iterrows():
                with st.expander(f"📢 {row['titulo']}  —  🕒 {row['fecha']}"):
                    st.markdown(f"🗒️ {row['descripcion']}")

    elif opcion == "Control de versiones":
        log_trazabilidad(st.session_state["username"], "Control de versiones", "El admin accedió a la sección de control de versiones.")
        mostrar_control_versiones()


def mostrar_leyenda_en_streamlit():
    """Muestra la leyenda directamente en Streamlit (no en el mapa)"""
    with st.expander("📍 Leyenda del Mapa", expanded=True):
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("""
            **Colores de los marcadores:**
            - 🟢 **Verde:** Serviciable
            - 🔴 **Rojo:** No serviciable  
            - 🟠 **Naranja:** Contrato Sí
            """)

        with col2:
            st.markdown("""
            **Continuación:**
            - ⚫ **Gris:** No interesado
            - 🟣 **Morado:** Incidencia
            - 🔵 **Azul:** No Visitado
            """)
######kpis contratos####

def mostrar_kpis_seguimiento_contratos():
    """Muestra KPIs y análisis de la tabla seguimiento_contratos"""
    st.info("📊 **KPIs Seguimiento de Contratos** - Análisis de estado de contratos e instalaciones")

    with st.spinner("⏳ Cargando datos de seguimiento de contratos..."):
        try:
            # Cargar datos de seguimiento_contratos
            conn = obtener_conexion()
            if conn is None:
                st.toast("❌ No se pudo conectar a la base de datos")
                return

            cursor = conn.cursor()

            # Verificar que la tabla existe
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='seguimiento_contratos'")
            if not cursor.fetchone():
                st.warning("⚠️ La tabla 'seguimiento_contratos' no existe en la base de datos")
                conn.close()
                return

            # Cargar todos los datos de la tabla (INCLUYENDO NUEVAS COLUMNAS)
            query = """
            SELECT 
                id, num_contrato, cliente, coordenadas, estado,
                fecha_inicio_contrato, fecha_ingreso, comercial,
                fecha_instalacion, apartment_id, fecha_estado,
                fecha_fin_contrato, comentarios, divisor, puerto,
                SAT, Tipo_cliente, tecnico, metodo_entrada, billing
            FROM seguimiento_contratos
            """

            df_contratos = pd.read_sql(query, conn)
            conn.close()

            if df_contratos.empty:
                st.warning("⚠️ No se encontraron registros en seguimiento_contratos")
                return

            # Procesar fechas
            columnas_fecha = ['fecha_inicio_contrato', 'fecha_ingreso',
                              'fecha_instalacion', 'fecha_estado', 'fecha_fin_contrato']

            for col in columnas_fecha:
                if col in df_contratos.columns:
                    df_contratos[col] = pd.to_datetime(df_contratos[col], errors='coerce')

            # Mostrar KPIs principales
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                total_contratos = len(df_contratos)
                st.metric("Total Contratos", f"{total_contratos:,}")

            with col2:
                if 'estado' in df_contratos.columns:
                    estados_unicos = df_contratos['estado'].nunique()
                    st.metric("Estados Diferentes", f"{estados_unicos}")
                else:
                    st.metric("Estados Diferentes", "N/A")

            with col3:
                if 'comercial' in df_contratos.columns:
                    comerciales_unicos = df_contratos['comercial'].nunique()
                    st.metric("Comerciales", f"{comerciales_unicos}")
                else:
                    st.metric("Comerciales", "N/A")

            with col4:
                if 'tecnico' in df_contratos.columns:
                    tecnicos_unicos = df_contratos['tecnico'].nunique()
                    st.metric("Técnicos Únicos", f"{tecnicos_unicos}")
                else:
                    st.metric("Técnicos", "N/A")

            # Segunda fila de KPIs
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                if 'SAT' in df_contratos.columns:
                    sat_unicos = df_contratos['SAT'].nunique()
                    st.metric("SAT Únicos", f"{sat_unicos}")
                else:
                    st.metric("SAT", "N/A")

            with col2:
                if 'Tipo_cliente' in df_contratos.columns:
                    tipos_cliente = df_contratos['Tipo_cliente'].nunique()
                    st.metric("Tipos Cliente", f"{tipos_cliente}")
                else:
                    st.metric("Tipos Cliente", "N/A")

            with col3:
                if 'metodo_entrada' in df_contratos.columns:
                    metodos_entrada = df_contratos['metodo_entrada'].nunique()
                    st.metric("Métodos Entrada", f"{metodos_entrada}")
                else:
                    st.metric("Métodos Entrada", "N/A")

            with col4:
                if 'billing' in df_contratos.columns:
                    billing_unicos = df_contratos['billing'].nunique()
                    st.metric("Billing Únicos", f"{billing_unicos}")
                else:
                    st.metric("Billing", "N/A")

            # ============================
            # ANÁLISIS POR MÉTODO DE ENTRADA
            # ============================

            if 'metodo_entrada' in df_contratos.columns:
                st.subheader("🚪 Análisis por Método de Entrada")

                # Crear pestañas para diferentes análisis
                tab1, tab2, tab3, tab4 = st.tabs(["📊 Distribución", "📈 Evolución", "👥 Por Comercial", "📅 Por Mes"])

                with tab1:
                    # Distribución general
                    # Limpiar valores vacíos o nulos
                    df_metodos = df_contratos.copy()
                    df_metodos['metodo_entrada'] = df_metodos['metodo_entrada'].fillna('No especificado')
                    df_metodos['metodo_entrada'] = df_metodos['metodo_entrada'].replace('', 'No especificado')

                    # Estadísticas por método
                    metodo_stats = df_metodos['metodo_entrada'].value_counts().reset_index()
                    metodo_stats.columns = ['Método de Entrada', 'Cantidad']
                    metodo_stats['Porcentaje'] = (metodo_stats['Cantidad'] / len(df_metodos) * 100).round(2)

                    col1, col2 = st.columns([2, 1])

                    with col1:
                        try:
                            import plotly.express as px
                            fig = px.bar(
                                metodo_stats,
                                x='Método de Entrada',
                                y='Cantidad',
                                title='Distribución por Método de Entrada',
                                color='Método de Entrada',
                                text='Cantidad'
                            )
                            fig.update_layout(height=400, showlegend=False)
                            fig.update_xaxes(tickangle=45)
                            st.plotly_chart(fig, config={'responsive': True})
                        except:
                            st.dataframe(metodo_stats)

                    with col2:
                        st.dataframe(
                            metodo_stats,
                            height=400,
                            width='stretch'
                        )

                        # KPIs rápidos
                        metodo_principal = metodo_stats.iloc[0][
                            'Método de Entrada'] if not metodo_stats.empty else "N/A"
                        porcentaje_principal = metodo_stats.iloc[0]['Porcentaje'] if not metodo_stats.empty else 0

                        st.metric("Método más común", metodo_principal)
                        st.metric(f"% del total", f"{porcentaje_principal}%")

                with tab2:
                    # Evolución temporal por método
                    if 'fecha_inicio_contrato' in df_contratos.columns:
                        # Preparar datos para evolución mensual
                        df_evolucion = df_contratos.copy()
                        df_evolucion['metodo_entrada'] = df_evolucion['metodo_entrada'].fillna('No especificado')
                        df_evolucion['metodo_entrada'] = df_evolucion['metodo_entrada'].replace('', 'No especificado')

                        # Crear columna de mes
                        df_evolucion['mes'] = df_evolucion['fecha_inicio_contrato'].dt.to_period('M')

                        # Agrupar por mes y método
                        evolucion_mensual = df_evolucion.groupby(['mes', 'metodo_entrada']).size().reset_index()
                        evolucion_mensual.columns = ['Mes', 'Método de Entrada', 'Contratos']
                        evolucion_mensual['Mes'] = evolucion_mensual['Mes'].astype(str)

                        # Ordenar por mes
                        evolucion_mensual = evolucion_mensual.sort_values('Mes')

                        col1, col2 = st.columns([2, 1])

                        with col1:
                            try:
                                import plotly.express as px
                                fig = px.line(
                                    evolucion_mensual,
                                    x='Mes',
                                    y='Contratos',
                                    color='Método de Entrada',
                                    title='Evolución Mensual por Método de Entrada',
                                    markers=True
                                )
                                fig.update_layout(height=400)
                                fig.update_xaxes(tickangle=45)
                                st.plotly_chart(fig, config={'responsive': True})
                            except:
                                st.dataframe(evolucion_mensual)

                        with col2:
                            # Métodos con tendencia creciente
                            st.markdown("**Tendencias por Método**")

                            # Calcular tendencia para cada método
                            metodos_unicos = evolucion_mensual['Método de Entrada'].unique()
                            for metodo in metodos_unicos[:5]:  # Mostrar solo los primeros 5
                                df_metodo = evolucion_mensual[evolucion_mensual['Método de Entrada'] == metodo]
                                if len(df_metodo) > 1:
                                    crecimiento = df_metodo['Contratos'].iloc[-1] - df_metodo['Contratos'].iloc[0]
                                    st.write(f"**{metodo}**: {crecimiento:+.0f} contratos")
                    else:
                        st.info("⚠️ No hay datos de fecha para análisis temporal")

                with tab3:
                    # Análisis por comercial y método
                    if 'comercial' in df_contratos.columns:
                        # Preparar datos
                        df_comercial_metodo = df_contratos.copy()
                        df_comercial_metodo['metodo_entrada'] = df_comercial_metodo['metodo_entrada'].fillna(
                            'No especificado')
                        df_comercial_metodo['metodo_entrada'] = df_comercial_metodo['metodo_entrada'].replace('',
                                                                                                              'No especificado')

                        # Filtrar comerciales con más de X contratos
                        comercial_counts = df_comercial_metodo['comercial'].value_counts()
                        comerciales_top = comercial_counts[comercial_counts >= 5].index.tolist()
                        df_top = df_comercial_metodo[df_comercial_metodo['comercial'].isin(comerciales_top)]

                        if not df_top.empty:
                            # Crear tabla pivot
                            pivot_table = pd.crosstab(
                                df_top['comercial'],
                                df_top['metodo_entrada'],
                                margins=True,
                                margins_name='Total'
                            )

                            # Calcular porcentajes por fila
                            pivot_percent = pivot_table.div(pivot_table.sum(axis=1), axis=0) * 100
                            pivot_percent = pivot_percent.round(1)

                            col1, col2 = st.columns(2)

                            with col1:
                                st.markdown("**Cantidad de Contratos**")
                                st.dataframe(
                                    pivot_table,
                                    height=300,
                                    width='stretch'
                                )

                            with col2:
                                st.markdown("**Porcentaje por Comercial**")
                                st.dataframe(
                                    pivot_percent,
                                    height=300,
                                    width='stretch'
                                )

                            # Gráfico de calor
                            st.markdown("**Mapa de Calor - Métodos por Comercial**")
                            try:
                                import plotly.express as px

                                # Preparar datos para el heatmap
                                heatmap_data = pivot_table.drop('Total', axis=0).drop('Total', axis=1)

                                fig = px.imshow(
                                    heatmap_data,
                                    text_auto=True,
                                    aspect="auto",
                                    title='Distribución de Métodos por Comercial'
                                )
                                fig.update_layout(height=400)
                                st.plotly_chart(fig, config={'responsive': True})
                            except:
                                st.info("No se pudo generar el mapa de calor")
                        else:
                            st.info("No hay suficientes datos para el análisis por comercial")
                    else:
                        st.info("⚠️ No hay datos de comercial para este análisis")

                with tab4:
                    # Método de entrada por mes
                    if 'fecha_inicio_contrato' in df_contratos.columns:
                        # Preparar datos
                        df_mes_metodo = df_contratos.copy()
                        df_mes_metodo['metodo_entrada'] = df_mes_metodo['metodo_entrada'].fillna('No especificado')
                        df_mes_metodo['metodo_entrada'] = df_mes_metodo['metodo_entrada'].replace('', 'No especificado')

                        # Crear columna de mes
                        df_mes_metodo['mes'] = df_mes_metodo['fecha_inicio_contrato'].dt.strftime('%Y-%m')

                        # Agrupar por mes y método
                        mes_metodo_stats = pd.crosstab(
                            df_mes_metodo['mes'],
                            df_mes_metodo['metodo_entrada']
                        )

                        # Ordenar por mes
                        mes_metodo_stats = mes_metodo_stats.sort_index()

                        col1, col2 = st.columns([2, 1])

                        with col1:
                            try:
                                import plotly.express as px

                                # Gráfico de área apilada
                                fig = px.area(
                                    mes_metodo_stats,
                                    title='Evolución Mensual por Método (Acumulado)',
                                    labels={'value': 'Contratos', 'variable': 'Método de Entrada'}
                                )
                                fig.update_layout(height=400)
                                st.plotly_chart(fig, config={'responsive': True})
                            except:
                                st.dataframe(mes_metodo_stats)

                        with col2:
                            # Último mes análisis
                            if not mes_metodo_stats.empty:
                                ultimo_mes = mes_metodo_stats.iloc[-1]
                                st.markdown(f"**Último mes: {mes_metodo_stats.index[-1]}**")

                                for metodo, valor in ultimo_mes.nlargest(5).items():
                                    if valor > 0:
                                        st.write(f"**{metodo}**: {valor} contratos")
                    else:
                        st.info("⚠️ No hay datos de fecha para análisis mensual")

            # Análisis por estado (mantener el existente)
            if 'estado' in df_contratos.columns:
                # Estadísticas por estado
                estado_stats = df_contratos['estado'].value_counts().reset_index()
                estado_stats.columns = ['Estado', 'Cantidad']
                estado_stats['Porcentaje'] = (estado_stats['Cantidad'] / total_contratos * 100).round(2)

                col1, col2 = st.columns([2, 1])

                with col1:
                    try:
                        import plotly.express as px
                        fig = px.bar(
                            estado_stats,
                            x='Estado',
                            y='Cantidad',
                            title='Contratos por Estado',
                            color='Estado',
                            text='Cantidad'
                        )
                        fig.update_layout(height=400, showlegend=False)
                        st.plotly_chart(fig, config={'responsive': True})
                    except:
                        st.dataframe(estado_stats)

                with col2:
                    st.dataframe(
                        estado_stats,
                        height=400,
                        width='stretch'
                    )

            # Análisis por SAT (mantener el existente)
            if 'SAT' in df_contratos.columns:
                sat_stats = df_contratos['SAT'].value_counts().reset_index()
                sat_stats.columns = ['SAT', 'Cantidad']
                sat_stats = sat_stats[sat_stats['SAT'] != ''].head(10)

                if not sat_stats.empty:
                    col1, col2 = st.columns([2, 1])

                    with col1:
                        try:
                            import plotly.express as px
                            fig = px.bar(
                                sat_stats,
                                x='SAT',
                                y='Cantidad',
                                title='Top 10 SAT',
                                color='SAT',
                                text='Cantidad'
                            )
                            fig.update_layout(height=400, showlegend=False)
                            st.plotly_chart(fig, config={'responsive': True})
                        except:
                            st.dataframe(sat_stats)

                    with col2:
                        st.dataframe(
                            sat_stats,
                            height=400,
                            width='stretch'
                        )

            # Análisis por técnico (mantener el existente)
            if 'tecnico' in df_contratos.columns:
                tecnico_stats = df_contratos['tecnico'].value_counts().reset_index()
                tecnico_stats.columns = ['Técnico', 'Cantidad']
                tecnico_stats = tecnico_stats[tecnico_stats['Técnico'] != ''].head(10)

                if not tecnico_stats.empty:
                    col1, col2 = st.columns([2, 1])

                    with col1:
                        try:
                            import plotly.express as px
                            fig = px.bar(
                                tecnico_stats,
                                x='Técnico',
                                y='Cantidad',
                                title='Top 10 Técnicos',
                                color='Técnico',
                                text='Cantidad'
                            )
                            fig.update_layout(height=400, showlegend=False)
                            st.plotly_chart(fig, config={'responsive': True})
                        except:
                            st.dataframe(tecnico_stats)

                    with col2:
                        st.dataframe(
                            tecnico_stats,
                            height=400,
                            width='stretch'
                        )

            # Análisis por tipo de cliente (mantener el existente)
            if 'Tipo_cliente' in df_contratos.columns:
                tipo_cliente_stats = df_contratos['Tipo_cliente'].value_counts().reset_index()
                tipo_cliente_stats.columns = ['Tipo Cliente', 'Cantidad']

                if not tipo_cliente_stats.empty:
                    col1, col2 = st.columns([2, 1])

                    with col1:
                        try:
                            import plotly.express as px
                            fig = px.pie(
                                tipo_cliente_stats,
                                values='Cantidad',
                                names='Tipo Cliente',
                                title='Distribución por Tipo de Cliente',
                                hole=0.3
                            )
                            fig.update_layout(height=400)
                            st.plotly_chart(fig, config={'responsive': True})
                        except:
                            st.dataframe(tipo_cliente_stats)

                    with col2:
                        st.dataframe(
                            tipo_cliente_stats,
                            height=400,
                            width='stretch'
                        )

            # ============================
            # EVOLUCIÓN TEMPORAL - REAGRUPADO
            # ============================

            if 'fecha_inicio_contrato' in df_contratos.columns:

                # ============================
                # EVOLUCIÓN TEMPORAL - REAGRUPADO
                # ============================

                if 'fecha_inicio_contrato' in df_contratos.columns:
                    st.subheader("📅 Evolución Temporal de Contratos")

                    # CORREGIDO: Ahora con 5 pestañas
                    tab1, tab2, tab3, tab4, tab5 = st.tabs([
                        "📈 Mensual",
                        "📅 Semanal",
                        "👥 Por Comercial",
                        "📊 Comparativa",
                        "📋 Últimos Contratos"
                    ])

                    with tab1:
                        # Evolución mensual
                        st.markdown("#### Evolución Mensual")

                        # Crear columna de mes
                        df_contratos['mes_inicio'] = df_contratos['fecha_inicio_contrato'].dt.to_period('M')
                        mensual = df_contratos.groupby('mes_inicio').size().reset_index()
                        mensual.columns = ['Mes', 'Contratos']
                        mensual['Mes'] = mensual['Mes'].astype(str)

                        col1, col2 = st.columns([2, 1])

                        with col1:
                            try:
                                import plotly.express as px
                                fig = px.line(
                                    mensual,
                                    x='Mes',
                                    y='Contratos',
                                    title='Contratos por Mes',
                                    markers=True
                                )
                                fig.update_layout(height=400)
                                st.plotly_chart(fig, config={'responsive': True})
                            except:
                                st.dataframe(mensual)

                        with col2:
                            # Estadísticas mensuales
                            st.markdown("**Estadísticas Mensuales**")
                            st.metric("Promedio mensual", f"{mensual['Contratos'].mean():.1f}")
                            st.metric("Máximo mensual", f"{mensual['Contratos'].max()}")
                            st.metric("Mínimo mensual", f"{mensual['Contratos'].min()}")

                            # Último mes vs anterior
                            if len(mensual) >= 2:
                                ultimo = mensual['Contratos'].iloc[-1]
                                anterior = mensual['Contratos'].iloc[-2]
                                variacion = ((ultimo - anterior) / anterior * 100) if anterior > 0 else 0
                                st.metric("Último mes vs anterior", f"{ultimo}", f"{variacion:+.1f}%")

                    with tab2:
                        # Evolución semanal
                        st.markdown("#### Evolución Semanal")

                        # Crear columna de semana (formato: Año-Semana)
                        df_contratos['semana_inicio'] = df_contratos['fecha_inicio_contrato'].dt.strftime('%Y-W%U')

                        # Agrupar por semana
                        semanal = df_contratos.groupby('semana_inicio').size().reset_index()
                        semanal.columns = ['Semana', 'Contratos']

                        # Ordenar por semana
                        semanal = semanal.sort_values('Semana')

                        # Mostrar estadísticas de la última semana
                        if not semanal.empty:
                            ultima_semana = semanal.iloc[-1]
                            st.metric(f"Última semana ({ultima_semana['Semana']})", ultima_semana['Contratos'])

                        col1, col2 = st.columns([2, 1])

                        with col1:
                            try:
                                import plotly.express as px
                                fig = px.line(
                                    semanal,
                                    x='Semana',
                                    y='Contratos',
                                    title='Contratos por Semana',
                                    markers=True
                                )
                                fig.update_layout(height=400)
                                fig.update_xaxes(tickangle=45)
                                st.plotly_chart(fig, config={'responsive': True})
                            except:
                                st.dataframe(semanal)

                        with col2:
                            # Top 5 semanas con más contratos
                            st.markdown("**Top 5 Semanas**")
                            top_semanas = semanal.sort_values('Contratos', ascending=False).head(5)
                            for idx, row in top_semanas.iterrows():
                                st.write(f"**{row['Semana']}**: {row['Contratos']} contratos")

                            # Mejor semana del año
                            if not semanal.empty:
                                mejor_semana = semanal.loc[semanal['Contratos'].idxmax()]
                                st.metric("Mejor semana", f"{mejor_semana['Contratos']}", mejor_semana['Semana'])

                    with tab3:
                        # NUEVA PESTAÑA: Evolución por comercial
                        st.markdown("#### Evolución por Comercial")

                        # Crear pestañas para vista mensual/semanal por comercial
                        sub_tab1, sub_tab2 = st.tabs(["📅 Mensual por Comercial", "📊 Semanal por Comercial"])

                        with sub_tab1:
                            st.markdown("##### Evolución Mensual por Comercial")

                            if 'comercial' in df_contratos.columns:
                                # Preparar datos mensuales por comercial
                                df_mensual_comercial = df_contratos.copy()
                                df_mensual_comercial['mes'] = df_mensual_comercial[
                                    'fecha_inicio_contrato'].dt.to_period('M')

                                # Agrupar por mes y comercial
                                mensual_comercial = df_mensual_comercial.groupby(
                                    ['mes', 'comercial']).size().reset_index()
                                mensual_comercial.columns = ['Mes', 'Comercial', 'Contratos']
                                mensual_comercial['Mes'] = mensual_comercial['Mes'].astype(str)

                                # Filtrar comerciales con más actividad
                                comercial_totals = mensual_comercial.groupby('Comercial')['Contratos'].sum()
                                top_comerciales = comercial_totals.nlargest(8).index.tolist()
                                df_top = mensual_comercial[mensual_comercial['Comercial'].isin(top_comerciales)]

                                # Selector de comerciales
                                comerciales_disponibles = sorted(df_mensual_comercial['comercial'].dropna().unique())
                                comerciales_seleccionados = st.multiselect(
                                    "Selecciona comerciales:",
                                    options=comerciales_disponibles,
                                    default=top_comerciales[:3] if top_comerciales else comerciales_disponibles[:3],
                                    key="comerciales_mensual"
                                )

                                if comerciales_seleccionados:
                                    df_filtrado = mensual_comercial[
                                        mensual_comercial['Comercial'].isin(comerciales_seleccionados)]

                                    col1, col2 = st.columns([2, 1])

                                    with col1:
                                        # Gráfico de líneas por comercial
                                        try:
                                            import plotly.express as px
                                            fig = px.line(
                                                df_filtrado,
                                                x='Mes',
                                                y='Contratos',
                                                color='Comercial',
                                                title='Contratos Mensuales por Comercial',
                                                markers=True
                                            )
                                            fig.update_layout(height=400, xaxis_tickangle=45)
                                            st.plotly_chart(fig, config={'responsive': True})
                                        except Exception as e:
                                            st.toast(f"Error al generar gráfico: {e}")
                                            st.dataframe(df_filtrado)

                                    with col2:
                                        # Tabla de resumen por comercial
                                        st.markdown("**Resumen por Comercial**")
                                        resumen_comercial = df_filtrado.groupby('Comercial')['Contratos'].agg(
                                            ['sum', 'mean', 'max']).round(1)
                                        resumen_comercial.columns = ['Total', 'Promedio', 'Máximo']
                                        resumen_comercial = resumen_comercial.sort_values('Total', ascending=False)

                                        st.dataframe(resumen_comercial, height=300)

                                        # KPIs rápidos
                                        if not resumen_comercial.empty:
                                            top_comercial = resumen_comercial.iloc[0]
                                            st.metric("Top comercial", resumen_comercial.index[0],
                                                      f"{top_comercial['Total']} total")

                                else:
                                    st.info("Selecciona al menos un comercial para ver la evolución")
                            else:
                                st.warning("No hay datos de comercial disponibles")

                        with sub_tab2:
                            st.markdown("##### Evolución Semanal por Comercial")

                            if 'comercial' in df_contratos.columns:
                                # Preparar datos semanales por comercial
                                df_semanal_comercial = df_contratos.copy()
                                df_semanal_comercial['semana'] = df_semanal_comercial[
                                    'fecha_inicio_contrato'].dt.strftime('%Y-W%U')

                                # Agrupar por semana y comercial
                                semanal_comercial = df_semanal_comercial.groupby(
                                    ['semana', 'comercial']).size().reset_index()
                                semanal_comercial.columns = ['Semana', 'Comercial', 'Contratos']
                                semanal_comercial = semanal_comercial.sort_values('Semana')

                                # Selector de comerciales
                                comerciales_disponibles = sorted(df_semanal_comercial['comercial'].dropna().unique())
                                comerciales_seleccionados = st.multiselect(
                                    "Selecciona comerciales:",
                                    options=comerciales_disponibles,
                                    default=comerciales_disponibles[:3] if comerciales_disponibles else [],
                                    key="comerciales_semanal"
                                )

                                if comerciales_seleccionados:
                                    df_filtrado = semanal_comercial[
                                        semanal_comercial['Comercial'].isin(comerciales_seleccionados)]

                                    col1, col2 = st.columns([2, 1])

                                    with col1:
                                        # Gráfico de líneas
                                        try:
                                            import plotly.express as px
                                            fig = px.line(
                                                df_filtrado,
                                                x='Semana',
                                                y='Contratos',
                                                color='Comercial',
                                                title='Contratos Semanales por Comercial',
                                                markers=True
                                            )
                                            fig.update_layout(height=400, xaxis_tickangle=45)
                                            st.plotly_chart(fig, config={'responsive': True})
                                        except Exception as e:
                                            st.toast(f"Error al generar gráfico: {e}")
                                            st.dataframe(df_filtrado)

                                    with col2:
                                        # Estadísticas de las últimas 4 semanas
                                        st.markdown("**Últimas 4 semanas**")

                                        # Obtener últimas 4 semanas únicas
                                        ultimas_semanas = sorted(df_filtrado['Semana'].unique())[-4:]
                                        df_ultimas = df_filtrado[df_filtrado['Semana'].isin(ultimas_semanas)]

                                        if not df_ultimas.empty:
                                            # Pivotear para tabla
                                            pivot_ultimas = df_ultimas.pivot_table(
                                                index='Comercial',
                                                columns='Semana',
                                                values='Contratos',
                                                fill_value=0
                                            )
                                            st.dataframe(pivot_ultimas, height=300)

                                            # Calcular crecimiento último mes
                                            if len(ultimas_semanas) >= 2:
                                                primero = ultimas_semanas[0]
                                                ultimo = ultimas_semanas[-1]
                                                st.caption(f"Crecimiento de {primero} a {ultimo}")

                                else:
                                    st.info("Selecciona al menos un comercial para ver la evolución")
                            else:
                                st.warning("No hay datos de comercial disponibles")

                    with tab4:
                        # NUEVA PESTAÑA: Comparativa entre comerciales
                        st.markdown("#### Comparativa entre Comerciales")

                        if 'comercial' in df_contratos.columns:
                            # Selector de periodo
                            periodo_opciones = ['Último mes', 'Últimos 3 meses', 'Últimos 6 meses', 'Todo el período']
                            periodo_seleccionado = st.selectbox("Selecciona período:", periodo_opciones,
                                                                key="periodo_comparativa")

                            # Filtrar por periodo
                            df_comparativa = df_contratos.copy()
                            hoy = pd.Timestamp.now()

                            if periodo_seleccionado == 'Último mes':
                                fecha_limite = hoy - pd.Timedelta(days=30)
                            elif periodo_seleccionado == 'Últimos 3 meses':
                                fecha_limite = hoy - pd.Timedelta(days=90)
                            elif periodo_seleccionado == 'Últimos 6 meses':
                                fecha_limite = hoy - pd.Timedelta(days=180)
                            else:
                                fecha_limite = df_comparativa['fecha_inicio_contrato'].min()

                            df_periodo = df_comparativa[df_comparativa['fecha_inicio_contrato'] >= fecha_limite]

                            # Top comerciales del periodo
                            top_comerciales_periodo = df_periodo['comercial'].value_counts().head(10).index.tolist()
                            df_top_periodo = df_periodo[df_periodo['comercial'].isin(top_comerciales_periodo)]

                            col1, col2 = st.columns(2)

                            with col1:
                                # Gráfico de barras por comercial
                                try:
                                    import plotly.express as px
                                    comercial_counts = df_top_periodo['comercial'].value_counts().reset_index()
                                    comercial_counts.columns = ['Comercial', 'Contratos']

                                    fig = px.bar(
                                        comercial_counts,
                                        x='Comercial',
                                        y='Contratos',
                                        title=f'Top Comerciales - {periodo_seleccionado}',
                                        color='Comercial',
                                        text='Contratos'
                                    )
                                    fig.update_layout(height=400, showlegend=False, xaxis_tickangle=45)
                                    st.plotly_chart(fig, config={'responsive': True})
                                except Exception as e:
                                    st.toast(f"Error al generar gráfico: {e}")

                            with col2:
                                # Métricas por comercial
                                st.markdown("**Métricas Clave**")

                                if not df_top_periodo.empty:
                                    # Calcular varias métricas
                                    metrics_df = df_top_periodo.groupby('comercial').agg({
                                        'apartment_id': 'count',
                                        'fecha_inicio_contrato': ['min', 'max']
                                    }).round(1)

                                    metrics_df.columns = ['Contratos', 'Primer Contrato', 'Último Contrato']
                                    metrics_df = metrics_df.sort_values('Contratos', ascending=False)

                                    # Calcular días entre primer y último contrato
                                    metrics_df['Días Activo'] = (
                                                metrics_df['Último Contrato'] - metrics_df['Primer Contrato']).dt.days
                                    metrics_df['Contratos/Día'] = (
                                                metrics_df['Contratos'] / metrics_df['Días Activo']).round(2)

                                    st.dataframe(metrics_df.head(8), height=400)

                                    # Mejor ratio
                                    if 'Contratos/Día' in metrics_df.columns:
                                        mejor_ratio = metrics_df.loc[metrics_df['Contratos/Día'].idxmax()]
                                        st.metric("Mejor ratio contratos/día",
                                                  f"{mejor_ratio['Contratos/Día']}",
                                                  mejor_ratio.name)
                        else:
                            st.warning("No hay datos de comercial disponibles")

                    with tab5:
                        # Mantener la pestaña original de últimos contratos
                        st.markdown("#### Últimos Contratos Registrados")

                        # Ordenar por fecha más reciente
                        df_recent = df_contratos.sort_values('fecha_inicio_contrato', ascending=False).head(20)

                        # Seleccionar columnas relevantes para mostrar
                        columnas_mostrar = ['num_contrato', 'cliente', 'estado', 'fecha_inicio_contrato', 'comercial']
                        columnas_mostrar = [col for col in columnas_mostrar if col in df_recent.columns]

                        # Añadir columnas nuevas si están disponibles
                        nuevas_columnas = ['SAT', 'Tipo_cliente', 'tecnico', 'metodo_entrada']
                        for col in nuevas_columnas:
                            if col in df_recent.columns:
                                columnas_mostrar.append(col)

                        st.dataframe(
                            df_recent[columnas_mostrar],
                            height=400,
                            width='stretch'
                        )

                        # Estadísticas de los últimos 30 días
                        if not df_contratos.empty:
                            from datetime import datetime, timedelta
                            hoy = datetime.now()
                            hace_30_dias = hoy - timedelta(days=30)

                            contratos_30_dias = df_contratos[
                                df_contratos['fecha_inicio_contrato'] >= hace_30_dias
                                ].shape[0]

                            st.metric("Contratos últimos 30 días", contratos_30_dias)

                with tab3:
                    # Últimos contratos registrados
                    st.markdown("#### Últimos Contratos Registrados")

                    # Ordenar por fecha más reciente
                    df_recent = df_contratos.sort_values('fecha_inicio_contrato', ascending=False).head(20)

                    # Seleccionar columnas relevantes para mostrar
                    columnas_mostrar = ['num_contrato', 'cliente', 'estado', 'fecha_inicio_contrato', 'comercial']
                    columnas_mostrar = [col for col in columnas_mostrar if col in df_recent.columns]

                    # Añadir columnas nuevas si están disponibles
                    nuevas_columnas = ['SAT', 'Tipo_cliente', 'tecnico', 'metodo_entrada']
                    for col in nuevas_columnas:
                        if col in df_recent.columns:
                            columnas_mostrar.append(col)

                    st.dataframe(
                        df_recent[columnas_mostrar],
                        height=400,
                        width='stretch'
                    )

                    # Estadísticas de los últimos 30 días
                    if not df_contratos.empty:
                        from datetime import datetime, timedelta
                        hoy = datetime.now()
                        hace_30_dias = hoy - timedelta(days=30)

                        contratos_30_dias = df_contratos[
                            df_contratos['fecha_inicio_contrato'] >= hace_30_dias
                            ].shape[0]

                        st.metric("Contratos últimos 30 días", contratos_30_dias)

            # Análisis Geográfico
            st.subheader("🗺️ Análisis Geográfico")

            if 'coordenadas' in df_contratos.columns:
                df_geo = df_contratos.copy()

                # Eliminar valores nulos y vacíos
                df_geo = df_geo.dropna(subset=['coordenadas'])
                df_geo['coordenadas'] = df_geo['coordenadas'].astype(str).str.strip()
                df_geo = df_geo[df_geo['coordenadas'] != '']

                if not df_geo.empty:
                    # Intentar extraer coordenadas de manera más robusta
                    coords_list = []
                    valid_coords = []
                    estados_list = []  # Nueva lista para almacenar estados
                    colores_list = []  # Nueva lista para almacenar colores

                    for idx, row in df_geo.iterrows():
                        coord_str = row['coordenadas']
                        try:
                            # Dividir por coma
                            parts = coord_str.split(',')
                            if len(parts) == 2:
                                lat = float(parts[0].strip())
                                lon = float(parts[1].strip())
                                # Verificar que sean coordenadas razonables
                                if -90 <= lat <= 90 and -180 <= lon <= 180:
                                    coords_list.append((lat, lon))
                                    valid_coords.append(row)

                                    # Obtener el estado y asignar color
                                    estado = row.get('estado', 'DESCONOCIDO')
                                    estados_list.append(estado)

                                    # Asignar color basado en el estado
                                    if 'FINALIZADO' in str(estado).upper():
                                        colores_list.append('#00FF00')  # Verde para FINALIZADO
                                    else:
                                        colores_list.append('#FF0000')  # Rojo para otros estados
                        except (ValueError, AttributeError):
                            continue

                    if coords_list:
                        # Crear DataFrame con coordenadas válidas
                        df_valid = pd.DataFrame(valid_coords)
                        df_valid[['lat', 'lon']] = pd.DataFrame(coords_list, index=df_valid.index)
                        df_valid['estado'] = estados_list  # Agregar columna de estados

                        st.info(f"Coordenadas válidas encontradas: {len(df_valid)} de {len(df_geo)}")

                        # Opción 1: Mapa con colores usando plotly (más personalizable)
                        try:
                            import plotly.express as px
                            import plotly.graph_objects as go

                            # Crear columna de color para plotly
                            df_valid['color_mapa'] = df_valid['estado'].apply(
                                lambda x: '#00FF00' if 'FINALIZADO' in str(x).upper() else '#FF0000'
                            )

                            # Crear columna con texto del marcador
                            df_valid['texto_marcador'] = df_valid.apply(
                                lambda row: f"Contrato: {row.get('num_contrato', 'N/A')}<br>" +
                                            f"Cliente: {row.get('cliente', 'N/A')}<br>" +
                                            f"Estado: {row.get('estado', 'N/A')}<br>" +
                                            f"Técnico: {row.get('tecnico', 'N/A')}",
                                axis=1
                            )

                            # Crear mapa con plotly
                            fig = go.Figure()

                            # Agregar puntos FINALIZADOS (verdes)
                            df_finalizados = df_valid[df_valid['color_mapa'] == '#00FF00']
                            if not df_finalizados.empty:
                                fig.add_trace(go.Scattermapbox(
                                    lat=df_finalizados['lat'],
                                    lon=df_finalizados['lon'],
                                    mode='markers',
                                    marker=dict(size=12, color='#00FF00'),
                                    text=df_finalizados['texto_marcador'],
                                    name='FINALIZADO',
                                    hovertemplate='%{text}<extra></extra>'
                                ))

                            # Agregar puntos OTROS ESTADOS (rojos)
                            df_otros = df_valid[df_valid['color_mapa'] == '#FF0000']
                            if not df_otros.empty:
                                fig.add_trace(go.Scattermapbox(
                                    lat=df_otros['lat'],
                                    lon=df_otros['lon'],
                                    mode='markers',
                                    marker=dict(size=10, color='#FF0000'),
                                    text=df_otros['texto_marcador'],
                                    name='Otros estados',
                                    hovertemplate='%{text}<extra></extra>'
                                ))

                            # Configurar el layout del mapa
                            fig.update_layout(
                                mapbox=dict(
                                    style="open-street-map",  # Estilo gratuito
                                    center=dict(lat=df_valid['lat'].mean(), lon=df_valid['lon'].mean()),
                                    zoom=10
                                ),
                                height=600,
                                title="Mapa de Contratos por Estado",
                                legend=dict(
                                    yanchor="top",
                                    y=0.99,
                                    xanchor="left",
                                    x=0.01
                                ),
                                margin={"r": 0, "t": 30, "l": 0, "b": 0}
                            )

                            st.plotly_chart(fig, use_container_width=True)

                            # Leyenda de colores
                            col1, col2 = st.columns(2)
                            with col1:
                                st.markdown("""
                                <div style="background-color: #00FF00; padding: 10px; border-radius: 5px; color: black;">
                                <strong>🟢 FINALIZADO</strong>
                                </div>
                                """, unsafe_allow_html=True)
                            with col2:
                                st.markdown("""
                                <div style="background-color: #FF0000; padding: 10px; border-radius: 5px; color: white;">
                                <strong>🔴 Otros estados</strong>
                                </div>
                                """, unsafe_allow_html=True)

                        except Exception as e:
                            st.warning(f"No se pudo crear el mapa interactivo: {e}")
                            # Opción 2: Mapa simple de Streamlit (menos personalizable)
                            st.map(df_valid[['lat', 'lon']])

                            # Mostrar tabla con colores
                            st.info("Usando mapa básico de Streamlit (todos los puntos en azul)")

                    else:
                        st.warning("No se encontraron coordenadas en formato válido")
                        # Mostrar ejemplos
                        st.write("Ejemplos encontrados:")
                        st.write(df_geo['coordenadas'].head(5).tolist())
                else:
                    st.warning("La columna 'coordenadas' está vacía o contiene solo valores nulos")
            else:
                st.warning("No hay columna 'coordenadas' para análisis geográfico")

            # Filtros
            col1, col2, col3 = st.columns(3)

            with col1:
                if 'estado' in df_contratos.columns:
                    estados = ['Todos'] + sorted(df_contratos['estado'].dropna().unique().tolist())
                    estado_filtro = st.selectbox("Filtrar por estado:", estados)
                else:
                    estado_filtro = 'Todos'

            with col2:
                if 'comercial' in df_contratos.columns:
                    comerciales = ['Todos'] + sorted(df_contratos['comercial'].dropna().unique().tolist())
                    comercial_filtro = st.selectbox("Filtrar por comercial:", comerciales)
                else:
                    comercial_filtro = 'Todos'

            with col3:
                if 'metodo_entrada' in df_contratos.columns:
                    metodos = ['Todos'] + sorted(df_contratos['metodo_entrada'].dropna().unique().tolist())
                    metodo_filtro = st.selectbox("Filtrar por método:", metodos)
                else:
                    metodo_filtro = 'Todos'

            # Selección de columnas (separada)
            columnas_disponibles = df_contratos.columns.tolist()
            columnas_default = [
                'num_contrato', 'cliente', 'estado', 'fecha_inicio_contrato',
                'comercial', 'fecha_instalacion', 'comentarios',
                'SAT', 'Tipo_cliente', 'tecnico', 'metodo_entrada'
            ]
            # Filtrar solo columnas que existen
            columnas_default = [col for col in columnas_default if col in columnas_disponibles]

            columnas_seleccionadas = st.multiselect(
                "Columnas a mostrar:",
                columnas_disponibles,
                default=columnas_default
            )

            # Aplicar filtros
            df_filtrado = df_contratos.copy()

            if estado_filtro != 'Todos' and 'estado' in df_filtrado.columns:
                df_filtrado = df_filtrado[df_filtrado['estado'] == estado_filtro]

            if comercial_filtro != 'Todos' and 'comercial' in df_filtrado.columns:
                df_filtrado = df_filtrado[df_filtrado['comercial'] == comercial_filtro]

            if metodo_filtro != 'Todos' and 'metodo_entrada' in df_filtrado.columns:
                df_filtrado = df_filtrado[df_filtrado['metodo_entrada'] == metodo_filtro]

            st.info(f"Mostrando {len(df_filtrado)} de {len(df_contratos)} contratos")

            # Mostrar tabla
            if columnas_seleccionadas:
                st.dataframe(
                    df_filtrado[columnas_seleccionadas],
                    height=400,
                    width='stretch'
                )
            else:
                st.dataframe(df_filtrado, height=400, width='stretch')

            # Botones de exportación
            col1, col2 = st.columns(2)

            with col1:
                import io
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_contratos.to_excel(writer, index=False, sheet_name='Contratos')
                output.seek(0)

                st.download_button(
                    label="📥 Descargar Todos los Contratos (Excel)",
                    data=output,
                    file_name="seguimiento_contratos_completo.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    width='stretch'
                )

            with col2:
                output_filtrado = io.BytesIO()
                with pd.ExcelWriter(output_filtrado, engine='xlsxwriter') as writer:
                    df_filtrado.to_excel(writer, index=False, sheet_name='Contratos_Filtrados')
                output_filtrado.seek(0)

                st.download_button(
                    label="📊 Descargar Datos Filtrados (Excel)",
                    data=output_filtrado,
                    file_name="seguimiento_contratos_filtrado.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    width='stretch'
                )
            # ============================
            # GENERAR INFORME PDF CON REPORTLAB
            # ============================

            # Información sobre el dataset
            st.info(f"📊 Dataset actual: {len(df_contratos)} contratos | {len(df_contratos.columns)} columnas")

            # Panel de control en dos columnas
            col1, col2 = st.columns([1, 1])

            with col1:
                # Botón para preparar datos
                if st.button("🔄 Preparar datos para informe", type="secondary", use_container_width=True):
                    with st.spinner("Preparando datos para el informe PDF..."):
                        try:
                            # Preparar datos para el PDF
                            datos_pdf = preparar_datos_para_pdf(df_contratos)

                            # Guardar en session state
                            st.session_state['datos_pdf'] = datos_pdf
                            st.session_state['df_contratos_pdf'] = df_contratos.copy()

                            st.success(f"✅ Datos preparados correctamente")
                            st.balloons()

                        except Exception as e:
                            st.error(f"❌ Error preparando datos: {str(e)}")

                with col2:
                    # Estado de preparación
                    datos_preparados = 'datos_pdf' in st.session_state and 'df_contratos_pdf' in st.session_state

                    if datos_preparados:
                        st.success("✅ Datos preparados y listos para generar PDF")

                        # Botón para generar PDF
                        if st.button("📥 Generar y Descargar PDF", type="primary", use_container_width=True):
                            with st.spinner("Generando informe PDF con ReportLab..."):
                                try:
                                    # Generar el PDF usando ReportLab
                                    pdf_file = generar_pdf_reportlab(
                                        st.session_state['df_contratos_pdf'],
                                        st.session_state['datos_pdf']
                                    )

                                    # Nombre del archivo
                                    nombre_archivo = f"informe_kpis_contratos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"

                                    # Botón de descarga
                                    st.download_button(
                                        label="⬇️ Descargar Informe PDF Completo",
                                        data=pdf_file,
                                        file_name=nombre_archivo,
                                        mime="application/pdf",
                                        use_container_width=True,
                                        key="descarga_pdf_reportlab"
                                    )

                                    st.toast(f"✅ Informe PDF generado correctamente: {nombre_archivo}")

                                except Exception as e:
                                    st.toast(f"❌ Error generando PDF: {str(e)}")
                                    import traceback
                                    with st.expander("🔍 Ver detalles del error", expanded=False):
                                        st.code(traceback.format_exc())
                    else:
                        st.info("ℹ️ Prepara los datos primero usando el botón 'Preparar datos para informe'")
                        st.warning(
                            "**Nota:** Este proceso puede tardar unos segundos dependiendo del tamaño del dataset.")

                # Panel de configuración
                with st.expander("⚙️ Configuración del informe", expanded=False):
                    st.write("**Opciones de generación:**")

                    # Opciones básicas
                    incluir_graficos = st.checkbox("Incluir resumen ejecutivo", value=True)
                    limitar_filas = st.slider("Límite de filas por tabla", 5, 50, 15)
                    formato_fecha = st.selectbox("Formato de fecha", ["DD/MM/YYYY", "YYYY-MM-DD", "MM/DD/YYYY"])

                    # Guardar configuración
                    if st.button("💾 Guardar configuración", type="secondary"):
                        st.session_state['pdf_config'] = {
                            'incluir_graficos': incluir_graficos,
                            'limitar_filas': limitar_filas,
                            'formato_fecha': formato_fecha
                        }
                        st.toast("Configuración guardada")

        except Exception as e:
            st.toast(f"❌ Error al cargar datos de seguimiento de contratos: {str(e)}")
            import traceback
            with st.expander("🔍 Ver detalles del error", expanded=False):
                st.code(traceback.format_exc())
########################

def mostrar_certificacion():
    """Muestra el panel de certificación con análisis de ofertas y observaciones"""
    st.info("📋 **Certificación de Ofertas** - Análisis completo de visitas comerciales y estado de CTOs")

    with st.spinner("⏳ Cargando y procesando datos de certificación..."):
        try:
            # Cargar datos principales
            conn = obtener_conexion()
            if conn is None:
                st.toast("❌ No se pudo conectar a la base de datos")
                return

            # Primero, obtener las columnas disponibles de comercial_rafa
            cursor = conn.cursor()

            # Método 1: Usar PRAGMA para SQLite
            cursor.execute("PRAGMA table_info(comercial_rafa)")
            columnas_comercial_rafa = [row[1] for row in cursor.fetchall()]

            # Método alternativo: Usar consulta SELECT con LIMIT 0
            # cursor.execute("SELECT * FROM comercial_rafa LIMIT 0")
            # columnas_comercial_rafa = [desc[0] for desc in cursor.description]

            st.toast(f"📊 Columnas en comercial_rafa: {len(columnas_comercial_rafa)} encontradas")

            # Verificar columnas específicas
            columnas_a_incluir = []
            columnas_base = [
                'apartment_id', 'comercial', 'serviciable', 'incidencia',
                'Tipo_Vivienda', 'observaciones', 'contrato', 'fichero_imagen'
            ]

            # Buscar variaciones de fecha
            posibles_nombres_fecha = ['fecha_visita', 'fecha', 'fecha_visita_comercial',
                                      'visita_fecha', 'fecha_visita_1', 'fecha_visita_2']

            nombre_fecha = None
            for nombre in posibles_nombres_fecha:
                if nombre in columnas_comercial_rafa:
                    nombre_fecha = nombre
                    st.toast(f"✅ Columna de fecha encontrada: {nombre_fecha}")
                    break

            # Construir consulta dinámicamente
            columnas_seleccionadas = []

            # Columnas de comercial_rafa
            for col in columnas_base:
                if col in columnas_comercial_rafa:
                    columnas_seleccionadas.append(f"cr.{col}")
                else:
                    st.toast(f"⚠️ Columna '{col}' no encontrada en comercial_rafa")

            # Añadir columna de fecha si existe
            if nombre_fecha:
                columnas_seleccionadas.append(f"cr.{nombre_fecha}")

            # Si no hay suficientes columnas, usar todas
            if len(columnas_seleccionadas) < 5:
                st.warning("⚠️ Pocas columnas encontradas, usando SELECT *")
                columnas_seleccionadas = ["cr.*"]

            # Consulta dinámica
            columnas_str = ", ".join(columnas_seleccionadas)

            query_ofertas = f"""
            SELECT 
                {columnas_str},
                du.cto,
                du.olt,
                du.provincia AS provincia_du,
                du.municipio AS municipio_du,
                du.poblacion AS poblacion_du,
                du.vial AS vial_du,
                du.numero AS numero_du
            FROM comercial_rafa cr
            LEFT JOIN datos_uis du ON cr.apartment_id = du.apartment_id
            WHERE (cr.contrato IS NULL OR LOWER(TRIM(COALESCE(cr.contrato, ''))) != 'pendiente')
            AND cr.serviciable IS NOT NULL
            """

            # Mostrar consulta para depuración
            df_ofertas = pd.read_sql(query_ofertas, conn)

            if df_ofertas.empty:
                st.warning("⚠️ No se encontraron ofertas válidas para certificación.")
                conn.close()
                return

            # Paso 2: Calcular estadísticas por CTO
            query_ctos = """
            WITH visitas_realizadas AS (
                SELECT DISTINCT apartment_id 
                FROM comercial_rafa 
                WHERE observaciones IS NOT NULL 
                AND TRIM(COALESCE(observaciones, '')) != ''
            )
            SELECT
                du.cto,
                COUNT(DISTINCT du.apartment_id) AS total_viviendas_cto,
                COUNT(DISTINCT vr.apartment_id) AS viviendas_visitadas
            FROM datos_uis du
            LEFT JOIN visitas_realizadas vr ON du.apartment_id = vr.apartment_id
            WHERE du.cto IS NOT NULL AND du.cto != ''
            GROUP BY du.cto
            """

            df_ctos = pd.read_sql(query_ctos, conn)
            conn.close()

            if df_ctos.empty:
                st.warning("⚠️ No se encontraron datos de CTOs.")
                return

            # Calcular porcentaje
            df_ctos['porcentaje_visitado'] = (
                        df_ctos['viviendas_visitadas'] / df_ctos['total_viviendas_cto'] * 100).round(2)

            # Paso 3: Unir datos
            if 'cto' in df_ofertas.columns:
                df_final = pd.merge(
                    df_ofertas,
                    df_ctos,
                    on='cto',
                    how='left',
                    suffixes=('', '_cto_stats')
                )
            else:
                # Si no hay columna cto, no podemos hacer merge
                st.toast("❌ No se encontró la columna 'cto' para unir estadísticas")
                df_final = df_ofertas.copy()
                df_final['total_viviendas_cto'] = None
                df_final['viviendas_visitadas'] = None
                df_final['porcentaje_visitado'] = None

            # Renombrar columnas para claridad
            rename_map = {}
            if 'provincia_du' in df_final.columns:
                rename_map['provincia_du'] = 'provincia'
            if 'municipio_du' in df_final.columns:
                rename_map['municipio_du'] = 'municipio'
            if 'poblacion_du' in df_final.columns:
                rename_map['poblacion_du'] = 'poblacion'
            if 'vial_du' in df_final.columns:
                rename_map['vial_du'] = 'vial'
            if 'numero_du' in df_final.columns:
                rename_map['numero_du'] = 'numero'

            if rename_map:
                df_final = df_final.rename(columns=rename_map)

            # Mostrar información sobre el DataFrame
            # Clasificar observaciones
            df_final = clasificar_observaciones(df_final)

            # Mostrar resultados
            mostrar_resultados_certificacion(df_final)

        except Exception as e:
            st.toast(f"❌ Error en el proceso de certificación: {str(e)}")
            import traceback
            with st.expander("🔍 Ver detalles del error", expanded=False):
                st.code(traceback.format_exc())
            st.toast("Error al generar la certificación", icon="❌")


def clasificar_observaciones(df):
    """Clasifica automáticamente las observaciones en categorías"""

    # Verificar si existe la columna observaciones
    if 'observaciones' not in df.columns:
        st.warning("⚠️ No se encontró la columna 'observaciones'")
        df['categoria_observacion'] = 'Sin observaciones'
        return df

    # Definir categorías
    CATEGORIAS = {
        "Cliente con otro operador": [
            "movistar", "adamo", "digi", "vodafone", "orange", "jazztel",
            "euskaltel", "netcan", "o2", "yoigo", "masmovil", "másmóvil",
            "otro operador", "no se quiere cambiar",
            "con el móvil se arreglan", "datos ilimitados"
        ],
        "Segunda residencia / vacía": [
            "segunda residencia", "casa vacía", "casa cerrada", "vacacional",
            "deshabitada", "abandonada", "cerrada", "cerrado", "no vive nadie",
            "casa en ruinas", "abandonado", "abandonada"
        ],
        "No interesado": [
            "no quiere", "no le interesa", "no interesado",
            "no contratar", "decide no contratar", "anciano", "persona mayor",
            "sin internet", "no necesita fibra", "no necesita internet",
            "no tiene interes", "no tiene interés", "no estan en casa"
        ],
        "Pendiente / seguimiento": [
            "pendiente visita", "pendiente", "dejado contacto", "dejada info",
            "dejado folleto", "presentada oferta", "hablar con hijo",
            "volver más adelante", "me llamará", "lo tiene que pensar"
        ],
        "Cliente Verde": [
            "contratado con verde", "cliente de verde", "ya es cliente de verde",
            "verde", "otro comercial"
        ],
        "Reformas / obra": [
            "reforma", "obra", "reformando", "rehabilitando", "en obras"
        ],
        "Venta / Contrato realizado": [
            "venta realizada", "vendido", "venta hecha",
            "contrata fibra", "contrato solo fibra", "contrata tarifa"
        ]
    }

    def asignar_categoria(observacion):
        if not isinstance(observacion, str) or observacion.strip() == "":
            return "Sin observaciones"

        texto = observacion.lower()

        for categoria, palabras_clave in CATEGORIAS.items():
            for palabra in palabras_clave:
                if palabra in texto:
                    return categoria

        return "Otros / sin clasificar"

    df['categoria_observacion'] = df['observaciones'].apply(asignar_categoria)
    return df


def mostrar_resultados_certificacion(df):
    """Muestra los resultados de la certificación"""

    # Mostrar información sobre columnas disponibles
    # KPIs principales
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_ofertas = len(df)
        st.metric("Ofertas Analizadas", f"{total_ofertas:,}")

    with col2:
        if 'cto' in df.columns:
            ctos_unicos = df['cto'].nunique()
            st.metric("CTOs Diferentes", f"{ctos_unicos}")
        else:
            st.metric("CTOs Diferentes", "N/A")

    with col3:
        if 'porcentaje_visitado' in df.columns:
            porcentaje_promedio = df['porcentaje_visitado'].mean()
            st.metric("% Promedio Visitado", f"{porcentaje_promedio:.1f}%")
        else:
            st.metric("% Promedio Visitado", "N/A")

    with col4:
        if 'serviciable' in df.columns:
            serviciables = (df['serviciable'] == 'Sí').sum()
            st.metric("Serviciables", f"{serviciables}")
        else:
            st.metric("Serviciables", "N/A")

    # Análisis de observaciones
    if 'categoria_observacion' in df.columns:

        with st.expander("ℹ️ Información sobre las categorías", expanded=False):
            st.info("""
            Las observaciones se clasifican automáticamente en categorías predefinidas.
            - **Cliente con otro operador**: Ya tiene servicio con otra compañía
            - **Segunda residencia / vacía**: Vivienda no habitada permanentemente
            - **No interesado**: Cliente no muestra interés en el servicio
            - **Pendiente / seguimiento**: Requiere seguimiento futuro
            - **Cliente Verde**: Ya es cliente de Verde
            - **Reformas / obra**: Vivienda en obras o reformas
            - **Venta / Contrato realizado**: Venta exitosa
            - **Sin observaciones**: No hay comentarios registrados
            """)

        # Resumen por categoría
        resumen = df['categoria_observacion'].value_counts().reset_index()
        resumen.columns = ['Categoría', 'Cantidad']
        resumen['Porcentaje'] = (resumen['Cantidad'] / len(df) * 100).round(1)

        col1, col2 = st.columns([2, 1])

        with col1:
            # Gráfico de barras
            try:
                import plotly.express as px
                fig = px.bar(
                    resumen,
                    x='Categoría',
                    y='Cantidad',
                    title='Distribución por Categoría',
                    color='Categoría'
                )
                fig.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig, config={'width': 'stretch', 'theme': 'streamlit'})
            except:
                st.dataframe(resumen)

        with col2:
            st.dataframe(
                resumen,
                width='stretch',
                height=400
            )
    else:
        st.warning("⚠️ No se pudo clasificar las observaciones")

    # Filtrar columnas que realmente existen en el DataFrame
    columnas_disponibles = df.columns.tolist()

    # Definir columnas por defecto basadas en las disponibles
    posibles_columnas = [
        'apartment_id', 'comercial', 'provincia', 'municipio',
        'cto', 'serviciable', 'categoria_observacion',
        'observaciones'
    ]

    # Buscar columna de fecha
    posibles_fechas = [col for col in df.columns if 'fecha' in col.lower() or 'visita' in col.lower()]
    if posibles_fechas:
        posibles_columnas.append(posibles_fechas[0])

    columnas_default = [col for col in posibles_columnas if col in columnas_disponibles]

    # Si no hay columnas por defecto, usar las primeras 5
    if not columnas_default and len(columnas_disponibles) > 0:
        columnas_default = columnas_disponibles[:5]

    col1, col2 = st.columns([3, 1])

    with col1:
        columnas_seleccionadas = st.multiselect(
            "Selecciona columnas a mostrar:",
            columnas_disponibles,
            default=columnas_default,
            key="cert_cols_selector"
        )

    with col2:
        # Filtro por comercial si existe
        if 'comercial' in df.columns:
            comerciales = ['Todos'] + sorted(df['comercial'].dropna().unique().tolist())
            comercial_filtro = st.selectbox("Filtrar por comercial:", comerciales)
        else:
            comercial_filtro = 'Todos'

    # Aplicar filtro si es necesario
    df_filtrado = df.copy()
    if comercial_filtro != 'Todos' and 'comercial' in df.columns:
        df_filtrado = df_filtrado[df_filtrado['comercial'] == comercial_filtro]
        st.info(f"Mostrando {len(df_filtrado)} registros del comercial: {comercial_filtro}")

    if columnas_seleccionadas:
        st.dataframe(
            df_filtrado[columnas_seleccionadas],
            width='stretch',
            height=500
        )
    else:
        st.warning("Por favor, selecciona al menos una columna para mostrar")

    # Exportación
    col1, col2, col3 = st.columns(3)

    with col1:
        # Exportar a Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Certificación')
        output.seek(0)

        st.download_button(
            label="📥 Excel Completo",
            data=output,
            file_name="certificacion_ofertas.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            width='stretch'
        )

    with col2:
        # Exportar datos filtrados
        output_filtrado = io.BytesIO()
        with pd.ExcelWriter(output_filtrado, engine='xlsxwriter') as writer:
            df_filtrado.to_excel(writer, index=False, sheet_name='Datos_Filtrados')
        output_filtrado.seek(0)

        st.download_button(
            label="📊 Datos Filtrados",
            data=output_filtrado,
            file_name="certificacion_filtrada.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            width='stretch'
        )

    with col3:
        # Exportar resumen
        if 'categoria_observacion' in df.columns:
            resumen = df['categoria_observacion'].value_counts().reset_index()
            resumen.columns = ['Categoría', 'Cantidad']

            output_resumen = io.BytesIO()
            with pd.ExcelWriter(output_resumen, engine='xlsxwriter') as writer:
                resumen.to_excel(writer, index=False, sheet_name='Resumen')
            output_resumen.seek(0)

            st.download_button(
                label="📈 Resumen",
                data=output_resumen,
                file_name="resumen_certificacion.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width='stretch'
            )

def generar_informe(fecha_inicio, fecha_fin):
    # Conectar a la base de datos y realizar cada consulta
    def ejecutar_consulta(query, params=None):
        # Abrir la conexión para cada consulta
        conn = obtener_conexion()
        cursor = conn.cursor()
        cursor.execute(query, params if params else ())
        result = cursor.fetchone()
        conn.close()  # Cerrar la conexión inmediatamente después de ejecutar la consulta
        return result[0] if result else 0

    # 🔹 1️⃣ Total de asignaciones en el periodo T
    query_total = """
        SELECT COUNT(DISTINCT apartment_id) 
        FROM datos_uis
        WHERE STRFTIME('%Y-%m-%d', fecha) BETWEEN ? AND ?
    """
    total_asignaciones = ejecutar_consulta(query_total, (fecha_inicio, fecha_fin))

    # 🔹 2️⃣ Cantidad de visitas (apartment_id presente en ambas tablas, sin filtrar por fecha)
    query_visitados = """
        SELECT COUNT(DISTINCT d.apartment_id)
        FROM datos_uis d
        INNER JOIN comercial_rafa o 
            ON d.apartment_id = o.apartment_id
    """
    total_visitados = ejecutar_consulta(query_visitados)

    # 🔹 3️⃣ Cantidad de ventas (visitados donde contrato = 'Sí')
    query_ventas = """
        SELECT COUNT(DISTINCT d.apartment_id)
        FROM datos_uis d
        INNER JOIN comercial_rafa o 
            ON d.apartment_id = o.apartment_id
        WHERE LOWER(o.contrato) = 'sí'
    """
    total_ventas = ejecutar_consulta(query_ventas)

    # 🔹 4️⃣ Cantidad de incidencias (donde incidencia = 'Sí')
    query_incidencias = """
        SELECT COUNT(DISTINCT d.apartment_id)
        FROM datos_uis d
        INNER JOIN comercial_rafa o 
            ON d.apartment_id = o.apartment_id
        WHERE LOWER(o.incidencia) = 'sí'
    """
    total_incidencias = ejecutar_consulta(query_incidencias)

    # 🔹 5️⃣ Cantidad de viviendas no serviciables (donde serviciable = 'No')
    query_no_serviciables = """
        SELECT COUNT(DISTINCT apartment_id)
        FROM comercial_rafa
        WHERE LOWER(serviciable) = 'no'
    """
    total_no_serviciables = ejecutar_consulta(query_no_serviciables)

    # 🔹 6️⃣ Cálculo de porcentajes
    porcentaje_ventas = (total_ventas / total_visitados * 100) if total_visitados > 0 else 0
    porcentaje_visitas = (total_visitados / total_asignaciones * 100) if total_asignaciones > 0 else 0
    porcentaje_incidencias = (total_incidencias / total_visitados * 100) if total_visitados > 0 else 0
    porcentaje_no_serviciables = (total_no_serviciables / total_visitados * 100) if total_visitados > 0 else 0

    # 🔹 7️⃣ Crear DataFrame con los resultados
    informe = pd.DataFrame({
        'Total Asignaciones Directas': [total_asignaciones],
        'Visitados': [total_visitados],
        'Ventas': [total_ventas],
        'Incidencias': [total_incidencias],
        'Viviendas No Serviciables': [total_no_serviciables],
        '% Ventas': [porcentaje_ventas],
        '% Visitas': [porcentaje_visitas],
        '% Incidencias': [porcentaje_incidencias],
        '% Viviendas No Serviciables': [porcentaje_no_serviciables]
    })
    st.write("----------------------")
    # Crear tres columnas para los gráficos
    col1, col2, col3 = st.columns(3)

    with col1:
        labels = ['Ventas', 'Visitas']
        values = [porcentaje_ventas, porcentaje_visitas]
        fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.3,
                                     textinfo='percent+label',
                                     marker=dict(colors=['#66b3ff', '#ff9999']))])
        fig.update_layout(title="Distribución de Visitas y Ventas", title_x=0, plot_bgcolor='white', showlegend=False)
        st.plotly_chart(fig, config={'width': 'stretch', 'theme': 'streamlit'})

    with col2:
        labels_incidencias = ['Incidencias', 'Visitas']
        values_incidencias = [porcentaje_incidencias, porcentaje_visitas]
        fig_incidencias = go.Figure(data=[go.Pie(labels=labels_incidencias, values=values_incidencias, hole=0.3,
                                                 textinfo='percent+label',
                                                 marker=dict(colors=['#ff6666', '#99cc99']))])
        fig_incidencias.update_layout(title="Distribución de Visitas e Incidencias", title_x=0, plot_bgcolor='white',
                                      showlegend=False)
        st.plotly_chart(fig_incidencias, config={'width': 'stretch', 'theme': 'streamlit'})

    with col3:
        labels_serviciables = ['No Serviciables', 'Serviciables']
        values_serviciables = [porcentaje_no_serviciables, 100 - porcentaje_no_serviciables]
        fig_serviciables = go.Figure(data=[go.Bar(
            x=labels_serviciables,
            y=values_serviciables,
            text=values_serviciables,
            textposition='outside',
            marker=dict(color=['#ff6666', '#99cc99'])
        )])
        fig_serviciables.update_layout(
            title="Distribución Viviendas visitadas Serviciables/No Serviciables",
            title_x=0,
            plot_bgcolor='rgba(0, 0, 0, 0)',  # Fondo transparente
            showlegend=False,
            xaxis_title="Estado de Viviendas",
            yaxis_title="Porcentaje",
            xaxis=dict(tickangle=0),
            height=450
        )
        st.plotly_chart(fig_serviciables, config={'width': 'stretch', 'theme': 'streamlit'})

    # Resumen de los resultados
    resumen = f"""
    <div style="text-align: justify;">
    Durante el periodo analizado, que abarca desde el <strong>{fecha_inicio}</strong> hasta el <strong>{fecha_fin}</strong>, se han registrado un total de <strong>{total_asignaciones}</strong> asignaciones realizadas, lo que indica la cantidad de propiedades consideradas para asignación en este intervalo. De estas asignaciones, <strong>{total_visitados}</strong> propiedades fueron visitadas, lo que representa un <strong>{porcentaje_visitas:.2f}%</strong> del total de asignaciones. Esto refleja el grado de éxito en la conversión de asignaciones a visitas, lo que es un indicador de la efectividad de la asignación de propiedades.
    De las propiedades visitadas, <strong>{total_ventas}</strong> viviendas fueron finalmente vendidas, lo que constituye el <strong>{porcentaje_ventas:.2f}%</strong> de las propiedades visitadas. Este porcentaje es crucial, ya que nos muestra cuán efectivas han sido las visitas en convertir en ventas las oportunidades de negocio. A su vez, se han registrado <strong>{total_incidencias}</strong> incidencias durante las visitas, lo que equivale a un <strong>{porcentaje_incidencias:.2f}%</strong> de las asignaciones. Las incidencias indican problemas o dificultades encontradas en las propiedades, lo que podría afectar la decisión de los posibles compradores.
    Por otro lado, en cuanto a la calidad del inventario, <strong>{total_no_serviciables}</strong> propiedades fueron catalogadas como no serviciables, lo que representa un <strong>{porcentaje_no_serviciables:.2f}%</strong> del total de asignaciones.
    </div>
    <br>
    """
    st.markdown(resumen, unsafe_allow_html=True)

    # 🔹 VIABILIDADES: Cálculo y resumen textual
    conn = obtener_conexion()
    query_viabilidades = """
           SELECT 
               CASE 
                   WHEN LOWER(serviciable) = 'sí' THEN 'sí'
                   WHEN LOWER(serviciable) = 'no' THEN 'no'
                   ELSE 'desconocido'
               END AS serviciable,
               COUNT(*) as total
           FROM viabilidades
           WHERE STRFTIME('%Y-%m-%d', fecha_viabilidad) BETWEEN ? AND ?
           GROUP BY serviciable
       """
    df_viabilidades = pd.read_sql_query(query_viabilidades, conn, params=(fecha_inicio, fecha_fin))
    conn.close()

    total_viabilidades = df_viabilidades['total'].sum()
    total_serviciables = df_viabilidades[df_viabilidades['serviciable'] == 'sí']['total'].sum() if 'sí' in \
                                                                                                          df_viabilidades[
                                                                                                              'serviciable'].values else 0
    total_no_serviciables_v = df_viabilidades[df_viabilidades['serviciable'] == 'no']['total'].sum() if 'no' in \
                                                                                                               df_viabilidades[
                                                                                                                   'serviciable'].values else 0

    porcentaje_viables = (total_serviciables / total_viabilidades * 100) if total_viabilidades > 0 else 0
    porcentaje_no_viables = (total_no_serviciables_v / total_viabilidades * 100) if total_viabilidades > 0 else 0

    resumen_viabilidades = f"""
       <div style="text-align: justify;">
       Además, durante el mismo periodo se registraron <strong>{total_viabilidades}</strong> viabilidades realizadas. De estas, <strong>{total_serviciables}</strong> fueron consideradas <strong>serviciables</strong> (<strong>{porcentaje_viables:.2f}%</strong>) y <strong>{total_no_serviciables_v}</strong> fueron <strong>no serviciables</strong> (<strong>{porcentaje_no_viables:.2f}%</strong>). Las restantes, son viabilidades aun en estudio.
       </div>
       <br>
       """

    st.markdown(resumen_viabilidades, unsafe_allow_html=True)

    # ─────────────────────────────────────────────
    # 🔹 Informe de Trazabilidad (Asignación y Desasignación)
    # ─────────────────────────────────────────────
    st.write("----------------------")
    query_asignaciones_trazabilidad = """
        SELECT COUNT(*) 
        FROM trazabilidad
        WHERE LOWER(accion) LIKE '%asignación%' 
          AND STRFTIME('%Y-%m-%d', fecha) BETWEEN ? AND ?
    """
    query_desasignaciones = """
        SELECT COUNT(*) 
        FROM trazabilidad
        WHERE LOWER(accion) LIKE '%desasignación%' 
          AND STRFTIME('%Y-%m-%d', fecha) BETWEEN ? AND ?
    """
    total_asignaciones_trazabilidad = ejecutar_consulta(query_asignaciones_trazabilidad, (fecha_inicio, fecha_fin))
    total_desasignaciones = ejecutar_consulta(query_desasignaciones, (fecha_inicio, fecha_fin))
    total_movimientos = total_asignaciones_trazabilidad + total_desasignaciones

    porcentaje_asignaciones = (
                total_asignaciones_trazabilidad / total_movimientos * 100) if total_movimientos > 0 else 0
    porcentaje_desasignaciones = (total_desasignaciones / total_movimientos * 100) if total_movimientos > 0 else 0

    informe_trazabilidad = pd.DataFrame({
        'Asignaciones Gestor': [total_asignaciones_trazabilidad],
        'Desasignaciones Gestor': [total_desasignaciones],
        'Total Movimientos': [total_movimientos],
        '% Asignaciones': [porcentaje_asignaciones],
        '% Desasignaciones': [porcentaje_desasignaciones]
    })

    col_t1, col_t2 = st.columns(2)
    with col_t1:
        fig_mov = go.Figure()

        fig_mov.add_trace(go.Bar(
            x=[porcentaje_asignaciones],
            y=['Asignaciones'],
            orientation='h',
            name='Asignaciones',
            marker=dict(color='#3366cc'),
            text=f"{porcentaje_asignaciones:.1f}%",
            textposition="auto",
            width=0.5  # 👈 Más fino (por defecto es 0.8)
        ))

        fig_mov.add_trace(go.Bar(
            x=[porcentaje_desasignaciones],
            y=['Desasignaciones'],
            orientation='h',
            name='Desasignaciones',
            marker=dict(color='#ff9933'),
            text=f"{porcentaje_desasignaciones:.1f}%",
            textposition="auto",
            width=0.5  # 👈 Más fino
        ))

        fig_mov.update_layout(
            title="Distribución Asignaciones/Desasignaciones realizadas por el gestor",
            xaxis_title="Porcentaje (%)",
            yaxis_title="Tipo de Movimiento",
            barmode='stack',  # Esto apila las barras
            showlegend=False,
            title_x=0,
            bargap=0.05,  # Menor espacio entre las barras
            xaxis=dict(
                range=[0, 100],  # Para que la escala vaya del 0 al 100
            ),
            yaxis=dict(
                tickmode='array',
                tickvals=['Asignaciones', 'Desasignaciones'],
                ticktext=['Asignaciones', 'Desasignaciones']
            ),
            width=400,  # Ancho del gráfico
            height=300  # Ajusta la altura aquí (por ejemplo, 300px)
        )

        st.plotly_chart(fig_mov, config={'width': 'stretch', 'theme': 'streamlit'})

    with col_t2:
        st.markdown("<div style='margin-top:40px;'>", unsafe_allow_html=True)
        st.dataframe(informe_trazabilidad)
        resumen_trazabilidad = f"""
            <div style="text-align: justify;">
            En el periodo analizado, del <strong>{fecha_inicio}</strong> al <strong>{fecha_fin}</strong>, se han registrado un total de <strong>{total_movimientos}</strong> movimientos en la trazabilidad realizados por el gestor comercial. De ellos, <strong>{total_asignaciones_trazabilidad}</strong> corresponden a asignaciones (<strong>{porcentaje_asignaciones:.2f}%</strong>) y <strong>{total_desasignaciones}</strong> a desasignaciones (<strong>{porcentaje_desasignaciones:.2f}%</strong>). 
            </div>
            <br>
            """
        st.markdown(resumen_trazabilidad, unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    st.write("----------------------")

    # ─────────────────────────────────────────────
    # 🔹 VIABILIDADES: Resumen Detallado (Serviciable / Estado / Resultado)
    # ─────────────────────────────────────────────
    st.subheader("📋 Informe de Viabilidades")

    conn = obtener_conexion()

    # 1️⃣ Serviciable (Sí / No / Desconocido)
    query_serviciable = """
        SELECT 
            CASE 
                WHEN LOWER(serviciable) = 'sí' THEN 'Sí'
                WHEN LOWER(serviciable) = 'no' THEN 'No'
                ELSE 'Desconocido'
            END AS Serviciable,
            COUNT(*) AS Total
        FROM viabilidades
        WHERE STRFTIME('%Y-%m-%d', fecha_viabilidad) BETWEEN ? AND ?
        GROUP BY Serviciable
    """
    df_serviciable = pd.read_sql_query(query_serviciable, conn, params=(fecha_inicio, fecha_fin))
    total_viabilidades = df_serviciable["Total"].sum() if not df_serviciable.empty else 0

    # 2️⃣ Estado (fase administrativa)
    query_estado = """
        SELECT 
            COALESCE(estado, 'Sin estado') AS Estado,
            COUNT(*) AS Total
        FROM viabilidades
        WHERE STRFTIME('%Y-%m-%d', fecha_viabilidad) BETWEEN ? AND ?
        GROUP BY Estado
        ORDER BY Total DESC
    """
    df_estado = pd.read_sql_query(query_estado, conn, params=(fecha_inicio, fecha_fin))

    # 3️⃣ Resultado (dictamen final)
    query_resultado = """
        SELECT 
            COALESCE(resultado, 'Sin resultado') AS Resultado,
            COUNT(*) AS Total
        FROM viabilidades
        WHERE STRFTIME('%Y-%m-%d', fecha_viabilidad) BETWEEN ? AND ?
        GROUP BY Resultado
        ORDER BY Total DESC
    """
    df_resultado = pd.read_sql_query(query_resultado, conn, params=(fecha_inicio, fecha_fin))

    # 4️⃣ Viabilidades con comentarios del gestor
    query_comentarios = """
        SELECT COUNT(*) FROM viabilidades 
        WHERE comentarios_gestor IS NOT NULL AND TRIM(comentarios_gestor) <> ''
          AND STRFTIME('%Y-%m-%d', fecha_viabilidad) BETWEEN ? AND ?
    """
    total_comentarios = ejecutar_consulta(query_comentarios, (fecha_inicio, fecha_fin))
    porcentaje_comentarios = (total_comentarios / total_viabilidades * 100) if total_viabilidades > 0 else 0

    conn.close()

    # ──────────────────────────────
    # VISUALIZACIONES
    # ──────────────────────────────
    colv1, colv2 = st.columns(2)
    with colv1:
        fig_s = go.Figure(data=[go.Pie(
            labels=df_serviciable["Serviciable"],
            values=df_serviciable["Total"],
            hole=0.4,
            textinfo="percent+label",
            marker=dict(colors=["#81c784", "#e57373", "#bdbdbd"])
        )])
        fig_s.update_layout(
            title="Distribución de Viabilidades (Serviciables / No / Desconocidas)",
            title_x=0.1,
            showlegend=False
        )
        st.plotly_chart(fig_s, config={'width': 'stretch', 'theme': 'streamlit'})

    with colv2:
        fig_e = go.Figure(data=[go.Bar(
            x=df_estado["Estado"],
            y=df_estado["Total"],
            text=df_estado["Total"],
            textposition="outside"
        )])
        fig_e.update_layout(
            title="Distribución por Estado de Viabilidad",
            title_x=0.1,
            xaxis_title="Estado",
            yaxis_title="Número de Viabilidades",
            height=400
        )
        st.plotly_chart(fig_e, config={'width': 'stretch', 'theme': 'streamlit'})

    colv3, colv4 = st.columns(2)
    with colv3:
        fig_r = go.Figure(data=[go.Bar(
            x=df_resultado["Resultado"],
            y=df_resultado["Total"],
            text=df_resultado["Total"],
            textposition="outside"
        )])
        fig_r.update_layout(
            title="Distribución por Resultado de Viabilidad",
            title_x=0.1,
            xaxis_title="Resultado",
            yaxis_title="Número de Casos",
            height=400
        )
        st.plotly_chart(fig_r, config={'width': 'stretch', 'theme': 'streamlit'})

    with colv4:
        st.metric(label="💬 Viabilidades con Comentarios del Gestor",
                  value=f"{total_comentarios}",
                  delta=f"{porcentaje_comentarios:.2f}% del total")

    # ──────────────────────────────
    # RESUMEN DESCRIPTIVO
    # ──────────────────────────────
    resumen_viabilidades = f"""
    <div style="text-align: justify;">
    En el periodo comprendido entre <strong>{fecha_inicio}</strong> y <strong>{fecha_fin}</strong>, 
    se registraron un total de <strong>{total_viabilidades}</strong> viabilidades.  
    De ellas, las categorías de <strong>serviciabilidad</strong> se distribuyen así:
    <ul>
    {"".join([f"<li>{row['Serviciable']}: <strong>{row['Total']}</strong></li>" for _, row in df_serviciable.iterrows()])}
    </ul>
    Respecto al <strong>estado administrativo</strong>, los casos se reparten entre:
    <ul>
    {"".join([f"<li>{row['Estado']}: <strong>{row['Total']}</strong></li>" for _, row in df_estado.iterrows()])}
    </ul>
    Y en cuanto al <strong>resultado final</strong> de las viabilidades:
    <ul>
    {"".join([f"<li>{row['Resultado']}: <strong>{row['Total']}</strong></li>" for _, row in df_resultado.iterrows()])}
    </ul>
    Finalmente, <strong>{total_comentarios}</strong> viabilidades (<strong>{porcentaje_comentarios:.2f}%</strong>) 
    incluyen comentarios del gestor, lo que refleja el nivel de seguimiento técnico del proceso.
    </div>
    """
    st.markdown(resumen_viabilidades, unsafe_allow_html=True)

    # ─────────────────────────────────────────────
    # 🔹 INFORME DE PRECONTRATOS
    # ─────────────────────────────────────────────
    st.write("----------------------")
    st.subheader("📄 Informe de Precontratos")

    conn = obtener_conexion()

    # 1️⃣ Total de precontratos en el periodo
    query_total_precontratos = """
           SELECT COUNT(*) 
           FROM precontratos 
           WHERE STRFTIME('%Y-%m-%d', fecha) BETWEEN ? AND ?
       """
    total_precontratos = ejecutar_consulta(query_total_precontratos, (fecha_inicio, fecha_fin))

    # 2️⃣ Precontratos por comercial
    query_precontratos_comercial = """
           SELECT comercial, COUNT(*) as total
           FROM precontratos
           WHERE STRFTIME('%Y-%m-%d', fecha) BETWEEN ? AND ?
           GROUP BY comercial
           ORDER BY total DESC
       """
    df_precontratos_comercial = pd.read_sql_query(query_precontratos_comercial, conn, params=(fecha_inicio, fecha_fin))

    # 3️⃣ Precontratos por tarifa
    query_precontratos_tarifa = """
           SELECT tarifas, COUNT(*) as total
           FROM precontratos
           WHERE STRFTIME('%Y-%m-%d', fecha) BETWEEN ? AND ?
           GROUP BY tarifas
           ORDER BY total DESC
       """
    df_precontratos_tarifa = pd.read_sql_query(query_precontratos_tarifa, conn, params=(fecha_inicio, fecha_fin))

    # 4️⃣ Precontratos completados (con firma)
    query_precontratos_completados = """
           SELECT COUNT(*) 
           FROM precontratos 
           WHERE firma IS NOT NULL 
             AND TRIM(firma) <> ''
             AND STRFTIME('%Y-%m-%d', fecha) BETWEEN ? AND ?
       """
    total_precontratos_completados = ejecutar_consulta(query_precontratos_completados, (fecha_inicio, fecha_fin))
    porcentaje_completados = (
                total_precontratos_completados / total_precontratos * 100) if total_precontratos > 0 else 0

    conn.close()

    # Visualizaciones Precontratos
    colp1, colp2 = st.columns(2)

    with colp1:
        if not df_precontratos_comercial.empty:
            fig_prec_comercial2 = go.Figure(data=[go.Bar(
                x=df_precontratos_comercial['comercial'],
                y=df_precontratos_comercial['total'],
                text=df_precontratos_comercial['total'],
                textposition='outside',
                marker_color='#4CAF50'
            )])
            fig_prec_comercial2.update_layout(
                title="Precontratos por Comercial",
                xaxis_title="Comercial",
                yaxis_title="Número de Precontratos",
                height=400
            )
            # Corrección 1: Pasar el parámetro 'key' único
            st.plotly_chart(fig_prec_comercial2, config={'width': 'stretch', 'theme': 'streamlit'},
                            key="precontratos_comercial_bar")

    with colp2:
        if not df_precontratos_tarifa.empty:
            fig_prec_tarifa = go.Figure(data=[go.Pie(
                labels=df_precontratos_tarifa['tarifas'],
                values=df_precontratos_tarifa['total'],
                textinfo='percent+label',
                hole=0.4,
                marker=dict(colors=['#FF9800', '#2196F3', '#9C27B0', '#E91E63'])
            )])
            fig_prec_tarifa.update_layout(
                title="Distribución por Tarifa",
                showlegend=True
            )
            # Corrección 2: Usar la figura CORRECTA (fig_prec_tarifa) y un 'key' único
            st.plotly_chart(fig_prec_tarifa, config={'width': 'stretch', 'theme': 'streamlit'},
                            key="precontratos_tarifa_pie")

    # Métricas Precontratos
    col_met1, col_met2, col_met3 = st.columns(3)
    with col_met1:
        st.metric("Total Precontratos", total_precontratos)
    with col_met2:
        st.metric("Precontratos Completados", total_precontratos_completados)
    with col_met3:
        st.metric("Tasa de Completado", f"{porcentaje_completados:.1f}%")

    # Resumen Precontratos
    resumen_precontratos = f"""
       <div style="text-align: justify;">
       En el periodo analizado, se han generado <strong>{total_precontratos}</strong> precontratos. 
       De estos, <strong>{total_precontratos_completados}</strong> han sido completados por los clientes, 
       lo que representa una tasa de completado del <strong>{porcentaje_completados:.1f}%</strong>.
       {" El comercial con mayor número de precontratos es " + df_precontratos_comercial.iloc[0]['comercial'] + " con " + str(df_precontratos_comercial.iloc[0]['total']) + " precontratos." if not df_precontratos_comercial.empty else ""}
       {" La tarifa más utilizada es " + df_precontratos_tarifa.iloc[0]['tarifas'] + " con " + str(df_precontratos_tarifa.iloc[0]['total']) + " precontratos." if not df_precontratos_tarifa.empty else ""}
       </div>
       <br>
       """
    st.markdown(resumen_precontratos, unsafe_allow_html=True)

    # ─────────────────────────────────────────────
    # 🔹 INFORME DE CONTRATOS
    # ─────────────────────────────────────────────
    st.write("----------------------")
    st.subheader("📊 Informe de Contratos")

    conn = obtener_conexion()

    # 1️⃣ Total de contratos en el periodo
    query_total_contratos = """
           SELECT COUNT(*) 
           FROM seguimiento_contratos 
           WHERE STRFTIME('%Y-%m-%d', fecha_ingreso) BETWEEN ? AND ?
       """
    total_contratos = ejecutar_consulta(query_total_contratos, (fecha_inicio, fecha_fin))

    # 2️⃣ Contratos por estado
    query_contratos_estado = """
           SELECT estado, COUNT(*) as total
           FROM seguimiento_contratos
           WHERE STRFTIME('%Y-%m-%d', fecha_ingreso) BETWEEN ? AND ?
           GROUP BY estado
           ORDER BY total DESC
       """
    df_contratos_estado = pd.read_sql_query(query_contratos_estado, conn, params=(fecha_inicio, fecha_fin))

    # 3️⃣ Contratos por comercial
    query_contratos_comercial = """
           SELECT comercial, COUNT(*) as total
           FROM seguimiento_contratos
           WHERE STRFTIME('%Y-%m-%d', fecha_ingreso) BETWEEN ? AND ?
           GROUP BY comercial
           ORDER BY total DESC
       """
    df_contratos_comercial = pd.read_sql_query(query_contratos_comercial, conn, params=(fecha_inicio, fecha_fin))

    # 4️⃣ Contratos activos vs finalizados
    query_contratos_activos = """
           SELECT COUNT(*) 
           FROM seguimiento_contratos 
           WHERE estado IN ('Activo', 'En proceso', 'Pendiente')
             AND STRFTIME('%Y-%m-%d', fecha_ingreso) BETWEEN ? AND ?
       """
    total_contratos_activos = ejecutar_consulta(query_contratos_activos, (fecha_inicio, fecha_fin))
    porcentaje_activos = (total_contratos_activos / total_contratos * 100) if total_contratos > 0 else 0

    # 5️⃣ Contratos con fecha de instalación
    query_contratos_instalados = """
           SELECT COUNT(*) 
           FROM seguimiento_contratos 
           WHERE fecha_instalacion IS NOT NULL 
             AND TRIM(fecha_instalacion) <> ''
             AND STRFTIME('%Y-%m-%d', fecha_ingreso) BETWEEN ? AND ?
       """
    total_contratos_instalados = ejecutar_consulta(query_contratos_instalados, (fecha_inicio, fecha_fin))
    porcentaje_instalados = (total_contratos_instalados / total_contratos * 100) if total_contratos > 0 else 0

    conn.close()

    # Visualizaciones Contratos
    colc1, colc2 = st.columns(2)
    with colc1:
        if not df_contratos_estado.empty:
            fig_cont_estado = go.Figure(data=[go.Bar(
                x=df_contratos_estado['estado'],
                y=df_contratos_estado['total'],
                text=df_contratos_estado['total'],
                textposition='outside',
                marker_color='#2196F3'
            )])
            fig_cont_estado.update_layout(
                title="Contratos por Estado",
                xaxis_title="Estado",
                yaxis_title="Número de Contratos",
                height=400
            )
            st.plotly_chart(fig_cont_estado, config={'width': 'stretch', 'theme': 'streamlit'})

    with colc2:
        if not df_contratos_comercial.empty:
            fig_cont_comercial = go.Figure(data=[go.Pie(
                labels=df_contratos_comercial['comercial'],
                values=df_contratos_comercial['total'],
                textinfo='percent+label',
                hole=0.4,
                marker=dict(colors=['#FF5722', '#795548', '#607D8B', '#009688'])
            )])
            fig_cont_comercial.update_layout(
                title="Distribución por Comercial",
                showlegend=True
            )
            st.plotly_chart(fig_cont_comercial, config={'width': 'stretch', 'theme': 'streamlit'})

    # Métricas Contratos
    col_metc1, col_metc2, col_metc3, col_metc4 = st.columns(4)
    with col_metc1:
        st.metric("Total Contratos", total_contratos)
    with col_metc2:
        st.metric("Contratos Activos", total_contratos_activos)
    with col_metc3:
        st.metric("Tasa de Activos", f"{porcentaje_activos:.1f}%")
    with col_metc4:
        st.metric("Contratos Instalados", total_contratos_instalados)

    # Resumen Contratos
    resumen_contratos = f"""
       <div style="text-align: justify;">
       En el periodo analizado, se han registrado <strong>{total_contratos}</strong> contratos en el sistema. 
       De estos, <strong>{total_contratos_activos}</strong> se encuentran activos o en proceso 
       (<strong>{porcentaje_activos:.1f}%</strong> del total), y <strong>{total_contratos_instalados}</strong> 
       ya cuentan con fecha de instalación confirmada.
       {" El estado más común es " + df_contratos_estado.iloc[0]['estado'] + " con " + str(df_contratos_estado.iloc[0]['total']) + " contratos." if not df_contratos_estado.empty else ""}
       {" El comercial con mayor número de contratos es " + df_contratos_comercial.iloc[0]['comercial'] + " con " + str(df_contratos_comercial.iloc[0]['total']) + " contratos." if not df_contratos_comercial.empty else ""}
       </div>
       <br>
       """
    st.markdown(resumen_contratos, unsafe_allow_html=True)

    return informe

# Función para leer y mostrar el control de versiones
def mostrar_control_versiones():
    try:
        # Conexión a la base de datos
        conn = sqlitecloud.connect(
            "sqlitecloud://ceafu04onz.g6.sqlite.cloud:8860/usuarios.db?apikey=Qo9m18B9ONpfEGYngUKm99QB5bgzUTGtK7iAcThmwvY"
        )
        cursor = conn.cursor()

        st.subheader("Control de versiones")
        st.info("ℹ️ Aquí puedes ver el historial de cambios y versiones de la aplicación. Cada entrada incluye el número de versión y una breve descripción de lo que se ha actualizado o modificado.")

        # --- FORMULARIO PARA NUEVA VERSIÓN ---
        with st.form("form_nueva_version"):
            nueva_version = st.text_input("Versión (ej. v1.1.0)")
            descripcion = st.text_area("Descripción de la versión")
            enviar = st.form_submit_button("Agregar nueva versión")

            if enviar:
                if not nueva_version.strip() or not descripcion.strip():
                    st.toast("Por favor completa todos los campos.")
                else:
                    fecha = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    # Insertar en base de datos
                    cursor.execute(
                        "INSERT INTO versiones (version, descripcion, fecha) VALUES (?, ?, ?)",
                        (nueva_version.strip(), descripcion.strip(), fecha)
                    )
                    conn.commit()

                    # Obtener todos los emails de usuarios para notificación
                    cursor.execute("SELECT email FROM usuarios")
                    usuarios = cursor.fetchall()

                    for (email,) in usuarios:
                        correo_nueva_version(email, nueva_version.strip(), descripcion.strip())

                    st.toast("Versión agregada y notificaciones enviadas.")
                    st.rerun()  # Recarga para mostrar la nueva versión

        # --- LISTADO DE VERSIONES ---
        cursor.execute("SELECT version, descripcion, fecha FROM versiones ORDER BY id DESC")
        versiones = cursor.fetchall()

        if not versiones:
            st.warning("No hay versiones registradas todavía.")
        else:
            for version, descripcion, fecha in versiones:
                st.markdown(
                    f"<div style='background-color: #f7f7f7; padding: 10px; margin-bottom: 10px;'>"
                    f"<p style='font-size: 14px; color: #666; margin: 0;'>"
                    f"<strong style='color: #4CAF50; font-size: 16px;'>{version}</strong> "
                    f"<em style='color: #999; font-size: 12px;'>({fecha})</em> - {descripcion}</p>"
                    f"</div>", unsafe_allow_html=True
                )

        st.markdown(
            "<br><i style='font-size: 14px; color: #888;'>"
            "Nota técnica: Esta sección muestra el historial completo de cambios aplicados al sistema. "
            "Asegúrese de revisar las versiones anteriores para comprender las mejoras y correcciones implementadas."
            "</i>", unsafe_allow_html=True
        )

        conn.close()

    except Exception as e:
        st.toast(f"Ha ocurrido un error al cargar el control de versiones: {e}")

# Función para crear el gráfico interactivo de Serviciabilidad
def create_serviciable_graph(cursor) -> go.Figure:
    """Crea gráfico de distribución de serviciabilidad"""
    cursor.execute("""
        SELECT serviciable, COUNT(*) as count
        FROM comercial_rafa
        WHERE serviciable IN ('Sí', 'No')
        GROUP BY serviciable
        ORDER BY serviciable DESC
    """)

    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=["serviciable", "count"])

    # Asegurar que siempre existan ambas categorías
    categories = {"Sí": 0, "No": 0}
    for _, row in df.iterrows():
        categories[row["serviciable"]] = row["count"]

    df = pd.DataFrame({
        "serviciable": list(categories.keys()),
        "count": list(categories.values())
    })

    fig = px.bar(
        df,
        x="serviciable",
        y="count",
        title="Distribución de Serviciabilidad",
        labels={"serviciable": "Serviciable", "count": "Cantidad"},
        color="serviciable",
        color_discrete_map={"Sí": "#2E7D32", "No": "#C62828"}
    )

    fig.update_layout(
        barmode='group',
        height=400,
        showlegend=False,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )

    # Añadir etiquetas de valor
    fig.update_traces(
        texttemplate='%{y}',
        textposition='outside'
    )

    return fig


# Función para crear el gráfico interactivo de Incidencias por Provincia
def create_incidencias_graph(cursor) -> go.Figure:
    """Crea gráfico de incidencias por provincia"""
    cursor.execute("""
        SELECT 
            COALESCE(provincia, 'No especificada') as provincia,
            COUNT(*) AS total_incidencias
        FROM comercial_rafa
        WHERE LOWER(COALESCE(incidencia, '')) = 'sí'
        GROUP BY provincia
        ORDER BY total_incidencias DESC
        LIMIT 10
    """)

    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=["provincia", "count"])

    fig = px.bar(
        df,
        x="provincia",
        y="count",
        title="Top 10 - Incidencias por Provincia",
        labels={"provincia": "Provincia", "count": "Cantidad"},
        color="provincia",
        color_discrete_sequence=px.colors.qualitative.Pastel
    )

    fig.update_layout(
        barmode='group',
        height=400,
        showlegend=False,
        xaxis_tickangle=45,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )

    return fig


# Gráfico Distribución de Tipos de Vivienda
def create_tipo_vivienda_distribution_graph(cursor) -> go.Figure:
    """Crea gráfico de distribución de tipos de vivienda"""
    cursor.execute("""
        SELECT 
            COALESCE(NULLIF(Tipo_Vivienda, ''), 'No especificado') as Tipo_Vivienda,
            COUNT(*) as count
        FROM comercial_rafa 
        GROUP BY Tipo_Vivienda
        ORDER BY count DESC
        LIMIT 8
    """)

    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=["Tipo_Vivienda", "count"])

    # Crear gráfico de barras horizontales para mejor lectura
    fig = px.bar(
        df,
        x="count",
        y="Tipo_Vivienda",
        title="Top 8 - Distribución de Tipos de Vivienda",
        labels={"Tipo_Vivienda": "Tipo de Vivienda", "count": "Cantidad"},
        color="Tipo_Vivienda",
        orientation='h',
        color_discrete_sequence=px.colors.sequential.Blues
    )

    fig.update_layout(
        height=400,
        showlegend=False,
        yaxis={'categoryorder': 'total ascending'},
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )

    # Añadir etiquetas de valor
    fig.update_traces(
        texttemplate='%{x}',
        textposition='outside'
    )

    return fig


# Gráfico de Viabilidades por Municipio
def create_viabilities_by_municipio_graph(cursor) -> go.Figure:
    """Crea gráfico de viabilidades por municipio"""
    cursor.execute("""
        SELECT 
            COALESCE(municipio, 'No especificado') as municipio,
            COUNT(*) as count
        FROM viabilidades
        GROUP BY municipio
        ORDER BY count DESC
        LIMIT 8
    """)

    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=["municipio", "count"])

    # Usar gráfico de donut para mejor visualización
    fig = px.pie(
        df,
        values="count",
        names="municipio",
        title="Top 8 - Viabilidades por Municipio",
        hole=0.4,
        color_discrete_sequence=px.colors.sequential.RdBu
    )

    fig.update_layout(
        height=400,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5
        ),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )

    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        hovertemplate='<b>%{label}</b><br>Viabilidades: %{value}<br>Porcentaje: %{percent}'
    )

    return fig


# Función para crear métricas KPI
def create_kpi_metrics(cursor) -> None:
    """Crea y muestra métricas KPI principales"""
    kpi_queries = {
        "Total Registros": "SELECT COUNT(*) FROM comercial_rafa",
        "Serviciables": "SELECT COUNT(*) FROM comercial_rafa WHERE serviciable = 'Sí'",
        "Incidencias": "SELECT COUNT(*) FROM comercial_rafa WHERE LOWER(COALESCE(incidencia, '')) = 'sí'",
        "Viabilidades Totales": "SELECT COUNT(*) FROM viabilidades"
    }

    kpi_values = {}
    for name, query in kpi_queries.items():
        try:
            cursor.execute(query)
            kpi_values[name] = cursor.fetchone()[0]
        except:
            kpi_values[name] = 0

    # Mostrar métricas en 4 columnas
    cols = st.columns(4)
    kpi_config = {
        "Total Registros": {"icon": "📊", "color": "#4A90E2"},
        "Serviciables": {"icon": "✅", "color": "#2E7D32"},
        "Incidencias": {"icon": "⚠️", "color": "#FF9800"},
        "Viabilidades Totales": {"icon": "📋", "color": "#9C27B0"}
    }

    for (kpi_name, kpi_val), col in zip(kpi_values.items(), cols):
        config = kpi_config.get(kpi_name, {})
        col.metric(
            label=f"{config.get('icon', '📈')} {kpi_name}",
            value=f"{kpi_val:,}",
            delta=None
        )


# Función principal de la página optimizada
def home_page():
    """Página principal con resumen de datos relevantes"""

    # Obtener la conexión
    conn = obtener_conexion()
    cursor = conn.cursor()

    try:
        # Mostrar KPIs principales
        create_kpi_metrics(cursor)

        # Organizar los gráficos en columnas
        col1, col2 = st.columns(2)

        # Gráfico de Serviciabilidad
        with col1:
            # Corrección aplicada aquí
            st.plotly_chart(create_serviciable_graph(cursor), config={'width': 'stretch'})
        with col2:
            # Corrección aplicada aquí
            st.plotly_chart(create_incidencias_graph(cursor), config={'width': 'stretch'})
        with col1:
            # Corrección aplicada aquí
            st.plotly_chart(create_tipo_vivienda_distribution_graph(cursor), config={'width': 'stretch'})
        with col2:
            # Corrección aplicada aquí
            st.plotly_chart(create_viabilities_by_municipio_graph(cursor), config={'width': 'stretch'})

        # Opcional: Mostrar tabla de datos detallados
        with st.expander("📋 Ver datos detallados", expanded=False):
            cursor.execute("""
                SELECT 
                    provincia,
                    municipio,
                    serviciable,
                    incidencia,
                    Tipo_Vivienda,
                    COUNT(*) as total
                FROM comercial_rafa
                GROUP BY provincia, municipio, serviciable, incidencia, Tipo_Vivienda
                ORDER BY total DESC
                LIMIT 20
            """)
            detalle_data = cursor.fetchall()
            df_detalle = pd.DataFrame(detalle_data,
                                      columns=["Provincia", "Municipio", "Serviciable", "Incidencia", "Tipo_Vivienda",
                                               "Total"])
            st.dataframe(df_detalle, width='stretch')

    except Exception as e:
        st.toast(f"❌ Error al cargar los gráficos: {str(e)}")
        st.toast(f"Hubo un error al cargar los gráficos: {e}", icon="⚠️")

    finally:
        cursor.close()
        conn.close()


# Si necesitas mantener compatibilidad con la versión anterior
#def obtener_conexion():
#    """Wrapper para mantener compatibilidad"""
#    return obtener_conexion()  # Asumiendo que existe esta función


if __name__ == "__main__":
    admin_dashboard()
