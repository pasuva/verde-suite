# viabilidad_dashboard.py
import time
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

import folium
import pandas as pd
from modules.db import get_db_connection as _db_conn
import streamlit as st
from folium.plugins import Geocoder
from streamlit_cookies_controller import CookieController
from streamlit_folium import st_folium
from streamlit_option_menu import option_menu

from modules import login
from modules.minIO import upload_image_to_cloudinary
from modules.notificaciones import correo_viabilidad_comercial, correo_respuesta_comercial

import warnings
warnings.filterwarnings("ignore", category=UserWarning)

cookie_name = "my_app"


# ==================== CONEXIÓN A BASE DE DATOS ====================
def get_db_connection():
    """Retorna una conexión a la base de datos PostgreSQL."""
    return _db_conn()


# ==================== TRAZABILIDAD ====================
def log_trazabilidad(usuario: str, accion: str, detalles: str):
    """Inserta un registro en la tabla trazabilidad."""
    conn = get_db_connection()
    cursor = conn.cursor()
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute(
        """
        INSERT INTO trazabilidad (usuario_id, accion, detalles, fecha)
        VALUES (%s, %s, %s, %s)
        """,
        (usuario, accion, detalles, fecha),
    )
    conn.commit()
    conn.close()


# ==================== ANUNCIOS ====================
def mostrar_ultimo_anuncio():
    """Muestra el anuncio más reciente."""
    try:
        conn = get_db_connection()
        query = "SELECT titulo, descripcion, fecha FROM anuncios ORDER BY id DESC LIMIT 1"
        df = pd.read_sql_query(query, conn)
        conn.close()
        if not df.empty:
            st.info(
                f"📰 **{df.iloc[0]['titulo']}**  \n"
                f"{df.iloc[0]['descripcion']}  \n"
                f"📅 *Publicado el {df.iloc[0]['fecha']}*"
            )
    except Exception as e:
        st.warning(f"⚠️ No se pudo cargar el último anuncio: {e}")


# ==================== FUNCIONES DE VIABILIDAD (compartidas) ====================
@st.cache_data(ttl=3600)
def obtener_lista_olt_cache() -> List[str]:
    """Obtiene lista de OLTs con caché."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id_olt, nombre_olt FROM olt ORDER BY nombre_olt")
    lista = [f"{fila[0]}. {fila[1]}" for fila in cursor.fetchall()]
    conn.close()
    return lista


def generar_ticket() -> str:
    """Genera un ticket único con formato: añomesdía + número consecutivo."""
    conn = get_db_connection()
    cursor = conn.cursor()
    fecha_actual = datetime.now().strftime("%Y%m%d")
    cursor.execute(
        "SELECT MAX(CAST(SUBSTR(ticket, 9, 3) AS INTEGER)) FROM viabilidades WHERE ticket LIKE %s",
        (f"{fecha_actual}%",),
    )
    max_consecutivo = cursor.fetchone()[0]
    conn.close()
    if max_consecutivo is None:
        max_consecutivo = 0
    return f"{fecha_actual}{max_consecutivo + 1:03d}"


def guardar_viabilidad(datos):
    """
    Inserta los datos en la tabla Viabilidades.
    Orden esperado: lat, lon, provincia, municipio, poblacion, vial, numero, letra,
    cp, comentario, ticket, nombre_cliente, telefono, usuario, olt, apartment_id.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO viabilidades (
            latitud, longitud, provincia, municipio, poblacion, vial, numero, letra,
            cp, comentario, fecha_viabilidad, ticket, nombre_cliente, telefono,
            usuario, olt, apartment_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s)
        """,
        datos,
    )
    conn.commit()

    cursor.execute("SELECT email FROM usuarios WHERE role = 'admin'")
    emails_admin = [row[0] for row in cursor.fetchall()]

    # Determinar comercial jefe según provincia
    provincia_viabilidad = datos[2].upper().strip()
    if provincia_viabilidad == "CANTABRIA":
        cursor.execute("SELECT email FROM usuarios WHERE username = 'rafa sanz'")
    else:
        cursor.execute("SELECT email FROM usuarios WHERE username = 'juan'")
    resultado_jefe = cursor.fetchone()
    email_comercial_jefe = resultado_jefe[0] if resultado_jefe else None
    conn.close()

    ticket_id = datos[10]
    nombre_comercial = st.session_state.get("username")
    descripcion = f"""
        📝 Viabilidad para el ticket {ticket_id}:<br><br>
        🧑‍💼 Comercial: {nombre_comercial}<br>
        📍 Latitud: {datos[0]}<br>
        📍 Longitud: {datos[1]}<br>
        🏞️ Provincia: {datos[2]}<br>
        🏙️ Municipio: {datos[3]}<br>
        🏘️ Población: {datos[4]}<br>
        🛣️ Vial: {datos[5]}<br>
        🔢 Número: {datos[6]}<br>
        🔤 Letra: {datos[7]}<br>
        🏷️ CP: {datos[8]}<br>
        💬 Comentario: {datos[9]}<br>
        👥 Nombre Cliente: {datos[11]}<br>
        📞 Teléfono: {datos[12]}<br>
        🏢 OLT: {datos[14]}<br>
        🏘️ Apartment ID: {datos[15]}<br><br>
        ℹ️ Revise todos los detalles.
    """
    for email in emails_admin:
        try:
            correo_viabilidad_comercial(email, ticket_id, descripcion)
        except Exception as e:
            st.warning(f"Error notificando a admin {email}: {e}")

    if email_comercial_jefe:
        try:
            correo_viabilidad_comercial(email_comercial_jefe, ticket_id, descripcion)
        except Exception as e:
            st.warning(f"Error notificando a comercial jefe: {e}")

    st.success(f"✅ Viabilidad guardada correctamente.\n\n📌 **Ticket:** `{ticket_id}`")


def obtener_viabilidades() -> List[tuple]:
    """Recupera las viabilidades asociadas al usuario logueado."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT latitud, longitud, ticket, serviciable, apartment_id FROM viabilidades WHERE usuario = %s",
        (st.session_state["username"],),
    )
    viabilidades = cursor.fetchall()
    conn.close()
    return viabilidades


@st.cache_data(ttl=300)
def obtener_viabilidades_cache() -> List[tuple]:
    """Obtiene viabilidades con caché para mejorar rendimiento."""
    return obtener_viabilidades()


def guardar_imagenes_viabilidad(imagenes, ticket):
    """Guarda las imágenes asociadas a una viabilidad."""
    if not imagenes:
        return
    st.toast("📤 Subiendo imágenes...")
    for imagen in imagenes:
        try:
            archivo_bytes = imagen.getvalue()
            nombre_archivo = imagen.name
            url = upload_image_to_cloudinary(
                archivo_bytes,
                nombre_archivo,
                folder="viabilidades",
            )
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO imagenes_viabilidad (ticket, archivo_nombre, archivo_url)
                VALUES (%s, %s, %s)
                """,
                (ticket, nombre_archivo, url),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            st.warning(f"⚠️ No se pudo subir la imagen {nombre_archivo}: {e}")
    st.toast("✅ Imágenes guardadas correctamente.")


# ==================== SECCIÓN DE VIABILIDADES (mapa + formulario) ====================
def mostrar_leyenda():
    st.markdown("""**Leyenda:**
                 ⚫ Viabilidad ya existente
                 🔵 Viabilidad nueva aún sin estudio
                 🟢 Viabilidad serviciable y con Apartment ID ya asociado
                 🔴 Viabilidad no serviciable
                """)


def mostrar_instrucciones():
    st.info("ℹ️ Haz click en el mapa para agregar un marcador que represente el punto de viabilidad.")


def inicializar_estado_sesion():
    defaults = {
        "viabilidad_marker": None,
        "map_center": (43.463444, -3.790476),
        "map_zoom": 12,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def determinar_color_marcador(serviciable, apartment_id) -> str:
    if serviciable is None or str(serviciable).strip() == "":
        return "black"
    serv = str(serviciable).strip()
    apt = str(apartment_id).strip() if apartment_id is not None else ""
    if serv == "No":
        return "red"
    elif serv == "Sí" and apt not in ["", "N/D"]:
        return "green"
    else:
        return "black"


def agregar_marcadores_existentes(mapa, viabilidades):
    for v in viabilidades:
        lat, lon, ticket, serviciable, apt_id = v
        color = determinar_color_marcador(serviciable, apt_id)
        folium.Marker(
            [lat, lon],
            icon=folium.Icon(color=color),
            popup=f"Ticket: {ticket}",
        ).add_to(mapa)


def crear_y_mostrar_mapa(viabilidades):
    m = folium.Map(
        location=st.session_state.map_center,
        zoom_start=st.session_state.map_zoom,
        tiles="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}",
        attr="Google",
    )
    agregar_marcadores_existentes(m, viabilidades)

    if st.session_state.viabilidad_marker:
        lat = st.session_state.viabilidad_marker["lat"]
        lon = st.session_state.viabilidad_marker["lon"]
        folium.Marker([lat, lon], icon=folium.Icon(color="blue")).add_to(m)

    Geocoder().add_to(m)
    return st_folium(m, height=680, width="100%",
                         returned_objects=["last_clicked"])


def manejar_interaccion_mapa(map_data):
    if map_data and map_data.get("last_clicked"):
        click = map_data["last_clicked"]
        st.session_state.viabilidad_marker = {"lat": click["lat"], "lon": click["lng"]}
        st.session_state.map_center = (click["lat"], click["lng"])
        st.session_state.map_zoom = 15  # Zoom in al hacer clic
        st.rerun()

    if st.session_state.viabilidad_marker:
        if st.button("Eliminar marcador y crear uno nuevo"):
            st.session_state.viabilidad_marker = None
            st.session_state.map_center = (43.463444, -3.790476)
            st.rerun()


def resetear_marcador():
    st.session_state.viabilidad_marker = None
    st.session_state.map_center = (43.463444, -3.790476)


def mostrar_formulario_si_aplica():
    if not st.session_state.viabilidad_marker:
        return
    lat = st.session_state.viabilidad_marker["lat"]
    lon = st.session_state.viabilidad_marker["lon"]
    st.subheader("Completa los datos del punto de viabilidad")
    procesar_formulario(lat, lon)


def mostrar_campos_formulario(lat, lon) -> Dict[str, Any]:
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
    with col12:
        olt = st.selectbox("🏢 OLT", options=obtener_lista_olt_cache())
    with col13:
        apartment_id = st.text_input("🏘️ Apartment ID")

    comentario = st.text_area("📝 Comentario")

    imagenes = st.file_uploader(
        "Adjunta fotos (PNG, JPG, JPEG). Puedes seleccionar varias.",
        type=["png", "jpg", "jpeg"],
        accept_multiple_files=True,
        key=f"imagenes_viabilidad_{lat}_{lon}",
    )

    return {
        "lat": lat,
        "lon": lon,
        "provincia": provincia,
        "municipio": municipio,
        "poblacion": poblacion,
        "vial": vial,
        "numero": numero,
        "letra": letra,
        "cp": cp,
        "nombre_cliente": nombre_cliente,
        "telefono": telefono,
        "olt": olt,
        "apartment_id": apartment_id,
        "comentario": comentario,
        "imagenes": imagenes,
    }


def procesar_formulario(lat, lon):
    with st.form("viabilidad_form"):
        datos = mostrar_campos_formulario(lat, lon)
        if st.form_submit_button("Enviar Formulario"):
            guardar_viabilidad_completa(datos, lat, lon)


def guardar_viabilidad_completa(datos, lat, lon):
    ticket = generar_ticket()
    guardar_viabilidad(
        (
            datos["lat"],
            datos["lon"],
            datos["provincia"],
            datos["municipio"],
            datos["poblacion"],
            datos["vial"],
            datos["numero"],
            datos["letra"],
            datos["cp"],
            datos["comentario"],
            ticket,
            datos["nombre_cliente"],
            datos["telefono"],
            st.session_state["username"],
            datos["olt"],
            datos["apartment_id"],
        )
    )
    if datos["imagenes"]:
        guardar_imagenes_viabilidad(datos["imagenes"], ticket)
    st.success(f"✅ Viabilidad guardada correctamente.\n\n📌 **Ticket:** `{ticket}`")
    resetear_marcador()
    st.rerun()


def viabilidades_section():
    st.title("Viabilidades")
    mostrar_leyenda()
    mostrar_instrucciones()
    inicializar_estado_sesion()
    viabilidades = obtener_viabilidades_cache()
    map_data = crear_y_mostrar_mapa(viabilidades)
    manejar_interaccion_mapa(map_data)
    mostrar_formulario_si_aplica()


# ==================== SECCIÓN DE VISUALIZACIÓN DE DATOS ====================
@st.cache_data(ttl=300)
def cargar_datos_visualizacion(comercial_usuario: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Carga ofertas y viabilidades del comercial."""
    conn = get_db_connection()
    try:
        # Ofertas
        df_ofertas = pd.read_sql(
            "SELECT * FROM comercial_rafa WHERE LOWER(comercial) = LOWER(%s)",
            conn,
            params=(comercial_usuario,),
        )
        if not df_ofertas.empty:
            df_seguimiento = pd.read_sql(
                "SELECT apartment_id, estado FROM seguimiento_contratos WHERE LOWER(estado) = 'finalizado'",
                conn,
            )
            df_ofertas["Contrato_Activo"] = df_ofertas["apartment_id"].isin(
                df_seguimiento["apartment_id"]
            ).map({True: "✅ Activo", False: "❌ No Activo"})

        # Viabilidades
        df_viabilidades = pd.read_sql(
            """
            SELECT ticket, latitud, longitud, provincia, municipio, poblacion, vial, numero,
                   letra, cp, serviciable, coste, comentarios_comercial, justificacion,
                   resultado, respuesta_comercial
            FROM viabilidades
            WHERE LOWER(usuario) = LOWER(%s)
            """,
            conn,
            params=(comercial_usuario,),
        )
    finally:
        conn.close()
    return df_ofertas, df_viabilidades


def mostrar_tabla_viabilidades(df_viabilidades, comercial_usuario):
    """Muestra tabla de viabilidades, filtrando por usuarios con rol 'viabilidad' si corresponde."""
    # Obtener usuarios con rol "viabilidad"
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT username FROM usuarios WHERE role = 'viabilidad'")
        usuarios_viabilidad = [row[0] for row in cursor.fetchall()]
        conn.close()
    except Exception as e:
        st.error(f"❌ Error al obtener usuarios con rol viabilidad: {e}")
        usuarios_viabilidad = []

    if usuarios_viabilidad:
        # Buscar columna de usuario creador
        posibles = ["usuario_creacion", "creado_por", "usuario", "username", "comercial"]
        col_usuario = next((c for c in df_viabilidades.columns if c in posibles), None)
        if col_usuario:
            df_viabilidades = df_viabilidades[df_viabilidades[col_usuario].isin(usuarios_viabilidad)]
        else:
            st.warning("⚠️ No se encontró columna de usuario creador en las viabilidades")
            return

    if df_viabilidades.empty:
        st.warning(f"⚠️ No hay viabilidades para el comercial '{comercial_usuario}'")
        return

    st.subheader("📋 Tabla de Viabilidades")
    st.dataframe(df_viabilidades, use_container_width=True)

    # Procesar pendientes
    just_criticas = ["MAS PREVENTA", "PDTE. RAFA FIN DE OBRA"]
    res_criticos = ["PDTE INFORMACION RAFA", "OK", "SOBRECOSTE"]
    df_pend = df_viabilidades[
        (df_viabilidades["justificacion"].isin(just_criticas) | df_viabilidades["resultado"].isin(res_criticos))
        & (df_viabilidades["respuesta_comercial"].isna() | (df_viabilidades["respuesta_comercial"] == ""))
    ]
    if df_pend.empty:
        st.success("🎉 No tienes viabilidades pendientes de contestar")
        return

    st.warning(f"🔔 Tienes {len(df_pend)} viabilidades pendientes de contestar")
    st.subheader("📝 Gestión de Viabilidades Pendientes")
    for _, row in df_pend.iterrows():
        ticket = row["ticket"]
        with st.expander(f"🎫 Ticket {ticket} - {row['municipio']} {row['vial']} {row['numero']}"):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**📌 Justificación:** {row.get('justificacion', '—')}")
            with col2:
                st.markdown(f"**📊 Resultado:** {row.get('resultado', '—')}")
            with st.expander("ℹ️ Instrucciones para completar", expanded=False):
                st.markdown("""
                **Por favor, indica:**
                - ✅ Si estás de acuerdo o no con la resolución
                - 🏠 Información adicional de tu visita (cliente, obra, accesos, etc.)
                - 💰 Si el cliente acepta o no el presupuesto
                - 📝 Cualquier detalle que ayude a la oficina a cerrar la viabilidad
                """)
            with st.form(key=f"form_viab_{ticket}"):
                nuevo_comentario = st.text_area(
                    "✏️ Tu respuesta:",
                    value="",
                    placeholder="Ejemplo: El cliente confirma que esperará a fin de obra...",
                )
                if st.form_submit_button("💾 Guardar Respuesta", use_container_width=True):
                    if not nuevo_comentario.strip():
                        st.error("❌ El comentario no puede estar vacío")
                    else:
                        try:
                            conn = get_db_connection()
                            cursor = conn.cursor()
                            cursor.execute(
                                "UPDATE viabilidades SET respuesta_comercial = %s WHERE ticket = %s",
                                (nuevo_comentario, ticket),
                            )
                            conn.commit()
                            conn.close()
                            # Notificar
                            conn = get_db_connection()
                            cursor = conn.cursor()
                            cursor.execute("SELECT email FROM usuarios WHERE role IN ('admin','comercial_jefe')")
                            destinatarios = [row[0] for row in cursor.fetchall()]
                            conn.close()
                            for email in destinatarios:
                                try:
                                    correo_respuesta_comercial(email, ticket, comercial_usuario, nuevo_comentario)
                                except Exception as e:
                                    st.warning(f"Error notificando a {email}: {e}")
                            st.success(f"✅ Respuesta guardada para {ticket}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error: {e}")


def mostrar_metricas_ofertas(df):
    if df.empty:
        return
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Ofertas", len(df))
    with col2:
        activas = len(df[df["Contrato_Activo"] == "✅ Activo"])
        st.metric("Contratos Activos", activas)
    with col3:
        serv = len(df[df["serviciable"] == "Sí"])
        st.metric("Serviciables", serv)
    with col4:
        no_serv = len(df[df["serviciable"] == "No"])
        st.metric("No Serviciables", no_serv)


def seccion_visualizacion_datos():
    st.subheader("📊 Visualización de Datos")
    if "username" not in st.session_state:
        st.error("❌ No has iniciado sesión")
        return

    comercial = st.session_state["username"]
    try:
        df_ofertas, df_viab = cargar_datos_visualizacion(comercial)
    except Exception as e:
        st.error(f"❌ Error al cargar datos: {e}")
        return

    # Ofertas (opcional, se puede mostrar si se desea)
    if not df_ofertas.empty:
        st.subheader("📋 Tabla de Visitas/Ofertas")
        col1, col2 = st.columns(2)
        with col1:
            filtro_contrato = st.selectbox(
                "Filtrar por contrato:", ["Todos", "✅ Activo", "❌ No Activo"], key="filtro_contrato"
            )
        with col2:
            filtro_serv = st.selectbox(
                "Filtrar por serviciable:", ["Todos", "Sí", "No"], key="filtro_serv"
            )
        df_filtrado = df_ofertas.copy()
        if filtro_contrato != "Todos":
            df_filtrado = df_filtrado[df_filtrado["Contrato_Activo"] == filtro_contrato]
        if filtro_serv != "Todos":
            df_filtrado = df_filtrado[df_filtrado["serviciable"] == filtro_serv]
        mostrar_metricas_ofertas(df_filtrado)
        st.dataframe(df_filtrado, use_container_width=True)
        if st.button("📤 Exportar a CSV", key="export_ofertas"):
            csv = df_filtrado.to_csv(index=False)
            st.download_button(
                label="⬇️ Descargar CSV",
                data=csv,
                file_name=f"ofertas_{comercial}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )

    # Viabilidades
    mostrar_tabla_viabilidades(df_viab, comercial)


# ==================== FUNCIÓN PRINCIPAL ====================
def viabilidad_dashboard():
    """Dashboard principal para usuarios de viabilidad."""
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
            z-index: 999;
        }
        </style>
        <div class="footer">
            <p>© 2025 Verde tu operador · Desarrollado para uso interno</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.markdown(
            f"""
            <div style="text-align:center;">
                <div style="width:100px; height:100px; border-radius:50%; background-color:#ff7f00; color:white;
                            font-size:50px; display:flex; align-items:center; justify-content:center; margin:0 auto;">
                    👤
                </div>
                <div style="margin-top:10px; font-weight:bold;">Rol: Comercial Viabilidad</div>
                <div style="font-weight:bold; font-size:18px;">Bienvenido, {st.session_state.get('username', '')}</div>
                <hr>
            </div>
            """,
            unsafe_allow_html=True,
        )

        menu_opcion = option_menu(
            menu_title=None,
            options=["Viabilidades", "Visualización de Datos"],
            icons=["check-circle", "graph-up"],
            menu_icon="list",
            default_index=0,
            styles={
                "container": {"padding": "0px", "background-color": "#F0F7F2"},
                "icon": {"color": "#2C5A2E", "font-size": "18px"},
                "nav-link": {
                    "color": "#2C5A2E",
                    "font-size": "16px",
                    "text-align": "left",
                    "margin": "0px",
                    "--hover-color": "#66B032",
                    "border-radius": "0px",
                },
                "nav-link-selected": {
                    "background-color": "#66B032",
                    "color": "white",
                    "font-weight": "bold",
                },
            },
        )

        if st.button("Cerrar sesión"):
            detalles = f"El usuario {st.session_state.get('username', 'N/A')} cerró sesión."
            log_trazabilidad(st.session_state.get("username", "N/A"), "Cierre sesión", detalles)
            for key in [f"{cookie_name}_session_id", f"{cookie_name}_username", f"{cookie_name}_role"]:
                if controller.get(key):
                    controller.set(key, "", max_age=0, path="/")
            st.session_state["login_ok"] = False
            st.session_state["username"] = ""
            st.session_state["role"] = ""
            st.session_state["session_id"] = ""
            st.success("✅ Has cerrado sesión correctamente. Redirigiendo...")
            st.rerun()

    if "username" not in st.session_state or not st.session_state["username"]:
        st.warning("⚠️ No has iniciado sesión. Redirigiendo al login...")
        time.sleep(1.5)
        try:
            login.login()
        except Exception:
            pass
        return

    log_trazabilidad(
        st.session_state["username"],
        "Selección de vista",
        f"Seleccionó '{menu_opcion}'",
    )

    if menu_opcion == "Viabilidades":
        mostrar_ultimo_anuncio()
        viabilidades_section()
    elif menu_opcion == "Visualización de Datos":
        seccion_visualizacion_datos()


if __name__ == "__main__":
    viabilidad_dashboard()