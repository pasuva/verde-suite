# comercial_dashboard.py
import io
import os
import re
import time
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

import folium
import pandas as pd
from modules.db import get_db_connection as _db_conn
import streamlit as st
from folium.plugins import Geocoder, MarkerCluster
from streamlit_cookies_controller import CookieController
from streamlit_folium import st_folium
from streamlit_javascript import st_javascript
from streamlit_option_menu import option_menu

# Módulos locales
from modules import login
from modules.minIO import upload_image_to_cloudinary
from modules.notificaciones import (
    correo_oferta_comercial,
    correo_respuesta_comercial,
    correo_viabilidad_comercial,
)

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


# ==================== CARGA DE DATOS CON CACHÉ ====================
@st.cache_data(ttl=3600)  # 1 hora
def load_comercial_data(comercial: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Carga datos de comercial_rafa según el comercial.
    Para nestor/roberto se cargan ambos.
    """
    conn = get_db_connection()
    try:
        if comercial.lower() in ["nestor", "roberto"]:
            query = """
                SELECT apartment_id, latitud, longitud, comercial, serviciable, municipio, poblacion
                FROM comercial_rafa
                WHERE LOWER(comercial) IN ('nestor', 'roberto')
            """
            df = pd.read_sql(query, conn)
        else:
            query = """
                SELECT apartment_id, latitud, longitud, comercial, serviciable, municipio, poblacion
                FROM comercial_rafa
                WHERE LOWER(comercial) = LOWER(%s)
            """
            df = pd.read_sql(query, conn, params=(comercial,))

        query_ofertas = "SELECT apartment_id, contrato, municipio, poblacion FROM comercial_rafa"
        ofertas_df = pd.read_sql(query_ofertas, conn)

        query_ams = "SELECT apartment_id FROM datos_uis WHERE LOWER(serviciable) = 'sí'"
        ams_df = pd.read_sql(query_ams, conn)
    finally:
        conn.close()
    return df, ofertas_df, ams_df


@st.cache_data(ttl=300)  # 5 minutos
def cargar_datos_visualizacion(comercial_usuario: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Carga ofertas y viabilidades del comercial."""
    conn = get_db_connection()
    try:
        # Ofertas
        query_ofertas = "SELECT * FROM comercial_rafa WHERE LOWER(comercial) = LOWER(%s)"
        df_ofertas = pd.read_sql(query_ofertas, conn, params=(comercial_usuario,))

        # Enriquecer con contratos activos
        if not df_ofertas.empty:
            query_seg = "SELECT apartment_id, estado FROM seguimiento_contratos WHERE LOWER(estado) = 'finalizado'"
            df_seg = pd.read_sql(query_seg, conn)
            df_ofertas['Contrato_Activo'] = df_ofertas['apartment_id'].isin(
                df_seg['apartment_id']
            ).map({True: '✅ Activo', False: '❌ No Activo'})

        # Viabilidades
        query_viab = """
            SELECT ticket, latitud, longitud, provincia, municipio, poblacion, vial, numero,
                   letra, cp, serviciable, coste, comentarios_comercial, justificacion,
                   resultado, respuesta_comercial
            FROM viabilidades
            WHERE LOWER(usuario) = LOWER(%s)
        """
        df_viabilidades = pd.read_sql(query_viab, conn, params=(comercial_usuario,))
    finally:
        conn.close()
    return df_ofertas, df_viabilidades


@st.cache_data(ttl=3600)
def obtener_lista_olt_cache() -> List[str]:
    """Obtiene lista de OLTs con caché."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id_olt, nombre_olt FROM olt ORDER BY nombre_olt")
    lista = [f"{fila[0]}. {fila[1]}" for fila in cursor.fetchall()]
    conn.close()
    return lista


@st.cache_data(ttl=300)
def obtener_viabilidades_cache(usuario: str) -> List[Tuple]:
    """Obtiene viabilidades del usuario con caché."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT latitud, longitud, ticket, serviciable, apartment_id FROM viabilidades WHERE usuario = %s",
        (usuario,),
    )
    viabilidades = cursor.fetchall()
    conn.close()
    return viabilidades


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


# ==================== GUARDADO DE OFERTAS ====================
def guardar_en_base_de_datos(
    oferta_data: Dict[str, Any],
    imagen_incidencia: Optional[Any],
    apartment_id: str,
) -> None:
    """Guarda o actualiza la oferta en SQLite y sube imagen a Cloudinary si procede."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar si existe el apartment_id
        cursor.execute(
            "SELECT COUNT(*) FROM comercial_rafa WHERE apartment_id = %s",
            (apartment_id,),
        )
        if cursor.fetchone()[0] == 0:
            st.error("❌ El Apartment ID no existe en la base de datos.")
            conn.close()
            return

        # Subir imagen si hay incidencia
        imagen_url = None
        if oferta_data.get("incidencia") == "Sí" and imagen_incidencia:
            extension = os.path.splitext(imagen_incidencia.name)[1]
            filename = f"{apartment_id}{extension}"
            try:
                imagen_url = upload_image_to_cloudinary(
                    imagen_incidencia,
                    filename,
                    tipo="incidencia",
                    folder=datetime.now().strftime("%Y/%m"),
                )
            except Exception as e:
                st.warning(f"⚠️ Error al subir imagen: {e}")

        comercial_logueado = st.session_state.get("username", None)

        cursor.execute(
            """
            UPDATE comercial_rafa SET
                provincia = %s, municipio = %s, poblacion = %s, vial = %s, numero = %s, letra = %s,
                cp = %s, latitud = %s, longitud = %s, nombre_cliente = %s, telefono = %s,
                direccion_alternativa = %s, observaciones = %s, serviciable = %s, motivo_serviciable = %s,
                incidencia = %s, motivo_incidencia = %s, ocupado_por_tercero = %s, fichero_imagen = %s,
                fecha = %s, tipo_vivienda = %s, contrato = %s, comercial = %s
            WHERE apartment_id = %s
            """,
            (
                oferta_data["Provincia"],
                oferta_data["Municipio"],
                oferta_data["Población"],
                oferta_data["Vial"],
                oferta_data["Número"],
                oferta_data["Letra"],
                oferta_data["Código Postal"],
                oferta_data["Latitud"],
                oferta_data["Longitud"],
                oferta_data["Nombre Cliente"],
                oferta_data["Teléfono"],
                oferta_data["Dirección Alternativa"],
                oferta_data["Observaciones"],
                oferta_data["serviciable"],
                oferta_data["motivo_serviciable"],
                oferta_data["incidencia"],
                oferta_data["motivo_incidencia"],
                "Sí" if oferta_data.get("ocupado_por_tercero") else "No",
                imagen_url,
                oferta_data["fecha"].strftime('%Y-%m-%d %H:%M:%S'),
                oferta_data["Tipo_Vivienda"],
                oferta_data["Contrato"],
                comercial_logueado,
                apartment_id,
            ),
        )

        conn.commit()
        conn.close()
        st.success("✅ ¡Oferta actualizada con éxito en la base de datos!")

        # Notificar a administradores
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT email FROM usuarios WHERE role IN ('admin', 'comercial_jefe')"
        )
        destinatarios = [row[0] for row in cursor.fetchall()]
        conn.close()

        descripcion = (
            f"Se ha actualizado una oferta para el apartamento con ID {apartment_id}.\n\n"
            f"🏠 Ocupado por un tercero: {'Sí' if oferta_data.get('ocupado_por_tercero') else 'No'}\n\n"
            f"Detalles: {oferta_data}"
        )
        for email in destinatarios:
            try:
                correo_oferta_comercial(email, apartment_id, descripcion)
            except Exception as e:
                st.warning(f"⚠️ Error notificando a {email}: {e}")

        st.toast(f"📧 Se ha notificado a {len(destinatarios)} administrador(es).")
        log_trazabilidad(
            st.session_state["username"],
            "Actualizar Oferta",
            f"Oferta actualizada para Apartment ID: {apartment_id}",
        )

    except Exception as e:
        st.error(f"❌ Error al guardar o actualizar la oferta: {e}")


# ==================== FUNCIONES DE VIABILIDAD (compartidas) ====================
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


# ==================== LOCALIZACIÓN DEL USUARIO ====================
def get_user_location() -> Optional[Tuple[float, float]]:
    """Obtiene la ubicación actual del usuario mediante JavaScript."""
    result = st_javascript(
        "await new Promise((resolve, reject) => "
        "navigator.geolocation.getCurrentPosition(p => resolve({lat: p.coords.latitude, lon: p.coords.longitude}), "
        "err => resolve(null)));"
    )
    if result and "lat" in result and "lon" in result:
        return result["lat"], result["lon"]
    return None


# ==================== MAPA DE OFERTAS (subfunciones) ====================
def _crear_mapa_optimizado(
    df: pd.DataFrame,
    lat_centro: float,
    lon_centro: float,
    ofertas_df: pd.DataFrame,
    ams_df: pd.DataFrame,
) -> folium.Map:
    """Crea un mapa optimizado con los puntos y colores según estado."""
    # Pre-calcular datos fuera del bucle
    serviciable_set = set(ams_df["apartment_id"])
    contrato_dict = dict(zip(ofertas_df["apartment_id"], ofertas_df["contrato"]))

    def get_icon_for_olt(tipo_olt):
        if pd.isna(tipo_olt):
            return "info-sign"
        return "cloud" if "CTO VERDE" in str(tipo_olt) else "info-sign"

    def get_marker_color(row):
        apt_id = row["apartment_id"]
        serv_val = str(row.get("serviciable", "")).strip().lower()
        if serv_val == "no":
            return "red"
        if serv_val == "si":
            return "green"
        if apt_id in contrato_dict:
            contrato_val = str(contrato_dict[apt_id]).strip().lower()
            if contrato_val == "sí":
                return "orange"
            if contrato_val == "no interesado":
                return "black"
        return "blue"

    df["marker_color"] = df.apply(get_marker_color, axis=1)
    df["offset_index"] = df.groupby(["latitud", "longitud"]).cumcount()

    m = folium.Map(
        location=[lat_centro, lon_centro],
        zoom_start=12,
        max_zoom=21,
        tiles="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}",
        attr="Google",
    )
    Geocoder().add_to(m)

    cluster = MarkerCluster(
        maxClusterRadius=40,
        disableClusteringAtZoom=17,
        chunkedLoading=True,
        chunkInterval=100,
    ).add_to(m)

    for _, row in df.iterrows():
        lat_off = row["offset_index"] * 0.00003
        lon_off = row["offset_index"] * -0.00003
        icon_type = get_icon_for_olt(row.get("tipo_olt_rental", None))
        popup = (
            f"<b>🏠 {row['apartment_id']}</b><br>"
            f"📍 {row['latitud']}, {row['longitud']}<br>"
            f"🛰️ OLT: {row.get('tipo_olt_rental', '—')}"
        )
        folium.Marker(
            location=[row["latitud"] + lat_off, row["longitud"] + lon_off],
            popup=popup,
            icon=folium.Icon(color=row["marker_color"], icon=icon_type),
        ).add_to(cluster)

    # Leyenda flotante
    legend_html = """
    <div style="
        position: fixed; 
        bottom: 10px; 
        left: 10px; 
        width: 190px; 
        z-index: 1000; 
        font-size: 14px;
        background-color: white;
        border: 2px solid grey;
        border-radius: 8px;
        padding: 10px;
        box-shadow: 2px 2px 6px rgba(0,0,0,0.3);
    ">
    <b>Leyenda</b><br>
    <i style="color:green;">●</i> Serviciable<br>
    <i style="color:red;">●</i> No serviciable<br>
    <i style="color:orange;">●</i> Contrato Sí<br>
    <i style="color:black;">●</i> No interesado<br>
    <i style="color:purple;">●</i> Incidencia<br>
    <i style="color:blue;">●</i> No Visitado<br>
    <i class="fa fa-cloud"></i> CTO VERDE<br>
    <i class="fa fa-info-circle"></i> CTO COMPARTIDA<br>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    return m


def _mostrar_mapa_ofertas():
    """Subfunción que maneja la lógica del mapa de ofertas."""
    comercial = st.session_state.get("username", "").lower()
    mostrar_ultimo_anuncio()

    with st.spinner("⏳ Cargando datos optimizados..."):
        try:
            conn = get_db_connection()
            tables = pd.read_sql("SELECT table_name AS name FROM information_schema.tables WHERE table_schema = 'public'", conn)
            conn.close()
            if "comercial_rafa" not in tables["name"].values:
                st.error("❌ La tabla 'comercial_rafa' no existe.")
                st.stop()

            df, ofertas_df, ams_df = load_comercial_data(comercial)

            # Añadir tipo de OLT desde datos_uis
            try:
                conn = get_db_connection()
                datos_uis_df = pd.read_sql(
                    "SELECT apartment_id, tipo_olt_rental FROM datos_uis", conn
                )
                conn.close()
                df = df.merge(datos_uis_df, on="apartment_id", how="left")
            except Exception as e:
                st.warning(f"⚠️ No se pudo cargar 'datos_uis': {e}")
                df["tipo_olt_rental"] = None

            if df.empty:
                st.warning("⚠️ No hay datos asignados a este comercial.")
                st.stop()

            essential_cols = ["latitud", "longitud", "apartment_id"]
            missing = [c for c in essential_cols if c not in df.columns]
            if missing:
                st.error(f"❌ Faltan columnas: {missing}")
                st.stop()

        except Exception as e:
            st.error(f"❌ Error al cargar los datos: {e}")
            st.stop()

    # Obtener ubicación
    location = get_user_location()
    if location:
        lat, lon = location
        st.success(f"✅ Ubicación obtenida: {lat:.6f}, {lon:.6f}")
        st.session_state["ultima_lat"] = lat
        st.session_state["ultima_lon"] = lon
    else:
        st.warning("⚠️ No se pudo obtener ubicación automática.")
        lat = st.session_state.get("ultima_lat", 43.463444)
        lon = st.session_state.get("ultima_lon", -3.790476)

    # Filtros
    municipios = sorted(df["municipio"].dropna().unique())
    municipio_filtro = st.selectbox(
        "🏙️ Municipio", ["Selecciona un municipio"] + municipios, key="filtro_municipio"
    )
    if municipio_filtro == "Selecciona un municipio":
        st.warning("⚠️ Selecciona un municipio y una población para ver el mapa.")
        st.stop()

    poblaciones = sorted(
        df[df["municipio"] == municipio_filtro]["poblacion"].dropna().unique()
    )
    poblacion_filtro = st.selectbox(
        "👥 Población", ["Selecciona una población"] + poblaciones, key="filtro_poblacion"
    )
    if poblacion_filtro == "Selecciona una población":
        st.warning("⚠️ Selecciona una población.")
        st.stop()

    df_filtrado = df[
        (df["municipio"] == municipio_filtro) & (df["poblacion"] == poblacion_filtro)
    ].copy()

    # Filtro CTO
    opcion_cto = st.radio(
        "Selecciona el tipo de CTO:",
        ["Todas", "CTO VERDE", "CTO COMPARTIDA"],
        horizontal=True,
        key="filtro_cto",
    )
    if opcion_cto == "CTO VERDE":
        df_filtrado = df_filtrado[
            df_filtrado["tipo_olt_rental"].str.contains("CTO VERDE", case=False, na=False)
        ]
    elif opcion_cto == "CTO COMPARTIDA":
        df_filtrado = df_filtrado[
            df_filtrado["tipo_olt_rental"].str.contains("CTO COMPARTIDA", case=False, na=False)
        ]

    if df_filtrado.empty:
        st.warning("⚠️ No hay registros para los filtros seleccionados.")
        st.stop()

    lat_centro = df_filtrado["latitud"].mean()
    lon_centro = df_filtrado["longitud"].mean()

    m = _crear_mapa_optimizado(df_filtrado, lat_centro, lon_centro, ofertas_df, ams_df)

    # Añadir marcador de ubicación actual
    if location is not None:
        folium.Marker(
            location=location,
            popup="📍 Tu ubicación actual",
            icon=folium.Icon(color="red", icon="user"),
        ).add_to(m)

    st.info(f"📦 Mostrando {len(df_filtrado)} ubicaciones (de {len(df)} puntos totales)")
    map_data = st_folium(m, height=680, width="100%", key="optimized_map",
                             returned_objects=["last_object_clicked"])

    # Manejar clicks
    if "clicks" not in st.session_state:
        st.session_state.clicks = []

    if map_data and map_data.get("last_object_clicked"):
        st.session_state.clicks.append(map_data["last_object_clicked"])

    if st.session_state.clicks:
        last_click = st.session_state.clicks[-1]
        lat_click = last_click.get("lat")
        lon_click = last_click.get("lng")
        if lat_click and lon_click:
            maps_url = f"https://www.google.com/maps/search/?api=1&query={lat_click},{lon_click}"
            st.markdown(
                f"""
                <div style="text-align: center; margin: 5px 0;">
                    <a href="{maps_url}" target="_blank" style="
                        background-color: #0078ff;
                        color: white;
                        padding: 6px 12px;
                        font-size: 14px;
                        font-weight: bold;
                        border-radius: 6px;
                        text-decoration: none;
                        display: inline-flex;
                        align-items: center;
                        gap: 6px;
                    ">
                        🗺️ Ver en Google Maps
                    </a>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with st.spinner("⏳ Cargando formulario..."):
            mostrar_formulario(last_click)

    # Limpiar clicks antiguos
    if len(st.session_state.clicks) > 50:
        st.session_state.clicks = st.session_state.clicks[-20:]


# ==================== FORMULARIO DE OFERTA ====================
def mostrar_formulario(click_data: Dict[str, Any]):
    """Muestra el formulario para enviar una oferta en las coordenadas clickeadas."""
    st.subheader("📄 Enviar Oferta")

    popup_text = click_data.get("popup", "")
    apt_id_from_popup = popup_text.split(" - ")[0] if " - " in popup_text else "N/D"

    try:
        lat_value = float(click_data["lat"])
        lng_value = float(click_data["lng"])
    except (TypeError, ValueError):
        st.error("❌ Coordenadas inválidas.")
        return

    form_key = f"{lat_value}_{lng_value}"

    # Buscar datos en BD
    try:
        conn = get_db_connection()
        delta = 0.00001
        query = """
            SELECT * FROM datos_uis
            WHERE latitud BETWEEN %s AND %s AND longitud BETWEEN %s AND %s
        """
        params = (lat_value - delta, lat_value + delta, lng_value - delta, lng_value + delta)
        df = pd.read_sql(query, conn, params=params)
        conn.close()
    except Exception as e:
        st.error(f"❌ Error al consultar BD: {e}")
        return

    if df.empty:
        st.warning("⚠️ No se encontraron datos para estas coordenadas.")
        return

    if len(df) > 1:
        opciones = [
            f"{row['apartment_id']} – Vial: {row['vial']} – Nº: {row['numero']} – Letra: {row['letra']}"
            for _, row in df.iterrows()
        ]
        st.warning(
            "⚠️ Hay varias ofertas en estas coordenadas. Elige un Apartment ID del desplegable. "
            "¡NO TE OLVIDES DE GUARDAR CADA OFERTA POR SEPARADO!"
        )
        seleccion = st.selectbox("Elige un Apartment ID:", opciones, key=f"select_{form_key}")
        apt_id = seleccion.split()[0]
        df = df[df["apartment_id"] == apt_id]
    else:
        apt_id = df.iloc[0]["apartment_id"]

    row = df.iloc[0]

    # Formulario
    with st.form(key=f"oferta_form_{form_key}"):
        tipo_olt = str(row.get("tipo_olt_rental", ""))
        if "CTO VERDE" in tipo_olt.upper():
            st.badge("CTO VERDE", color="green")
        else:
            st.badge("CTO COMPARTIDA")

        st.text_input("🏢 Apartment ID", value=apt_id, disabled=True)

        col1, col2, col3 = st.columns(3)
        with col1:
            st.text_input("📍 Provincia", value=row["provincia"], disabled=True)
        with col2:
            st.text_input("🏙️ Municipio", value=row["municipio"], disabled=True)
        with col3:
            st.text_input("👥 Población", value=row["poblacion"], disabled=True)

        col4, col5, col6, col7 = st.columns([2, 1, 2, 1])
        with col4:
            st.text_input("🚦 Vial", value=row["vial"], disabled=True)
        with col5:
            st.text_input("🔢 Número", value=row["numero"], disabled=True)
        with col6:
            st.text_input("🔠 Letra", value=row["letra"], disabled=True)
        with col7:
            st.text_input("📮 CP", value=row["cp"], disabled=True)

        col8, col9, col10 = st.columns(3)
        with col8:
            st.text_input("📌 Latitud", value=lat_value, disabled=True)
        with col9:
            st.text_input("📌 Longitud", value=lng_value, disabled=True)
        with col10:
            st.text_input("📌 CTO", value=row.get("cto", ""), disabled=True)

        es_serviciable = st.radio(
            "🛠️ ¿Es serviciable?",
            ["Sí", "No"],
            index=0,
            horizontal=True,
            key=f"es_serviciable_{form_key}",
        )

        if es_serviciable == "No":
            motivo_serviciable = st.text_area(
                "❌ Motivo de No Servicio",
                key=f"motivo_serviciable_{form_key}",
                placeholder="Explicar por qué no es serviciable...",
                help="Obligatorio cuando no es serviciable",
            )
        else:
            motivo_serviciable = ""

        with st.expander("🏠 Datos de la Vivienda y Cliente", expanded=es_serviciable == "Sí"):
            if es_serviciable == "Sí":
                col1, col2 = st.columns(2)
                with col1:
                    tipo_vivienda = st.selectbox(
                        "🏠 Tipo de Ui",
                        ["Piso", "Casa", "Dúplex", "Negocio", "Ático", "Otro"],
                        key=f"tipo_vivienda_{form_key}",
                    )
                    tipo_vivienda_otro = (
                        st.text_input("📝 Especificar", key=f"tipo_vivienda_otro_{form_key}")
                        if tipo_vivienda == "Otro"
                        else ""
                    )
                    contrato = st.radio(
                        "📑 ¿Cliente interesado en contrato?",
                        ["Sí", "No Interesado"],
                        index=0,
                        horizontal=True,
                        key=f"contrato_{form_key}",
                    )
                with col2:
                    client_name = st.text_input(
                        "👤 Nombre del Cliente",
                        max_chars=100,
                        key=f"client_name_{form_key}",
                        placeholder="Nombre completo",
                    )
                    phone = st.text_input(
                        "📞 Teléfono",
                        max_chars=15,
                        key=f"phone_{form_key}",
                        placeholder="Número de teléfono",
                    )
            else:
                st.info("ℹ️ Solo relevante para ofertas serviciables")
                client_name = phone = tipo_vivienda = tipo_vivienda_otro = contrato = ""

        with st.expander("📍 Información Adicional", expanded=False):
            alt_address = st.text_input(
                "📌 Dirección Alternativa (si difiere)",
                key=f"alt_address_{form_key}",
            )
            observations = st.text_area(
                "📝 Observaciones Generales",
                key=f"observations_{form_key}",
            )

        with st.expander("⚠️ Gestión de Incidencias", expanded=False):
            if es_serviciable == "Sí":
                contiene_incidencias = st.radio(
                    "¿Contiene incidencias?",
                    ["Sí", "No"],
                    index=1,
                    horizontal=True,
                    key=f"contiene_incidencias_{form_key}",
                )
                motivo_incidencia = st.text_area(
                    "📄 Motivo de la Incidencia",
                    key=f"motivo_incidencia_{form_key}",
                )
                col_inc1, col_inc2 = st.columns(2)
                with col_inc1:
                    ocupado_tercero = st.checkbox(
                        "🏠 Ocupado por un tercero",
                        key=f"ocupado_tercero_{form_key}",
                    )
                with col_inc2:
                    imagen_incidencia = st.file_uploader(
                        "📷 Adjuntar Imagen (PNG, JPG, JPEG)",
                        type=["png", "jpg", "jpeg"],
                        key=f"imagen_incidencia_{form_key}",
                    )
            else:
                st.info("ℹ️ Solo relevante para ofertas serviciables")
                contiene_incidencias = motivo_incidencia = ""
                ocupado_tercero = False
                imagen_incidencia = None

        st.info(
            "💡 **Nota:** Complete todos los campos relevantes según el tipo de oferta."
        )

        submit = st.form_submit_button("🚀 Enviar Oferta")

    if submit:
        # Validaciones
        if es_serviciable == "No" and not motivo_serviciable:
            st.error("❌ Debe proporcionar el motivo de no servicio.")
            return
        if es_serviciable == "Sí":
            if not client_name or not phone:
                st.error("❌ Nombre y teléfono del cliente son obligatorios.")
                return
            if phone and not phone.isdigit():
                st.error("❌ El teléfono debe contener solo números.")
                return

        tipo_vivienda_final = (
            tipo_vivienda_otro if tipo_vivienda == "Otro" else tipo_vivienda
        ) if es_serviciable == "Sí" else ""

        oferta_data = {
            "Provincia": row["provincia"],
            "Municipio": row["municipio"],
            "Población": row["poblacion"],
            "Vial": row["vial"],
            "Número": row["numero"],
            "Letra": row["letra"],
            "Código Postal": row["cp"],
            "Latitud": lat_value,
            "Longitud": lng_value,
            "Nombre Cliente": client_name if es_serviciable == "Sí" else "",
            "Teléfono": phone if es_serviciable == "Sí" else "",
            "Dirección Alternativa": alt_address,
            "Observaciones": observations,
            "serviciable": es_serviciable,
            "motivo_serviciable": motivo_serviciable if es_serviciable == "No" else "",
            "incidencia": contiene_incidencias if es_serviciable == "Sí" else "",
            "motivo_incidencia": (
                motivo_incidencia
                if (es_serviciable == "Sí" and contiene_incidencias == "Sí")
                else ""
            ),
            "ocupado_por_tercero": (
                ocupado_tercero
                if (es_serviciable == "Sí" and contiene_incidencias == "Sí")
                else False
            ),
            "Tipo_Vivienda": tipo_vivienda_final,
            "Contrato": contrato if es_serviciable == "Sí" else "",
            "fecha": pd.Timestamp.now(tz="Europe/Madrid"),
        }

        with st.spinner("⏳ Guardando la oferta..."):
            guardar_en_base_de_datos(oferta_data, imagen_incidencia, apt_id)

            # Obtener emails para notificación
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT email FROM usuarios WHERE role IN ('admin', 'comercial_jefe')")
            emails_admin = [row[0] for row in cursor.fetchall()]
            email_comercial = st.session_state.get("email")
            conn.close()

            # Construir descripción para email
            desc = (
                f"🆕 Nueva oferta para {apt_id}.<br><br>"
                f"📑 <strong>Detalles realizados por {st.session_state['username']}:</strong><br>"
                f"🌍 Provincia: {row['provincia']}<br>"
                f"📌 Municipio: {row['municipio']}<br>"
                f"🏡 Población: {row['poblacion']}<br>"
                f"🛣️ Vial: {row['vial']}<br>"
                f"🔢 Número: {row['numero']}<br>"
                f"🔠 Letra: {row['letra']}<br>"
                f"📮 CP: {row['cp']}<br>"
                f"📅 Fecha: {oferta_data['fecha']}<br>"
                f"🔧 Serviciable: {es_serviciable}<br>"
            )
            if es_serviciable == "Sí":
                desc += (
                    f"📱 Teléfono: {phone}<br>"
                    f"👤 Nombre Cliente: {client_name}<br>"
                    f"🏘️ Tipo Vivienda: {tipo_vivienda_final}<br>"
                    f"✅ Contratado: {contrato}<br>"
                    f"⚠️ Incidencia: {contiene_incidencias}<br>"
                )
                if contiene_incidencias == "Sí":
                    desc += f"📄 Motivo Incidencia: {motivo_incidencia}<br>"
                    desc += f"🏠 Ocupado por tercero: {'Sí' if ocupado_tercero else 'No'}<br>"
            else:
                desc += f"❌ Motivo No Servicio: {motivo_serviciable}<br>"

            if alt_address:
                desc += f"📍 Dirección Alternativa: {alt_address}<br>"
            if observations:
                desc += f"💬 Observaciones: {observations}<br>"

            desc += "<br>ℹ️ Revise los detalles."

            for email in emails_admin:
                try:
                    correo_oferta_comercial(email, apt_id, desc)
                except Exception as e:
                    st.warning(f"Error notificando a {email}: {e}")
            if email_comercial:
                try:
                    correo_oferta_comercial(email_comercial, apt_id, desc)
                except Exception as e:
                    st.warning(f"Error notificando al comercial: {e}")

            st.success("✅ Oferta enviada correctamente.")


# ==================== SECCIÓN DE VIABILIDADES ====================
def _mostrar_viabilidades():
    """Subfunción que maneja la sección de viabilidades."""
    st.title("Viabilidades")
    st.markdown("""**Leyenda:**
                 ⚫ Viabilidad ya existente
                 🔵 Viabilidad nueva aún sin estudio
                 🟢 Viabilidad serviciable y con Apartment ID ya asociado
                 🔴 Viabilidad no serviciable
                """)
    st.info("ℹ️ Haz click en el mapa para agregar un marcador.")

    # Inicializar estado
    defaults = {
        "viabilidad_marker": None,
        "map_center": (43.463444, -3.790476),
        "map_zoom": 12,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

    viabilidades = obtener_viabilidades_cache(st.session_state["username"])

    # Crear mapa
    m = folium.Map(
        location=st.session_state.map_center,
        zoom_start=st.session_state.map_zoom,
        tiles="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}",
        attr="Google",
    )

    # Marcadores existentes
    for v in viabilidades:
        lat, lon, ticket, serviciable, apt_id = v
        if serviciable and str(serviciable).strip():
            serv = str(serviciable).strip()
            apt = str(apt_id).strip() if apt_id else ""
            if serv == "No":
                color = "red"
            elif serv == "Sí" and apt not in ["", "N/D"]:
                color = "green"
            else:
                color = "black"
        else:
            color = "black"
        folium.Marker(
            [lat, lon],
            icon=folium.Icon(color=color),
            popup=f"Ticket: {ticket}",
        ).add_to(m)

    # Marcador nuevo
    if st.session_state.viabilidad_marker:
        lat = st.session_state.viabilidad_marker["lat"]
        lon = st.session_state.viabilidad_marker["lon"]
        folium.Marker([lat, lon], icon=folium.Icon(color="blue")).add_to(m)

    Geocoder().add_to(m)
    map_data = st_folium(m, height=680, width="100%",
                             returned_objects=["last_clicked"])

    # Manejar clics
    if map_data and map_data.get("last_clicked"):
        click = map_data["last_clicked"]
        st.session_state.viabilidad_marker = {
            "lat": click["lat"],
            "lon": click["lng"],
        }
        st.session_state.map_center = (click["lat"], click["lng"])
        st.session_state.map_zoom = 15  # Zoom in al hacer clic
        st.rerun()

    if st.session_state.viabilidad_marker:
        if st.button("Eliminar marcador y crear uno nuevo"):
            st.session_state.viabilidad_marker = None
            st.session_state.map_center = (43.463444, -3.790476)
            st.rerun()

        # Formulario
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

            submit = st.form_submit_button("Enviar Formulario")

            if submit:
                ticket = generar_ticket()
                guardar_viabilidad(
                    (
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
                        st.session_state["username"],
                        olt,
                        apartment_id,
                    )
                )
                # Subir imágenes si las hay
                if imagenes:
                    st.toast("📤 Subiendo imágenes...")
                    for img in imagenes:
                        try:
                            archivo_bytes = img.getvalue()
                            nombre_archivo = img.name
                            unique_filename = f"{ticket}_{nombre_archivo}"
                            url = upload_image_to_cloudinary(
                                archivo_bytes,
                                unique_filename,
                                tipo="viabilidad",
                                folder=ticket,
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
                            st.warning(f"⚠️ Error subiendo {nombre_archivo}: {e}")
                    st.success("✅ Imágenes guardadas.")

                st.session_state.viabilidad_marker = None
                st.session_state.map_center = (43.463444, -3.790476)
                st.rerun()


# ==================== VISUALIZACIÓN DE DATOS ====================
def _mostrar_visualizacion_datos():
    """Subfunción que maneja la sección de visualización de datos."""
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

    # --- Ofertas ---
    st.subheader("📋 Tabla de Visitas/Ofertas")
    if df_ofertas.empty:
        st.warning(f"⚠️ No hay ofertas para '{comercial}'")
    else:
        col1, col2 = st.columns(2)
        with col1:
            filtro_contrato = st.selectbox(
                "Filtrar por contrato:",
                ["Todos", "✅ Activo", "❌ No Activo"],
                key="filtro_contrato",
            )
        with col2:
            filtro_serv = st.selectbox(
                "Filtrar por serviciable:",
                ["Todos", "Sí", "No"],
                key="filtro_serv",
            )

        df_filtrado = df_ofertas.copy()
        if filtro_contrato != "Todos":
            df_filtrado = df_filtrado[df_filtrado["Contrato_Activo"] == filtro_contrato]
        if filtro_serv != "Todos":
            df_filtrado = df_filtrado[df_filtrado["serviciable"] == filtro_serv]

        col_a, col_b, col_c, col_d = st.columns(4)
        with col_a:
            st.metric("Total Ofertas", len(df_filtrado))
        with col_b:
            activas = len(df_filtrado[df_filtrado["Contrato_Activo"] == "✅ Activo"])
            st.metric("Contratos Activos", activas)
        with col_c:
            serv = len(df_filtrado[df_filtrado["serviciable"] == "Sí"])
            st.metric("Serviciables", serv)
        with col_d:
            no_serv = len(df_filtrado[df_filtrado["serviciable"] == "No"])
            st.metric("No Serviciables", no_serv)

        st.dataframe(df_filtrado, width="stretch")
        if st.button("📤 Exportar Ofertas a CSV", key="export_ofertas"):
            csv = df_filtrado.to_csv(index=False)
            st.download_button(
                label="⬇️ Descargar CSV",
                data=csv,
                file_name=f"ofertas_{comercial}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )

    # --- Viabilidades ---
    st.subheader("📋 Tabla de Viabilidades")
    if df_viab.empty:
        st.warning(f"⚠️ No hay viabilidades para '{comercial}'")
    else:
        st.dataframe(df_viab, width="stretch")
        # Procesar pendientes
        criticas_j = ["MAS PREVENTA", "PDTE. RAFA FIN DE OBRA"]
        criticas_r = ["PDTE INFORMACION RAFA", "OK", "SOBRECOSTE"]
        df_pend = df_viab[
            (df_viab["justificacion"].isin(criticas_j) | df_viab["resultado"].isin(criticas_r))
            & (df_viab["respuesta_comercial"].isna() | (df_viab["respuesta_comercial"] == ""))
        ]
        if df_pend.empty:
            st.success("🎉 No tienes viabilidades pendientes de contestar")
        else:
            st.warning(f"🔔 Tienes {len(df_pend)} viabilidades pendientes de contestar")
            for _, row in df_pend.iterrows():
                ticket = row["ticket"]
                with st.expander(f"🎫 Ticket {ticket} - {row['municipio']} {row['vial']} {row['numero']}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**📌 Justificación:** {row.get('justificacion', '—')}")
                    with col2:
                        st.markdown(f"**📊 Resultado:** {row.get('resultado', '—')}")
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
                                    cursor.execute(
                                        "SELECT email FROM usuarios WHERE role IN ('admin','comercial_jefe')"
                                    )
                                    destinatarios = [r[0] for r in cursor.fetchall()]
                                    conn.close()
                                    for email in destinatarios:
                                        try:
                                            correo_respuesta_comercial(
                                                email, ticket, comercial, nuevo_comentario
                                            )
                                        except Exception as e:
                                            st.warning(f"Error notificando a {email}: {e}")
                                    st.success(f"✅ Respuesta guardada para {ticket}")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Error: {e}")


# ==================== FUNCIÓN PRINCIPAL ====================
def comercial_dashboard():
    """Dashboard principal del comercial."""
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

    # Sidebar
    with st.sidebar:
        st.markdown(
            f"""
            <div style="text-align:center;">
                <div style="width:100px; height:100px; border-radius:50%; background-color:#ff7f00; color:white;
                            font-size:50px; display:flex; align-items:center; justify-content:center; margin:0 auto;">
                    👤
                </div>
                <div style="margin-top:10px; font-weight:bold;">Rol: Comercial</div>
                <div style="font-weight:bold; font-size:18px;">Bienvenido, {st.session_state['username']}</div>
                <hr>
            </div>
            """,
            unsafe_allow_html=True,
        )

        menu_opcion = option_menu(
            menu_title=None,
            options=["Ofertas Comerciales", "Viabilidades", "Visualización de Datos"],
            icons=["bar-chart", "check-circle", "graph-up"],
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
            detalles = f"El comercial {st.session_state.get('username', 'N/A')} cerró sesión."
            log_trazabilidad(st.session_state.get("username", "N/A"), "Cierre sesión", detalles)

            for key in [f'{cookie_name}_session_id', f'{cookie_name}_username', f'{cookie_name}_role']:
                if controller.get(key):
                    controller.set(key, '', max_age=0, path='/')
            st.session_state["login_ok"] = False
            st.session_state["username"] = ""
            st.session_state["role"] = ""
            st.session_state["session_id"] = ""
            st.toast("✅ Sesión cerrada. Redirigiendo...")
            st.rerun()

    if "username" not in st.session_state:
        st.warning("⚠️ No has iniciado sesión. Redirigiendo al login...")
        time.sleep(2)
        login.login()
        return

    log_trazabilidad(
        st.session_state["username"],
        "Selección de vista",
        f"Seleccionó '{menu_opcion}'",
    )

    if menu_opcion == "Ofertas Comerciales":
        _mostrar_mapa_ofertas()
    elif menu_opcion == "Viabilidades":
        _mostrar_viabilidades()
    elif menu_opcion == "Visualización de Datos":
        _mostrar_visualizacion_datos()


if __name__ == "__main__":
    comercial_dashboard()