# comercial_dashboard_vip.py
import io
import os
import re
import secrets
import urllib.parse
import warnings
from datetime import datetime, timedelta, time
from typing import Optional, Tuple, List, Dict, Any

import folium
import pandas as pd
import sqlitecloud
import streamlit as st
from branca.element import MacroElement, Template
from folium.plugins import Geocoder, MarkerCluster
from streamlit_cookies_controller import CookieController
from streamlit_folium import st_folium
from streamlit_option_menu import option_menu

# Módulos locales
from modules import login
from modules.minIO import upload_image_to_cloudinary
from modules.notificaciones import (
    correo_oferta_comercial,
    correo_respuesta_comercial,
    correo_viabilidad_comercial,
)

warnings.filterwarnings("ignore", category=UserWarning)

cookie_name = "my_app"

# ==================== CONEXIÓN A BASE DE DATOS ====================
def get_db_connection():
    """Retorna una conexión a la base de datos SQLite Cloud."""
    return sqlitecloud.connect(
        "sqlitecloud://ceafu04onz.g6.sqlite.cloud:8860/usuarios.db?apikey=Qo9m18B9ONpfEGYngUKm99QB5bgzUTGtK7iAcThmwvY"
    )


# ==================== TRAZABILIDAD ====================
def log_trazabilidad(usuario: str, accion: str, detalles: str):
    """Inserta un registro en la tabla trazabilidad."""
    conn = get_db_connection()
    cursor = conn.cursor()
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute(
        """
        INSERT INTO trazabilidad (usuario_id, accion, detalles, fecha)
        VALUES (?, ?, ?, ?)
        """,
        (usuario, accion, detalles, fecha),
    )
    conn.commit()
    conn.close()


# ==================== CARGA DE DATOS CON CACHÉ ====================
@st.cache_data(ttl=300)  # 5 minutos
def cargar_tarifas() -> pd.DataFrame:
    """Carga la tabla de tarifas."""
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT id, nombre, descripcion, precio FROM tarifas", conn)
    finally:
        conn.close()
    return df


@st.cache_data(ttl=300)
def obtener_provincias() -> List[str]:
    """Obtiene lista de provincias."""
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT DISTINCT provincia FROM datos_uis ORDER BY provincia", conn)
        provincias = df["provincia"].dropna().tolist()
    finally:
        conn.close()
    return provincias


@st.cache_data(ttl=300)
def obtener_municipios(provincia: str) -> List[str]:
    """Obtiene municipios de una provincia."""
    conn = get_db_connection()
    try:
        df = pd.read_sql(
            "SELECT DISTINCT municipio FROM datos_uis WHERE provincia = ? ORDER BY municipio",
            conn,
            params=(provincia,),
        )
        municipios = df["municipio"].dropna().tolist()
    finally:
        conn.close()
    return municipios


@st.cache_data(ttl=300)
def obtener_poblaciones(provincia: str, municipio: str) -> List[str]:
    """Obtiene poblaciones de un municipio."""
    conn = get_db_connection()
    try:
        df = pd.read_sql(
            "SELECT DISTINCT poblacion FROM datos_uis WHERE provincia = ? AND municipio = ? ORDER BY poblacion",
            conn,
            params=(provincia, municipio),
        )
        poblaciones = df["poblacion"].dropna().tolist()
    finally:
        conn.close()
    return poblaciones


@st.cache_data(ttl=300)
def cargar_viabilidades_con_apartment() -> List[tuple]:
    """Carga viabilidades que tienen apartment_id no nulo."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ticket, apartment_id, provincia, municipio, poblacion, vial, numero, letra, nombre_cliente
        FROM viabilidades
        WHERE apartment_id IS NOT NULL
    """)
    viabilidades = cursor.fetchall()
    conn.close()
    return viabilidades


# ==================== FUNCIONES DE VIABILIDAD (compartidas) ====================
def generar_ticket() -> str:
    """Genera un ticket único con formato: añomesdía + número consecutivo."""
    conn = get_db_connection()
    cursor = conn.cursor()
    fecha_actual = datetime.now().strftime("%Y%m%d")
    cursor.execute(
        "SELECT MAX(CAST(SUBSTR(ticket, 9, 3) AS INTEGER)) FROM viabilidades WHERE ticket LIKE ?",
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?)
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
    """Obtiene todas las viabilidades (para el mapa)."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT latitud, longitud, ticket, serviciable, apartment_id FROM viabilidades")
    viabilidades = cursor.fetchall()
    conn.close()
    return viabilidades


# ==================== GUARDADO DE OFERTAS ====================
def guardar_en_base_de_datos_vip(
    oferta_data: Dict[str, Any],
    imagen_incidencia: Optional[Any],
    apartment_id: str,
) -> None:
    """Guarda o actualiza la oferta en SQLite para comercial VIP."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Subir imagen si hay incidencia
        imagen_url = None
        if oferta_data["incidencia"] == "Sí" and imagen_incidencia:
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

        # Verificar si ya existe en comercial_rafa
        cursor.execute("SELECT comercial FROM comercial_rafa WHERE apartment_id = ?", (apartment_id,))
        row = cursor.fetchone()

        if row:
            comercial_asignado = row[0]
            if comercial_asignado and str(comercial_asignado).strip() != "":
                st.error(
                    f"❌ El Apartment ID {apartment_id} ya está asignado al comercial '{comercial_asignado}'. "
                    f"No se puede modificar desde este panel."
                )
                conn.close()
                return

            # UPDATE
            cursor.execute(
                """
                UPDATE comercial_rafa SET
                    provincia = ?, municipio = ?, poblacion = ?, vial = ?, numero = ?, letra = ?,
                    cp = ?, latitud = ?, longitud = ?, nombre_cliente = ?, telefono = ?,
                    direccion_alternativa = ?, observaciones = ?, serviciable = ?, motivo_serviciable = ?,
                    incidencia = ?, motivo_incidencia = ?, fichero_imagen = ?, fecha = ?, Tipo_Vivienda = ?,
                    Contrato = ?, comercial = ?
                WHERE apartment_id = ?
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
                    imagen_url,
                    oferta_data["fecha"].strftime("%Y-%m-%d %H:%M:%S"),
                    oferta_data["Tipo_Vivienda"],
                    oferta_data["Contrato"],
                    comercial_logueado,
                    apartment_id,
                ),
            )
            st.success(f"✅ ¡Oferta actualizada en comercial_rafa para {apartment_id}!")

        else:
            # INSERT
            cursor.execute(
                """
                SELECT provincia, municipio, poblacion, vial, numero, letra, cp, latitud, longitud
                FROM datos_uis WHERE apartment_id = ?
                """,
                (apartment_id,),
            )
            row = cursor.fetchone()
            if not row:
                st.error(f"❌ El apartment_id {apartment_id} no existe en datos_uis.")
                conn.close()
                return

            provincia, municipio, poblacion, vial, numero, letra, cp, lat, lon = row
            cursor.execute(
                """
                INSERT INTO comercial_rafa (
                    apartment_id, provincia, municipio, poblacion, vial, numero, letra, cp, latitud, longitud,
                    nombre_cliente, telefono, direccion_alternativa, observaciones, serviciable, motivo_serviciable,
                    incidencia, motivo_incidencia, fichero_imagen, fecha, Tipo_Vivienda, Contrato, comercial
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    apartment_id,
                    provincia,
                    municipio,
                    poblacion,
                    vial,
                    numero,
                    letra,
                    cp,
                    lat,
                    lon,
                    oferta_data["Nombre Cliente"],
                    oferta_data["Teléfono"],
                    oferta_data["Dirección Alternativa"],
                    oferta_data["Observaciones"],
                    oferta_data["serviciable"],
                    oferta_data["motivo_serviciable"],
                    oferta_data["incidencia"],
                    oferta_data["motivo_incidencia"],
                    imagen_url,
                    oferta_data["fecha"].strftime("%Y-%m-%d %H:%M:%S"),
                    oferta_data["Tipo_Vivienda"],
                    oferta_data["Contrato"],
                    comercial_logueado,
                ),
            )
            st.success(f"✅ ¡Oferta insertada en comercial_rafa para {apartment_id}!")

        conn.commit()
        conn.close()

        log_trazabilidad(
            comercial_logueado,
            "Guardar/Actualizar Oferta",
            f"Oferta guardada para Apartment ID: {apartment_id}",
        )

    except Exception as e:
        st.error(f"❌ Error al guardar/actualizar la oferta: {e}")


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


# ==================== SECCIÓN DE OFERTAS COMERCIALES ====================
def _mostrar_ofertas_vip():
    """Subfunción que maneja la sección de Ofertas Comerciales para VIP."""
    comercial = st.session_state.get("username", "").lower()
    mostrar_ultimo_anuncio()

    # Filtros
    provincias = obtener_provincias()
    provincia_sel = st.selectbox("🌍 Selecciona provincia", ["Todas"] + provincias, key="vip_provincia")

    municipios = []
    if provincia_sel != "Todas":
        municipios = obtener_municipios(provincia_sel)
    municipio_sel = st.selectbox(
        "🏘️ Selecciona municipio", ["Todos"] + municipios, key="vip_municipio"
    ) if municipios else "Todos"

    poblaciones = []
    if municipio_sel != "Todos":
        poblaciones = obtener_poblaciones(provincia_sel, municipio_sel)
    poblacion_sel = st.selectbox(
        "🏡 Selecciona población", ["Todas"] + poblaciones, key="vip_poblacion"
    ) if poblaciones else "Todas"

    sin_comercial = st.checkbox("Mostrar solo apartamentos sin comercial asignado", key="vip_sin_comercial")
    solo_mios = st.checkbox("Mostrar solo mis puntos asignados", key="vip_solo_mios")

    colA, colB = st.columns(2)
    with colA:
        aplicar = st.button("🔍 Aplicar filtros", key="vip_apply")
    with colB:
        limpiar = st.button("🧹 Limpiar filtros", key="vip_clear")

    if limpiar:
        for key in ["vip_filtered_df", "vip_filters"]:
            st.session_state.pop(key, None)
        st.success("🧹 Filtros limpiados.")
        st.rerun()

    if aplicar:
        with st.spinner("⏳ Cargando puntos filtrados..."):
            try:
                conn = get_db_connection()
                query = """
                    SELECT d.apartment_id, d.provincia, d.municipio, d.poblacion,
                           d.vial, d.numero, d.letra, d.cp, d.latitud, d.longitud,
                           d.serviciable, c.comercial, c.Contrato
                    FROM datos_uis d
                    LEFT JOIN comercial_rafa c ON d.apartment_id = c.apartment_id
                    WHERE 1=1
                """
                params = []

                if provincia_sel != "Todas":
                    query += " AND d.provincia = ?"
                    params.append(provincia_sel)
                if municipio_sel != "Todos":
                    query += " AND d.municipio = ?"
                    params.append(municipio_sel)
                if poblacion_sel != "Todas":
                    query += " AND d.poblacion = ?"
                    params.append(poblacion_sel)
                if sin_comercial:
                    query += " AND (c.comercial IS NULL OR TRIM(c.comercial) = '')"
                if solo_mios:
                    query += " AND LOWER(TRIM(c.comercial)) = LOWER(TRIM(?))"
                    params.append(comercial)

                df = pd.read_sql(query, conn, params=params)
                conn.close()

                if df.empty:
                    st.warning("⚠️ No hay datos para los filtros seleccionados.")
                else:
                    st.session_state["vip_filtered_df"] = df
                    st.session_state["vip_filters"] = {
                        "provincia": provincia_sel,
                        "municipio": municipio_sel,
                        "poblacion": poblacion_sel,
                        "sin_comercial": sin_comercial,
                        "solo_mios": solo_mios,
                    }
                    st.success(f"✅ Se han cargado {len(df)} puntos.")
            except Exception as e:
                st.error(f"❌ Error al cargar los datos: {e}")

    df_to_show = st.session_state.get("vip_filtered_df")
    if df_to_show is not None:
        _mostrar_mapa_vip(df_to_show)


def _mostrar_mapa_vip(df: pd.DataFrame):
    """Renderiza el mapa con los datos filtrados."""
    if "clicks" not in st.session_state:
        st.session_state.clicks = []

    # Centro del mapa
    if len(df) == 1:
        lat, lon = df.iloc[0]["latitud"], df.iloc[0]["longitud"]
        zoom = 18
    else:
        lat, lon = df["latitud"].mean(), df["longitud"].mean()
        zoom = 12

    m = folium.Map(
        location=[lat, lon],
        zoom_start=zoom,
        max_zoom=21,
        tiles="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}",
        attr="Google",
    )
    Geocoder().add_to(m)

    if len(df) >= 500:
        cluster = MarkerCluster(maxClusterRadius=5, minClusterSize=3).add_to(m)
    else:
        cluster = m

    # Contar duplicados para desplazar
    df["offset_idx"] = df.groupby(["latitud", "longitud"]).cumcount()

    for _, row in df.iterrows():
        apt_id = row["apartment_id"]
        comercial_asignado = row["comercial"] if row["comercial"] else "Sin asignar"
        contrato_val = row["Contrato"] if row["Contrato"] else "N/A"
        serv_val = str(row.get("serviciable", "")).strip().lower()

        if serv_val == "no":
            color = "red"
        elif serv_val == "si":
            color = "green"
        elif isinstance(contrato_val, str) and contrato_val.strip().lower() == "sí":
            color = "orange"
        elif isinstance(contrato_val, str) and contrato_val.strip().lower() == "no interesado":
            color = "black"
        else:
            color = "blue"

        popup = f"""
        🏠 ID: {apt_id}<br>
        📍 {row['latitud']}, {row['longitud']}<br>
        ✅ Serviciable: {row.get('serviciable', 'N/D')}<br>
        👤 Comercial: {comercial_asignado}<br>
        📑 Contrato: {contrato_val}
        """
        offset = row["offset_idx"] * 0.00003
        folium.Marker(
            location=[row["latitud"] + offset, row["longitud"] - offset],
            popup=popup,
            icon=folium.Icon(color=color, icon="info-sign"),
        ).add_to(cluster)

    # Leyenda
    legend = """
    {% macro html(this, kwargs) %}
    <div style="
        position: fixed; 
        bottom: 0px; left: 0px; width: 220px; 
        z-index:9999; 
        font-size:14px;
        background-color: white;
        border:2px solid grey;
        border-radius:8px;
        padding: 10px;
        box-shadow: 2px 2px 6px rgba(0,0,0,0.3);
    ">
    <b>Leyenda</b><br>
    <i style="color:green;">●</i> Serviciable<br>
    <i style="color:red;">●</i> No serviciable<br>
    <i style="color:orange;">●</i> Contrato Sí<br>
    <i style="color:black;">●</i> No interesado<br>
    <i style="color:blue;">●</i> Sin información/No visitado<br>
    </div>
    {% endmacro %}
    """
    m.get_root().html.add_child(folium.Element(legend))

    map_data = st_folium(m, height=680, width="100%")

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

    if len(st.session_state.clicks) > 50:
        st.session_state.clicks = st.session_state.clicks[-20:]


# ==================== FORMULARIO DE OFERTA (compartido) ====================
def mostrar_formulario(click_data: Dict[str, Any]):
    """Muestra el formulario para enviar una oferta en las coordenadas clickeadas."""
    st.subheader("📄 Enviar Oferta")

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
            WHERE latitud BETWEEN ? AND ? AND longitud BETWEEN ? AND ?
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

    with st.form(key=f"oferta_form_{form_key}"):
        st.text_input("🏢 Apartment ID", value=apt_id, disabled=True)
        col1, col2, col3 = st.columns(3)
        with col1:
            st.text_input("📍 Provincia", value=row["provincia"], disabled=True)
        with col2:
            st.text_input("🏙️ Municipio", value=row["municipio"], disabled=True)
        with col3:
            st.text_input("👥 Población", value=row["poblacion"], disabled=True)

        col4, col5, col6, col7 = st.columns([2, 1, 1, 1])
        with col4:
            st.text_input("🚦 Vial", value=row["vial"], disabled=True)
        with col5:
            st.text_input("🔢 Número", value=row["numero"], disabled=True)
        with col6:
            st.text_input("🔠 Letra", value=row["letra"], disabled=True)
        with col7:
            st.text_input("📮 CP", value=row["cp"], disabled=True)

        col8, col9 = st.columns(2)
        with col8:
            st.text_input("📌 Latitud", value=lat_value, disabled=True)
        with col9:
            st.text_input("📌 Longitud", value=lng_value, disabled=True)

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
                    if tipo_vivienda == "Otro":
                        tipo_vivienda_otro = st.text_input("📝 Especificar", key=f"tipo_vivienda_otro_{form_key}")
                    else:
                        tipo_vivienda_otro = ""
                    contrato = st.radio(
                        "📑 Tipo de Contrato",
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
                "📝 Observaciones",
                key=f"observations_{form_key}",
            )

        with st.expander("⚠️ Gestión de Incidencias", expanded=False):
            if es_serviciable == "Sí":
                contiene_incidencias = st.radio(
                    "⚠️ ¿Contiene incidencias?",
                    ["Sí", "No"],
                    index=1,
                    horizontal=True,
                    key=f"contiene_incidencias_{form_key}",
                )
                motivo_incidencia = st.text_area(
                    "📄 Motivo de la Incidencia",
                    key=f"motivo_incidencia_{form_key}",
                )
                imagen_incidencia = st.file_uploader(
                    "📷 Adjuntar Imagen (PNG, JPG, JPEG)",
                    type=["png", "jpg", "jpeg"],
                    key=f"imagen_incidencia_{form_key}",
                )
            else:
                st.info("ℹ️ Solo relevante para ofertas serviciables")
                contiene_incidencias = motivo_incidencia = ""
                imagen_incidencia = None

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
            "Tipo_Vivienda": tipo_vivienda_final,
            "Contrato": contrato if es_serviciable == "Sí" else "",
            "fecha": pd.Timestamp.now(tz="Europe/Madrid"),
        }

        with st.spinner("⏳ Guardando la oferta..."):
            guardar_en_base_de_datos_vip(oferta_data, imagen_incidencia, apt_id)

            # Notificaciones
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT email FROM usuarios WHERE role IN ('admin')")
            emails_admin = [row[0] for row in cursor.fetchall()]
            email_comercial = st.session_state.get("email")
            conn.close()

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
            else:
                desc += f"❌ Motivo No Servicio: {motivo_serviciable}<br>"

            if alt_address:
                desc += f"📍 Dirección Alternativa: {alt_address}<br>"
            if observations:
                desc += f"💬 Observaciones: {observations}<br>"

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

    viabilidades = obtener_viabilidades()

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

    if st.session_state.viabilidad_marker:
        lat = st.session_state.viabilidad_marker["lat"]
        lon = st.session_state.viabilidad_marker["lon"]
        folium.Marker([lat, lon], icon=folium.Icon(color="blue")).add_to(m)

    Geocoder().add_to(m)
    map_data = st_folium(m, height=680, width="100%")

    if map_data and map_data.get("last_clicked"):
        click = map_data["last_clicked"]
        st.session_state.viabilidad_marker = {"lat": click["lat"], "lon": click["lng"]}
        st.session_state.map_center = (click["lat"], click["lng"])
        st.session_state.map_zoom = map_data["zoom"]
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
            # Obtener lista de OLTs (cacheada)
            lista_olt = obtener_lista_olt_cache()
            with col12:
                olt = st.selectbox("🏢 OLT", options=lista_olt)
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
                                VALUES (?, ?, ?)
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


@st.cache_data(ttl=3600)
def obtener_lista_olt_cache() -> List[str]:
    """Obtiene lista de OLTs con caché."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id_olt, nombre_olt FROM olt ORDER BY nombre_olt")
    lista = [f"{fila[0]}. {fila[1]}" for fila in cursor.fetchall()]
    conn.close()
    return lista


# ==================== SECCIÓN DE VISUALIZACIÓN DE DATOS ====================
def _mostrar_visualizacion_datos():
    """Subfunción que maneja la sección de Visualización de Datos."""
    st.subheader("📊 Visualización de Datos")

    if "username" not in st.session_state:
        st.error("❌ No has iniciado sesión.")
        return

    comercial_usuario = st.session_state["username"]
    try:
        conn = get_db_connection()

        # Ofertas
        df_ofertas = pd.read_sql(
            "SELECT * FROM comercial_rafa WHERE LOWER(comercial) = LOWER(?)",
            conn,
            params=(comercial_usuario,),
        )
        # Contratos activos
        df_seguimiento = pd.read_sql(
            "SELECT apartment_id, estado FROM seguimiento_contratos WHERE LOWER(estado) = 'finalizado'",
            conn,
        )
        df_ofertas["Contrato_Activo"] = df_ofertas["apartment_id"].isin(df_seguimiento["apartment_id"]).map(
            {True: "✅ Activo", False: "❌ No Activo"}
        )

        # Viabilidades
        df_viabilidades = pd.read_sql(
            """
            SELECT ticket, latitud, longitud, provincia, municipio, poblacion, vial, numero, letra, cp,
                   serviciable, coste, comentarios_comercial, comentarios_internos, nombre_cliente, telefono,
                   justificacion, Presupuesto_enviado, resultado, respuesta_comercial
            FROM viabilidades
            WHERE LOWER(usuario) = LOWER(?)
            """,
            conn,
            params=(comercial_usuario,),
        )
        conn.close()
    except Exception as e:
        st.error(f"❌ Error al cargar datos: {e}")
        return

    # --- Ofertas ---
    st.subheader("📋 Tabla de Visitas/Ofertas")
    if df_ofertas.empty:
        st.warning(f"⚠️ No hay ofertas para '{comercial_usuario}'.")
    else:
        st.dataframe(df_ofertas, width="stretch")

    # --- Viabilidades ---
    st.subheader("📋 Tabla de Viabilidades")
    if df_viabilidades.empty:
        st.warning(f"⚠️ No hay viabilidades para '{comercial_usuario}'.")
    else:
        st.dataframe(df_viabilidades, width="stretch")

        # Filtrar pendientes
        criticas_j = ["MAS PREVENTA", "PDTE. RAFA FIN DE OBRA"]
        criticas_r = ["PDTE INFORMACION RAFA", "OK", "SOBRECOSTE"]
        df_pend = df_viabilidades[
            (df_viabilidades["justificacion"].isin(criticas_j) | df_viabilidades["resultado"].isin(criticas_r))
            & (df_viabilidades["respuesta_comercial"].isna() | (df_viabilidades["respuesta_comercial"] == ""))
        ]
        if df_pend.empty:
            st.success("🎉 No tienes viabilidades pendientes de contestar.")
        else:
            st.warning(f"🔔 Tienes {len(df_pend)} viabilidades pendientes de contestar.")
            for _, row in df_pend.iterrows():
                ticket = row["ticket"]
                with st.expander(f"🎫 Ticket {ticket} - {row['municipio']} {row['vial']} {row['numero']}"):
                    st.markdown(f"""
                        **👤 Nombre del cliente:** {row.get('nombre_cliente', '—')}
                        **📌 Justificación oficina:** {row.get('justificacion', '—')}
                        **📊 Resultado oficina:** {row.get('resultado', '—')}
                        **💬 Comentarios a comercial:** {row.get('comentarios_comercial', '—')}
                        **🧩 Comentarios internos:** {row.get('comentarios_internos', '—')}
                    """)
                    with st.form(key=f"form_viab_{ticket}"):
                        nuevo_comentario = st.text_area(
                            "✏️ Tu respuesta:",
                            value="",
                            placeholder="Ejemplo: El cliente confirma que esperará a fin de obra...",
                        )
                        if st.form_submit_button("💾 Guardar Respuesta", use_container_width=True):
                            if not nuevo_comentario.strip():
                                st.error("❌ El comentario no puede estar vacío.")
                            else:
                                try:
                                    conn = get_db_connection()
                                    cursor = conn.cursor()
                                    cursor.execute(
                                        "UPDATE viabilidades SET respuesta_comercial = ? WHERE ticket = ?",
                                        (nuevo_comentario, ticket),
                                    )
                                    conn.commit()
                                    conn.close()
                                    # Notificar
                                    conn = get_db_connection()
                                    cursor = conn.cursor()
                                    cursor.execute("SELECT email FROM usuarios WHERE role IN ('admin')")
                                    destinatarios = [row[0] for row in cursor.fetchall()]
                                    conn.close()
                                    for email in destinatarios:
                                        try:
                                            correo_respuesta_comercial(
                                                email, ticket, comercial_usuario, nuevo_comentario
                                            )
                                        except Exception as e:
                                            st.warning(f"Error notificando a {email}: {e}")
                                    st.success(f"✅ Respuesta guardada para {ticket}")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Error: {e}")


# ==================== SECCIÓN DE PRECONTRATOS ====================
def _mostrar_precontratos():
    """Subfunción que maneja la sección de Precontratos."""
    st.title("📑 Gestión de Precontratos")
    tab1, tab2 = st.tabs(["🆕 Crear Nuevo Precontrato", "📋 Precontratos Existentes"])

    with tab1:
        _formulario_precontrato_standalone()

    with tab2:
        _listado_precontratos()


def _formulario_precontrato_standalone():
    """Formulario para crear un precontrato sin Apartment ID asociado (standalone)."""
    st.subheader("Crear Nuevo Precontrato")
    st.info(
        "💡 **Información:**\n"
        "- Puedes crear precontratos sin Apartment ID.\n"
        "- Solo tarifa, precio y permanencia son obligatorios.\n"
        "- El cliente completará los datos faltantes a través del enlace."
    )

    with st.form(key="form_precontrato_standalone"):
        # Cargar tarifas
        tarifas_df = cargar_tarifas()
        if tarifas_df.empty:
            st.warning("⚠️ No hay tarifas registradas.")
            st.form_submit_button("Guardar", disabled=True)
            return

        opciones = [
            f"{row['nombre']} – {row['descripcion']} ({row['precio']}€)"
            for _, row in tarifas_df.iterrows()
        ]
        tarifa_sel = st.selectbox("💰 Selecciona una tarifa:*", opciones, key="tarifa_standalone")
        tarifa_nombre = tarifa_sel.split(" – ")[0]

        apartment_id = st.text_input("🏢 Apartment ID (opcional)", key="ap_id_standalone")
        precio = st.text_input("💵 Precio Total (€ I.V.A Incluido)*", key="precio_standalone", placeholder="Ej: 1200,50")
        permanencia = st.radio("📆 Permanencia (meses)*", options=[12, 24], key="perm_standalone", horizontal=True)

        # Campos opcionales
        st.subheader("📋 Datos del Cliente (Opcionales)")
        col1, col2, col3 = st.columns(3)
        with col1:
            nombre = st.text_input("👤 Nombre / Razón social", key="nombre_standalone")
            cif = st.text_input("🏢 CIF", key="cif_standalone")
            nombre_legal = st.text_input("👥 Nombre Legal", key="nombre_legal_standalone")
        with col2:
            nif = st.text_input("🪪 NIF / DNI", key="nif_standalone")
            telefono1 = st.text_input("📞 Teléfono 1", key="tel1_standalone")
            telefono2 = st.text_input("📞 Teléfono 2", key="tel2_standalone")
        with col3:
            mail = st.text_input("✉️ Email", key="mail_standalone", placeholder="usuario@dominio.com")
            comercial = st.text_input("🧑‍💼 Comercial", value=st.session_state.get("username", ""), key="comercial_standalone")
            fecha = st.date_input("📅 Fecha", datetime.now().date(), key="fecha_standalone")

        direccion = st.text_input("🏠 Dirección", key="dir_standalone")
        col4, col5, col6 = st.columns(3)
        with col4:
            cp = st.text_input("📮 Código Postal", key="cp_standalone")
        with col5:
            poblacion = st.text_input("🏘️ Población", key="pob_standalone")
        with col6:
            provincia = st.text_input("🌍 Provincia", key="prov_standalone")

        col7, col8 = st.columns(2)
        with col7:
            iban = st.text_input("🏦 IBAN", key="iban_standalone", placeholder="ES00 0000 0000 0000 0000 0000")
        with col8:
            bic = st.text_input("🏦 BIC", key="bic_standalone", placeholder="AAAAESMMXXX")

        observaciones = st.text_area("📝 Observaciones", key="obs_standalone")
        servicio_adicional = st.text_area("➕ Servicio Adicional", key="serv_adicional_standalone")

        # Líneas (simplificadas, se pueden expandir igual que en el otro formulario)
        st.markdown("#### 📞 Línea Fija (opcional)")
        colf1, colf2, colf3 = st.columns(3)
        with colf1:
            fija_tipo = st.selectbox("Tipo", ["nuevo", "portabilidad"], key="fija_tipo_std")
            fija_numero = st.text_input("Número", key="fija_numero_std")
        with colf2:
            fija_titular = st.text_input("Titular", key="fija_titular_std")
            fija_dni = st.text_input("DNI Titular", key="fija_dni_std")
        with colf3:
            fija_operador = st.text_input("Operador Donante", key="fija_operador_std")
            fija_icc = st.text_input("ICC", key="fija_icc_std")

        st.markdown("#### 📱 Línea Móvil Principal (opcional)")
        colm1, colm2, colm3 = st.columns(3)
        with colm1:
            movil_tipo = st.selectbox("Tipo", ["nuevo", "portabilidad"], key="movil_tipo_std")
            movil_numero = st.text_input("Número", key="movil_numero_std")
        with colm2:
            movil_titular = st.text_input("Titular", key="movil_titular_std")
            movil_dni = st.text_input("DNI Titular", key="movil_dni_std")
        with colm3:
            movil_operador = st.text_input("Operador Donante", key="movil_operador_std")
            movil_icc = st.text_input("ICC", key="movil_icc_std")

        # Líneas adicionales (omitidas por brevedad, se pueden añadir igual que en el otro formulario)

        submit = st.form_submit_button("💾 Guardar precontrato")

        if submit:
            if not tarifa_nombre or not precio or not permanencia:
                st.error("❌ Tarifa, precio y permanencia son obligatorios.")
                return
            try:
                precio_limpio = precio.replace(",", ".").replace(" ", "")
                float(precio_limpio)
            except ValueError:
                st.error("❌ Precio inválido.")
                return

            # Insertar en BD
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO precontratos (
                    apartment_id, tarifas, observaciones, precio, comercial,
                    nombre, cif, nombre_legal, nif, telefono1, telefono2, mail, direccion,
                    cp, poblacion, provincia, iban, bic, fecha, firma, permanencia,
                    servicio_adicional, precontrato_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    apartment_id if apartment_id else None,
                    tarifa_nombre,
                    observaciones,
                    precio_limpio,
                    comercial,
                    nombre or "",
                    cif or "",
                    nombre_legal or "",
                    nif or "",
                    telefono1 or "",
                    telefono2 or "",
                    mail or "",
                    direccion or "",
                    cp or "",
                    poblacion or "",
                    provincia or "",
                    iban or "",
                    bic or "",
                    str(fecha),
                    "",
                    permanencia,
                    servicio_adicional or "",
                    f"PRE-{int(datetime.now().timestamp())}",
                ),
            )
            pre_id = cursor.lastrowid

            # Insertar líneas (solo las que tienen número)
            lineas = []
            if fija_numero:
                lineas.append(("fija", fija_tipo, fija_numero, fija_titular, fija_dni, fija_operador, fija_icc))
            if movil_numero:
                lineas.append(("movil", movil_tipo, movil_numero, movil_titular, movil_dni, movil_operador, movil_icc))

            for tipo, tipo_port, num, titular, dni, op, icc in lineas:
                cursor.execute(
                    """
                    INSERT INTO lineas (precontrato_id, tipo, numero_nuevo_portabilidad, numero_a_portar,
                                         titular, dni, operador_donante, icc)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (pre_id, tipo, tipo_port, num, titular, dni, op, icc),
                )

            # Generar token
            token_valido = False
            intentos = 0
            while not token_valido and intentos < 5:
                token = secrets.token_urlsafe(16)
                cursor.execute("SELECT id FROM precontrato_links WHERE token = ?", (token,))
                if cursor.fetchone() is None:
                    token_valido = True
                intentos += 1

            if not token_valido:
                st.error("❌ No se pudo generar un token único.")
            else:
                expiracion = datetime.now() + timedelta(hours=24)
                cursor.execute(
                    "INSERT INTO precontrato_links (precontrato_id, token, expiracion, usado) VALUES (?, ?, ?, 0)",
                    (pre_id, token, expiracion),
                )
                conn.commit()
                conn.close()

                base_url = "https://verde-suite.verdesuite.sytes.net/"
                link = f"{base_url}?precontrato_id={pre_id}&token={urllib.parse.quote(token)}"
                st.success("✅ Precontrato guardado correctamente.")
                st.markdown(f"📎 **Enlace para el cliente (válido 24 h):**")
                st.code(link, language="text")
                st.info("💡 Copia este enlace y envíalo al cliente.")
            conn.close()


def _listado_precontratos():
    """Muestra la lista de precontratos existentes."""
    st.subheader("Precontratos Existentes")
    conn = get_db_connection()
    cursor = conn.cursor()

    # Consulta que devuelve UNA fila por precontrato, con indicador de si tiene algún enlace usado
    cursor.execute("""
        SELECT 
            p.id, 
            p.precontrato_id, 
            p.apartment_id, 
            p.nombre, 
            p.tarifas, 
            p.precio,
            p.fecha, 
            p.comercial,
            -- TRUE si existe al menos un enlace usado para este precontrato
            EXISTS(SELECT 1 FROM precontrato_links WHERE precontrato_id = p.id AND usado = 1) as usado,
            p.mail, 
            p.permanencia, 
            p.telefono1, 
            p.telefono2
        FROM precontratos p
        ORDER BY p.fecha DESC
        LIMIT 50
    """)

    precontratos = cursor.fetchall()
    conn.close()

    if not precontratos:
        st.info("📝 No hay precontratos registrados aún.")
        return

    st.write(f"**Últimos {len(precontratos)} precontratos:**")
    for pre in precontratos:
        with st.expander(f"📄 {pre[1]} - {pre[3] or 'Sin nombre'} - {pre[4]}", expanded=False):
            col1, col2, col3 = st.columns(3)
            with col1:
                st.write(f"**ID:** {pre[1]}")
                st.write(f"**Apartment ID:** {pre[2] or 'No asignado'}")
                st.write(f"**Tarifa:** {pre[4]}")
                st.write(f"**Precio:** {pre[5]}€")
            with col2:
                st.write(f"**Fecha:** {pre[6]}")
                st.write(f"**Comercial:** {pre[7]}")
                st.write(f"**Permanencia:** {pre[10] or 'No especificada'}")
            with col3:
                estado = "✅ Usado" if pre[8] else "🟢 Activo"
                st.write(f"**Estado:** {estado}")
                st.write(f"**Email:** {pre[9] or 'No especificado'}")
                st.write(f"**Teléfono 1:** {pre[11] or 'No especificado'}")
                if pre[12]:
                    st.write(f"**Teléfono 2:** {pre[12]}")

            # Botón para regenerar enlace (clave única = id del precontrato)
            if st.button(f"🔄 Generar/Regenerar enlace para {pre[1]}", key=f"regen_{pre[0]}"):
                try:
                    conn = get_db_connection()
                    cursor = conn.cursor()

                    # Generar token único
                    token_valido = False
                    intentos = 0
                    while not token_valido and intentos < 5:
                        token = secrets.token_urlsafe(16)
                        cursor.execute("SELECT id FROM precontrato_links WHERE token = ?", (token,))
                        if cursor.fetchone() is None:
                            token_valido = True
                        intentos += 1

                    if token_valido:
                        expiracion = datetime.now() + timedelta(hours=24)
                        # Insertar nuevo enlace (NO reemplazar, porque queremos mantener el histórico)
                        cursor.execute(
                            """
                            INSERT INTO precontrato_links (precontrato_id, token, expiracion, usado)
                            VALUES (?, ?, ?, 0)
                            """,
                            (pre[0], token, expiracion),
                        )
                        conn.commit()
                        conn.close()

                        base_url = "https://verde-suite.verdesuite.sytes.net/"
                        link = f"{base_url}?precontrato_id={pre[0]}&token={urllib.parse.quote(token)}"
                        st.success("✅ Nuevo enlace generado correctamente.")
                        st.code(link, language="text")
                    else:
                        st.error("❌ No se pudo generar un token único.")

                except Exception as e:
                    st.error(f"❌ Error al generar enlace: {e}")


# ==================== FUNCIÓN PRINCIPAL ====================
def comercial_dashboard_vip():
    """Dashboard principal del comercial VIP."""
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
                <div style="margin-top:10px; font-weight:bold;">Rol: Comercial VIP</div>
                <div style="font-weight:bold; font-size:18px;">Bienvenido, {st.session_state.get('username', '')}</div>
                <hr>
            </div>
            """,
            unsafe_allow_html=True,
        )

        menu_opcion = option_menu(
            menu_title=None,
            options=["Ofertas Comerciales", "Viabilidades", "Visualización de Datos", "Precontratos"],
            icons=["bar-chart", "check-circle", "graph-up", "file-text"],
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

    if "username" not in st.session_state or not st.session_state["username"]:
        st.warning("⚠️ No has iniciado sesión.")
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

    if menu_opcion == "Ofertas Comerciales":
        _mostrar_ofertas_vip()
    elif menu_opcion == "Viabilidades":
        _mostrar_viabilidades()
    elif menu_opcion == "Visualización de Datos":
        _mostrar_visualizacion_datos()
    elif menu_opcion == "Precontratos":
        _mostrar_precontratos()


if __name__ == "__main__":
    comercial_dashboard_vip()