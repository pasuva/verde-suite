# demo_dashboard.py
import contextlib
import hashlib
import time
from functools import lru_cache
from typing import List, Tuple, Optional, Dict, Any

import folium
import pandas as pd
import sqlitecloud
import streamlit as st
from branca.element import MacroElement, Template
from folium.plugins import MarkerCluster, Geocoder, Draw
from streamlit_cookies_controller import CookieController
from streamlit_folium import st_folium

from modules import login

# Constantes
COOKIE_NAME = "my_app"
DB_CONNECTION_STRING = "sqlitecloud://ceafu04onz.g6.sqlite.cloud:8860/usuarios.db?apikey=Qo9m18B9ONpfEGYngUKm99QB5bgzUTGtK7iAcThmwvY"
ALLOWED_OLT_TYPES = ["CTO VERDE", "CTO COMPARTIDA"]


# ==================== CONEXIÓN A BASE DE DATOS ====================
def get_db_connection():
    """Obtiene conexión a la base de datos SQLite Cloud."""
    return sqlitecloud.connect(DB_CONNECTION_STRING)


@lru_cache(maxsize=32)
def cached_db_query(query: str, *params) -> pd.DataFrame:
    """Ejecuta consultas con caché en memoria para mejorar rendimiento."""
    with contextlib.closing(get_db_connection()) as conn:
        return pd.read_sql(query, conn, params=params)


# ==================== CONFIGURACIÓN DE PÁGINA ====================
def setup_page():
    """Configuración inicial de la página."""
    st.set_page_config(page_title="Dashboard Demo - Verde tu Operador", layout="wide")
    st.markdown("""
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
        .stApp { background-color: #f0f2f6; }
        div.stSpinner > div { opacity: 0; transition: opacity 0.3s ease-in-out; }
        div.stSpinner[data-testid="stSpinner"] > div { opacity: 1; }
        .element-container { contain: layout style paint; }
        </style>
        <div class="footer">
            <p>© 2025 Verde tu operador · Desarrollado para uso interno</p>
        </div>
    """, unsafe_allow_html=True)


# ==================== AUTENTICACIÓN Y SESIÓN ====================
def logout_user():
    """Cierra sesión del usuario."""
    controller = CookieController(key="cookies")
    for cookie in [f'{COOKIE_NAME}_session_id', f'{COOKIE_NAME}_username', f'{COOKIE_NAME}_role']:
        if controller.get(cookie):
            controller.set(cookie, '', max_age=0, path='/')

    st.session_state.update({
        "login_ok": False,
        "username": "",
        "role": "",
        "session_id": ""
    })
    st.toast("✅ Has cerrado sesión correctamente.")
    st.rerun()


def check_authentication() -> bool:
    """Verifica si el usuario está autenticado y tiene rol demo."""
    if "username" not in st.session_state or not st.session_state["username"]:
        st.warning("⚠️ No has iniciado sesión. Por favor, inicia sesión para continuar.")
        time.sleep(1.5)
        try:
            login.login()
        except Exception:
            pass
        return False

    if st.session_state.get("role") != "demo":
        st.error("❌ No tienes permisos para acceder al dashboard de demostración.")
        st.info("🔐 Esta área es solo para usuarios con rol 'demo'")
        return False

    return True


def create_user_sidebar():
    """Crea la barra lateral con información del usuario."""
    with st.sidebar:
        st.markdown(f"""
            <style>
                .user-circle {{
                    width: 100px; height: 100px; border-radius: 50%; background-color: #4CAF50;
                    color: white; font-size: 50px; display: flex; align-items: center;
                    justify-content: center; margin: 0 auto 10px auto; text-align: center;
                }}
                .user-info {{ text-align: center; font-size: 16px; color: #333; margin-bottom: 10px; }}
                .welcome-msg {{ text-align: center; font-weight: bold; font-size: 18px; margin-top: 0; }}
            </style>
            <div class="user-circle">👁️</div>
            <div class="user-info">Rol: Demo</div>
            <div class="welcome-msg">Bienvenido, <strong>{st.session_state.get('username', 'N/A')}</strong></div>
            <hr>
        """, unsafe_allow_html=True)

        if st.button("🚪 Cerrar sesión"):
            logout_user()


# ==================== CARGA DE FILTROS ====================
def load_filter_options() -> Tuple[List[str], List[str]]:
    """Carga provincias y tipos OLT permitidos."""
    try:
        provincias = cached_db_query(
            "SELECT DISTINCT provincia FROM datos_uis WHERE provincia IS NOT NULL ORDER BY provincia"
        )["provincia"].tolist()

        tipos_olt = cached_db_query(
            "SELECT DISTINCT tipo_olt_rental FROM datos_uis WHERE tipo_olt_rental IS NOT NULL ORDER BY tipo_olt_rental"
        )["tipo_olt_rental"].tolist()

        tipos_olt = [t for t in tipos_olt if t in ALLOWED_OLT_TYPES]
        return provincias, tipos_olt
    except Exception as e:
        st.error(f"❌ Error al cargar opciones de filtro: {e}")
        return [], []


def load_ctos(provincia_sel: str, municipio_sel: str, poblacion_sel: str) -> List[str]:
    """Carga las CTOs según los filtros seleccionados."""
    query = "SELECT DISTINCT cto FROM datos_uis WHERE cto IS NOT NULL AND cto != ''"
    params = []
    conditions = []

    if provincia_sel != "Todas":
        conditions.append("provincia = ?")
        params.append(provincia_sel)
    if municipio_sel != "Todos":
        conditions.append("municipio = ?")
        params.append(municipio_sel)
    if poblacion_sel != "Todas":
        conditions.append("poblacion = ?")
        params.append(poblacion_sel)

    if conditions:
        query += " AND " + " AND ".join(conditions)
    query += " ORDER BY cto"

    try:
        return cached_db_query(query, *params)["cto"].tolist()
    except Exception:
        return cached_db_query(
            "SELECT DISTINCT cto FROM datos_uis WHERE cto IS NOT NULL AND cto != '' ORDER BY cto"
        )["cto"].tolist()


def create_dependent_filters(provincia_sel: str) -> Tuple[str, str, str]:
    """Crea filtros dependientes (municipio, población, CTO)."""
    municipio_sel = poblacion_sel = cto_filter = "Todas"

    if provincia_sel != "Todas":
        municipios = cached_db_query(
            "SELECT DISTINCT municipio FROM datos_uis WHERE provincia = ? AND municipio IS NOT NULL ORDER BY municipio",
            provincia_sel
        )["municipio"].tolist()
        municipio_sel = st.selectbox("🏘️ Municipio", ["Todos"] + municipios, key="demo_municipio")

        if municipio_sel != "Todos":
            poblaciones = cached_db_query(
                "SELECT DISTINCT poblacion FROM datos_uis WHERE provincia = ? AND municipio = ? AND poblacion IS NOT NULL ORDER BY poblacion",
                provincia_sel, municipio_sel
            )["poblacion"].tolist()
            poblacion_sel = st.selectbox("🏡 Población", ["Todas"] + poblaciones, key="demo_poblacion")

    ctos = load_ctos(provincia_sel, municipio_sel, poblacion_sel)
    cto_filter = st.selectbox("📡 CTO", ["Todas"] + ctos, key="demo_cto")

    return municipio_sel, poblacion_sel, cto_filter


def create_area_filter():
    """Crea el filtro por área geográfica en el mapa."""
    st.markdown("---")
    st.subheader("🗺️ Filtro por Área")
    st.info("💡 **Filtro independiente:** Este filtro funciona por separado de los filtros de campos anteriores")

    if "drawn_bounds" not in st.session_state:
        st.session_state.update({
            "drawn_bounds": None,
            "apply_area_filter": False,
            "area_filtered_df": None
        })

    if st.session_state.drawn_bounds:
        b = st.session_state.drawn_bounds
        st.info(f"📍 Área seleccionada: Lat {b['south']:.4f} a {b['north']:.4f}, Lon {b['west']:.4f} a {b['east']:.4f}")

    area_tipo_olt_filter = st.selectbox(
        "🏢 Tipo OLT en el Área",
        ["Todos", "CTO VERDE", "CTO COMPARTIDA"],
        key="area_tipo_olt"
    )

    col1, col2 = st.columns(2)
    with col1:
        if st.button("📍 Cargar datos del área", type="primary", use_container_width=True):
            load_area_data(area_tipo_olt_filter)
    with col2:
        if st.button("🗑️ Limpiar filtro de área", use_container_width=True):
            st.session_state.update({
                "apply_area_filter": False,
                "drawn_bounds": None,
                "area_filtered_df": None
            })
            st.rerun()


def load_area_data(area_tipo_olt_filter: str):
    """Carga datos del área seleccionada en el mapa."""
    if not st.session_state.drawn_bounds:
        st.warning("⚠️ Primero debes dibujar un área en el mapa")
        return

    with st.spinner("⏳ Cargando datos del área..."):
        try:
            b = st.session_state.drawn_bounds
            query = """
                SELECT apartment_id, provincia, municipio, poblacion, vial, numero, letra, cp,
                       latitud, longitud, olt, cto, cto_id, tipo_olt_rental
                FROM datos_uis 
                WHERE latitud BETWEEN ? AND ? AND longitud BETWEEN ? AND ?
            """
            params = [b['south'], b['north'], b['west'], b['east']]

            if area_tipo_olt_filter != "Todos":
                query += " AND tipo_olt_rental = ?"
                params.append(area_tipo_olt_filter)
            else:
                query += " AND tipo_olt_rental IN ('CTO VERDE', 'CTO COMPARTIDA')"

            area_df = cached_db_query(query, *params)

            if area_df.empty:
                st.warning("⚠️ No hay datos en el área seleccionada.")
                st.session_state.area_filtered_df = None
            else:
                st.session_state.update({
                    "area_filtered_df": area_df,
                    "demo_filtered_df": None
                })
                st.success(f"✅ Se cargaron {len(area_df)} puntos del área seleccionada")

        except Exception as e:
            st.error(f"❌ Error al cargar datos del área: {e}")


def apply_field_filters(provincia_sel: str, municipio_sel: str, poblacion_sel: str,
                        cto_filter: str, tipo_olt_filter: str):
    """Aplica los filtros de campos y guarda el DataFrame resultante en session_state."""
    with st.spinner("⏳ Cargando datos filtrados..."):
        try:
            query = """
                SELECT apartment_id, provincia, municipio, poblacion, vial, numero, letra, cp,
                       latitud, longitud, olt, cto, cto_id, tipo_olt_rental
                FROM datos_uis 
                WHERE 1=1
            """
            params = []

            if provincia_sel != "Todas":
                query += " AND provincia = ?"
                params.append(provincia_sel)
            if municipio_sel != "Todos":
                query += " AND municipio = ?"
                params.append(municipio_sel)
            if poblacion_sel != "Todas":
                query += " AND poblacion = ?"
                params.append(poblacion_sel)
            if cto_filter != "Todas":
                query += " AND cto = ?"
                params.append(cto_filter)
            if tipo_olt_filter != "Todos":
                query += " AND tipo_olt_rental = ?"
                params.append(tipo_olt_filter)

            df = cached_db_query(query, *params)

            if df.empty:
                st.warning("⚠️ No hay datos para los filtros seleccionados.")
                st.session_state.update({"demo_filtered_df": None, "area_filtered_df": None})
            else:
                st.session_state.update({"demo_filtered_df": df, "area_filtered_df": None})
                st.success(f"✅ Se cargaron {len(df)} puntos en el mapa")

        except Exception as e:
            st.error(f"❌ Error al cargar los datos: {e}")


def create_filters(provincias: List[str], tipos_olt: List[str]) -> Tuple:
    """Crea todos los controles de filtro en la barra lateral."""
    with st.sidebar:
        st.header("🔍 Filtros de Visualización")

        with st.expander("ℹ️ Información del Modo Demo", expanded=False):
            st.markdown("""
                **💡 Modo Demostración**
                Este dashboard es solo para visualización.

                **Características disponibles:**
                - Visualización de puntos en mapa
                - Filtrado por ubicación geográfica
                - Filtrado por CTO y tipo OLT
                - Selección de área en el mapa
                - Descarga de datos en CSV
                - Estadísticas básicas
            """)

        provincia_sel = st.selectbox("🌍 Provincia", ["Todas"] + provincias, key="demo_provincia")
        municipio_sel, poblacion_sel, cto_filter = create_dependent_filters(provincia_sel)

        tipo_olt_filter = st.selectbox("🏢 Tipo OLT Rental", ["Todos"] + tipos_olt, key="demo_tipo_olt")

        col1, col2 = st.columns(2)
        with col1:
            aplicar_filtros = st.button("🔍 Aplicar Filtros", type="primary", use_container_width=True)
        with col2:
            limpiar_filtros = st.button("🧹 Limpiar", use_container_width=True)

        create_area_filter()

        return provincia_sel, municipio_sel, poblacion_sel, cto_filter, tipo_olt_filter, aplicar_filtros, limpiar_filtros


# ==================== FUNCIONES DEL MAPA ====================
def create_complete_popup(row: pd.Series) -> str:
    """Crea el HTML del popup con toda la información del punto."""
    return f"""
    <div style="font-family: Arial; font-size: 12px; min-width: 280px;">
        <div style="background-color: #f0f2f6; padding: 8px; border-radius: 5px; margin-bottom: 8px;">
            <strong>🏢 ID:</strong> {row['apartment_id']}<br>
        </div>
        <div style="margin-bottom: 8px;">
            <strong>📍 Ubicación:</strong><br>
            {row['provincia']}, {row['municipio']}<br>
            {row['vial']} {row['numero']}{row['letra'] or ''}<br>
            CP: {row['cp']}<br>
            📍 {row['latitud']:.6f}, {row['longitud']:.6f}
        </div>
        <div style="background-color: #e8f4fd; padding: 8px; border-radius: 5px;">
            <strong>🔧 Infraestructura:</strong><br>
            🏢 OLT: {row.get('olt', 'N/D')}<br>
            📡 CTO: {row.get('cto', 'N/D')}<br>
            🔢 CTO ID: {row.get('cto_id', 'N/D')}<br>
            🏭 Tipo OLT: {row.get('tipo_olt_rental', 'N/D')}
        </div>
    </div>
    """


def get_marker_color(tipo_olt: str) -> str:
    """Determina el color del marcador según el tipo de OLT."""
    tipo = str(tipo_olt).strip()
    return 'darkgreen' if tipo == "CTO VERDE" else 'purple' if tipo == "CTO COMPARTIDA" else 'gray'


def add_legend(m: folium.Map):
    """Añade la leyenda de colores al mapa."""
    legend = """
        {% macro html(this, kwargs) %}
        <div style="position: fixed; bottom: 50px; left: 50px; width: 180px; z-index:9999; font-size:14px;
                    background-color: white; border:2px solid grey; border-radius:8px; padding: 10px;
                    box-shadow: 2px 2px 6px rgba(0,0,0,0.3);">
        <b>🎨 Leyenda de Colores</b><br>
        <i style="color:darkgreen;">●</i> CTO VERDE<br>
        <i style="color:purple;">●</i> CTO COMPARTIDA<br>
        </div>
        {% endmacro %}
    """
    macro = MacroElement()
    macro._template = Template(legend)
    m.get_root().add_child(macro)


def add_map_controls(m: folium.Map):
    """Añade controles de geocodificador y dibujo al mapa."""
    Geocoder().add_to(m)
    draw_options = {
        'rectangle': {'shapeOptions': {'color': '#3388ff', 'fillColor': '#3388ff', 'fillOpacity': 0.2}},
        'polygon': {'shapeOptions': {'color': '#3388ff', 'fillColor': '#3388ff', 'fillOpacity': 0.2}},
        'circle': False, 'marker': False, 'circlemarker': False, 'polyline': False
    }
    Draw(export=False, position="topleft", draw_options=draw_options).add_to(m)


def create_marker(layer, row: pd.Series, lat_offset: float, lon_offset: float):
    """Crea un marcador individual con popup completo y lo añade a la capa."""
    color = get_marker_color(row.get("tipo_olt_rental", ""))
    folium.Marker(
        location=[row['latitud'] + lat_offset, row['longitud'] + lon_offset],
        popup=folium.Popup(create_complete_popup(row), max_width=300),
        tooltip=f"🏢 {row['apartment_id']} - {row['vial']} {row['numero']}",
        icon=folium.Icon(color=color, icon='info-sign')
    ).add_to(layer)


def create_empty_map() -> folium.Map:
    """Crea un mapa vacío con controles básicos."""
    m = folium.Map(
        location=[40.4168, -3.7038],
        zoom_start=6,
        max_zoom=21,
        tiles="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}",
        attr="Google",
        prefer_canvas=True
    )
    add_map_controls(m)
    return m


def add_high_performance_markers(m: folium.Map, df_display: pd.DataFrame):
    """Añade marcadores optimizados para grandes volúmenes de puntos usando un cluster."""
    marker_cluster = MarkerCluster(
        name="Puntos",
        overlay=True,
        control=True,
        maxClusterRadius=15,
        minClusterSize=3,
        disableClusteringAtZoom=18,
        spiderfyOnMaxZoom=True,
        show_coverage_on_hover=False,
        zoom_to_bounds_on_click=True
    )
    for _, row in df_display.iterrows():
        color = get_marker_color(row.get("tipo_olt_rental", ""))
        folium.Marker(
            location=[row['latitud'], row['longitud']],
            popup=folium.Popup(create_complete_popup(row), max_width=300),
            tooltip=f"🏢 {row['apartment_id']}",
            icon=folium.Icon(color=color, icon='info-sign')
        ).add_to(marker_cluster)
    marker_cluster.add_to(m)


def create_map(df_display: pd.DataFrame) -> folium.Map:
    """Crea y configura el mapa según la cantidad de puntos."""
    if df_display.empty:
        return create_empty_map()

    # Para muchos puntos, usar configuración optimizada
    if len(df_display) > 1000:
        lat, lon = df_display['latitud'].mean(), df_display['longitud'].mean()
        m = folium.Map(
            location=[lat, lon],
            zoom_start=10,
            max_zoom=19,
            tiles="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}",
            attr="Google",
            prefer_canvas=True,
            control_scale=True
        )
        bounds = [[df_display['latitud'].min(), df_display['longitud'].min()],
                  [df_display['latitud'].max(), df_display['longitud'].max()]]
        m.fit_bounds(bounds, padding=(10, 10))
        add_map_controls(m)
        add_high_performance_markers(m, df_display)
        add_legend(m)
        return m

    # Para pocos puntos, mapa normal con cluster si procede
    if len(df_display) == 1:
        lat, lon = df_display.iloc[0]['latitud'], df_display.iloc[0]['longitud']
        m = folium.Map(location=[lat, lon], zoom_start=18, max_zoom=21,
                       tiles="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}",
                       attr="Google", prefer_canvas=True)
    else:
        lat, lon = df_display['latitud'].mean(), df_display['longitud'].mean()
        m = folium.Map(location=[lat, lon], zoom_start=12, max_zoom=21,
                       tiles="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}",
                       attr="Google", prefer_canvas=True)
        bounds = [[df_display['latitud'].min(), df_display['longitud'].min()],
                  [df_display['latitud'].max(), df_display['longitud'].max()]]
        m.fit_bounds(bounds)

    add_map_controls(m)

    # Elegir tipo de agrupación según cantidad
    if len(df_display) < 100:
        for _, row in df_display.iterrows():
            create_marker(m, row, 0, 0)
    elif len(df_display) < 1000:
        cluster = MarkerCluster(
            name="Puntos", overlay=True, control=True,
            maxClusterRadius=10, minClusterSize=2, spiderfyOnMaxZoom=True
        )
        for _, row in df_display.iterrows():
            create_marker(cluster, row, 0, 0)
        cluster.add_to(m)
    else:
        add_high_performance_markers(m, df_display)

    add_legend(m)
    return m


def get_map_config_hash(df_display: Optional[pd.DataFrame]) -> str:
    """Genera un hash único para la configuración del mapa basado en los datos."""
    if df_display is None or df_display.empty:
        return "empty_map"
    coords_hash = hashlib.md5(
        pd.util.hash_pandas_object(df_display[['latitud', 'longitud']].dropna()).values.tobytes()
    ).hexdigest()[:16]
    return f"map_{len(df_display)}_{coords_hash}"


def process_drawn_area(map_data: Dict[str, Any]):
    """Procesa el área dibujada en el mapa y actualiza session_state."""
    if not (map_data and map_data.get("last_active_drawing") and map_data["last_active_drawing"].get("geometry")):
        return

    geom = map_data["last_active_drawing"]["geometry"]
    if geom["type"] not in ("Polygon", "Rectangle"):
        return

    coords = geom["coordinates"][0]  # Asumimos que es un polígono o rectángulo
    lats = [c[1] for c in coords]
    lons = [c[0] for c in coords]

    st.session_state.drawn_bounds = {
        'north': max(lats), 'south': min(lats),
        'east': max(lons), 'west': min(lons)
    }
    st.toast(f"📍 Área seleccionada: Lat {min(lats):.4f} a {max(lats):.4f}, Lon {min(lons):.4f} a {max(lons):.4f}")


def display_data_table(df_display: pd.DataFrame):
    """Muestra la tabla de datos y ofrece descarga CSV."""
    st.subheader("📋 Datos Detallados")
    columnas_mostrar = [
        'apartment_id', 'provincia', 'municipio', 'poblacion', 'vial', 'numero', 'letra', 'cp',
        'olt', 'cto', 'cto_id', 'tipo_olt_rental', 'latitud', 'longitud'
    ]
    df_table = df_display[columnas_mostrar].copy()

    if len(df_table) > 500:
        st.info(f"📊 Mostrando {len(df_table)} registros. Use la descarga CSV para ver todos los datos.")
    st.dataframe(df_table, use_container_width=True)

    csv = df_table.to_csv(index=False)
    st.download_button(
        label="📥 Descargar datos como CSV",
        data=csv,
        file_name=f"datos_demo_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
    )


def get_data_to_display() -> Optional[pd.DataFrame]:
    """Determina qué conjunto de datos mostrar (filtros de campo o área)."""
    if st.session_state.get("area_filtered_df") is not None:
        df = st.session_state.area_filtered_df
        st.info(f"📊 **Visualizando:** {len(df)} puntos filtrados por ÁREA GEOGRÁFICA")
        return df
    if st.session_state.get("demo_filtered_df") is not None:
        df = st.session_state.demo_filtered_df
        st.info(f"📊 **Visualizando:** {len(df)} puntos filtrados por CAMPOS")
        return df
    st.info("👆 **Selecciona un método de filtrado:** Usa los filtros de campos o dibuja un área en el mapa")
    return None


def initialize_session_state():
    """Inicializa variables de sesión relacionadas con el mapa."""
    if "map_initialized" not in st.session_state:
        st.session_state.map_initialized = False
    if "current_map_data" not in st.session_state:
        st.session_state.current_map_data = None
    if "map_hash" not in st.session_state:
        st.session_state.map_hash = None


def _handle_filter_events(aplicar: bool, limpiar: bool, provincia_sel, municipio_sel,
                          poblacion_sel, cto_filter, tipo_olt_filter):
    """Maneja los eventos de aplicar y limpiar filtros."""
    if aplicar:
        apply_field_filters(provincia_sel, municipio_sel, poblacion_sel, cto_filter, tipo_olt_filter)
    if limpiar:
        for key in ["demo_filtered_df", "area_filtered_df"]:
            st.session_state.pop(key, None)
        st.session_state.update({"apply_area_filter": False, "drawn_bounds": None})
        st.rerun()


def _render_map_and_table(df_to_show: Optional[pd.DataFrame]):
    """Renderiza el mapa y la tabla de datos en el contenedor principal."""
    map_container = st.container()
    with map_container:
        if df_to_show is not None:
            current_hash = get_map_config_hash(df_to_show)

            # Solo regenerar el mapa si los datos han cambiado
            if (not st.session_state.map_initialized or
                    st.session_state.map_hash != current_hash or
                    st.session_state.current_map_data is None or
                    len(st.session_state.current_map_data) != len(df_to_show)):

                if len(df_to_show) > 500:
                    progress = st.progress(0, text=f"Renderizando {len(df_to_show)} puntos...")
                    m = create_map(df_to_show)
                    progress.progress(50, text="Mapa creado, cargando interfaz...")
                    map_data = st_folium(
                        m, height=700, width="100%",
                        key=f"demo_map_{current_hash}",
                        returned_objects=["last_active_drawing", "bounds"]
                    )
                    progress.progress(100, text="¡Mapa cargado!")
                    time.sleep(0.5)
                    progress.empty()
                else:
                    m = create_map(df_to_show)
                    map_data = st_folium(
                        m, height=700, width="100%",
                        key=f"demo_map_{current_hash}",
                        returned_objects=["last_active_drawing", "bounds"]
                    )

                st.session_state.update({
                    "map_initialized": True,
                    "current_map_data": df_to_show.copy(),
                    "map_hash": current_hash
                })
            else:
                # Reutilizar el mapa existente
                m = create_map(df_to_show)
                map_data = st_folium(
                    m, height=700, width="100%",
                    key=f"demo_map_{current_hash}",
                    returned_objects=["last_active_drawing", "bounds"]
                )

            process_drawn_area(map_data)
            display_data_table(df_to_show)
        else:
            m = create_empty_map()
            map_data = st_folium(
                m, height=500, width="100%",
                key="demo_map_empty",
                returned_objects=["last_active_drawing", "bounds"]
            )
            process_drawn_area(map_data)


# ==================== FUNCIÓN PRINCIPAL ====================
def demo_dashboard():
    """Dashboard de demostración para visualización de puntos en mapa."""
    setup_page()
    initialize_session_state()

    if not check_authentication():
        return

    create_user_sidebar()

    provincias, tipos_olt = load_filter_options()
    if not provincias:
        return

    provincia_sel, municipio_sel, poblacion_sel, cto_filter, tipo_olt_filter, aplicar, limpiar = create_filters(
        provincias, tipos_olt
    )

    _handle_filter_events(aplicar, limpiar, provincia_sel, municipio_sel, poblacion_sel, cto_filter, tipo_olt_filter)

    df_to_show = get_data_to_display()
    _render_map_and_table(df_to_show)


if __name__ == "__main__":
    demo_dashboard()