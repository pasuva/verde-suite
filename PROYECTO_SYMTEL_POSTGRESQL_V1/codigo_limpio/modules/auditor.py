# auditor.py
# Módulo para auditoría de facturación comparando contratos internos con ficheros de Adamo, Likes y Bayma.
# Adamo: comparación simple por billing.
# Likes y Bayma: comparación por nombre del cliente con limpieza y matching difuso.

import streamlit as st
import pandas as pd
import io
import sqlite3
import sqlitecloud
import unicodedata
import re
import difflib
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from streamlit_cookies_controller import CookieController
import warnings

cookie_name = "my_app"

# -------------------------------------------------------------------
# Funciones de conexión y trazabilidad
# -------------------------------------------------------------------
def obtener_conexion():
    """Retorna una nueva conexión a la base de datos SQLite Cloud."""
    try:
        conn = sqlitecloud.connect(
            "sqlitecloud://ceafu04onz.g6.sqlite.cloud:8860/usuarios.db?apikey=Qo9m18B9ONpfEGYngUKm99QB5bgzUTGtK7iAcThmwvY"
        )
        return conn
    except sqlite3.Error as e:
        print(f"Error al conectar con la base de datos: {e}")
        return None

def log_trazabilidad(usuario, accion, detalles):
    """Inserta un registro en la tabla de trazabilidad (opcional)."""
    try:
        conn = obtener_conexion()
        cursor = conn.cursor()
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("""
            INSERT INTO trazabilidad (usuario_id, accion, detalles, fecha)
            VALUES (?, ?, ?, ?)
        """, (usuario, accion, detalles, fecha))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error registrando trazabilidad: {e}")

# -------------------------------------------------------------------
# Normalización de texto para comparación por nombre (para Likes y Bayma)
# -------------------------------------------------------------------
def normalizar_texto(texto):
    """Elimina tildes, pasa a minúsculas y elimina espacios extras."""
    if pd.isna(texto) or texto is None:
        return ""
    texto = str(texto)
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    texto = re.sub(r'\s+', ' ', texto.strip().lower())
    return texto

def limpiar_nombre_para_comparacion(nombre):
    """
    Limpia el nombre eliminando números, signos de puntuación y palabras vacías comunes.
    """
    if pd.isna(nombre) or nombre is None:
        return ""
    nombre = str(nombre)
    nombre = re.sub(r'\d+', '', nombre)
    nombre = re.sub(r'[.,;:\-\(\)]', ' ', nombre)
    stopwords = [
        'SEDE', 'CENTRO', 'BAR', 'CAFE', 'RESTAURANTE', 'LOCAL', 'NAVE', 'TALLER',
        'OFICINA', 'DESPACHO', 'ALMACEN', 'GARAJE', 'APARTAMENTO', 'VIVIENDA',
        'CASA', 'CHALET', 'ADOSADO', 'PAREADO', 'UNIFAMILIAR', 'BLOQUE', 'PORTAL',
        'ESCALERA', 'BAJO', 'ENTREPLANTA', 'ATICO', 'DUPLEX', 'ESTUDIO',
        'POLIGONO', 'PARQUE', 'MERCADO', 'GALERIA', 'PASEO', 'AVENIDA', 'CALLE',
        'PLAZA', 'CARRETERA', 'CAMINO', 'URBANIZACION', 'RESIDENCIAL', 'BARRIO',
        'CONJUNTO', 'COMPLEJO', 'EDIFICIO', 'TORRE',
        'SL', 'SLU', 'SC', 'SA', 'SLL', 'L', 'S', 'C', 'A', 'U',
        'NAN'
    ]
    for palabra in stopwords:
        nombre = re.sub(r'\b' + re.escape(palabra) + r'\b', '', nombre, flags=re.IGNORECASE)
    nombre = re.sub(r'\s+', ' ', nombre).strip()
    return nombre

# -------------------------------------------------------------------
# Carga de datos desde la base de datos (seguimiento_contratos)
# -------------------------------------------------------------------
@st.cache_data(ttl=600, show_spinner="Cargando contratos desde la BD...")
def cargar_contratos_bd() -> pd.DataFrame:
    """Carga todos los registros de la tabla seguimiento_contratos."""
    conn = obtener_conexion()
    if conn is None:
        st.error("No se pudo conectar a la base de datos.")
        return pd.DataFrame()
    try:
        query = "SELECT * FROM seguimiento_contratos"
        df = pd.read_sql(query, conn)
        df.columns = [c.lower() for c in df.columns]
        return df
    except Exception as e:
        st.error(f"Error al cargar contratos: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

# -------------------------------------------------------------------
# Procesamiento de la comparación (versión simple que funcionaba para Adamo)
# -------------------------------------------------------------------
def procesar_comparacion_simple(df_bd: pd.DataFrame, df_partner: pd.DataFrame,
                                col_bd: str, col_partner: str):
    """
    Compara dos DataFrames usando las columnas indicadas.
    Devuelve tres DataFrames: coincidentes, solo_bd, solo_partner.
    """
    if df_bd.empty or df_partner.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if col_bd not in df_bd.columns:
        st.error(f"La columna '{col_bd}' no existe en los datos de la base de datos.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    if col_partner not in df_partner.columns:
        st.error(f"La columna '{col_partner}' no existe en el fichero subido.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df_bd = df_bd.copy()
    df_partner = df_partner.copy()
    df_bd['_key'] = df_bd[col_bd].astype(str).str.strip()
    df_partner['_key'] = df_partner[col_partner].astype(str).str.strip()

    merged = df_bd.merge(df_partner, left_on='_key', right_on='_key',
                         how='outer', indicator=True, suffixes=('_bd', '_partner'))

    coincidentes = merged[merged['_merge'] == 'both']
    solo_bd = merged[merged['_merge'] == 'left_only']
    solo_partner = merged[merged['_merge'] == 'right_only']

    for df_temp in [coincidentes, solo_bd, solo_partner]:
        if '_key' in df_temp.columns:
            df_temp.drop(columns=['_key'], inplace=True)

    return coincidentes, solo_bd, solo_partner

# -------------------------------------------------------------------
# Función auxiliar para mostrar tablas con AgGrid (reutilizable)
# -------------------------------------------------------------------
def mostrar_tabla_con_aggrid(df, key_suffix, columnas_extra=None):
    if df.empty:
        st.info("No hay registros en esta categoría.")
        return
    # Seleccionar todas las columnas excepto las internas (que empiezan con '_')
    cols_a_mostrar = [c for c in df.columns if not c.startswith('_')]
    # Priorizar columnas importantes
    columnas_importantes = ['billing', 'num_contrato', 'cliente', 'estado',
                            'fecha_inicio_contrato', 'comercial']
    if columnas_extra:
        columnas_importantes.extend(columnas_extra)
    cols_prioritarias = [c for c in cols_a_mostrar if any(imp in c for imp in columnas_importantes)]
    otras_cols = [c for c in cols_a_mostrar if c not in cols_prioritarias]
    cols_ordenadas = cols_prioritarias + otras_cols
    df_display = df[cols_ordenadas].copy()

    gb = GridOptionsBuilder.from_dataframe(df_display)
    gb.configure_default_column(
        filter=True,
        floatingFilter=True,
        sortable=True,
        resizable=True,
        minWidth=100,
        flex=1
    )
    gb.configure_pagination(paginationAutoPageSize=True)
    gridOptions = gb.build()

    AgGrid(
        df_display,
        gridOptions=gridOptions,
        enable_enterprise_modules=True,
        update_mode=GridUpdateMode.NO_UPDATE,
        height=400,
        theme='alpine-dark',
        key=f"grid_{key_suffix}"
    )

# -------------------------------------------------------------------
# Función principal de la sección de auditoría
# -------------------------------------------------------------------
def mostrar_auditoria():
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

    st.markdown("""
        <style>
        .block-container { padding-top: 1rem; }
        </style>
    """, unsafe_allow_html=True)

    sub_seccion = st.radio(
        "Selecciona una vista",
        ["Cargar fichero", "Informe comparativo"],
        horizontal=True,
        label_visibility="collapsed"
    )

    df_bd = cargar_contratos_bd()

    if df_bd.empty:
        st.warning("No se pudieron cargar contratos desde la base de datos.")
        return

    st.sidebar.markdown("### 📊 Datos internos")
    st.sidebar.info(f"Total contratos en BD: **{len(df_bd):,}**")
    if 'billing' in df_bd.columns:
        st.sidebar.info(f"Billing no nulos: **{df_bd['billing'].notna().sum():,}**")
    else:
        st.sidebar.warning("La columna 'billing' no existe en la BD.")

    # -------------------------------------------------------------------
    # Cargar fichero
    # -------------------------------------------------------------------
    if sub_seccion == "Cargar fichero":
        st.header("📁 Cargar fichero del partner")

        tipo_fichero = st.radio(
            "Selecciona el tipo de fichero:",
            ["Adamo", "Likes", "Bayma"],
            horizontal=True,
            key="tipo_carga"
        )

        if tipo_fichero == "Adamo":
            session_key_df = 'df_partner_adamo'
            session_key_filename = 'partner_filename_adamo'
            session_key_col = 'partner_id_col_adamo'
            texto_ayuda = "normalmente **Servicio Id**"
        elif tipo_fichero == "Likes":
            session_key_df = 'df_partner_likes'
            session_key_filename = 'partner_filename_likes'
            session_key_nombre = 'partner_nombre_cols_likes'
            texto_ayuda = "selecciona las columnas que forman el nombre del cliente"
        else:  # Bayma
            session_key_df = 'df_partner_bayma'
            session_key_filename = 'partner_filename_bayma'
            session_key_nombre = 'partner_nombre_cols_bayma'
            texto_ayuda = "selecciona las columnas que forman el nombre del cliente"

        st.markdown(f"Sube el archivo Excel o CSV de **{tipo_fichero}**. {texto_ayuda}.")

        uploaded_file = st.file_uploader(
            "Selecciona archivo",
            type=["xlsx", "xls", "csv"],
            key=f"uploader_{tipo_fichero}"
        )

        if uploaded_file is not None:
            try:
                if uploaded_file.name.endswith('.csv'):
                    df_partner = pd.read_csv(uploaded_file, sep=';')
                else:
                    df_partner = pd.read_excel(uploaded_file)

                st.success(f"Fichero cargado correctamente: {len(df_partner)} filas.")
                st.dataframe(df_partner.head(10), width='stretch')

                opciones = df_partner.columns.tolist()

                if tipo_fichero == "Adamo":
                    sugerida = None
                    for col in opciones:
                        if 'servicio id' in col.lower():
                            sugerida = col
                            break
                    indice_default = opciones.index(sugerida) if sugerida else 0
                    columna_id = st.selectbox(
                        "Selecciona la columna que contiene el identificador de alta (Servicio Id):",
                        options=opciones,
                        index=indice_default,
                        key=f"select_{tipo_fichero}"
                    )
                    st.session_state[session_key_df] = df_partner
                    st.session_state[session_key_filename] = uploaded_file.name
                    st.session_state[session_key_col] = columna_id

                else:  # Likes o Bayma
                    st.markdown("#### Configuración del nombre del cliente")
                    st.info("Selecciona las columnas que contienen el nombre y apellidos. Se concatenarán en el orden: Nombre, Primer apellido, Segundo apellido.")

                    # Intentar encontrar las columnas por defecto (para Likes se mantiene la misma lógica)
                    opciones_con_ninguno = ["(ninguno)"] + opciones
                    indice_nombre = 0
                    indice_apellido1 = 0
                    indice_apellido2 = 0

                    # Para Bayma no hay nombres predefinidos, se dejan en 0
                    if tipo_fichero == "Likes":
                        for i, col in enumerate(opciones):
                            col_lower = col.lower()
                            if col_lower == 'name':
                                indice_nombre = i
                            elif col_lower == 'firstsurname':
                                indice_apellido1 = i + 1
                            elif col_lower == 'lastsurname':
                                indice_apellido2 = i + 1

                    col_nombre = st.selectbox(
                        "Columna que contiene el nombre (obligatorio):",
                        options=opciones,
                        index=indice_nombre,
                        key=f"col_nombre_{tipo_fichero}"
                    )
                    col_apellido1 = st.selectbox(
                        "Columna que contiene el primer apellido (opcional):",
                        options=opciones_con_ninguno,
                        index=indice_apellido1,
                        key=f"col_apellido1_{tipo_fichero}"
                    )
                    col_apellido2 = st.selectbox(
                        "Columna que contiene el segundo apellido (opcional):",
                        options=opciones_con_ninguno,
                        index=indice_apellido2,
                        key=f"col_apellido2_{tipo_fichero}"
                    )

                    st.session_state[session_key_df] = df_partner
                    st.session_state[session_key_filename] = uploaded_file.name
                    st.session_state[session_key_nombre] = {
                        'nombre': col_nombre,
                        'apellido1': None if col_apellido1 == "(ninguno)" else col_apellido1,
                        'apellido2': None if col_apellido2 == "(ninguno)" else col_apellido2
                    }

                st.info(f"Fichero de {tipo_fichero} guardado. Ahora ve a la pestaña **Informe comparativo** y selecciona '{tipo_fichero}' para ver el análisis.")
            except Exception as e:
                st.error(f"Error al leer el archivo: {e}")

    # -------------------------------------------------------------------
    # Informe comparativo
    # -------------------------------------------------------------------
    else:
        st.header("📋 Informe comparativo")

        tipo_informe = st.radio(
            "Selecciona el informe a visualizar:",
            ["Adamo", "Likes", "Bayma"],
            horizontal=True,
            key="tipo_informe"
        )

        # =================================================================
        # CASO ADAMO: comparación simple por billing (código original)
        # =================================================================
        if tipo_informe == "Adamo":
            session_key_df = 'df_partner_adamo'
            session_key_filename = 'partner_filename_adamo'
            session_key_col = 'partner_id_col_adamo'
            col_bd = 'billing'

            if session_key_df not in st.session_state or st.session_state[session_key_df] is None:
                st.warning(f"Primero debes cargar un fichero de {tipo_informe} en la pestaña 'Cargar fichero'.")
                return
            df_partner = st.session_state[session_key_df]
            partner_filename = st.session_state.get(session_key_filename, f'fichero_{tipo_informe.lower()}')
            partner_id_col = st.session_state.get(session_key_col, None)
            if partner_id_col is None:
                st.warning(f"No se ha seleccionado la columna identificadora para {tipo_informe}. Ve a 'Cargar fichero' y selecciona una.")
                return

            with st.spinner(f"Comparando datos con {tipo_informe}..."):
                coincidentes, solo_bd, solo_partner = procesar_comparacion_simple(
                    df_bd, df_partner,
                    col_bd=col_bd,
                    col_partner=partner_id_col
                )

            # Para Adamo, el número de registros coincide con el de clientes (IDs únicos)
            num_coincidentes_unicos = len(coincidentes)
            num_solo_bd_unicos = len(solo_bd)
            num_solo_likes_unicos = len(solo_partner)

            # Separar coincidentes según estado
            estados_validos = ['FINALIZADO']
            if not coincidentes.empty and 'estado' in coincidentes.columns:
                coincidentes_problematicos = coincidentes[~coincidentes['estado'].isin(estados_validos)]
            else:
                coincidentes_problematicos = pd.DataFrame()

            # Mostrar alerta si hay problemáticos
            if not coincidentes_problematicos.empty:
                st.error(f"⚠️ **Atención:** Se han encontrado **{len(coincidentes_problematicos)}** contratos coincidentes con estado distinto de FINALIZADO. Revisa si se está cobrando indebidamente.")

            # Pestañas: Coincidentes totales, Problemáticos, Solo BD, Solo Partner
            tab_titles = [
                f"✅ Coincidentes totales ({len(coincidentes)})",
                f"⚠️ Coincidentes no finalizados ({len(coincidentes_problematicos)})",
                f"🔵 Solo en BD ({len(solo_bd)})",
                f"🟠 Solo en {tipo_informe} ({len(solo_partner)})"
            ]
            tabs = st.tabs(tab_titles)

            with tabs[0]:
                mostrar_tabla_con_aggrid(coincidentes, "adamo_coincidentes", [partner_id_col])
            with tabs[1]:
                if not coincidentes_problematicos.empty:
                    mostrar_tabla_con_aggrid(coincidentes_problematicos, "adamo_problematicos", [partner_id_col])
                    st.markdown("#### Distribución de estados problemáticos")
                    estado_counts = coincidentes_problematicos['estado'].value_counts().reset_index()
                    estado_counts.columns = ['Estado', 'Cantidad']
                    st.dataframe(estado_counts, width='stretch', hide_index=True)
                else:
                    st.success("¡No hay coincidentes con estados problemáticos!")
            with tabs[2]:
                mostrar_tabla_con_aggrid(solo_bd, "adamo_solo_bd", [partner_id_col])
            with tabs[3]:
                mostrar_tabla_con_aggrid(solo_partner, "adamo_solo_partner", [partner_id_col])

            # Análisis adicional de estados (expandible)
            if not coincidentes.empty and 'estado' in coincidentes.columns:
                with st.expander("🔍 Ver distribución completa de estados en coincidentes"):
                    estado_counts = coincidentes['estado'].value_counts().reset_index()
                    estado_counts.columns = ['Estado', 'Cantidad']
                    st.dataframe(estado_counts, width='stretch', hide_index=True)

            # Registrar en trazabilidad
            log_trazabilidad(
                st.session_state.get("username", "auditor"),
                f"Auditoría de facturación - {tipo_informe}",
                f"Comparación con fichero {partner_filename}. Coincidentes={len(coincidentes)}, Problemáticos={len(coincidentes_problematicos)}, Solo BD={len(solo_bd)}, Solo {tipo_informe}={len(solo_partner)}"
            )

        # =================================================================
        # CASO LIKES O BAYMA: comparación por nombre con limpieza y matching difuso
        # =================================================================
        else:  # Likes o Bayma
            if tipo_informe == "Likes":
                session_key_df = 'df_partner_likes'
                session_key_filename = 'partner_filename_likes'
                session_key_nombre = 'partner_nombre_cols_likes'
                partner_nombre_display = "Likes"
            else:  # Bayma
                session_key_df = 'df_partner_bayma'
                session_key_filename = 'partner_filename_bayma'
                session_key_nombre = 'partner_nombre_cols_bayma'
                partner_nombre_display = "Bayma"

            if session_key_df not in st.session_state or st.session_state[session_key_df] is None:
                st.warning(f"Primero debes cargar un fichero de {tipo_informe} en la pestaña 'Cargar fichero'.")
                return
            df_partner = st.session_state[session_key_df]
            partner_filename = st.session_state.get(session_key_filename, f'fichero_{tipo_informe.lower()}')
            config_nombre = st.session_state.get(session_key_nombre, None)
            if config_nombre is None:
                st.warning(f"No se ha configurado el nombre para {tipo_informe}. Ve a 'Cargar fichero' y selecciona las columnas.")
                return

            opciones_bd = df_bd.columns.tolist()
            indice_bd = opciones_bd.index('cliente') if 'cliente' in opciones_bd else 0
            col_bd_nombre = st.selectbox(
                "Selecciona la columna de la BD que contiene el nombre del cliente:",
                options=opciones_bd,
                index=indice_bd,
                key=f"col_bd_nombre_{tipo_informe}"
            )

            # Parámetros de matching difuso
            usar_match_aproximado = st.checkbox("Usar coincidencias aproximadas (umbral 0.8)", value=True, key=f"usar_match_{tipo_informe}")
            umbral_match = st.slider("Umbral de similitud", min_value=0.5, max_value=1.0, value=0.8, step=0.05, key=f"umbral_match_{tipo_informe}", disabled=not usar_match_aproximado)

            # Construir nombre original en partner
            def construir_nombre_fila(row):
                partes = []
                if config_nombre['nombre'] and config_nombre['nombre'] in row:
                    partes.append(str(row[config_nombre['nombre']]))
                if config_nombre['apellido1'] and config_nombre['apellido1'] in row:
                    partes.append(str(row[config_nombre['apellido1']]))
                if config_nombre['apellido2'] and config_nombre['apellido2'] in row:
                    partes.append(str(row[config_nombre['apellido2']]))
                return ' '.join(partes).strip()

            df_partner['_key_original'] = df_partner.apply(construir_nombre_fila, axis=1)
            df_partner['_key_limpio'] = df_partner['_key_original'].apply(limpiar_nombre_para_comparacion)
            df_partner['_key'] = df_partner['_key_limpio'].apply(normalizar_texto)

            df_bd['_key_original'] = df_bd[col_bd_nombre].astype(str)
            df_bd['_key_limpio'] = df_bd['_key_original'].apply(limpiar_nombre_para_comparacion)
            df_bd['_key'] = df_bd['_key_limpio'].apply(normalizar_texto)

            # Obtener nombres únicos y sus claves
            bd_unicos = df_bd[['_key', '_key_original']].drop_duplicates('_key').set_index('_key')['_key_original'].to_dict()
            partner_unicos = df_partner[['_key', '_key_original']].drop_duplicates('_key').set_index('_key')['_key_original'].to_dict()

            # Mapeos de clave a nombre original (para usar más tarde)
            partner_nombre_map = partner_unicos
            bd_nombre_map = bd_unicos
            bd_nombres_originales = list(bd_nombre_map.values())

            bd_keys_set = set(bd_unicos.keys())
            partner_keys_set = set(partner_unicos.keys())

            # Coincidencias exactas
            exactas_keys = bd_keys_set & partner_keys_set

            # Inicializar asignaciones
            bd_asignado = {k: False for k in bd_keys_set}
            partner_asignado = {k: False for k in partner_keys_set}
            match_dict = {}  # key_partner -> key_bd

            # Primero asignar las exactas
            for k in exactas_keys:
                match_dict[k] = k
                bd_asignado[k] = True
                partner_asignado[k] = True

            if usar_match_aproximado:
                # Para los partner no asignados, buscar el mejor match en BD no asignado con similitud >= umbral
                partner_no_asignados = [k for k in partner_keys_set if not partner_asignado[k]]
                bd_no_asignados = [k for k in bd_keys_set if not bd_asignado[k]]

                if partner_no_asignados and bd_no_asignados:
                    partner_nombres = [partner_unicos[k] for k in partner_no_asignados]
                    bd_nombres = [bd_unicos[k] for k in bd_no_asignados]

                    for i, partner_key in enumerate(partner_no_asignados):
                        partner_nombre = partner_nombres[i]
                        matches = difflib.get_close_matches(partner_nombre, bd_nombres, n=1, cutoff=umbral_match)
                        if matches:
                            match_nombre = matches[0]
                            idx = bd_nombres.index(match_nombre)
                            bd_key = bd_no_asignados[idx]
                            if not bd_asignado[bd_key]:
                                match_dict[partner_key] = bd_key
                                bd_asignado[bd_key] = True
                                partner_asignado[partner_key] = True
                                bd_no_asignados.pop(idx)
                                bd_nombres.pop(idx)

            # Determinar conjuntos finales
            coincidentes_keys = set(match_dict.keys())
            solo_bd_keys = {k for k in bd_keys_set if not bd_asignado[k]}
            solo_partner_keys = {k for k in partner_keys_set if not partner_asignado[k]}

            num_coincidentes_unicos = len(coincidentes_keys)
            num_solo_bd_unicos = len(solo_bd_keys)
            num_solo_partner_unicos = len(solo_partner_keys)

            # Construir DataFrames de detalle (todas las filas)
            with st.spinner(f"Comparando datos con {tipo_informe}..."):
                merged = df_bd.merge(df_partner, on='_key', how='outer', indicator=True, suffixes=('_bd', '_partner'))
                coincidentes = merged[merged['_key'].isin(coincidentes_keys)].copy()
                solo_bd = merged[merged['_key'].isin(solo_bd_keys) & (merged['_merge'] == 'left_only')].copy()
                solo_partner = merged[merged['_key'].isin(solo_partner_keys) & (merged['_merge'] == 'right_only')].copy()

                # Guardar versiones con clave para análisis de estados (antes de limpiar)
                coincidentes_con_key = coincidentes.copy()
                solo_bd_con_key = solo_bd.copy()
                solo_partner_con_key = solo_partner.copy()

                # Limpiar columnas auxiliares de los DataFrames que mostraremos
                for df_temp in [coincidentes, solo_bd, solo_partner]:
                    for col in ['_key', '_key_original', '_key_limpio', '_key_original_bd', '_key_original_partner', '_key_limpio_bd', '_key_limpio_partner']:
                        if col in df_temp.columns:
                            df_temp.drop(columns=[col], inplace=True)

            # Análisis de estados
            estados_validos = ['FINALIZADO']

            # Coincidentes problemáticos (estado != FINALIZADO)
            if not coincidentes_con_key.empty and 'estado' in coincidentes_con_key.columns:
                coincidentes_problematicos = coincidentes_con_key[~coincidentes_con_key['estado'].isin(estados_validos)].copy()
                # Limpiar columnas internas
                for col in ['_key', '_key_original', '_key_limpio', '_key_original_bd', '_key_original_partner', '_key_limpio_bd', '_key_limpio_partner']:
                    if col in coincidentes_problematicos.columns:
                        coincidentes_problematicos.drop(columns=[col], inplace=True)
            else:
                coincidentes_problematicos = pd.DataFrame()

            # Solo en BD que están FINALIZADOS
            if not solo_bd_con_key.empty and 'estado' in solo_bd_con_key.columns:
                solo_bd_finalizados = solo_bd_con_key[solo_bd_con_key['estado'].isin(estados_validos)].copy()
                # Limpiar columnas internas
                for col in ['_key', '_key_original', '_key_limpio', '_key_original_bd', '_key_original_partner', '_key_limpio_bd', '_key_limpio_partner']:
                    if col in solo_bd_finalizados.columns:
                        solo_bd_finalizados.drop(columns=[col], inplace=True)
                # Obtener claves únicas de estos finalizados
                solo_bd_finalizados_keys = set(solo_bd_con_key[solo_bd_con_key['estado'].isin(estados_validos)]['_key'].unique())
                solo_bd_finalizados_nombres = [bd_nombre_map.get(k, k) for k in solo_bd_finalizados_keys]
            else:
                solo_bd_finalizados = pd.DataFrame()
                solo_bd_finalizados_nombres = []

            def get_close_matches_names(nombre, corte=0.6, max_n=3):
                return difflib.get_close_matches(nombre, bd_nombres_originales, n=max_n, cutoff=corte)

            # -------------------------------------------------------------------
            # Métricas resumen
            # -------------------------------------------------------------------
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Contratos en BD", len(df_bd))
            with col2:
                st.metric(f"Registros {partner_nombre_display}", len(df_partner))
            with col3:
                st.metric("Coincidentes", f"{num_coincidentes_unicos} clientes\n({len(coincidentes)} registros)")
            with col4:
                st.metric("Solo en BD", f"{num_solo_bd_unicos} clientes\n({len(solo_bd)} registros)")
            st.caption(f"Solo en {partner_nombre_display}: {num_solo_partner_unicos} clientes ({len(solo_partner)} registros)")

            # Mostrar alerta si hay coincidentes problemáticos
            if not coincidentes_problematicos.empty:
                st.error(f"⚠️ **Atención:** Se han encontrado **{len(coincidentes_problematicos)}** contratos coincidentes con estado distinto de FINALIZADO. Revisa si se está cobrando indebidamente.")

            # -------------------------------------------------------------------
            # Pestañas
            # -------------------------------------------------------------------
            tab_titles = [
                f"✅ Coincidentes ({len(coincidentes)} registros, {num_coincidentes_unicos} clientes)",
                f"⚠️ Coincidentes no finalizados ({len(coincidentes_problematicos)} registros)",
                f"🔵 Solo en BD ({len(solo_bd)} registros, {num_solo_bd_unicos} clientes)",
                f"🟠 Solo en {partner_nombre_display} ({len(solo_partner)} registros, {num_solo_partner_unicos} clientes)"
            ]
            tabs = st.tabs(tab_titles)

            with tabs[0]:  # Coincidentes totales
                mostrar_tabla_con_aggrid(coincidentes, f"{tipo_informe}_coincidentes")
                if num_coincidentes_unicos > 0:
                    with st.expander(f"📋 Lista de clientes únicos coincidentes"):
                        df_nombres = pd.DataFrame({
                            'Cliente': [bd_nombre_map.get(k, k) for k in coincidentes_keys if k in bd_nombre_map]
                        }).sort_values('Cliente')
                        st.dataframe(df_nombres, width='stretch', hide_index=True)

            with tabs[1]:  # Coincidentes no finalizados
                if not coincidentes_problematicos.empty:
                    mostrar_tabla_con_aggrid(coincidentes_problematicos, f"{tipo_informe}_problematicos")
                    st.markdown("#### Distribución de estados problemáticos")
                    estado_counts = coincidentes_problematicos['estado'].value_counts().reset_index()
                    estado_counts.columns = ['Estado', 'Cantidad']
                    st.dataframe(estado_counts, width='stretch', hide_index=True)
                else:
                    st.success("¡No hay coincidentes con estados problemáticos!")

            with tabs[2]:  # Solo en BD
                mostrar_tabla_con_aggrid(solo_bd, f"{tipo_informe}_solo_bd")
                if num_solo_bd_unicos > 0:
                    with st.expander(f"📋 Lista de clientes únicos solo en BD ({num_solo_bd_unicos})"):
                        df_nombres = pd.DataFrame({
                            'Cliente': [bd_nombre_map[k] for k in solo_bd_keys if k in bd_nombre_map]
                        }).sort_values('Cliente')
                        st.dataframe(df_nombres, width='stretch', hide_index=True)

                # Mostrar los que están FINALIZADOS dentro de solo en BD
                if not solo_bd_finalizados.empty:
                    with st.expander(f"🔍 Clientes solo en BD con estado FINALIZADO ({len(solo_bd_finalizados)} registros, {len(solo_bd_finalizados_nombres)} clientes)"):
                        st.markdown("Estos clientes están en nuestra BD como finalizados, pero no aparecen en el fichero. Podrían ser ingresos perdidos si deberían estar facturándose.")
                        mostrar_tabla_con_aggrid(solo_bd_finalizados, f"{tipo_informe}_solo_bd_finalizados")
                        if solo_bd_finalizados_nombres:
                            df_nombres_fin = pd.DataFrame({
                                'Cliente': sorted(solo_bd_finalizados_nombres)
                            })
                            st.dataframe(df_nombres_fin, width='stretch', hide_index=True)

            with tabs[3]:  # Solo en partner
                mostrar_tabla_con_aggrid(solo_partner, f"{tipo_informe}_solo_partner")
                if num_solo_partner_unicos > 0:
                    with st.expander(f"📋 Lista de clientes únicos solo en {partner_nombre_display} ({num_solo_partner_unicos}) con posibles coincidencias en BD"):
                        data = []
                        for key in solo_partner_keys:
                            nombre_partner = partner_nombre_map.get(key, key)
                            matches = get_close_matches_names(nombre_partner, corte=0.6, max_n=3)
                            if matches:
                                matches_str = ", ".join(matches)
                            else:
                                matches_str = "Sin coincidencias cercanas"
                            data.append({
                                f'Cliente en {partner_nombre_display}': nombre_partner,
                                'Posibles coincidencias en BD': matches_str
                            })
                        df_sugerencias = pd.DataFrame(data).sort_values(f'Cliente en {partner_nombre_display}')
                        st.dataframe(df_sugerencias, width='stretch', hide_index=True)
                        st.caption("Se muestran hasta 3 posibles coincidencias por nombre usando similitud difusa (corte 0.6). Revisa manualmente si alguna corresponde.")

            # -------------------------------------------------------------------
            # Interpretación de resultados
            # -------------------------------------------------------------------
            st.markdown("---")
            st.subheader(f"📌 Interpretación de resultados ({partner_nombre_display})")
            st.markdown(f"""
            **{partner_nombre_display}** es nuestro proveedor. El objetivo de esta auditoría es verificar que **todos los clientes que nos factura {partner_nombre_display} están dados de alta en nuestro sistema (`seguimiento_contratos`)**.

            - **Clientes únicos en {partner_nombre_display}:** {num_solo_partner_unicos + num_coincidentes_unicos}  
            - **Clientes únicos en BD:** {num_solo_bd_unicos + num_coincidentes_unicos}

            **Lo que nos interesa:**

            ✅ **Coincidentes totales ({num_coincidentes_unicos} clientes, {len(coincidentes)} registros):**  
            Estos clientes están correctamente dados de alta en nuestro sistema (coincidencia exacta o aproximada con umbral {umbral_match if usar_match_aproximado else 'exacta'}).

            ⚠️ **Coincidentes no finalizados ({len(coincidentes_problematicos)} registros):**  
            Son clientes que aparecen en la factura de {partner_nombre_display} y también en BD, pero su contrato **no está FINALIZADO**. Si se está cobrando, habría que revisar si es correcto.

            ❌ **Solo en {partner_nombre_display} ({num_solo_partner_unicos} clientes, {len(solo_partner)} registros):**  
            **¡ATENCIÓN!** Estos clientes aparecen en la factura de {partner_nombre_display} pero **no están en nuestra base de datos** (ni exacta ni aproximadamente).  
            → **Acción:** Revisa la lista de nombres en el expander de la pestaña "Solo en {partner_nombre_display}". Allí se muestran posibles coincidencias en la BD con un umbral más bajo (0.6) para ayudar a identificar falsos negativos.

            🔵 **Solo en BD ({num_solo_bd_unicos} clientes, {len(solo_bd)} registros):**  
            Estos clientes están en nuestra base de datos pero no aparecen en la factura de {partner_nombre_display}.  
            → **Acción:** Verificar si son clientes de solo fibra (en cuyo caso es normal) o si deberían tener también línea móvil y no se está facturando.

            **Dentro de Solo en BD, hay {len(solo_bd_finalizados)} registros con estado FINALIZADO ({len(solo_bd_finalizados_nombres)} clientes).**  
            Estos podrían ser **ingresos perdidos** si deberían estar siendo facturados por {partner_nombre_display}. Revísalos en el expander correspondiente.

            **Resumen de la deuda/reclamación:**  
            - **Posible facturación indebida:** {len(coincidentes_problematicos)} registros coincidentes no finalizados + {num_solo_partner_unicos} clientes facturados sin contrato (revisar coincidencias sugeridas).
            - **Posible ingreso perdido:** {len(solo_bd_finalizados)} registros de clientes finalizados en BD que no están en {partner_nombre_display}.
            """)

            # Registrar en trazabilidad
            log_trazabilidad(
                st.session_state.get("username", "auditor"),
                f"Auditoría de facturación - {tipo_informe}",
                f"Comparación con fichero {partner_filename}. Coincidentes={len(coincidentes)} regs ({num_coincidentes_unicos} cltes), Problemáticos={len(coincidentes_problematicos)}, Solo BD={len(solo_bd)} regs ({num_solo_bd_unicos} cltes), Solo {tipo_informe}={len(solo_partner)} regs ({num_solo_partner_unicos} cltes), BD finalizados sin factura={len(solo_bd_finalizados)} regs ({len(solo_bd_finalizados_nombres)} cltes) (umbral_match={umbral_match if usar_match_aproximado else 'exacto'})"
            )

        # -------------------------------------------------------------------
        # Botones de descarga (comunes para ambos casos)
        # -------------------------------------------------------------------
        col_d1, col_d2, col_d3 = st.columns(3)
        with col_d1:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df_bd.to_excel(writer, sheet_name='Contratos_BD', index=False)
                df_partner.to_excel(writer, sheet_name=f'Fichero_{tipo_informe}', index=False)
                coincidentes.to_excel(writer, sheet_name='Coincidentes', index=False)
                if tipo_informe == "Adamo" and 'coincidentes_problematicos' in locals() and not coincidentes_problematicos.empty:
                    coincidentes_problematicos.to_excel(writer, sheet_name='Coincidentes_Problematicos', index=False)
                if tipo_informe != "Adamo" and 'coincidentes_problematicos' in locals() and not coincidentes_problematicos.empty:
                    coincidentes_problematicos.to_excel(writer, sheet_name='Coincidentes_Problematicos', index=False)
                if tipo_informe != "Adamo" and 'solo_bd_finalizados' in locals() and not solo_bd_finalizados.empty:
                    solo_bd_finalizados.to_excel(writer, sheet_name='Solo_BD_Finalizados', index=False)
                solo_bd.to_excel(writer, sheet_name='Solo_BD', index=False)
                solo_partner.to_excel(writer, sheet_name=f'Solo_{tipo_informe}', index=False)
            output.seek(0)

            st.download_button(
                label=f"📥 Descargar informe {tipo_informe} (Excel)",
                data=output,
                file_name=f"auditoria_{tipo_informe.lower()}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

        with col_d2:
            output_disc = io.BytesIO()
            with pd.ExcelWriter(output_disc, engine='xlsxwriter') as writer:
                solo_bd.to_excel(writer, sheet_name='Solo_BD', index=False)
                solo_partner.to_excel(writer, sheet_name=f'Solo_{tipo_informe}', index=False)
            output_disc.seek(0)

            st.download_button(
                label=f"📥 Descargar solo discrepancias ({tipo_informe})",
                data=output_disc,
                file_name=f"discrepancias_{tipo_informe.lower()}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

        with col_d3:
            if st.button("🔄 Refrescar datos de BD", use_container_width=True):
                st.cache_data.clear()
                st.rerun()

# -------------------------------------------------------------------
# Para pruebas independientes (descomentar si se ejecuta solo)
# -------------------------------------------------------------------
# if __name__ == "__main__":
#     st.set_page_config(page_title="Auditoría de facturación", layout="wide")
#     mostrar_auditoria()