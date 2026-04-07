# mapa_dashboard.py
import io
import json
import os
import sqlite3
import unicodedata
import warnings
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import folium
import pandas as pd
import sqlitecloud
import streamlit as st
from branca.element import MacroElement, Template
from folium.plugins import Geocoder, MarkerCluster
from geopy.distance import geodesic
from streamlit_cookies_controller import CookieController
from streamlit_folium import st_folium
from streamlit_option_menu import option_menu

# Módulos locales
from modules.notificaciones import (
    correo_asignacion_administracion,
    correo_asignacion_administracion2,
    correo_confirmacion_viab_admin,
    correo_desasignacion_administracion,
    correo_reasignacion_entrante,
    correo_reasignacion_saliente,
    correo_viabilidad_comercial,
    notificar_actualizacion_ticket,
    notificar_creacion_ticket,
)

warnings.filterwarnings("ignore", category=UserWarning)

cookie_name = "my_app"

# ==================== CONEXIÓN A BASE DE DATOS ====================
def get_db_connection():
    """Retorna una conexión a la base de datos SQLite Cloud."""
    return sqlitecloud.connect(
        "sqlitecloud://ceafu04onz.g6.sqlite.cloud:8860/usuarios.db?apikey=Qo9m18B9ONpfEGYngUKm99QB5bgzUTGtK7iAcThmwvY"
    )


# ==================== CONTEXTO DE USUARIO ====================
@st.cache_data(ttl=600)
def get_user_context(username: str) -> Dict:
    """
    Devuelve un diccionario con reglas de filtrado según el usuario.
    """
    username_lower = username.strip().lower()
    context = {
        "excluir_comerciales": [],
        "comerciales_permitidos": [],
        "provincias_permitidas": [],
        "excluir_viabilidades": [],
    }

    if username_lower == "juan":
        context["excluir_comerciales"] = [
            "nestor", "rafaela", "jose ramon", "roberto", "marian", "juan pablo"
        ]
        context["comerciales_permitidos"] = ["comercial2", "comercial3"]
        context["provincias_permitidas"] = ["lugo", "asturias"]
        context["excluir_viabilidades"] = [
            "roberto", "jose ramon", "nestor", "rafaela",
            "rebe", "marian", "rafa sanz", "juan pablo"
        ]
    elif username_lower == "rafa sanz":
        context["excluir_comerciales"] = ["juan pablo"]
        context["comerciales_permitidos"] = ["roberto", "nestor", "jose ramon"]
        context["excluir_viabilidades"] = [
            "juan pablo", "roberto", "nestor", "comercial2", "comercial3", "juan", "marian"
        ]
    # Para otros usuarios (comerciales normales) se dejan listas vacías
    return context


# ==================== FUNCIONES DE CARGA CON CACHÉ ====================
@st.cache_data(ttl=600)
def cargar_datos(usuario: Optional[str] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Carga datos_uis filtrados por el comercial y toda la tabla comercial_rafa.
    """
    if usuario is None:
        usuario = st.session_state.get("username", "").strip()
    conn = get_db_connection()

    if usuario:
        query_uis = """
            SELECT apartment_id, latitud, longitud, fecha, provincia, municipio,
                   vial, numero, letra, poblacion, tipo_olt_rental, serviciable
            FROM datos_uis
            WHERE comercial = ?
        """
        datos_uis = pd.read_sql(query_uis, conn, params=(usuario,))
    else:
        query_uis = """
            SELECT apartment_id, latitud, longitud, fecha, provincia, municipio,
                   vial, numero, letra, poblacion, tipo_olt_rental, serviciable
            FROM datos_uis
        """
        datos_uis = pd.read_sql(query_uis, conn)

    query_rafa = """
        SELECT apartment_id, serviciable, Contrato, municipio, poblacion, comercial
        FROM comercial_rafa
    """
    comercial_rafa = pd.read_sql(query_rafa, conn)
    conn.close()
    return datos_uis, comercial_rafa


@st.cache_data(ttl=600)
def cargar_total_ofertas() -> pd.DataFrame:
    """Carga toda la tabla comercial_rafa."""
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM comercial_rafa", conn)
    except Exception as e:
        st.error(f"Error cargando total_ofertas: {e}")
        df = pd.DataFrame()
    conn.close()
    return df


@st.cache_data(ttl=600)
def cargar_viabilidades() -> pd.DataFrame:
    """Carga toda la tabla viabilidades."""
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM viabilidades ORDER BY id DESC", conn)
    except Exception as e:
        st.error(f"Error cargando viabilidades: {e}")
        df = pd.DataFrame()
    conn.close()
    return df


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


# ==================== ANUNCIOS ====================
def mostrar_ultimo_anuncio():
    """Muestra el anuncio más reciente a los usuarios normales."""
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


# ==================== SOPORTE (TICKETS) ====================
def obtener_emails_administradores() -> List[str]:
    """Devuelve lista de emails de todos los administradores."""
    try:
        conn = get_db_connection()
        query = "SELECT email FROM usuarios WHERE role = 'admin' AND email IS NOT NULL"
        df = pd.read_sql(query, conn)
        conn.close()
        return df['email'].tolist()
    except Exception as e:
        st.warning(f"No se pudieron obtener correos de administradores: {e}")
        return []


def mostrar_mis_tickets_gestor():
    """Muestra los tickets creados por el gestor comercial actual."""
    user_id = st.session_state.get("user_id")
    if not user_id:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM usuarios WHERE username = ?",
                (st.session_state['username'],)
            )
            result = cursor.fetchone()
            conn.close()
            if result:
                user_id = result[0]
            else:
                st.error("❌ No se pudo identificar al usuario.")
                return
        except Exception:
            st.error("❌ Error al obtener información del usuario.")
            return

    st.subheader("📋 Mis Tickets Reportados")
    st.markdown("---")

    try:
        conn = get_db_connection()
        query = """
            SELECT t.ticket_id, t.fecha_creacion, t.categoria, t.prioridad, t.estado,
                   u.username as asignado_a, t.titulo, t.descripcion, t.comentarios
            FROM tickets t
            LEFT JOIN usuarios u ON t.asignado_a = u.id
            WHERE t.usuario_id = ?
            ORDER BY t.fecha_creacion DESC
        """
        df_tickets = pd.read_sql(query, conn, params=(user_id,))
        conn.close()
    except Exception as e:
        st.error(f"Error al cargar tickets: {e}")
        return

    if df_tickets.empty:
        st.info("🎉 No has creado ningún ticket aún.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Tickets", len(df_tickets))
    with col2:
        abiertos = len(df_tickets[df_tickets['estado'] == 'Abierto'])
        st.metric("Abiertos", abiertos)
    with col3:
        resueltos = len(df_tickets[df_tickets['estado'].isin(['Resuelto', 'Cancelado'])])
        st.metric("Resueltos", resueltos)

    st.markdown("---")

    for _, ticket in df_tickets.iterrows():
        prioridad_color = {'Alta': '🔴', 'Media': '🟡', 'Baja': '🟢'}.get(ticket['prioridad'], '⚪')
        estado_color = {
            'Abierto': '🟢', 'En Progreso': '🟡',
            'Resuelto': '🔵', 'Cancelado': '⚫'
        }.get(ticket['estado'], '⚪')

        with st.expander(f"{estado_color} Ticket #{ticket['ticket_id']}: {ticket['titulo']} {prioridad_color}"):
            col_info1, col_info2 = st.columns(2)
            with col_info1:
                st.markdown(f"**📅 Creado:** {ticket['fecha_creacion']}")
                st.markdown(f"**🏷️ Categoría:** {ticket['categoria']}")
                st.markdown(f"**🚨 Prioridad:** {ticket['prioridad']}")
            with col_info2:
                st.markdown(f"**📊 Estado:** {ticket['estado']}")
                st.markdown(f"**👥 Asignado a:** {ticket['asignado_a'] or 'Pendiente'}")
                st.markdown(f"**🎫 ID:** #{ticket['ticket_id']}")

            st.markdown("---")
            st.markdown("**📄 Descripción:**")
            st.info(ticket['descripcion'])

            if ticket['comentarios']:
                st.markdown("---")
                st.markdown("**💬 Comentarios del equipo:**")
                for comentario in ticket['comentarios'].split('\n\n'):
                    if comentario.strip():
                        st.warning(comentario.strip())

            if ticket['estado'] in ['Abierto', 'En Progreso']:
                st.markdown("---")
                st.markdown("**📝 Añadir información adicional:**")
                with st.form(key=f"add_info_{ticket['ticket_id']}"):
                    nueva_info = st.text_area(
                        "Información adicional:",
                        placeholder="Si tienes más detalles...",
                        height=100,
                        key=f"info_{ticket['ticket_id']}"
                    )
                    enviar_info = st.form_submit_button("📤 Enviar información", use_container_width=True)

                    if enviar_info and nueva_info.strip():
                        try:
                            conn = get_db_connection()
                            cursor = conn.cursor()
                            cursor.execute(
                                """
                                SELECT t.titulo, t.prioridad, t.categoria,
                                       u.email as asignado_email, u.username as asignado_username,
                                       u2.email as creador_email, u2.username as creador_username
                                FROM tickets t
                                LEFT JOIN usuarios u ON t.asignado_a = u.id
                                LEFT JOIN usuarios u2 ON t.usuario_id = u2.id
                                WHERE t.ticket_id = ?
                                """,
                                (ticket['ticket_id'],)
                            )
                            ticket_data = cursor.fetchone()

                            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
                            info_formateada = f"\n\n[{timestamp}] {st.session_state['username']} (cliente):\n{nueva_info.strip()}"
                            cursor.execute(
                                """
                                UPDATE tickets
                                SET comentarios = COALESCE(comentarios || ?, ?)
                                WHERE ticket_id = ?
                                """,
                                (
                                    info_formateada,
                                    f"[{timestamp}] {st.session_state['username']} (cliente):\n{nueva_info.strip()}",
                                    ticket['ticket_id']
                                )
                            )
                            conn.commit()
                            conn.close()

                            # Notificaciones
                            if ticket_data:
                                ticket_info = {
                                    'ticket_id': ticket['ticket_id'],
                                    'titulo': ticket_data[0],
                                    'actualizado_por': st.session_state['username'],
                                    'tipo_actualizacion': 'informacion_adicional',
                                    'descripcion_cambio': nueva_info.strip(),
                                    'enlace': f"https://tu-dominio.com/ticket/{ticket['ticket_id']}"
                                }
                                if ticket_data[3] and ticket_data[4] != st.session_state['username']:
                                    try:
                                        notificar_actualizacion_ticket(ticket_data[3], ticket_info)
                                    except Exception as e:
                                        st.warning(f"Error notificando al asignado: {e}")
                                admin_emails = obtener_emails_administradores()
                                for email in admin_emails:
                                    if email != st.session_state.get('email', ''):
                                        try:
                                            notificar_actualizacion_ticket(email, ticket_info)
                                        except Exception as e:
                                            st.warning(f"Error notificando a admin {email}: {e}")

                            log_trazabilidad(
                                st.session_state["username"],
                                "Información adicional en ticket",
                                f"Añadió información al ticket #{ticket['ticket_id']}"
                            )
                            st.success("✅ Información añadida al ticket")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error al añadir información: {e}")


def crear_ticket_cliente():
    """Formulario para que los gestores comerciales creen tickets como clientes."""
    st.subheader("➕ Crear Nuevo Ticket")
    st.markdown("---")

    if st.session_state.get('ticket_creado'):
        ticket_info = st.session_state.get('ticket_info', {})
        st.success(f"✅ **Ticket #{ticket_info.get('id')} creado correctamente**")
        with st.expander("📋 Ver resumen del ticket", expanded=True):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**🎫 ID:** #{ticket_info.get('id')}")
                st.markdown(f"**📝 Asunto:** {ticket_info.get('titulo')}")
                st.markdown(f"**🏷️ Tipo:** {ticket_info.get('categoria')}")
                st.markdown(f"**🚨 Urgencia:** {ticket_info.get('prioridad')}")
            with col2:
                st.markdown(f"**📊 Estado:** Abierto")
                st.markdown(f"**👤 Reportado por:** {st.session_state.get('username')}")
                st.markdown(f"**📅 Fecha:** {ticket_info.get('fecha')}")
        col_opc1, col_opc2 = st.columns(2)
        with col_opc1:
            if st.button("📋 Ver mis tickets", use_container_width=True):
                st.session_state['ticket_creado'] = False
                st.rerun()
        with col_opc2:
            if st.button("➕ Crear otro ticket", use_container_width=True):
                st.session_state['ticket_creado'] = False
                st.rerun()
        return

    st.markdown("---")
    with st.form("form_ticket_cliente"):
        titulo = st.text_input(
            "📝 **Asunto del ticket** *",
            placeholder="Ej: Problema con la visualización de datos en el mapa de asignaciones"
        )
        col_cat, col_pri = st.columns(2)
        with col_cat:
            categoria = st.selectbox(
                "🏷️ **Tipo de incidencia** *",
                [
                    "Problema técnico", "Error en datos", "Consulta sobre funcionalidad",
                    "Solicitud de nueva característica", "Problema de acceso", "Otro"
                ]
            )
        with col_pri:
            prioridad = st.selectbox(
                "🚨 **Urgencia** *",
                ["Baja", "Media", "Alta"],
                help="Alta = Bloqueante, Media = Importante, Baja = Mejora"
            )

        descripcion = st.text_area(
            "📄 **Descripción detallada** *",
            placeholder="""Describe la incidencia con el mayor detalle posible...""",
            height=300
        )

        with st.expander("🔧 Información técnica adicional (opcional)"):
            col_tech1, col_tech2 = st.columns(2)
            with col_tech1:
                navegador = st.selectbox(
                    "Navegador:",
                    ["Chrome", "Firefox", "Edge", "Safari", "Otro", "No sé"]
                )
                sistema_operativo = st.selectbox(
                    "Sistema operativo:",
                    ["Windows", "macOS", "Linux", "iOS", "Android", "Otro"]
                )
            with col_tech2:
                dispositivo = st.selectbox(
                    "Dispositivo:",
                    ["PC/Laptop", "Móvil", "Tablet", "Otro"]
                )
                url_pagina = st.text_input("URL de la página (si aplica):", placeholder="https://...")

        with st.expander("📎 Adjuntar archivos (opcional)"):
            archivos = st.file_uploader(
                "Selecciona archivos:",
                type=['png', 'jpg', 'jpeg', 'pdf', 'txt', 'csv'],
                accept_multiple_files=True
            )
            if archivos:
                st.success(f"✅ {len(archivos)} archivo(s) listo(s)")

        st.markdown("---")
        col_btn1, col_btn2 = st.columns([1, 1])
        with col_btn1:
            enviar = st.form_submit_button("✅ **Enviar Ticket**", type="primary", use_container_width=True)
        with col_btn2:
            cancelar = st.form_submit_button("❌ **Cancelar**", use_container_width=True)

        if enviar:
            if not titulo or not descripcion:
                st.error("⚠️ Por favor, completa todos los campos obligatorios (*)")
            else:
                try:
                    user_id = st.session_state.get("user_id")
                    if not user_id:
                        conn = get_db_connection()
                        cursor = conn.cursor()
                        cursor.execute(
                            "SELECT id FROM usuarios WHERE username = ?",
                            (st.session_state['username'],)
                        )
                        result = cursor.fetchone()
                        if result:
                            user_id = result[0]
                        else:
                            st.error("❌ No se pudo identificar al usuario.")
                            return
                        conn.close()

                    descripcion_completa = descripcion + "\n\n--- INFORMACIÓN TÉCNICA ---\n"
                    descripcion_completa += f"• Navegador: {navegador}\n"
                    descripcion_completa += f"• Sistema operativo: {sistema_operativo}\n"
                    descripcion_completa += f"• Dispositivo: {dispositivo}\n"
                    if url_pagina:
                        descripcion_completa += f"• URL: {url_pagina}\n"
                    descripcion_completa += f"• Fecha reporte: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"

                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        INSERT INTO tickets
                        (usuario_id, categoria, prioridad, estado, titulo, descripcion)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (user_id, categoria, prioridad, "Abierto", titulo, descripcion_completa)
                    )
                    conn.commit()
                    ticket_id = cursor.lastrowid

                    cursor.execute(
                        """
                        UPDATE tickets SET comentarios = ?
                        WHERE ticket_id = ?
                        """,
                        (
                            f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Ticket creado por gestor comercial {st.session_state['username']}. Pendiente de asignación.",
                            ticket_id
                        )
                    )
                    conn.commit()

                    cursor.execute("SELECT email FROM usuarios WHERE id = ?", (user_id,))
                    creador_info = cursor.fetchone()
                    conn.close()

                    admin_emails = obtener_emails_administradores()
                    ticket_info = {
                        'ticket_id': ticket_id,
                        'titulo': titulo,
                        'creado_por': st.session_state['username'],
                        'prioridad': prioridad,
                        'categoria': categoria,
                        'estado': "Abierto",
                        'descripcion': descripcion[:100] + '...' if len(descripcion) > 100 else descripcion,
                        'enlace': f"https://tu-dominio.com/ticket/{ticket_id}"
                    }
                    for email in admin_emails:
                        try:
                            notificar_creacion_ticket(email, ticket_info)
                        except Exception as e:
                            st.warning(f"Error notificando a admin {email}: {e}")
                    if creador_info and creador_info[0]:
                        try:
                            notificar_creacion_ticket(creador_info[0], ticket_info)
                        except Exception as e:
                            st.warning(f"Error notificando al creador: {e}")

                    log_trazabilidad(
                        st.session_state["username"],
                        "Creación de ticket (cliente)",
                        f"Ticket #{ticket_id} creado como cliente: {titulo}"
                    )

                    st.session_state['ticket_creado'] = True
                    st.session_state['ticket_info'] = {
                        'id': ticket_id,
                        'titulo': titulo,
                        'categoria': categoria,
                        'prioridad': prioridad,
                        'fecha': datetime.now().strftime('%d/%m/%Y %H:%M')
                    }
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error al crear el ticket: {e}")

        if cancelar:
            st.info("Formulario cancelado.")


def mostrar_soporte_gestor_comercial():
    """Panel de soporte para gestores comerciales."""
    st.title("🎫 Soporte Técnico")
    st.markdown("---")
    tab1, tab2 = st.tabs(["📋 Mis Tickets", "➕ Nuevo Ticket"])
    with tab1:
        mostrar_mis_tickets_gestor()
    with tab2:
        crear_ticket_cliente()


# ==================== FUNCIONES DE VIABILIDADES (compartidas) ====================
def generar_ticket() -> str:
    """Genera un ticket único con formato: añomesdía + número consecutivo."""
    conn = get_db_connection()
    cursor = conn.cursor()
    fecha_actual = datetime.now().strftime("%Y%m%d")
    cursor.execute(
        "SELECT MAX(CAST(SUBSTR(ticket, 9, 3) AS INTEGER)) FROM viabilidades WHERE ticket LIKE ?",
        (f"{fecha_actual}%",)
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
        datos
    )
    conn.commit()

    cursor.execute("SELECT email FROM usuarios WHERE role = 'admin'")
    emails_admin = [row[0] for row in cursor.fetchall()]

    ticket_id = datos[10]
    nombre_comercial = datos[13]
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

    st.success(f"✅ Viabilidad guardada correctamente.\n\n📌 **Ticket:** `{ticket_id}`")
    conn.close()


def obtener_viabilidades():
    """Devuelve lista de viabilidades según el rol/usuario."""
    conn = get_db_connection()
    cursor = conn.cursor()
    usuario_actual = st.session_state.get("username", "")
    rol_actual = st.session_state.get("role", "")

    if rol_actual == "admin":
        cursor.execute("SELECT latitud, longitud, ticket, serviciable, apartment_id FROM viabilidades")
    elif usuario_actual == "rafa sanz":
        comerciales = ("roberto", "nestor", "rafaela", "jose ramon", "rafa sanz")
        placeholders = ','.join(['?'] * len(comerciales))
        cursor.execute(
            f"SELECT latitud, longitud, ticket, serviciable, apartment_id FROM viabilidades WHERE usuario IN ({placeholders})",
            comerciales
        )
    elif usuario_actual == "juan":
        comerciales = ("juan", "Comercial2", "Comercial3")
        placeholders = ','.join(['?'] * len(comerciales))
        cursor.execute(
            f"SELECT latitud, longitud, ticket, serviciable, apartment_id FROM viabilidades WHERE usuario IN ({placeholders})",
            comerciales
        )
    else:
        cursor.execute(
            "SELECT latitud, longitud, ticket, serviciable, apartment_id FROM viabilidades WHERE usuario = ?",
            (usuario_actual,)
        )
    viabilidades = cursor.fetchall()
    conn.close()
    return viabilidades


# ==================== BÚSQUEDA POR COORDENADAS ====================
def mostrar_coordenadas():
    st.info(
        "📍 **Búsqueda por coordenadas**\n\n"
        "Visualiza puntos serviciables dentro de un radio a partir de coordenadas."
    )
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        lat = st.number_input("🌐 Latitud", value=40.4168, format="%.6f", key="coord_lat")
    with col2:
        lon = st.number_input("🌐 Longitud", value=-3.7038, format="%.6f", key="coord_lon")
    with col3:
        radio_km = st.number_input("📏 Radio (km)", value=1.0, min_value=0.1, max_value=50.0, step=0.5, key="coord_radio")

    if st.button("🔍 Buscar coordenadas", width='stretch'):
        with st.spinner("🗺️ Calculando puntos..."):
            try:
                conn = get_db_connection()
                df = pd.read_sql(
                    "SELECT municipio, poblacion, latitud, longitud, vial, numero, cp FROM datos_uis",
                    conn
                )
                conn.close()
                df = df[(df["latitud"].between(-90, 90)) & (df["longitud"].between(-180, 180))].dropna(
                    subset=["latitud", "longitud"]
                )
                if df.empty:
                    st.warning("⚠️ No hay coordenadas válidas en la base de datos.")
                    return

                from geopy.distance import geodesic
                df["distancia_km"] = df.apply(
                    lambda row: geodesic((lat, lon), (row["latitud"], row["longitud"])).km,
                    axis=1
                )
                df_radio = df[df["distancia_km"] <= radio_km].copy().reset_index(drop=True)
                if df_radio.empty:
                    st.warning("⚠️ No se encontraron puntos dentro del radio.")
                    return

                st.session_state["busqueda_coordenadas"] = {
                    "lat": lat, "lon": lon, "radio_km": radio_km, "df_radio": df_radio
                }
                st.success(f"✅ Se encontraron {len(df_radio)} puntos dentro de {radio_km:.2f} km.")
            except Exception as e:
                st.error(f"❌ Error al buscar coordenadas: {e}")

    if "busqueda_coordenadas" in st.session_state:
        datos = st.session_state["busqueda_coordenadas"]
        lat, lon, radio_km, df_radio = datos["lat"], datos["lon"], datos["radio_km"], datos["df_radio"]

        m = folium.Map(location=[lat, lon], zoom_start=15, control_scale=True, max_zoom=19)
        folium.TileLayer(
            tiles="https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}",
            attr="Google Satellite", name="🛰️ Google Satélite", overlay=False, control=True, max_zoom=20
        ).add_to(m)
        folium.TileLayer(
            tiles="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}",
            attr="Google Hybrid", name="🗺️ Etiquetas", overlay=True, control=True, max_zoom=20
        ).add_to(m)

        folium.Circle(
            location=[lat, lon],
            radius=radio_km * 1000,
            color="blue",
            weight=2,
            fill=True,
            fill_color="#3186cc",
            fill_opacity=0.15,
            popup=f"Radio: {radio_km} km"
        ).add_to(m)

        folium.Marker(
            location=[lat, lon],
            popup="📍 Coordenadas buscadas",
            icon=folium.Icon(color="red", icon="glyphicon-screenshot")
        ).add_to(m)

        cluster = MarkerCluster().add_to(m)
        for _, row in df_radio.iterrows():
            folium.Marker(
                location=[row["latitud"], row["longitud"]],
                popup=f"{row.get('municipio', '—')} - {row.get('poblacion', '—')}",
                tooltip=f"📍 {row.get('municipio', '—')} ({row['distancia_km']:.2f} km)",
                icon=folium.Icon(color="green", icon="glyphicon-tint")
            ).add_to(cluster)

        mapa_output = st_folium(m, height=680, width="100%", key="mapa_busqueda_coordenadas")

        if mapa_output and mapa_output.get("last_object_clicked"):
            click_lat = mapa_output["last_object_clicked"]["lat"]
            click_lon = mapa_output["last_object_clicked"]["lng"]
            df_radio["distancia_click"] = df_radio.apply(
                lambda row: geodesic((click_lat, click_lon), (row["latitud"], row["longitud"])).meters,
                axis=1
            )
            punto = df_radio.loc[df_radio["distancia_click"].idxmin()]
            st.session_state["punto_seleccionado"] = punto

        if st.session_state.get("punto_seleccionado") is not None:
            sel = st.session_state["punto_seleccionado"].fillna("—")
            st.markdown("### 📋 Detalles del punto seleccionado")
            st.markdown(
                f"""
                <div style='background-color:#f9f9f9; padding:15px; border-radius:10px; border:1px solid #ddd;'>
                    <p><strong>🏘️ Municipio:</strong> {sel.get('municipio', '—')}</p>
                    <p><strong>🏡 Población:</strong> {sel.get('poblacion', '—')}</p>
                    <p><strong>📍 Dirección:</strong> {sel.get('vial', '—')} {sel.get('numero', '—')}</p>
                    <p><strong>📮 Código Postal:</strong> {sel.get('cp', '—')}</p>
                    <p><strong>🌐 Latitud:</strong> {sel.get('latitud', '—')}</p>
                    <p><strong>🌐 Longitud:</strong> {sel.get('longitud', '—')}</p>
                    <p><strong>📏 Distancia al punto:</strong> {sel.get('distancia_km', '—')} km</p>
                </div>
                """,
                unsafe_allow_html=True
            )

        if st.button("🧹 Limpiar búsqueda"):
            for key in ["busqueda_coordenadas", "punto_seleccionado"]:
                st.session_state.pop(key, None)
            st.rerun()


# ==================== MAPA DE ASIGNACIONES (SUBFUNCIONES) ====================
def _filtros_mapa(datos_uis: pd.DataFrame) -> pd.DataFrame:
    """Aplica los filtros de provincia, tipo de CTO, novedades y rango de fechas."""
    username = st.session_state.get("username", "").strip()

    provincias = datos_uis['provincia'].unique()
    provincia_sel = st.selectbox("Seleccione una provincia:", provincias)
    datos_uis = datos_uis[datos_uis["provincia"] == provincia_sel]

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        mostrar_verde = st.checkbox("CTO Verde", value=True)
    with col2:
        mostrar_compartida = st.checkbox("CTO Compartida", value=True)

    if "tipo_olt_rental" in datos_uis.columns:
        condiciones = []
        if mostrar_verde:
            condiciones.append(datos_uis["tipo_olt_rental"].str.contains("verde", case=False, na=False))
        if mostrar_compartida:
            condiciones.append(datos_uis["tipo_olt_rental"].str.contains("compartida", case=False, na=False))
        if condiciones:
            datos_uis = datos_uis[pd.concat(condiciones, axis=1).any(axis=1)]
        else:
            st.warning("⚠️ Debes seleccionar al menos un tipo de CTO.")
            st.stop()
    else:
        st.warning("⚠️ No se encontró la columna 'tipo_olt_rental'.")
        st.stop()

    # Novedades del mes
    if st.checkbox("Mostrar novedades: huella cargada más reciente"):
        if 'fecha' in datos_uis.columns:
            datos_uis['fecha'] = pd.to_datetime(datos_uis['fecha'], errors='coerce')
            hoy = datetime.now()
            datos_uis = datos_uis[
                (datos_uis['fecha'].dt.year == hoy.year) &
                (datos_uis['fecha'].dt.month == hoy.month)
            ]
            if datos_uis.empty:
                st.warning("⚠️ No hay puntos nuevos en el mes actual.")
                st.stop()
        else:
            st.warning("⚠️ No se encontró la columna 'fecha'.")
            st.stop()

    # Rango de fechas
    if 'fecha' in datos_uis.columns and not datos_uis['fecha'].isnull().all():
        fecha_min = datos_uis['fecha'].min().date()
        fecha_max = datos_uis['fecha'].max().date()
        rango = st.date_input(
            "(Opcional) Seleccione rango de fechas:",
            value=(fecha_min, fecha_max),
            min_value=fecha_min,
            max_value=fecha_max
        )
        if len(rango) == 2:
            datos_uis = datos_uis[
                (datos_uis['fecha'].dt.date >= rango[0]) &
                (datos_uis['fecha'].dt.date <= rango[1])
            ]
            if datos_uis.empty:
                st.warning("⚠️ No hay puntos en el rango seleccionado.")
                st.stop()

    return datos_uis


def _asignar_zona(datos_uis: pd.DataFrame):
    """Lógica de asignación de zona."""
    username = st.session_state.get("username", "").strip().lower()
    context = get_user_context(username)

    municipios = sorted(datos_uis['municipio'].dropna().unique())
    municipio_sel = st.selectbox("Seleccione un municipio:", municipios, key="municipio_sel")

    tipo_asignacion = st.radio(
        "¿Qué desea asignar?",
        ["Población específica", "Municipio completo"],
        horizontal=True,
        key="tipo_asignacion"
    )

    poblacion_sel = None
    if tipo_asignacion == "Población específica" and municipio_sel:
        poblaciones = sorted(datos_uis[datos_uis['municipio'] == municipio_sel]['poblacion'].dropna().unique())
        poblacion_sel = st.selectbox("Seleccione una población:", poblaciones, key="poblacion_sel")

    # Lista de comerciales disponibles
    comerciales_disponibles = context.get("comerciales_permitidos", [])
    if not comerciales_disponibles:
        # Si el contexto no define, usar los de la BD (pero con filtro)
        conn = get_db_connection()
        df_com = pd.read_sql("SELECT DISTINCT comercial FROM comercial_rafa", conn)
        conn.close()
        comerciales_disponibles = df_com['comercial'].dropna().unique().tolist()

    comerciales_seleccionados = st.multiselect(
        "Asignar equitativamente a:", comerciales_disponibles,
        key="comerciales_seleccionados"
    )

    if municipio_sel and comerciales_seleccionados and (
            tipo_asignacion == "Municipio completo" or poblacion_sel
    ):
        conn = get_db_connection()
        cursor = conn.cursor()

        if tipo_asignacion == "Municipio completo":
            cond_where = "municipio = ? AND comercial = ?"
            params = (municipio_sel, username)
        else:
            cond_where = "municipio = ? AND poblacion = ? AND comercial = ?"
            params = (municipio_sel, poblacion_sel, username)

        cursor.execute(f"SELECT COUNT(*) FROM datos_uis WHERE {cond_where}", params)
        total_puntos = cursor.fetchone()[0] or 0

        cursor.execute(
            f"SELECT COUNT(*) FROM comercial_rafa WHERE {cond_where.replace('comercial = ?', '1=1')}",
            params[:-1] if len(params) > 1 else params
        )
        asignados = cursor.fetchone()[0] or 0

        conn.close()

        if asignados >= total_puntos and total_puntos > 0:
            st.warning("🚫 Esta zona ya ha sido asignada completamente.")
        else:
            if st.button("Asignar Zona"):
                conn = get_db_connection()
                cursor = conn.cursor()

                if tipo_asignacion == "Municipio completo":
                    cursor.execute(
                        """
                        SELECT apartment_id, provincia, municipio, poblacion, vial, numero, letra, cp, latitud, longitud
                        FROM datos_uis
                        WHERE municipio = ? AND comercial = ?
                          AND apartment_id NOT IN (SELECT apartment_id FROM comercial_rafa WHERE municipio = ?)
                        """,
                        (municipio_sel, username, municipio_sel)
                    )
                else:
                    cursor.execute(
                        """
                        SELECT apartment_id, provincia, municipio, poblacion, vial, numero, letra, cp, latitud, longitud
                        FROM datos_uis
                        WHERE municipio = ? AND poblacion = ? AND comercial = ?
                          AND apartment_id NOT IN (SELECT apartment_id FROM comercial_rafa WHERE municipio = ? AND poblacion = ?)
                        """,
                        (municipio_sel, poblacion_sel, username, municipio_sel, poblacion_sel)
                    )

                puntos = cursor.fetchall()
                if not puntos:
                    st.warning("⚠️ No se encontraron puntos pendientes.")
                    conn.close()
                    return

                total_nuevos = len(puntos)
                num_comerciales = len(comerciales_seleccionados)
                puntos_por_comercial = total_nuevos // num_comerciales
                resto = total_nuevos % num_comerciales

                progress = st.progress(0)
                indice = 0
                for i, comercial in enumerate(comerciales_seleccionados):
                    asignar_count = puntos_por_comercial + (1 if i < resto else 0)
                    for _ in range(asignar_count):
                        if indice >= total_nuevos:
                            break
                        punto = puntos[indice]
                        cursor.execute(
                            """
                            INSERT OR IGNORE INTO comercial_rafa
                            (apartment_id, provincia, municipio, poblacion, vial, numero, letra, cp, latitud, longitud, comercial, Contrato)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (*punto, comercial, 'Pendiente')
                        )
                        cursor.execute(
                            "UPDATE comercial_rafa SET comercial = ? WHERE apartment_id = ?",
                            (comercial, punto[0])
                        )
                        indice += 1
                        progress.progress(indice / total_nuevos)

                conn.commit()
                progress.empty()

                # Notificaciones
                for comercial in comerciales_seleccionados:
                    cursor.execute("SELECT email FROM usuarios WHERE username = ?", (comercial,))
                    row = cursor.fetchone()
                    email = row[0] if row else None
                    if email:
                        try:
                            desc = f"📍 Se le ha asignado la zona {municipio_sel}" + (
                                f" - {poblacion_sel}" if poblacion_sel else " (municipio completo)"
                            )
                            correo_asignacion_administracion(email, municipio_sel, poblacion_sel or "", desc)
                        except Exception as e:
                            st.warning(f"Error notificando a {comercial}: {e}")

                cursor.execute("SELECT email FROM usuarios WHERE role = 'admin'")
                admins = [r[0] for r in cursor.fetchall()]
                desc_admin = (
                    f"📢 Nueva asignación.\n"
                    f"Zona: {municipio_sel}" + (f" - {poblacion_sel}" if poblacion_sel else "")
                    + f"\nAsignado a: {', '.join(comerciales_seleccionados)}"
                )
                for email in admins:
                    try:
                        correo_asignacion_administracion2(email, municipio_sel, poblacion_sel or "", desc_admin)
                    except Exception as e:
                        st.warning(f"Error notificando a admin {email}: {e}")

                st.success("✅ Zona asignada correctamente.")
                st.info(f"Total puntos: {total_puntos} | Ya asignados: {asignados} | Nuevos: {total_nuevos}")
                log_trazabilidad(
                    username,
                    "Asignación múltiple",
                    f"Zona {municipio_sel} {poblacion_sel or ''} repartida entre {', '.join(comerciales_seleccionados)}"
                )
                conn.close()


def _desasignar_zona():
    """Lógica de desasignación de zona."""
    username = st.session_state.get("username", "").strip().lower()
    context = get_user_context(username)

    conn = get_db_connection()
    assigned_zones = pd.read_sql("SELECT DISTINCT municipio, poblacion FROM comercial_rafa", conn)
    conn.close()

    if assigned_zones.empty:
        st.warning("No hay zonas asignadas para desasignar.")
        return

    assigned_zones = assigned_zones.dropna(subset=['municipio', 'poblacion'])
    assigned_zones['zona'] = assigned_zones['municipio'] + " - " + assigned_zones['poblacion']
    zonas_list = sorted(assigned_zones['zona'].unique())
    zona_seleccionada = st.selectbox("Seleccione la zona asignada a desasignar", zonas_list, key="zona_seleccionada")

    if zona_seleccionada:
        municipio_sel, poblacion_sel = [x.strip() for x in zona_seleccionada.split(" - ")]

        conn = get_db_connection()
        query = """
            SELECT DISTINCT comercial
            FROM comercial_rafa
            WHERE LOWER(TRIM(municipio)) = LOWER(TRIM(?))
              AND LOWER(TRIM(poblacion)) = LOWER(TRIM(?))
        """
        comerciales_asignados = pd.read_sql(query, conn, params=(municipio_sel, poblacion_sel))
        conn.close()

        # Filtrar según contexto
        excluir = context.get("excluir_comerciales", [])
        if excluir:
            comerciales_asignados = comerciales_asignados[
                ~comerciales_asignados['comercial'].str.lower().isin(excluir)
            ]

        if comerciales_asignados.empty:
            st.warning("No hay comerciales asignados a esta zona.")
            return

        comercial_a_eliminar = st.selectbox(
            "Seleccione el comercial a desasignar",
            comerciales_asignados["comercial"].tolist()
        )

        if st.button("Desasignar Comercial de Zona"):
            conn = get_db_connection()
            cursor = conn.cursor()

            # Verificar si hay registros bloqueados por Contrato != 'Pendiente'
            cursor.execute(
                """
                SELECT COUNT(*) FROM comercial_rafa
                WHERE municipio = ? AND poblacion = ? AND comercial = ? AND Contrato != 'Pendiente'
                """,
                (municipio_sel, poblacion_sel, comercial_a_eliminar)
            )
            bloqueados = cursor.fetchone()[0]

            # Guardar en tabla temporal
            cursor.execute(
                """
                INSERT INTO puntos_liberados_temp
                SELECT * FROM comercial_rafa
                WHERE municipio = ? AND poblacion = ? AND comercial = ?
                """,
                (municipio_sel, poblacion_sel, comercial_a_eliminar)
            )

            # Eliminar
            cursor.execute(
                "DELETE FROM comercial_rafa WHERE municipio = ? AND poblacion = ? AND comercial = ?",
                (municipio_sel, poblacion_sel, comercial_a_eliminar)
            )
            eliminados = cursor.rowcount
            conn.commit()

            if eliminados > 0:
                # Notificar al comercial
                cursor.execute("SELECT email FROM usuarios WHERE username = ?", (comercial_a_eliminar,))
                row = cursor.fetchone()
                email_comercial = row[0] if row else None
                if email_comercial:
                    try:
                        desc = f"📍 Se le ha desasignado la zona {municipio_sel} - {poblacion_sel}. Total puntos: {eliminados}"
                        correo_desasignacion_administracion(email_comercial, municipio_sel, poblacion_sel, desc)
                    except Exception as e:
                        st.warning(f"Error notificando a comercial: {e}")

                # Notificar a admins
                cursor.execute("SELECT email FROM usuarios WHERE role = 'admin'")
                admins = [r[0] for r in cursor.fetchall()]
                desc_admin = f"📢 Desasignación de zona.\nZona: {municipio_sel} - {poblacion_sel}\nComercial: {comercial_a_eliminar}\nPuntos: {eliminados}"
                for email in admins:
                    try:
                        correo_asignacion_administracion2(email, municipio_sel, poblacion_sel, desc_admin)
                    except Exception as e:
                        st.warning(f"Error notificando a admin {email}: {e}")

                st.success(f"✅ Zona desasignada. Total puntos eliminados: {eliminados}")
                if bloqueados > 0:
                    st.info(f"Nota: {bloqueados} registros tenían Contrato distinto de 'Pendiente' y se han movido a la tabla temporal.")
                log_trazabilidad(
                    username,
                    "Desasignación total",
                    f"Zona {municipio_sel}-{poblacion_sel} desasignada de {comercial_a_eliminar} - {eliminados} eliminados"
                )
            else:
                st.info("No había puntos para desasignar.")
            conn.close()


def _mostrar_mapa(datos_uis: pd.DataFrame, comercial_rafa: pd.DataFrame):
    """Renderiza el mapa con los puntos filtrados."""
    with st.spinner("⏳ Cargando mapa..."):
        if datos_uis.empty:
            st.warning("No hay datos para mostrar en el mapa.")
            return

        center = [datos_uis.iloc[0]['latitud'], datos_uis.iloc[0]['longitud']]
        zoom_start = 12
        if "municipio_sel" in st.session_state and "poblacion_sel" in st.session_state:
            zone_data = datos_uis[
                (datos_uis["municipio"] == st.session_state["municipio_sel"]) &
                (datos_uis["poblacion"] == st.session_state["poblacion_sel"])
            ]
            if not zone_data.empty:
                center = [zone_data["latitud"].mean(), zone_data["longitud"].mean()]
                zoom_start = 14

        m = folium.Map(
            location=center,
            zoom_start=zoom_start,
            tiles="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}",
            attr="Google"
        )
        cluster = MarkerCluster(disableClusteringAtZoom=18, maxClusterRadius=70, spiderfyOnMaxZoom=True).add_to(m)

        # Filtrar puntos para el mapa (solo los de la zona seleccionada)
        if "municipio_sel" in st.session_state and "poblacion_sel" in st.session_state:
            datos_filtrados = datos_uis[
                (datos_uis["municipio"] == st.session_state["municipio_sel"]) &
                (datos_uis["poblacion"] == st.session_state["poblacion_sel"])
            ]
        else:
            datos_filtrados = pd.DataFrame()  # No mostrar puntos si no hay filtros

        for _, row in datos_filtrados.iterrows():
            lat, lon = row['latitud'], row['longitud']
            apt = row['apartment_id']
            vial = row.get('vial', 'No Disponible')
            numero = row.get('numero', 'No Disponible')
            letra = row.get('letra', 'No Disponible')
            serv_uis = str(row.get('serviciable', '')).strip().lower()

            oferta = comercial_rafa[comercial_rafa['apartment_id'] == apt]
            color = 'blue'

            if not oferta.empty:
                incidencia = str(oferta.iloc[0].get('incidencia', '')).strip().lower()
                if incidencia == 'sí':
                    color = 'purple'
                else:
                    oferta_serv = str(oferta.iloc[0].get('serviciable', '')).strip().lower()
                    contrato = str(oferta.iloc[0].get('Contrato', '')).strip().lower()

                    if serv_uis == "si":
                        color = 'green'
                    elif serv_uis == "no" or oferta_serv == "no":
                        color = 'red'
                    elif contrato == "sí":
                        color = 'orange'
                    elif contrato == "no interesado":
                        color = 'black'

            tipo_olt = str(row.get('tipo_olt_rental', '')).strip()
            icon_name = "cloud" if "CTO VERDE" in tipo_olt else "info-circle"

            popup = f"""
                <b>Apartment ID:</b> {apt}<br>
                <b>Vial:</b> {vial}<br>
                <b>Número:</b> {numero}<br>
                <b>Letra:</b> {letra}<br>
            """
            folium.Marker(
                [lat, lon],
                icon=folium.Icon(icon=icon_name, color=color, prefix="fa"),
                popup=folium.Popup(popup, max_width=300)
            ).add_to(cluster)

        # Leyenda
        legend_html = """
        {% macro html(this, kwargs) %}
        <div style="position: fixed; bottom: 0px; left: 0px; width: 190px; z-index:9999; font-size:14px;
                    background-color: white; border:2px solid grey; border-radius:8px; padding: 10px;
                    box-shadow: 2px 2px 6px rgba(0,0,0,0.3);">
            <b>Leyenda</b><br>
            <i style="color:green;">●</i> Serviciable y Finalizado<br>
            <i style="color:red;">●</i> No serviciable<br>
            <i style="color:orange;">●</i> Contrato Sí<br>
            <i style="color:black;">●</i> No interesado<br>
            <i style="color:purple;">●</i> Incidencia<br>
            <i style="color:blue;">●</i> No Visitado<br>
            <i class="fa fa-cloud" style="color:#2C5A2E;"></i> CTO VERDE<br>
            <i class="fa fa-info-circle" style="color:#2C5A2E;"></i> CTO COMPARTIDA
        </div>
        {% endmacro %}
        """
        macro = MacroElement()
        macro._template = Template(legend_html)
        m.get_root().add_child(macro)

        st_folium(m, height=500, width=700)


def mostrar_mapa_de_asignaciones():
    username = st.session_state.get("username", "").strip()
    with st.spinner("Cargando datos..."):
        datos_uis, comercial_rafa = cargar_datos(username)
        if datos_uis.empty:
            st.warning("⚠️ No hay datos disponibles para mostrar.")
            st.stop()

    with st.expander("📊 Información sobre el funcionamiento del mapa", expanded=False):
        st.info("""
        Por cuestiones de eficiencia, el mapa solo mostrará los puntos relativos a los filtros elegidos.
        Usa los filtros de Provincia, Municipio y Población para ver las zonas que necesites.
        """)

    datos_uis = _filtros_mapa(datos_uis)

    # Limpiar coordenadas
    datos_uis = datos_uis.dropna(subset=['latitud', 'longitud'])
    datos_uis['latitud'] = datos_uis['latitud'].astype(float)
    datos_uis['longitud'] = datos_uis['longitud'].astype(float)

    col1, col2 = st.columns([3, 3])
    with col2:
        accion = st.radio("Seleccione la acción requerida:", ["Asignar Zona", "Desasignar Zona"], key="accion")
        if accion == "Asignar Zona":
            _asignar_zona(datos_uis)
        else:
            _desasignar_zona()

        # Sección de reasignación de puntos liberados
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM puntos_liberados_temp")
        count = cursor.fetchone()[0]
        conn.close()

        if count > 0:
            st.subheader("Reasignar Puntos Liberados")
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT username FROM usuarios WHERE role = 'comercial_rafa'")
            lista_comerciales = [row[0] for row in cursor.fetchall()]
            conn.close()

            with st.form("reasignar_puntos_form"):
                nuevos_comerciales = st.multiselect("Selecciona comerciales para reasignar los puntos liberados", options=lista_comerciales)
                reasignar_btn = st.form_submit_button("Confirmar reasignación")

                if reasignar_btn and nuevos_comerciales:
                    try:
                        conn = get_db_connection()
                        cursor = conn.cursor()
                        cursor.execute(
                            """
                            SELECT apartment_id, provincia, municipio, poblacion, vial, numero, letra, cp, latitud, longitud, comercial, Contrato
                            FROM puntos_liberados_temp
                            """
                        )
                        puntos = cursor.fetchall()

                        if not puntos:
                            st.warning("⚠️ No hay puntos liberados.")
                        else:
                            total = len(puntos)
                            n = len(nuevos_comerciales)
                            reparto = {com: [] for com in nuevos_comerciales}
                            for i, p in enumerate(puntos):
                                reparto[nuevos_comerciales[i % n]].append(p)

                            for com, pts in reparto.items():
                                for p in pts:
                                    cursor.execute(
                                        """
                                        INSERT OR REPLACE INTO comercial_rafa
                                        (apartment_id, provincia, municipio, poblacion, vial, numero, letra, cp, latitud, longitud, comercial, Contrato)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                        """,
                                        (p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], com, 'Pendiente')
                                    )

                            cursor.execute("DELETE FROM puntos_liberados_temp")
                            conn.commit()

                            # Notificaciones
                            resumen = "\n".join([f"👤 {c}: {len(pts)} pts" for c, pts in reparto.items()])
                            for com, pts in reparto.items():
                                if pts:
                                    cursor.execute("SELECT email FROM usuarios WHERE username = ?", (com,))
                                    row = cursor.fetchone()
                                    if row:
                                        try:
                                            correo_asignacion_administracion2(
                                                row[0], pts[0][2], pts[0][3],
                                                f"📍 Ha recibido una nueva asignación.\n{resumen}"
                                            )
                                        except Exception as e:
                                            st.warning(f"Error notificando a {com}: {e}")

                            cursor.execute("SELECT email FROM usuarios WHERE role = 'admin'")
                            admins = [r[0] for r in cursor.fetchall()]
                            desc_admin = f"📢 Reasignación de zona.\n{resumen}"
                            for email in admins:
                                try:
                                    correo_asignacion_administracion2(email, "", "", desc_admin)
                                except Exception as e:
                                    st.warning(f"Error notificando a admin {email}: {e}")

                            st.success(f"✅ Puntos reasignados correctamente.\n{resumen}")
                            st.rerun()
                    except Exception as e:
                        if 'conn' in locals():
                            conn.rollback()
                        st.error(f"❌ Error al reasignar: {e}")
                    finally:
                        if 'conn' in locals():
                            conn.close()

    with col1:
        _mostrar_mapa(datos_uis, comercial_rafa)


# ==================== VER DATOS (descargas y visualización) ====================
def mostrar_descarga_datos():
    SUB_SECCIONES = {
        "Zonas asignadas": {"icon": "geo-alt", "description": "Total de asignaciones realizadas por el gestor."},
        "Ofertas realizadas": {"icon": "bar-chart-line", "description": "Ofertas comerciales asignadas."},
        "Viabilidades estudiadas": {"icon": "check2-square", "description": "Viabilidades reportadas."},
        "Datos totales": {"icon": "database", "description": "Visualización total de los datos"}
    }

    sub_seccion = option_menu(
        menu_title=None,
        options=list(SUB_SECCIONES.keys()),
        icons=[SUB_SECCIONES[s]["icon"] for s in SUB_SECCIONES],
        default_index=0,
        orientation="horizontal",
        styles={
            "container": {"padding": "0!important", "margin": "0px", "background-color": "#F0F7F2",
                          "border-radius": "0px", "max-width": "none"},
            "icon": {"color": "#2C5A2E", "font-size": "25px"},
            "nav-link": {"color": "#2C5A2E", "font-size": "18px", "text-align": "center", "margin": "0px",
                         "--hover-color": "#66B032", "border-radius": "0px"},
            "nav-link-selected": {"background-color": "#66B032", "color": "white", "font-weight": "bold"}
        }
    )

    username = st.session_state.get("username", "").strip().lower()
    context = get_user_context(username)

    conn = get_db_connection()

    if sub_seccion in ["Zonas asignadas", "Ofertas realizadas"]:
        # Cargar datos base
        assigned_zones = pd.read_sql("SELECT DISTINCT municipio, poblacion, comercial FROM comercial_rafa", conn)
        total_ofertas = pd.read_sql("SELECT * FROM comercial_rafa", conn)

        # Filtrar según contexto
        excluir = context.get("excluir_comerciales", [])
        if excluir:
            assigned_zones = assigned_zones[~assigned_zones['comercial'].str.lower().isin(excluir)]
            total_ofertas = total_ofertas[~total_ofertas['comercial'].str.lower().isin(excluir)]

        # Marcar contratos activos
        contratos = pd.read_sql(
            "SELECT apartment_id FROM seguimiento_contratos WHERE TRIM(LOWER(estado)) = 'finalizado'",
            conn
        )
        total_ofertas['Contrato_Activo'] = total_ofertas['apartment_id'].isin(contratos['apartment_id']).map(
            {True: '✅ Activo', False: '❌ No Activo'}
        )

    if sub_seccion == "Zonas asignadas":
        comerciales_mios = context.get("comerciales_permitidos", [])
        if comerciales_mios:
            zonas_filtradas = assigned_zones[assigned_zones['comercial'].str.lower().isin(comerciales_mios)]
        else:
            zonas_filtradas = assigned_zones

        if not zonas_filtradas.empty:
            resumen = zonas_filtradas.groupby("comercial").size().reset_index(name='total_zonas').sort_values('total_zonas', ascending=False)
            cols = st.columns(len(resumen))
            for i, row in enumerate(resumen.itertuples()):
                with cols[i]:
                    st.metric(label=row.comercial.title(), value=row.total_zonas)
            st.dataframe(zonas_filtradas, width='stretch')
        else:
            st.warning("No hay zonas asignadas para los comerciales de este gestor.")

    elif sub_seccion == "Ofertas realizadas":
        st.dataframe(total_ofertas, width='stretch')

    elif sub_seccion == "Viabilidades estudiadas":
        viabilidades = pd.read_sql("SELECT * FROM viabilidades ORDER BY id DESC", conn)
        excluir_v = context.get("excluir_viabilidades", [])
        if excluir_v:
            viabilidades['usuario'] = viabilidades['usuario'].fillna('').str.strip().str.lower()
            viabilidades = viabilidades[~viabilidades['usuario'].isin(excluir_v)]
        st.dataframe(viabilidades, width='stretch')

    elif sub_seccion == "Datos totales":
        if username == "juan":
            query = """
                SELECT apartment_id, address_id, provincia, municipio, poblacion,
                       vial, numero, parcela_catastral, letra, cp, olt, cto,
                       latitud, longitud, comercial
                FROM datos_uis
                WHERE LOWER(TRIM(provincia)) IN ('lugo', 'asturias')
            """
        elif username == "rafa sanz":
            query = """
                SELECT apartment_id, address_id, provincia, municipio, poblacion,
                       vial, numero, parcela_catastral, letra, cp, olt, cto,
                       latitud, longitud, comercial
                FROM datos_uis
                WHERE LOWER(TRIM(comercial)) = 'rafa sanz'
            """
        else:
            query = "SELECT * FROM datos_uis WHERE 1=0"  # vacío
        df_uis = pd.read_sql(query, conn)
        st.dataframe(df_uis, width='stretch', height=580)

    conn.close()


# ==================== VIABILIDADES (gestor) ====================
def mostrar_viabilidades():
    sub_seccion = option_menu(
        menu_title=None,
        options=["Viabilidades pendientes de confirmación", "Seguimiento de viabilidades", "Crear viabilidades"],
        icons=["exclamation-circle", "clipboard-check", "plus-circle"],
        default_index=0,
        orientation="horizontal",
        styles={
            "container": {"padding": "0!important", "margin": "0px", "background-color": "#F0F7F2",
                          "border-radius": "0px", "max-width": "none"},
            "icon": {"color": "#2C5A2E", "font-size": "25px"},
            "nav-link": {"color": "#2C5A2E", "font-size": "18px", "text-align": "center", "margin": "0px",
                         "--hover-color": "#66B032", "border-radius": "0px"},
            "nav-link-selected": {"background-color": "#66B032", "color": "white", "font-weight": "bold"}
        }
    )

    if sub_seccion == "Viabilidades pendientes de confirmación":
        _mostrar_viabilidades_pendientes()
    elif sub_seccion == "Seguimiento de viabilidades":
        _mostrar_seguimiento_viabilidades()
    elif sub_seccion == "Crear viabilidades":
        _crear_viabilidad()


def _mostrar_viabilidades_pendientes():
    username = st.session_state.get("username", "").strip().lower()
    context = get_user_context(username)

    conn = get_db_connection()

    # Construir consulta con exclusión
    query_base = """
        SELECT id, ticket, provincia, municipio, poblacion, vial, numero, letra,
               latitud, longitud, serviciable, resultado, justificacion, respuesta_comercial,
               usuario AS comercial_reporta, confirmacion_rafa
        FROM viabilidades
        WHERE (confirmacion_rafa IS NULL OR confirmacion_rafa = '')
    """
    params = []

    excluir = context.get("excluir_viabilidades", [])
    if excluir:
        placeholders = ",".join(["?"] * len(excluir))
        query_base += f" AND LOWER(usuario) NOT IN ({placeholders})"
        params = [e.lower() for e in excluir]

    df_viab = pd.read_sql(query_base, conn, params=params)

    # Obtener lista de comerciales y admins
    comerciales_rafa = pd.read_sql("SELECT username FROM usuarios WHERE role = 'comercial_rafa'", conn)["username"].tolist()
    admins = pd.read_sql("SELECT email FROM usuarios WHERE role = 'admin'", conn)["email"].tolist()
    conn.close()

    with st.expander("🧭 Guía para la gestión de viabilidades", expanded=False):
        st.info("Revisa las viabilidades pendientes. Puedes confirmarlas o reasignarlas.")

    if df_viab.empty:
        st.success("🎉 No hay viabilidades pendientes de confirmación.")
        return

    # Mapa de viabilidades pendientes
    df_viab_map = df_viab.dropna(subset=['latitud', 'longitud']).copy()
    if not df_viab_map.empty:
        center = [df_viab_map['latitud'].mean(), df_viab_map['longitud'].mean()]
        m = folium.Map(location=center, zoom_start=12,
                       tiles="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}", attr="Google")
        cluster = MarkerCluster().add_to(m)
        for _, row in df_viab_map.iterrows():
            popup = f"""
                <b>ID Viabilidad:</b> {row.id}<br>
                <b>Comercial:</b> {row.comercial_reporta}<br>
                <b>Provincia:</b> {row.provincia}<br>
                <b>Municipio:</b> {row.municipio}<br>
                <b>Población:</b> {row.poblacion}<br>
                <b>Vial:</b> {row.vial}<br>
                <b>Número:</b> {row.numero}{row.letra or ''}<br>
                <b>Serviciable:</b> {row.serviciable or 'Sin dato'}
            """
            folium.Marker(
                [row['latitud'], row['longitud']],
                icon=folium.Icon(icon="info-sign", color="blue"),
                popup=folium.Popup(popup, max_width=300)
            ).add_to(cluster)
        st_folium(m, height=500, width=1750)

    # Inicializar conjunto de ocultas si no existe
    if 'viabilidades_ocultas' not in st.session_state:
        st.session_state.viabilidades_ocultas = set()

    for _, row in df_viab.iterrows():
        if row.id in st.session_state.viabilidades_ocultas:
            continue
        with st.expander(
                f"ID {row.id} — {row.municipio} / {row.vial} {row.numero}{row.letra or ''}",
                expanded=False
        ):
            st.markdown(
                f"**Comercial:** {row.comercial_reporta}<br>"
                f"**Resultado:** {row.resultado or 'Sin dato'}<br>"
                f"**Justificación:** {row.justificacion or 'Sin justificación'}<br>"
                f"**Respuesta Oficina:** {row.respuesta_comercial or 'Sin respuesta'}<br>",
                unsafe_allow_html=True
            )
            if pd.notna(row.latitud) and pd.notna(row.longitud):
                maps_url = f"https://www.google.com/maps/search/?api=1&query={row.latitud},{row.longitud}"
                st.markdown(f"[🌍 Ver en GoogleMaps]({maps_url})")

            col_ok, col_rea = st.columns([1, 2])
            with col_ok:
                comentarios = st.text_area("💬 Comentarios gestor", key=f"coment_{row.id}", placeholder="Opcional...")
                if st.button("✅ Confirmar", key=f"ok_{row.id}"):
                    try:
                        conn = get_db_connection()
                        cursor = conn.cursor()
                        cursor.execute(
                            "UPDATE viabilidades SET confirmacion_rafa = 'OK', comentarios_gestor = ? WHERE id = ?",
                            (comentarios, row.id)
                        )
                        conn.commit()
                        conn.close()
                        for email in admins:
                            try:
                                correo_confirmacion_viab_admin(email, row.id, row.comercial_reporta)
                            except Exception as e:
                                st.warning(f"Error notificando a admin {email}: {e}")
                        st.success(f"Viabilidad {row.id} confirmada.")
                        st.session_state.viabilidades_ocultas.add(row.id)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error al confirmar: {e}")

            with col_rea:
                destinos = [c for c in comerciales_rafa if c != row.comercial_reporta]
                nuevo_com = st.selectbox("🔄 Reasignar a", options=[""] + destinos, key=f"sel_{row.id}")
                if st.button("↪️ Reasignar", key=f"reasig_{row.id}"):
                    if not nuevo_com:
                        st.warning("Selecciona un comercial.")
                    else:
                        try:
                            conn = get_db_connection()
                            cursor = conn.cursor()
                            cursor.execute(
                                "UPDATE viabilidades SET usuario = ?, confirmacion_rafa = 'Reasignada' WHERE id = ?",
                                (nuevo_com, row.id)
                            )
                            conn.commit()
                            conn.close()
                            try:
                                correo_reasignacion_saliente(row.comercial_reporta, row.id, nuevo_com)
                                correo_reasignacion_entrante(nuevo_com, row.id, row.comercial_reporta)
                            except Exception as e:
                                st.warning(f"Error enviando notificaciones: {e}")
                            st.success(f"Viabilidad {row.id} reasignada a {nuevo_com}.")
                            st.session_state.viabilidades_ocultas.add(row.id)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al reasignar: {e}")


def _mostrar_seguimiento_viabilidades():
    username = st.session_state.get("username", "").strip().lower()
    context = get_user_context(username)
    conn = get_db_connection()
    viabilidades = pd.read_sql("SELECT * FROM viabilidades ORDER BY id DESC", conn)
    conn.close()
    excluir = context.get("excluir_viabilidades", [])
    if excluir:
        viabilidades['usuario'] = viabilidades['usuario'].fillna('').str.strip().str.lower()
        viabilidades = viabilidades[~viabilidades['usuario'].isin(excluir)]
    st.info("ℹ️ Listado completo de viabilidades y su estado actual.")
    st.dataframe(viabilidades, width='stretch')


def _crear_viabilidad():
    st.info("🆕 Aquí podrás crear nuevas viabilidades manualmente.")
    st.markdown("**Leyenda:** ⚫ existente, 🔵 nueva, 🟢 serviciable, 🔴 no serviciable")

    # Inicializar estado
    for key in ["viabilidad_marker", "map_center", "map_zoom"]:
        if key not in st.session_state:
            st.session_state[key] = None if key == "viabilidad_marker" else (
                (43.463444, -3.790476) if "center" in key else 12
            )

    m = folium.Map(
        location=st.session_state.map_center,
        zoom_start=st.session_state.map_zoom,
        tiles="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}",
        attr="Google"
    )

    # Puntos existentes
    viabilidades = obtener_viabilidades()
    for v in viabilidades:
        lat, lon, ticket, serviciable, apartment_id = v
        serv = str(serviciable).strip() if serviciable else ""
        apt = str(apartment_id).strip() if apartment_id else ""
        if serv == "No":
            color = "red"
        elif serv == "Sí" and apt not in ["", "N/D"]:
            color = "green"
        else:
            color = "black"
        folium.Marker([lat, lon], icon=folium.Icon(color=color), popup=f"Ticket: {ticket}").add_to(m)

    # Marcador nuevo
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
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT id_olt, nombre_olt FROM olt ORDER BY nombre_olt")
            lista_olt = [f"{fila[0]}. {fila[1]}" for fila in cursor.fetchall()]
            conn.close()
            with col12:
                olt = st.selectbox("🏢 OLT", options=lista_olt)
            with col13:
                apartment_id = st.text_input("🏘️ Apartment ID")

            comentario = st.text_area("📝 Comentario")

            # Selección de comercial según usuario
            usuario_actual = st.session_state.get("username", "")
            if usuario_actual == "rafa sanz":
                opciones_com = ["roberto", "nestor", "rafaela", "jose ramon", "rafa sanz"]
            elif usuario_actual == "juan":
                opciones_com = ["juan", "Comercial2", "Comercial3"]
            else:
                opciones_com = [usuario_actual]
            comercial = st.selectbox("🧑‍💼 Comercial responsable", options=opciones_com)

            submit = st.form_submit_button("Enviar Formulario")

            if submit:
                ticket = generar_ticket()
                guardar_viabilidad((
                    lat, lon, provincia, municipio, poblacion, vial, numero, letra,
                    cp, comentario, ticket, nombre_cliente, telefono, comercial,
                    olt, apartment_id
                ))
                st.session_state.viabilidad_marker = None
                st.session_state.map_center = (43.463444, -3.790476)
                st.rerun()


# ==================== DESCARGA DE DATOS ====================
def download_datos(datos_uis, total_ofertas, viabilidades):
    st.info("ℹ️ Dependiendo del tamaño, la descarga puede tardar.")
    dataset_opcion = st.selectbox("¿Qué deseas descargar?", ["Datos", "Ofertas asignadas", "Viabilidades", "Todo"])
    nombre_base = st.text_input("Nombre base del archivo:", "datos")
    fecha_actual = datetime.now().strftime("%Y-%m-%d")
    nombre_archivo_final = f"{nombre_base}_{fecha_actual}"

    username = st.session_state.get("username", "").strip().lower()
    context = get_user_context(username)

    # Aplicar filtros personalizados si es Juan
    if username == "juan":
        datos_filtrados = datos_uis[datos_uis['provincia'].str.strip().str.lower().isin(["lugo", "asturias"])]
        excluir = context.get("excluir_comerciales", [])
        if excluir:
            total_ofertas = total_ofertas[~total_ofertas['comercial'].str.lower().isin(excluir)]
        excluir_v = context.get("excluir_viabilidades", [])
        if excluir_v:
            viabilidades = viabilidades[~viabilidades['usuario'].str.lower().isin(excluir_v)]
        viabilidades['fecha_viabilidad'] = pd.to_datetime(viabilidades['fecha_viabilidad'], errors='coerce')
    else:
        datos_filtrados = datos_uis.copy()
        ofertas_filtradas = total_ofertas.copy()
        viabilidades_filtradas = viabilidades.copy()

    def descargar_excel(dfs_dict, nombre_archivo):
        with io.BytesIO() as output:
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                for sheet_name, df in dfs_dict.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
            output.seek(0)
            st.download_button(
                label=f"📥 Descargar {nombre_archivo}",
                data=output,
                file_name=f"{nombre_archivo}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    if dataset_opcion == "Datos":
        try:
            conn = get_db_connection()
            query = """
                SELECT apartment_id, address_id, provincia, municipio, poblacion, vial, numero,
                       parcela_catastral, letra, cp, cto_id, olt, latitud, longitud, tipo_olt_rental
                FROM datos_uis
            """
            df_db = pd.read_sql_query(query, conn)
            conn.close()
            if username == "juan":
                df_db = df_db[df_db['provincia'].str.strip().str.lower().isin(["lugo", "asturias"])]
            descargar_excel({"Datos Gestor": df_db}, nombre_archivo_final)
        except Exception as e:
            st.error(f"Error al obtener datos: {e}")

    elif dataset_opcion == "Ofertas asignadas":
        descargar_excel({"Ofertas Asignadas": total_ofertas}, nombre_archivo_final)

    elif dataset_opcion == "Viabilidades":
        descargar_excel({"Viabilidades": viabilidades}, nombre_archivo_final)

    elif dataset_opcion == "Todo":
        descargar_excel({
            "Datos Gestor": datos_filtrados,
            "Ofertas Asignadas": total_ofertas,
            "Viabilidades": viabilidades
        }, nombre_archivo_final)


# ==================== FUNCIÓN PRINCIPAL ====================
def mapa_dashboard():
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

    st.sidebar.markdown(f"""
        <div style="text-align:center;">
            <div style="width:100px; height:100px; border-radius:50%; background-color:#0073e6; color:white;
                        font-size:50px; display:flex; align-items:center; justify-content:center; margin:0 auto;">
                👤
            </div>
            <div style="margin-top:10px; font-weight:bold;">Rol: Gestor Comercial</div>
            <div style="font-weight:bold; font-size:18px;">¡Bienvenido, {st.session_state['username']}!</div>
            <hr>
        </div>
    """, unsafe_allow_html=True)

    datos_uis, comercial_rafa = cargar_datos()
    total_ofertas = cargar_total_ofertas()
    viabilidades = cargar_viabilidades()

    with st.sidebar:
        opcion = option_menu(
            menu_title=None,
            options=["Mapa Asignaciones", "Viabilidades", "Ver Datos", "Buscar Coordenadas", "Descargar Datos", "Soporte"],
            icons=["globe", "check-circle", "bar-chart", "compass", "download", "ticket"],
            menu_icon="list",
            default_index=0,
            styles={
                "container": {"padding": "0px", "background-color": "#F0F7F2"},
                "icon": {"color": "#2C5A2E", "font-size": "18px"},
                "nav-link": {"color": "#2C5A2E", "font-size": "16px", "text-align": "left",
                             "margin": "0px", "--hover-color": "#66B032", "border-radius": "0px"},
                "nav-link-selected": {"background-color": "#66B032", "color": "white", "font-weight": "bold"}
            }
        )

        if st.button("Cerrar sesión"):
            detalles = f"El gestor {st.session_state.get('username', 'N/A')} cerró sesión."
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

    # Lógica principal
    if opcion == "Mapa Asignaciones":
        mostrar_ultimo_anuncio()
        mostrar_mapa_de_asignaciones()
    elif opcion == "Viabilidades":
        mostrar_viabilidades()
    elif opcion == "Ver Datos":
        mostrar_descarga_datos()
    elif opcion == "Buscar Coordenadas":
        mostrar_coordenadas()
    elif opcion == "Descargar Datos":
        download_datos(datos_uis, total_ofertas, viabilidades)
    elif opcion == "Soporte":
        mostrar_soporte_gestor_comercial()


if __name__ == "__main__":
    mapa_dashboard()