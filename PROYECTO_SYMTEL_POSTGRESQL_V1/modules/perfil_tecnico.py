# tecnico_dashboard.py
import time
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

import pandas as pd
from modules.db import get_db_connection as _db_conn
import streamlit as st
from streamlit_cookies_controller import CookieController
from streamlit_option_menu import option_menu

from modules import login
from modules.notificaciones import notificar_creacion_ticket, notificar_asignacion_ticket

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


# ==================== ACTUALIZAR ESTADO DE TICKET ====================
def actualizar_estado_ticket(ticket_id: int, nuevo_estado: str) -> bool:
    """Actualiza el estado de un ticket y registra la acción."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT titulo, estado FROM tickets WHERE ticket_id = %s", (ticket_id,))
        ticket_info = cursor.fetchone()
        if not ticket_info:
            st.error(f"❌ Ticket #{ticket_id} no encontrado")
            return False

        titulo, estado_anterior = ticket_info

        # Verificar si existe campo fecha_cierre
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'tickets'")
        columnas = [col[0] for col in cursor.fetchall()]
        tiene_fecha_cierre = "fecha_cierre" in columnas

        if nuevo_estado in ["Resuelto", "Cancelado"] and tiene_fecha_cierre:
            cursor.execute(
                "UPDATE tickets SET estado = %s, fecha_cierre = CURRENT_TIMESTAMP WHERE ticket_id = %s",
                (nuevo_estado, ticket_id),
            )
        else:
            cursor.execute(
                "UPDATE tickets SET estado = %s WHERE ticket_id = %s",
                (nuevo_estado, ticket_id),
            )

        # Añadir comentario automático
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        username = st.session_state.get("username", "Usuario")

        cursor.execute("SELECT asignado_a, usuario_id FROM tickets WHERE ticket_id = %s", (ticket_id,))
        asignacion = cursor.fetchone()
        contexto = "técnico" if asignacion and asignacion[0] == st.session_state.get("user_id") else "usuario"

        comentario = f"\n\n[{timestamp}] {username} ({contexto}) cambió el estado de '{estado_anterior}' a '{nuevo_estado}'."
        cursor.execute(
            "UPDATE tickets SET comentarios = COALESCE(comentarios || %s, %s) WHERE ticket_id = %s",
            (comentario, comentario.strip(), ticket_id),
        )

        conn.commit()
        conn.close()

        log_trazabilidad(
            username,
            "Actualización de estado de ticket",
            f"Cambió estado del ticket #{ticket_id} ('{titulo}') de '{estado_anterior}' a '{nuevo_estado}'",
        )

        st.success(f"✅ Ticket #{ticket_id} actualizado a '{nuevo_estado}'")
        return True

    except Exception as e:
        st.error(f"⚠️ Error al actualizar ticket #{ticket_id}: {e}")
        return False


# ==================== MIS TICKETS (SUBFUNCIONES) ====================
def _mostrar_resumen_tickets(df_tickets: pd.DataFrame):
    """Muestra el resumen estadístico de tickets."""
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Asignados", len(df_tickets))
    with col2:
        cancelados = len(df_tickets[df_tickets["estado"] == "Cancelado"])
        st.metric("Cancelados", cancelados)
    with col3:
        progreso = len(df_tickets[df_tickets["estado"] == "En Progreso"])
        st.metric("En Progreso", progreso)
    with col4:
        abiertos = len(df_tickets[df_tickets["estado"] == "Abierto"])
        st.metric("Abiertos", abiertos)
    with col5:
        resueltos = len(df_tickets[df_tickets["estado"] == "Resuelto"])
        st.metric("Resueltos", resueltos)


def _mostrar_filtros_tickets(df_tickets: pd.DataFrame) -> pd.DataFrame:
    """Muestra los filtros rápidos y devuelve el DataFrame filtrado."""
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        estados = st.multiselect(
            "Estado",
            options=df_tickets["estado"].unique(),
            default=df_tickets["estado"].unique(),
        )
    with col_f2:
        prioridades = st.multiselect(
            "Prioridad",
            options=df_tickets["prioridad"].unique(),
            default=df_tickets["prioridad"].unique(),
        )
    with col_f3:
        categorias = st.multiselect(
            "Categoría",
            options=df_tickets["categoria"].unique(),
            default=df_tickets["categoria"].unique(),
        )
    mask = (
        df_tickets["estado"].isin(estados)
        & df_tickets["prioridad"].isin(prioridades)
        & df_tickets["categoria"].isin(categorias)
    )
    return df_tickets[mask]


def _render_ticket_expander(ticket: pd.Series):
    """Renderiza un ticket individual en un expander con pestañas."""
    fecha_creacion = pd.to_datetime(ticket["fecha_creacion"])
    dias = (datetime.now() - fecha_creacion).days

    if dias > 7:
        borde_color = "#FF0000"
        antiguedad = f"⚠️ {dias} días"
    elif dias > 3:
        borde_color = "#FF9900"
        antiguedad = f"📅 {dias} días"
    else:
        borde_color = "#4CAF50"
        antiguedad = f"🆕 {dias} días"

    prioridad_icono = {"Alta": "🔴", "Media": "🟡", "Baja": "🟢"}.get(ticket["prioridad"], "⚪")
    estado_icono = {
        "Abierto": "📥",
        "En Progreso": "⚙️",
        "Resuelto": "✅",
        "Cancelado": "🔒",
    }.get(ticket["estado"], "📋")

    with st.expander(
        f"{estado_icono} {prioridad_icono} Ticket #{ticket['ticket_id']}: {ticket['titulo']}"
    ):
        col_h1, col_h2 = st.columns([2, 1])
        with col_h1:
            st.markdown(f"**📅 Creado:** {fecha_creacion.strftime('%d/%m/%Y %H:%M')}")
            st.markdown(f"**👤 Reportado por:** {ticket['reportado_por']}")
            st.markdown(f"**🏷️ Categoría:** {ticket['categoria']}")
        with col_h2:
            st.markdown(f"**🚨 Prioridad:** {ticket['prioridad']}")
            st.markdown(f"**📊 Estado:** {ticket['estado']}")
            st.markdown(f"**⏳ Antigüedad:** {antiguedad}")

        st.markdown("---")
        tab_desc, tab_com, tab_acc = st.tabs(["📄 Descripción", "💬 Comentarios", "🔧 Acciones"])

        with tab_desc:
            st.info(ticket["descripcion"])

        with tab_com:
            if ticket["comentarios"]:
                for comentario in ticket["comentarios"].split("\n\n"):
                    if comentario.strip():
                        if "(cliente):" in comentario or "Ticket creado por" in comentario:
                            st.info(comentario.strip())
                        else:
                            st.warning(comentario.strip())
            else:
                st.info("No hay comentarios aún.")

            st.markdown("---")
            st.markdown("**💬 Añadir comentario:**")
            with st.form(key=f"comentario_form_{ticket['ticket_id']}"):
                nuevo_comentario = st.text_area(
                    "Escribe tu comentario:",
                    placeholder="Actualización, solución, preguntas para el cliente...",
                    height=120,
                    key=f"comentario_{ticket['ticket_id']}",
                )
                tipo_comentario = st.selectbox(
                    "Tipo de comentario:",
                    ["Actualización", "Pregunta al cliente", "Solución", "Esperando respuesta"],
                    key=f"tipo_{ticket['ticket_id']}",
                )
                es_interno = st.checkbox(
                    "Comentario interno (solo visible para equipo)",
                    key=f"interno_{ticket['ticket_id']}",
                )

                if st.form_submit_button("💬 Enviar comentario", use_container_width=True):
                    if nuevo_comentario.strip():
                        try:
                            conn = get_db_connection()
                            cursor = conn.cursor()
                            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
                            usuario = st.session_state.get("username", "Técnico")
                            tipo = (
                                f"[{tipo_comentario}]"
                                if not es_interno
                                else f"[{tipo_comentario} - INTERNO]"
                            )
                            comentario_formateado = f"\n\n[{timestamp}] {usuario} {tipo}:\n{nuevo_comentario.strip()}"
                            cursor.execute(
                                """
                                UPDATE tickets 
                                SET comentarios = COALESCE(comentarios || %s, %s)
                                WHERE ticket_id = %s
                                """,
                                (
                                    comentario_formateado,
                                    f"[{timestamp}] {usuario} {tipo}:\n{nuevo_comentario.strip()}",
                                    ticket["ticket_id"],
                                ),
                            )
                            conn.commit()
                            conn.close()
                            log_trazabilidad(
                                usuario,
                                "Comentario en ticket (técnico)",
                                f"Añadió comentario al ticket #{ticket['ticket_id']}",
                            )
                            st.success("✅ Comentario añadido")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error al añadir comentario: {e}")

        with tab_acc:
            st.markdown("**⚡ Acciones disponibles:**")
            col_est1, col_est2 = st.columns(2)
            with col_est1:
                idx = (
                    0
                    if ticket["estado"] == "Abierto"
                    else 1
                    if ticket["estado"] == "En Progreso"
                    else 2
                    if ticket["estado"] == "Resuelto"
                    else 3
                )
                nuevo_estado = st.selectbox(
                    "Cambiar estado:",
                    ["Abierto", "En Progreso", "Resuelto", "Cancelado"],
                    index=idx,
                    key=f"cambiar_estado_{ticket['ticket_id']}",
                )
            with col_est2:
                if st.button("🔄 Actualizar estado", key=f"btn_estado_{ticket['ticket_id']}"):
                    if nuevo_estado != ticket["estado"]:
                        actualizar_estado_ticket(ticket["ticket_id"], nuevo_estado)
                        st.rerun()

            st.markdown("---")
            col_acc1, col_acc2 = st.columns(2)
            with col_acc1:
                if st.button("📧 Solicitar más info", key=f"solicitar_{ticket['ticket_id']}"):
                    try:
                        conn = get_db_connection()
                        cursor = conn.cursor()
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
                        mensaje = f"\n\n[{timestamp}] {st.session_state['username']} [PREGUNTA AL CLIENTE]:\nSolicito más información para poder resolver este ticket. Por favor, proporcione detalles adicionales sobre el problema."
                        cursor.execute(
                            """
                            UPDATE tickets 
                            SET comentarios = COALESCE(comentarios || %s, %s),
                                estado = 'En Progreso'
                            WHERE ticket_id = %s
                            """,
                            (
                                mensaje,
                                mensaje.strip(),
                                ticket["ticket_id"],
                            ),
                        )
                        conn.commit()
                        conn.close()
                        st.success("✅ Solicitud de información enviada")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error: {e}")


def mis_tickets():
    """Muestra los tickets asignados al técnico actual."""
    st.title("🎫 Mis Tickets Asignados")
    st.markdown("---")

    user_id = st.session_state.get("user_id")
    if not user_id:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM usuarios WHERE username = %s", (st.session_state["username"],))
            result = cursor.fetchone()
            conn.close()
            if result:
                user_id = int(result[0])
            else:
                st.error("❌ No se pudo identificar al usuario.")
                return
        except Exception as e:
            st.error(f"❌ Error al obtener información del usuario: {e}")
            return

    try:
        conn = get_db_connection()
        query = """
        SELECT 
            t.ticket_id,
            t.fecha_creacion,
            u.username as reportado_por,
            t.categoria,
            t.prioridad,
            t.estado,
            t.titulo,
            t.descripcion,
            t.comentarios,
            t.usuario_id as id_cliente
        FROM tickets t
        LEFT JOIN usuarios u ON t.usuario_id = u.id
        WHERE t.asignado_a = %s
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
    except Exception as e:
        st.error(f"❌ Error al cargar tickets: {e}")
        return

    if df_tickets.empty:
        st.success("✅ ¡Genial! No tienes tickets asignados en este momento.")
        st.info("Los tickets que te asigne el administrador aparecerán aquí.")
        return

    _mostrar_resumen_tickets(df_tickets)
    df_filtrado = _mostrar_filtros_tickets(df_tickets)

    st.markdown(f"### 📋 Tickets Asignados ({len(df_filtrado)})")
    for _, ticket in df_filtrado.iterrows():
        _render_ticket_expander(ticket)

    # Estadísticas al final
    if len(df_filtrado) > 0:
        st.markdown("---")
        st.markdown("### 📈 Estadísticas")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.write("**Por estado:**")
            st.dataframe(df_filtrado["estado"].value_counts())
        with col2:
            st.write("**Por prioridad:**")
            st.dataframe(df_filtrado["prioridad"].value_counts())
        with col3:
            abiertos = df_filtrado[df_filtrado["estado"].isin(["Abierto", "En Progreso"])].copy()
            if not abiertos.empty:
                abiertos["dias"] = (datetime.now() - pd.to_datetime(abiertos["fecha_creacion"])).dt.days
                st.metric("Promedio días abierto", f"{abiertos['dias'].mean():.1f} días")


# ==================== CREAR TICKET (SUBFUNCIONES) ====================
@st.cache_data(ttl=600)
def _cargar_usuarios_asignables() -> pd.DataFrame:
    """Carga la lista de usuarios que pueden recibir tickets."""
    conn = get_db_connection()
    df = pd.read_sql(
        """
        SELECT id, username, role, email 
        FROM usuarios 
        WHERE role IN ('admin', 'tecnico', 'agent', 'soporte', 'comercial')
        ORDER BY username
        """,
        conn,
    )
    conn.close()
    return df


def crear_tickets():
    """Permite al técnico crear tickets internos o para otros usuarios."""
    st.title("➕ Crear Ticket (Modo Técnico)")
    st.markdown("---")
    st.info(
        """
    **Modo Técnico:** 
    Como técnico, puedes crear tickets para:
    - 📋 **Problemas internos** (equipo, servidores, sistemas)
    - 🔧 **Seguimiento de tareas** 
    - 👥 **Derivar trabajo** a otro compañero
    - 📝 **Documentar incidencias** técnicas
    """
    )

    ticket_creado = False
    ticket_id = None
    resumen = None

    with st.form("form_ticket_tecnico"):
        st.markdown("### 📝 Información del Ticket")
        titulo = st.text_input(
            "**Título/Asunto** *",
            placeholder="Ej: Problema con el servidor de base de datos",
        )
        col_cat, col_pri = st.columns(2)
        with col_cat:
            categoria = st.selectbox(
                "**Categoría** *",
                [
                    "Problema Técnico Interno",
                    "Tarea de Mantenimiento",
                    "Solicitud de Equipo",
                    "Documentación",
                    "Capacitación",
                    "Otro",
                ],
            )
        with col_pri:
            prioridad = st.selectbox("**Prioridad** *", ["Baja", "Media", "Alta"])

        st.markdown("### 👤 Asignación")
        try:
            usuarios_df = _cargar_usuarios_asignables()
            opciones = ["Sin asignar (abierto)"] + usuarios_df["username"].tolist()
            usuario_asignado = st.selectbox(
                "Asignar a (opcional):",
                options=opciones,
                index=0,
                help="Deja 'Sin asignar' para que el administrador lo asigne después",
            )
            asignado_id = None
            asignado_email = None
            asignado_username = None
            if usuario_asignado != "Sin asignar (abierto)":
                info = usuarios_df[usuarios_df["username"] == usuario_asignado].iloc[0]
                asignado_id = int(info["id"])
                asignado_email = info["email"]
                asignado_username = usuario_asignado
        except Exception as e:
            st.warning(f"No se pudo cargar la lista de usuarios: {e}")
            asignado_id = asignado_email = asignado_username = None
            usuario_asignado = "Sin asignar (abierto)"

        st.markdown("### 📄 Descripción Detallada *")
        descripcion = st.text_area(
            label="",
            placeholder="""Describe el problema o tarea con todo detalle:

• ¿Qué está ocurriendo?
• ¿Cuándo comenzó?
• ¿Qué sistemas/componentes están afectados?
• ¿Qué impacto tiene?
• ¿Qué se ha intentado hasta ahora?

Si es una tarea:
• Objetivo:
• Pasos requeridos:
• Recursos necesarios:
• Plazo estimado:""",
            height=250,
            label_visibility="collapsed",
        )

        with st.expander("🔧 Información Técnica (opcional)"):
            col_tech1, col_tech2 = st.columns(2)
            with col_tech1:
                sistema_afectado = st.selectbox(
                    "Sistema afectado:",
                    ["Base de datos", "Servidor web", "API", "Frontend", "Backend", "Infraestructura", "Otro"],
                )
                entorno = st.selectbox("Entorno:", ["Producción", "Desarrollo", "Testing", "Staging"])
            with col_tech2:
                urgencia = st.select_slider("Nivel de urgencia:", options=["Baja", "Media", "Alta", "Crítica"])
                tiempo_estimado = st.number_input(
                    "Tiempo estimado (horas):", min_value=0.5, max_value=100.0, value=2.0, step=0.5
                )

        st.markdown("---")
        st.markdown("**\* Campos obligatorios**")

        col_btn1, col_btn2 = st.columns([1, 1])
        with col_btn1:
            enviar = st.form_submit_button("✅ **Crear Ticket**", type="primary", use_container_width=True)
        with col_btn2:
            cancelar = st.form_submit_button("❌ **Cancelar**", use_container_width=True)

    if cancelar:
        st.info("Formulario cancelado.")
        st.rerun()

    if enviar:
        if not titulo or not descripcion:
            st.error("⚠️ Por favor, completa todos los campos obligatorios (*)")
        else:
            try:
                user_id = st.session_state.get("user_id")
                if not user_id:
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute("SELECT id FROM usuarios WHERE username = %s", (st.session_state["username"],))
                    result = cursor.fetchone()
                    conn.close()
                    if result:
                        user_id = int(result[0])
                    else:
                        st.error("❌ No se pudo identificar al usuario.")
                        return

                estado_inicial = "En Progreso" if asignado_id else "Abierto"
                comentario_asignacion = (
                    f"Asignado inicialmente a {asignado_username}" if asignado_id else "Creado por técnico, pendiente de asignación"
                )

                descripcion_completa = descripcion
                if "sistema_afectado" in locals():
                    info_tecnica = "\n\n--- INFORMACIÓN TÉCNICA ---\n"
                    info_tecnica += f"• Sistema afectado: {sistema_afectado}\n"
                    info_tecnica += f"• Entorno: {entorno}\n"
                    info_tecnica += f"• Nivel de urgencia: {urgencia}\n"
                    info_tecnica += f"• Tiempo estimado: {tiempo_estimado} horas\n"
                    info_tecnica += f"• Creado por técnico: {st.session_state['username']}\n"
                    descripcion_completa += info_tecnica

                conn = get_db_connection()
                cursor = conn.cursor()

                if asignado_id:
                    cursor.execute(
                        """
                        INSERT INTO tickets 
                        (usuario_id, categoria, prioridad, estado, asignado_a, titulo, descripcion)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING ticket_id
                        """,
                        (user_id, categoria, prioridad, estado_inicial, asignado_id, titulo, descripcion_completa),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO tickets 
                        (usuario_id, categoria, prioridad, estado, titulo, descripcion)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING ticket_id
                        """,
                        (user_id, categoria, prioridad, estado_inicial, titulo, descripcion_completa),
                    )

                conn.commit()
                ticket_id = cursor.fetchone()[0]

                # Comentario inicial
                cursor.execute(
                    "UPDATE tickets SET comentarios = %s WHERE ticket_id = %s",
                    (
                        f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] {comentario_asignacion} por {st.session_state['username']}.",
                        ticket_id,
                    ),
                )
                conn.commit()

                # Notificaciones
                if asignado_id and asignado_email:
                    try:
                        cursor.execute("SELECT email, username FROM usuarios WHERE id = %s", (user_id,))
                        creador = cursor.fetchone()
                        ticket_info = {
                            "ticket_id": ticket_id,
                            "titulo": titulo,
                            "asignado_por": st.session_state["username"],
                            "prioridad": prioridad,
                            "categoria": categoria,
                            "enlace": f"https://tu-dominio.com/ticket/{ticket_id}",
                        }
                        notificar_asignacion_ticket(asignado_email, ticket_info)
                        if creador and creador[0]:
                            notificar_creacion_ticket(
                                creador[0],
                                {
                                    "ticket_id": ticket_id,
                                    "titulo": titulo,
                                    "creado_por": st.session_state["username"],
                                    "prioridad": prioridad,
                                    "categoria": categoria,
                                    "estado": estado_inicial,
                                    "descripcion": descripcion[:100] + ("..." if len(descripcion) > 100 else ""),
                                    "enlace": f"https://tu-dominio.com/ticket/{ticket_id}",
                                },
                            )
                        st.success(f"📧 Notificación de asignación enviada a {asignado_username}")
                    except Exception as e:
                        st.warning(f"No se pudo enviar la notificación por correo: {e}")

                elif not asignado_id:
                    try:
                        cursor.execute("SELECT email FROM usuarios WHERE role = 'admin' LIMIT 1")
                        admin = cursor.fetchone()
                        if admin and admin[0]:
                            notificar_creacion_ticket(
                                admin[0],
                                {
                                    "ticket_id": ticket_id,
                                    "titulo": titulo,
                                    "creado_por": st.session_state["username"],
                                    "prioridad": prioridad,
                                    "categoria": categoria,
                                    "estado": estado_inicial,
                                    "descripcion": descripcion[:100] + ("..." if len(descripcion) > 100 else ""),
                                    "enlace": f"https://tu-dominio.com/ticket/{ticket_id}",
                                },
                            )
                            st.success("📧 Notificación enviada al administrador para asignación")
                    except Exception as e:
                        st.warning(f"No se pudo enviar notificación al administrador: {e}")

                conn.close()

                log_trazabilidad(
                    st.session_state["username"],
                    "Creación de ticket (técnico)",
                    f"Ticket técnico #{ticket_id} creado: {titulo}",
                )

                st.success(f"✅ **Ticket #{ticket_id} creado correctamente**")
                ticket_creado = True
                resumen = {
                    "titulo": titulo,
                    "categoria": categoria,
                    "prioridad": prioridad,
                    "estado": estado_inicial,
                    "usuario_asignado": asignado_username if asignado_id else None,
                }

            except Exception as e:
                st.error(f"❌ Error al crear el ticket: {e}")

    if ticket_creado and resumen:
        with st.expander("📋 Ver resumen del ticket creado", expanded=True):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**🎫 ID:** #{ticket_id}")
                st.markdown(f"**📝 Asunto:** {resumen['titulo']}")
                st.markdown(f"**🏷️ Categoría:** {resumen['categoria']}")
                st.markdown(f"**🚨 Prioridad:** {resumen['prioridad']}")
            with col2:
                st.markdown(f"**📊 Estado:** {resumen['estado']}")
                st.markdown(f"**👤 Creado por:** {st.session_state['username']}")
                if resumen["usuario_asignado"]:
                    st.markdown(f"**👥 Asignado a:** {resumen['usuario_asignado']}")
                else:
                    st.markdown("**👥 Asignado a:** Pendiente")
                st.markdown(f"**📅 Fecha:** {datetime.now().strftime('%d/%m/%Y %H:%M')}")

        st.markdown("---")
        if st.button("📋 Ver mis tickets asignados", use_container_width=True):
            st.rerun()


# ==================== FUNCIÓN PRINCIPAL ====================
def tecnico_dashboard():
    """Dashboard principal del técnico."""
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
                <div style="margin-top:10px; font-weight:bold;">Rol: Técnico</div>
                <div style="font-weight:bold; font-size:18px;">Bienvenido, {st.session_state.get('username', '')}</div>
                <hr>
            </div>
            """,
            unsafe_allow_html=True,
        )

        menu_opcion = option_menu(
            menu_title=None,
            options=["Mis tickets asignados", "Crear ticket"],
            icons=["ticket-detailed", "ticket-fill"],
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
            detalles = f"El técnico {st.session_state.get('username', 'N/A')} cerró sesión."
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

    if menu_opcion == "Mis tickets asignados":
        mis_tickets()
    elif menu_opcion == "Crear ticket":
        crear_tickets()


if __name__ == "__main__":
    tecnico_dashboard()