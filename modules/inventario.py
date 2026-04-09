import time
from datetime import datetime
from typing import Dict, Optional
import pandas as pd
import streamlit as st
from streamlit_cookies_controller import CookieController
from streamlit_option_menu import option_menu

from modules.db import get_db_connection as _db_conn
from modules import login
from modules.minIO import upload_image_to_cloudinary

st.set_page_config(layout="wide", page_title="Inventario Oficina")
# ==================== CONEXIÓN BD ====================
def get_db_connection():
    return _db_conn()


# ==================== TRAZABILIDAD ====================
def log_trazabilidad(usuario: str, accion: str, detalles: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO trazabilidad_inventario (usuario_id, accion, detalles, fecha) VALUES (%s, %s, %s, %s)",
        (usuario, accion, detalles, datetime.now())
    )
    conn.commit()
    conn.close()


# ==================== CRUD PERSONAL ====================
def obtener_personal_activo() -> pd.DataFrame:
    conn = get_db_connection()
    df = pd.read_sql(
        "SELECT id, nombre_completo, email, departamento FROM personal WHERE activo = TRUE ORDER BY nombre_completo",
        conn)
    conn.close()
    return df


def agregar_persona(nombre: str, email: str, departamento: str, usuario_creador: str) -> bool:
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO personal (nombre_completo, email, departamento) VALUES (%s, %s, %s)",
            (nombre, email, departamento)
        )
        conn.commit()
        conn.close()
        log_trazabilidad(usuario_creador, "Agregar personal", f"Se agregó a {nombre}")
        return True
    except Exception as e:
        st.error(f"Error al agregar persona: {e}")
        return False


# ==================== CRUD DISPOSITIVOS ====================
@st.cache_data(ttl=300)
def cargar_dispositivos(filtros: Dict = None) -> pd.DataFrame:
    conn = get_db_connection()
    query = "SELECT * FROM dispositivos ORDER BY id DESC"
    df = pd.read_sql(query, conn)
    conn.close()
    if filtros:
        if filtros.get("tipo"):
            df = df[df["tipo"] == filtros["tipo"]]
        if filtros.get("estado"):
            df = df[df["estado"] == filtros["estado"]]
    return df


def guardar_dispositivo(data: Dict, imagen, usuario: str) -> bool:
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        imagen_url = None
        if imagen:
            ext = imagen.name.split('.')[-1]
            filename = f"{data['numero_serie']}.{ext}"
            imagen_url = upload_image_to_cloudinary(imagen, filename, tipo="inventario",
                                                    folder=datetime.now().strftime("%Y/%m"))

        cursor.execute("""
            INSERT INTO dispositivos (tipo, marca, modelo, numero_serie, estado, ubicacion, comentarios, imagen_url, creado_por)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data["tipo"], data["marca"], data["modelo"], data["numero_serie"],
            data["estado"], data["ubicacion"], data["comentarios"], imagen_url, usuario
        ))
        conn.commit()
        conn.close()
        log_trazabilidad(usuario, "Crear dispositivo", f"Dispositivo {data['numero_serie']} creado")
        return True
    except Exception as e:
        st.error(f"Error al guardar: {e}")
        return False


def actualizar_dispositivo(id_disp, data: Dict, imagen, usuario: str) -> bool:
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        imagen_url = None
        if imagen:
            ext = imagen.name.split('.')[-1]
            filename = f"{data['numero_serie']}.{ext}"
            imagen_url = upload_image_to_cloudinary(imagen, filename, tipo="inventario",
                                                    folder=datetime.now().strftime("%Y/%m"))

        if imagen_url:
            cursor.execute("""
                UPDATE dispositivos SET tipo=%s, marca=%s, modelo=%s, numero_serie=%s, estado=%s,
                ubicacion=%s, comentarios=%s, imagen_url=%s, fecha_ultima_modificacion=CURRENT_TIMESTAMP
                WHERE id=%s
            """, (data["tipo"], data["marca"], data["modelo"], data["numero_serie"],
                  data["estado"], data["ubicacion"], data["comentarios"], imagen_url, id_disp))
        else:
            cursor.execute("""
                UPDATE dispositivos SET tipo=%s, marca=%s, modelo=%s, numero_serie=%s, estado=%s,
                ubicacion=%s, comentarios=%s, fecha_ultima_modificacion=CURRENT_TIMESTAMP
                WHERE id=%s
            """, (data["tipo"], data["marca"], data["modelo"], data["numero_serie"],
                  data["estado"], data["ubicacion"], data["comentarios"], id_disp))
        conn.commit()
        conn.close()
        log_trazabilidad(usuario, "Actualizar dispositivo", f"Dispositivo ID {id_disp} actualizado")
        return True
    except Exception as e:
        st.error(f"Error al actualizar: {e}")
        return False


def eliminar_dispositivo(id_disp, usuario: str) -> bool:
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM dispositivos WHERE id = %s", (id_disp,))
        conn.commit()
        conn.close()
        log_trazabilidad(usuario, "Eliminar dispositivo", f"Dispositivo ID {id_disp} eliminado")
        return True
    except Exception as e:
        st.error(f"Error al eliminar: {e}")
        return False


# ==================== ASIGNACIONES ====================
def asignar_dispositivo(disp_id: int, persona_id: int, motivo: str, usuario: str) -> bool:
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO asignaciones (dispositivo_id, persona_id, motivo, asignado_por)
            VALUES (%s, %s, %s, %s)
        """, (disp_id, persona_id, motivo, usuario))
        cursor.execute(
            "UPDATE dispositivos SET estado = 'Prestado', fecha_ultima_modificacion = CURRENT_TIMESTAMP WHERE id = %s",
            (disp_id,))
        conn.commit()
        conn.close()
        log_trazabilidad(usuario, "Asignar dispositivo", f"Dispositivo ID {disp_id} asignado a persona ID {persona_id}")
        return True
    except Exception as e:
        st.error(f"Error en asignación: {e}")
        return False


def devolver_dispositivo(asignacion_id: int, disp_id: int, usuario: str) -> bool:
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE asignaciones SET fecha_devolucion = CURRENT_TIMESTAMP WHERE id = %s", (asignacion_id,))
        cursor.execute(
            "UPDATE dispositivos SET estado = 'Operativo', fecha_ultima_modificacion = CURRENT_TIMESTAMP WHERE id = %s",
            (disp_id,))
        conn.commit()
        conn.close()
        log_trazabilidad(usuario, "Devolver dispositivo", f"Dispositivo ID {disp_id} devuelto")
        return True
    except Exception as e:
        st.error(f"Error en devolución: {e}")
        return False


def obtener_asignaciones_activas() -> pd.DataFrame:
    conn = get_db_connection()
    query = """
        SELECT a.id, d.id as dispositivo_id, d.tipo, d.marca, d.modelo, d.numero_serie,
               p.nombre_completo as asignado_a, a.fecha_asignacion, a.motivo
        FROM asignaciones a
        JOIN dispositivos d ON a.dispositivo_id = d.id
        JOIN personal p ON a.persona_id = p.id
        WHERE a.fecha_devolucion IS NULL
        ORDER BY a.fecha_asignacion DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def obtener_historial_dispositivo(disp_id: int) -> pd.DataFrame:
    conn = get_db_connection()
    query = """
        SELECT p.nombre_completo, a.fecha_asignacion, a.fecha_devolucion, a.motivo, a.asignado_por
        FROM asignaciones a
        JOIN personal p ON a.persona_id = p.id
        WHERE a.dispositivo_id = %s
        ORDER BY a.fecha_asignacion DESC
    """
    df = pd.read_sql(query, conn, params=(disp_id,))
    conn.close()
    return df


# ==================== INTERFAZ PRINCIPAL ====================
def inventario_dashboard():
    controller = CookieController(key="cookies_inventario")

    with st.sidebar:
        st.markdown(f"**Bienvenido, {st.session_state['username']}**")
        menu_opcion = option_menu(
            menu_title=None,
            options=["Inventario", "Asignaciones activas", "Historial", "Personal", "Informes"],
            icons=["archive", "people", "clock-history", "person-badge", "bar-chart"],
            default_index=0,
            styles={
                "container": {"padding": "0px", "background-color": "#F0F7F2"},
                "icon": {"color": "#2C5A2E", "font-size": "18px"},
                "nav-link": {"color": "#2C5A2E", "font-size": "16px"},
                "nav-link-selected": {"background-color": "#66B032", "color": "white"},
            }
        )
        if st.button("Cerrar sesión"):
            log_trazabilidad(st.session_state["username"], "Cierre sesión", "Usuario cerró sesión")
            for key in ['inventario_session_id', 'inventario_username', 'inventario_role']:
                if controller.get(key):
                    controller.set(key, '', max_age=0, path='/')
            st.session_state["login_ok"] = False
            st.session_state["username"] = ""
            st.rerun()

    if "username" not in st.session_state:
        st.warning("No has iniciado sesión")
        time.sleep(2)
        login.login()
        return

    # SECCIÓN: GESTIÓN DE PERSONAL
    if menu_opcion == "Personal":
        st.title("👥 Personal (personas asignables)")
        with st.expander("➕ Agregar nueva persona", expanded=False):
            with st.form("nueva_persona"):
                nombre = st.text_input("Nombre completo *")
                email = st.text_input("Email")
                departamento = st.text_input("Departamento")
                submitted = st.form_submit_button("Guardar")
                if submitted and nombre:
                    if agregar_persona(nombre, email, departamento, st.session_state["username"]):
                        st.success(f"✅ {nombre} agregado al catálogo")
                        st.rerun()
                elif submitted:
                    st.error("El nombre es obligatorio")

        df_personal = obtener_personal_activo()
        if df_personal.empty:
            st.info("No hay personal registrado aún.")
        else:
            st.dataframe(df_personal, use_container_width=True)

    # SECCIÓN: INVENTARIO (dispositivos)
    elif menu_opcion == "Inventario":
        st.title("📦 Inventario de oficina")

        # Formulario nuevo dispositivo
        with st.expander("➕ Agregar nuevo dispositivo", expanded=False):
            with st.form("nuevo_dispositivo"):
                col1, col2, col3 = st.columns(3)
                with col1:
                    tipo = st.selectbox("Tipo", ["PC", "Portátil", "Monitor", "Ratón", "Teclado", "Otro"])
                    marca = st.text_input("Marca")
                with col2:
                    modelo = st.text_input("Identificador")
                    numero_serie = st.text_input("Modelo *")
                with col3:
                    estado = st.selectbox("Estado", ["Operativo", "Reparación", "Baja", "Prestado"])
                    ubicacion = st.text_input("Ubicación")
                comentarios = st.text_area("Comentarios")
                imagen = st.file_uploader("Foto", type=["jpg", "jpeg", "png"])
                if st.form_submit_button("Guardar dispositivo"):
                    if not numero_serie:
                        st.error("El modelo es obligatorio")
                    else:
                        data = {"tipo": tipo, "marca": marca, "modelo": modelo, "numero_serie": numero_serie,
                                "estado": estado, "ubicacion": ubicacion, "comentarios": comentarios}
                        if guardar_dispositivo(data, imagen, st.session_state["username"]):
                            st.success("✅ Dispositivo guardado")
                            st.rerun()

        # Filtros y listado
        st.subheader("📋 Listado de dispositivos")
        tipos = ["Todos"] + sorted(
            pd.read_sql("SELECT DISTINCT tipo FROM dispositivos", get_db_connection())["tipo"].tolist())
        estados = ["Todos", "Operativo", "Reparación", "Baja", "Prestado"]
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            filtro_tipo = st.selectbox("Filtrar por tipo", tipos)
        with col_f2:
            filtro_estado = st.selectbox("Filtrar por estado", estados)

        filtros = {}
        if filtro_tipo != "Todos":
            filtros["tipo"] = filtro_tipo
        if filtro_estado != "Todos":
            filtros["estado"] = filtro_estado

        df = cargar_dispositivos(filtros)
        if df.empty:
            st.info("No hay dispositivos con esos filtros.")
        else:
            # Métricas
            col_m1, col_m2, col_m3, col_m4 = st.columns(4)
            col_m1.metric("Total", len(df))
            col_m2.metric("Operativos", len(df[df["estado"] == "Operativo"]))
            col_m3.metric("Reparación", len(df[df["estado"] == "Reparación"]))
            col_m4.metric("Prestados", len(df[df["estado"] == "Prestado"]))

            # Mostrar cada dispositivo en un expander
            for _, row in df.iterrows():
                with st.expander(f"{row['tipo']} - {row['marca']} {row['modelo']} (Modelo: {row['numero_serie']})"):
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.markdown(f"**Estado:** {row['estado']}  |  **Ubicación:** {row['ubicacion'] or '—'}")
                        st.markdown(f"**Comentarios:** {row['comentarios'] or '—'}")
                        if row['imagen_url']:
                            st.image(row['imagen_url'], width=200)
                    with col2:
                        if st.button(f"✏️ Editar", key=f"edit_{row['id']}"):
                            st.session_state["edit_id"] = row['id']
                            st.session_state["edit_data"] = row.to_dict()
                        if st.button(f"🗑️ Eliminar", key=f"del_{row['id']}"):
                            if eliminar_dispositivo(row['id'], st.session_state["username"]):
                                st.success("Eliminado")
                                st.rerun()
                        if row['estado'] != "Prestado":
                            if st.button(f"🔁 Asignar", key=f"assign_{row['id']}"):
                                st.session_state["asignar_id"] = row['id']

        # Formulario de edición
        if "edit_id" in st.session_state:
            st.subheader("✏️ Editar dispositivo")
            edit_data = st.session_state["edit_data"]
            with st.form("editar_form"):
                tipo_e = st.selectbox("Tipo", ["PC", "Portátil", "Monitor", "Ratón", "Teclado", "Otro"],
                                      index=["PC", "Portátil", "Monitor", "Ratón", "Teclado", "Otro"].index(
                                          edit_data["tipo"]))
                marca_e = st.text_input("Marca", value=edit_data["marca"] or "")
                modelo_e = st.text_input("Identificador", value=edit_data["modelo"] or "")
                serie_e = st.text_input("Modelo", value=edit_data["numero_serie"])
                estado_e = st.selectbox("Estado", ["Operativo", "Reparación", "Baja", "Prestado"],
                                        index=["Operativo", "Reparación", "Baja", "Prestado"].index(
                                            edit_data["estado"]))
                ubicacion_e = st.text_input("Ubicación", value=edit_data["ubicacion"] or "")
                comentarios_e = st.text_area("Comentarios", value=edit_data["comentarios"] or "")
                imagen_e = st.file_uploader("Nueva foto (opcional)", type=["jpg", "jpeg", "png"])
                col_btn1, col_btn2 = st.columns(2)
                with col_btn1:
                    if st.form_submit_button("💾 Guardar cambios"):
                        data = {"tipo": tipo_e, "marca": marca_e, "modelo": modelo_e, "numero_serie": serie_e,
                                "estado": estado_e, "ubicacion": ubicacion_e, "comentarios": comentarios_e}
                        if actualizar_dispositivo(edit_data["id"], data, imagen_e, st.session_state["username"]):
                            st.success("Actualizado")
                            del st.session_state["edit_id"]
                            st.rerun()
                with col_btn2:
                    if st.form_submit_button("❌ Cancelar"):
                        del st.session_state["edit_id"]
                        st.rerun()

        # Formulario de asignación
        if "asignar_id" in st.session_state:
            disp_id = st.session_state["asignar_id"]
            disp_info = df[df["id"] == disp_id].iloc[0]
            st.subheader(f"Asignar {disp_info['tipo']} - {disp_info['numero_serie']}")
            df_personal = obtener_personal_activo()
            if df_personal.empty:
                st.warning("No hay personal registrado. Ve a la sección 'Personal' para agregar.")
            else:
                with st.form("asignar_form"):
                    persona_opts = {f"{row['nombre_completo']} ({row['departamento'] or 'Sin dep.'})": row['id'] for
                                    _, row in df_personal.iterrows()}
                    seleccion = st.selectbox("Persona a quien asignar", list(persona_opts.keys()))
                    motivo = st.text_area("Motivo de la asignación")
                    col_as1, col_as2 = st.columns(2)
                    with col_as1:
                        if st.form_submit_button("✅ Confirmar asignación"):
                            persona_id = persona_opts[seleccion]
                            if asignar_dispositivo(disp_id, persona_id, motivo, st.session_state["username"]):
                                st.success("Dispositivo asignado")
                                del st.session_state["asignar_id"]
                                st.rerun()
                    with col_as2:
                        if st.form_submit_button("Cancelar"):
                            del st.session_state["asignar_id"]
                            st.rerun()

    # SECCIÓN: ASIGNACIONES ACTIVAS
    elif menu_opcion == "Asignaciones activas":
        st.title("🔁 Dispositivos prestados actualmente")
        df_asign = obtener_asignaciones_activas()
        if df_asign.empty:
            st.info("No hay dispositivos prestados.")
        else: #ojo el modelo en base de datos es el identificador nuestro y el numero de serie es el modelo
            for _, row in df_asign.iterrows():
                with st.expander(f"{row['tipo']} {row['marca']} {row['modelo']} → {row['asignado_a']}"):
                    st.write(f"**Modelo:** {row['numero_serie']}")
                    st.write(f"**Fecha asignación:** {row['fecha_asignacion']}")
                    st.write(f"**Motivo:** {row['motivo']}")
                    if st.button("🔁 Registrar devolución", key=f"devolver_{row['id']}"):
                        if devolver_dispositivo(row['id'], row['dispositivo_id'], st.session_state["username"]):
                            st.success("Devuelto correctamente")
                            st.rerun()

    # SECCIÓN: HISTORIAL POR DISPOSITIVO
    elif menu_opcion == "Historial":
        st.title("📜 Historial de movimientos por dispositivo")
        conn = get_db_connection()
        disp_list = pd.read_sql("SELECT id, tipo, marca, modelo, numero_serie FROM dispositivos ORDER BY id", conn)
        conn.close()
        if disp_list.empty:
            st.info("No hay dispositivos registrados.")
        else:
            disp_opts = {
                f"{row['id']} - {row['tipo']} {row['marca']} {row['modelo']} ({row['numero_serie']})": row['id'] for
                _, row in disp_list.iterrows()}
            seleccion = st.selectbox("Selecciona un dispositivo", list(disp_opts.keys()))
            disp_id = disp_opts[seleccion]
            historial = obtener_historial_dispositivo(disp_id)
            if historial.empty:
                st.info("No hay asignaciones previas para este dispositivo.")
            else:
                st.dataframe(historial, use_container_width=True)

    # SECCIÓN: INFORMES
    elif menu_opcion == "Informes":
        st.title("📊 Informes y estadísticas")
        df_total = cargar_dispositivos()
        if not df_total.empty:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Por estado")
                st.bar_chart(df_total["estado"].value_counts())
            with col2:
                st.subheader("Por tipo")
                st.bar_chart(df_total["tipo"].value_counts())
            csv = df_total.to_csv(index=False)
            st.download_button("📥 Exportar inventario a CSV", data=csv,
                               file_name=f"inventario_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv")
        else:
            st.info("No hay datos para mostrar")


if __name__ == "__main__":
    inventario_dashboard()