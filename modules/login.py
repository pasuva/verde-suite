# login.py
import uuid
import os
import base64
import bcrypt
import streamlit as st
from datetime import datetime
from streamlit_cookies_controller import CookieController
from functools import lru_cache
import warnings

# Conexion centralizada a PostgreSQL
from modules.db import get_db_connection

warnings.filterwarnings("ignore", category=UserWarning)

# Configuracion global
COOKIE_NAME = "my_app"
COOKIE_CONFIG = {"max_age": 24 * 60 * 60, "path": '/', "same_site": 'Lax', "secure": True}

# Inicializacion de estado de sesion
if "login_ok" not in st.session_state:
    st.session_state.update({
        "login_ok": False,
        "username": None,
        "role": None,
        "session_id": None
    })


@lru_cache(maxsize=1)
def get_latest_version():
    """Obtiene la ultima version con cache para mejorar rendimiento."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT version FROM versiones ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return row[0] if row else "desconocido"
    except Exception as e:
        st.error(f"Error al obtener version: {e}")
        return "desconocido"


def verify_user(username, password):
    """Verifica las credenciales del usuario."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT password, role FROM usuarios WHERE username = %s",
            (username,)
        )
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if result and bcrypt.checkpw(password.encode(), result[0].encode()):
            return result[1]
    except Exception as e:
        st.error(f"Error de autenticacion: {e}")
    return None


def log_trazabilidad(usuario, accion, detalles):
    """Registro de trazas en la base de datos."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO trazabilidad (usuario_id, accion, detalles, fecha) VALUES (%s, %s, %s, %s)",
            (usuario, accion, detalles, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        st.error(f"Error en trazabilidad: {e}")


def set_user_session(controller, username, role, session_id):
    """Establece la sesion del usuario y las cookies."""
    st.session_state.update({
        "login_ok": True,
        "username": username,
        "role": role,
        "session_id": session_id
    })

    controller.set(f'{COOKIE_NAME}_session_id', session_id, **COOKIE_CONFIG)
    controller.set(f'{COOKIE_NAME}_username', username, **COOKIE_CONFIG)
    controller.set(f'{COOKIE_NAME}_role', role, **COOKIE_CONFIG)


def load_and_encode_image(image_path):
    """Carga y codifica imagen en base64 con cache."""
    if 'cached_logo' not in st.session_state:
        try:
            with open(image_path, 'rb') as f:
                st.session_state.cached_logo = base64.b64encode(f.read()).decode()
        except FileNotFoundError:
            st.session_state.cached_logo = ""
    return st.session_state.cached_logo


def render_login_form():
    """Renderiza el formulario de login."""
    st.markdown("""
        <style>
            .user-circle {
                width: 100px;
                height: 100px;
                border-radius: 50%;
                background-color: #6c757d;
                color: white;
                font-size: 50px;
                display: flex;
                align-items: center;
                justify-content: center;
                margin-bottom: 30px;
                margin-left: auto;
                margin-right: auto;
            }
        </style>
        <div class="user-circle">👤</div>
    """, unsafe_allow_html=True)

    logo_base64 = load_and_encode_image('img/Adobe_Express_file.png')
    st.markdown(f"""
        <h1 style="text-align: center; font-size: 50px; color: #007041;">
            <img src="data:image/png;base64,{logo_base64}" 
                 style="vertical-align: middle; width: 40px; height: 40px; margin-right: 10px;" />
            VERDE SUITE
        </h1>
    """, unsafe_allow_html=True)


def handle_automatic_login(controller):
    """Maneja el login automatico mediante cookies."""
    try:
        cookie_session_id = controller.get(f'{COOKIE_NAME}_session_id')
        cookie_username = controller.get(f'{COOKIE_NAME}_username')
        cookie_role = controller.get(f'{COOKIE_NAME}_role')
    except (TypeError, AttributeError):
        return

    if all([cookie_session_id, cookie_username, cookie_role]):
        st.session_state.update({
            "login_ok": True,
            "username": cookie_username,
            "role": cookie_role,
            "session_id": cookie_session_id
        })
        st.success(f"Bienvenido de nuevo, {st.session_state['username']}!")
        st.rerun()


def login():
    """Funcion principal de login."""
    controller = CookieController(key="cookies")

    if not st.session_state["login_ok"]:
        handle_automatic_login(controller)

    if not st.session_state["login_ok"]:
        render_login_form()

        session_id = str(uuid.uuid4())
        st.session_state["session_id"] = session_id

        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.success("Por favor, inicia sesion con tu usuario y contrasena.")

            username = st.text_input("Usuario")
            password = st.text_input("Contrasena", type="password")

            if st.button("Iniciar sesion", type="primary"):
                if not username or not password:
                    st.error("Por favor ingresa usuario y contrasena")
                    return

                role = verify_user(username, password)
                if role:
                    set_user_session(controller, username, role, session_id)
                    log_trazabilidad(username, "Inicio sesion", f"Usuario '{username}' inicio sesion")
                    st.success(f"Bienvenido, {username} ({role})")
                    st.rerun()
                else:
                    st.error("Usuario o contrasena incorrectos")

            version_actual = get_latest_version()
            st.markdown(
                f"<div style='text-align: center; margin-top: 50px;'>"
                f"<strong>Version actual:</strong> {version_actual}</div>",
                unsafe_allow_html=True
            )
