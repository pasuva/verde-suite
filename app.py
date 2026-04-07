import streamlit as st
from modules import login, admin, comercial_jefe, comercial_rafa, comercial_vip, demo, perfil_tecnico, marketing, rol_viabilidad, auditor
from modules.formulario_cliente import formulario_cliente

st.set_page_config(page_title="VERDE SUITE", page_icon="img/Adobe-Express-file.ico", layout="wide")


# ==========================================================
# 🔸 Detectar si hay token en la URL (para formulario cliente)
# ==========================================================
def get_url_params():
    """Función robusta para obtener parámetros de URL"""
    params = st.query_params

    # Obtener precontrato_id
    precontrato_id = None
    if "precontrato_id" in params:
        precontrato_val = params["precontrato_id"]
        if isinstance(precontrato_val, list):
            precontrato_id = precontrato_val[0] if precontrato_val else None
        else:
            precontrato_id = precontrato_val

    # Obtener token - MANERA ESPECIAL para evitar truncamiento
    token = None
    if "token" in params:
        token_val = params["token"]
        if isinstance(token_val, list):
            token = token_val[0] if token_val else None
        else:
            token = token_val

    # Si el token parece truncado, intentar reconstruirlo desde la URL completa
    if token and len(token) < 10:  # Los tokens deberían ser más largos
        st.warning(f"⚠️ Token parece truncado: '{token}'")
        # Intentar obtener el token de manera alternativa
        try:
            import urllib.parse
            current_url = st.query_params.to_dict()
            st.write(f"🔍 URL completa parseada: {current_url}")
        except Exception as e:
            st.write(f"🔍 Error al parsear URL: {e}")

    return precontrato_id, token


# Obtener parámetros
precontrato_id, token = get_url_params()

if token and precontrato_id:
    # Verificar si el token parece válido (debería tener al menos 10 caracteres)
    if len(str(token)) >= 10:
        #st.write("🔍 Token parece válido, cargando formulario...")
        formulario_cliente(precontrato_id, token)
        st.stop()
    else:
        st.error(f"❌ Token inválido o truncado: '{token}'")
        st.info("💡 El token parece estar incompleto. Por favor, verifica el enlace o contacta con el comercial.")

# ==========================================================
# 🔸 Si no hay token, sigue el flujo normal del login
# ==========================================================
if "login_ok" not in st.session_state:
    st.session_state["login_ok"] = False

# Si no está logueado, mostramos el login
if not st.session_state["login_ok"]:
    login.login()
else:
    rol = st.session_state.get("role", "")

    if rol == "admin":
        admin.admin_dashboard()
    elif rol == "comercial_jefe":
        comercial_jefe.mapa_dashboard()
    elif rol == "comercial_rafa":
        comercial_rafa.comercial_dashboard()
    elif rol == "comercial_vip":
        comercial_vip.comercial_dashboard_vip()
    elif rol == "demo":
        demo.demo_dashboard()
    elif rol == "tecnico":
        perfil_tecnico.tecnico_dashboard()
    elif rol == "marketing":
        marketing.marketing_dashboard()
    elif rol == "viabilidad":
        rol_viabilidad.viabilidad_dashboard()
    elif rol == "auditor":
        auditor.mostrar_auditoria()
    else:
        st.error("Rol no reconocido")


