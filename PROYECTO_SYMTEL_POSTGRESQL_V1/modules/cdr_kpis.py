# cdr_kpis.py
import json
import os
import tempfile
from datetime import datetime
from io import BytesIO
from typing import Dict, Optional

import altair as alt
import gspread
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, PageBreak

# ==================== CONFIGURACIÓN DEPARTAMENTAL ====================
MAPEO_DEPARTAMENTOS = {
    '1001': 'Administración',
    '1002': 'Comercial',
    '1003': 'Soporte Técnico',
    # Añade aquí todas las extensiones que conozcas
}


def asignar_departamento(numero):
    """Asigna un departamento a un número de extensión o externo."""
    if str(numero) in MAPEO_DEPARTAMENTOS:
        return MAPEO_DEPARTAMENTOS[str(numero)]
    elif str(numero).isdigit() and len(str(numero)) >= 9:
        return 'Externo (Teléfono)'
    elif str(numero).startswith('s') or str(numero) == 's':
        return 'Servicio/IVR'
    else:
        return 'Desconocido/Externo'


def clasificar_interaccion(fila):
    """Clasifica el tipo de interacción entre departamentos."""
    origen = fila['dept_origen']
    destino = fila['dept_destino']

    if origen == destino and origen in ['Administración', 'Comercial', 'Soporte Técnico']:
        return 'Interna (mismo dept)'
    elif origen in ['Administración', 'Comercial', 'Soporte Técnico'] and destino in ['Administración', 'Comercial',
                                                                                      'Soporte Técnico']:
        return 'Colaboración (dept a dept)'
    elif origen in ['Administración', 'Comercial', 'Soporte Técnico'] and destino == 'Externo (Teléfono)':
        return 'Llamada Saliente'
    elif origen == 'Externo (Teléfono)' and destino in ['Administración', 'Comercial', 'Soporte Técnico']:
        return 'Llamada Entrante'
    else:
        return 'Otra'


# ==================== CARGA DE DATOS (CON CACHÉ) ====================
@st.cache_data(ttl=3600, show_spinner="Cargando CDR desde Google Sheets...")
def cargar_y_procesar_cdr():
    try:
        # Credenciales (misma lógica que en otros módulos)
        posibles_rutas = [
            "modules/carga-contratos-verde-c5068516c7cf.json",
            "/etc/secrets/carga-contratos-verde-c5068516c7cf.json",
            os.path.join(os.path.dirname(__file__), "carga-contratos-verde-c5068516c7cf.json"),
        ]
        ruta_credenciales = None
        for r in posibles_rutas:
            if os.path.exists(r):
                ruta_credenciales = r
                break

        if not ruta_credenciales and "GOOGLE_APPLICATION_CREDENTIALS_JSON" in os.environ:
            creds_dict = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
            creds = Credentials.from_service_account_info(
                creds_dict,
                scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            )
        elif ruta_credenciales:
            creds = Credentials.from_service_account_file(
                ruta_credenciales,
                scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            )
        else:
            raise ValueError("No se encontraron credenciales de Google Service Account.")

        client = gspread.authorize(creds)
        sheet = client.open("CDR VERDE PBX").worksheet("CDR VERDE PBX")
        data = sheet.get_all_records()

        if not data:
            return pd.DataFrame(), {}

        df = pd.DataFrame(data)
        df.columns = df.columns.map(lambda x: str(x).strip().upper() if x else "")

        # Mapeo de columnas
        column_mapping = {
            'CALLDATE': 'calldate',
            'CLID': 'clid',
            'SRC': 'src',
            'DST': 'dst',
            'DCONTEXT': 'dcontext',
            'CHANNEL': 'channel',
            'DSTCHANNEL': 'dstchannel',
            'LASTAPP': 'lastapp',
            'LASTDATA': 'lastdata',
            'DURATION': 'duration',
            'BILLSEC': 'billsec',
            'DISPOSITION': 'disposition',
            'AMAFLAGS': 'amaflags',
            'ACCOUNTCODE': 'accountcode',
            'UNIQUEID': 'uniqueid',
            'USERFIELD': 'userfield',
            'DID': 'did',
            'CNUM': 'cnum',
            'CNAM': 'cnam',
            'OUTBOUND_CNUM': 'outbound_cnum',
            'OUTBOUND_CNAM': 'outbound_cnam',
            'DST_CNAM': 'dst_cnam',
            'RECORDINGFILE': 'recordingfile',
            'LINKEDID': 'linkedid',
            'PEERACCOUNT': 'peeraccount',
            'SEQUENCE': 'sequence'
        }
        df.rename(columns={col: column_mapping[col] for col in column_mapping if col in df.columns}, inplace=True)

        # Conversión de tipos
        if 'calldate' in df.columns:
            df['calldate'] = pd.to_datetime(df['calldate'], dayfirst=True, errors='coerce')

        numeric_cols = ['duration', 'billsec']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        kpis = calcular_kpis_cdr(df)
        return df, kpis

    except Exception as e:
        st.error(f"Error en cargar_y_procesar_cdr: {e}")
        return None, None


# ==================== FUNCIONES DE CÁLCULO DE KPIS ====================
def calcular_kpis_cdr(df):
    if df.empty:
        return {}

    kpis = {
        'total_llamadas': len(df),
        'llamadas_contestadas': len(df[df['disposition'] == 'ANSWERED']) if 'disposition' in df.columns else 0,
        'llamadas_no_contestadas': len(
            df[df['disposition'].isin(['NO ANSWER', 'BUSY', 'FAILED'])]) if 'disposition' in df.columns else 0,
        'duracion_total_segundos': df['duration'].sum() if 'duration' in df.columns else 0,
        'duracion_promedio_segundos': df['duration'].mean() if 'duration' in df.columns else 0,
        'facturacion_total_segundos': df['billsec'].sum() if 'billsec' in df.columns else 0,
        'extensiones_unicas': df['src'].nunique() if 'src' in df.columns else 0,
    }

    if 'calldate' in df.columns and not df['calldate'].isnull().all():
        df['fecha'] = df['calldate'].dt.date
        llamadas_por_dia = df.groupby('fecha').size().to_dict()
        kpis['llamadas_por_dia'] = llamadas_por_dia

    return kpis


def calcular_kpis_cdr_ampliada(df):
    if df.empty:
        return {}

    kpis = calcular_kpis_cdr(df)

    # Eficiencia operativa
    if 'disposition' in df.columns:
        total = len(df)
        contestadas = len(df[df['disposition'] == 'ANSWERED'])
        no_contestadas = len(df[df['disposition'].isin(['NO ANSWER', 'BUSY'])])
        fallidas = len(df[df['disposition'] == 'FAILED'])

        kpis['tasa_respuesta'] = (contestadas / total * 100) if total > 0 else 0
        kpis['tasa_abandono'] = (no_contestadas / total * 100) if total > 0 else 0
        kpis['llamadas_fallidas'] = fallidas

    # Patrones temporales
    if 'calldate' in df.columns:
        df['hora'] = df['calldate'].dt.hour
        df['dia_semana'] = df['calldate'].dt.day_name()
        df['es_fin_semana'] = df['calldate'].dt.weekday >= 5

        llamadas_por_hora = df.groupby('hora').size()
        kpis['llamadas_por_hora_dict'] = llamadas_por_hora.to_dict()
        kpis['hora_pico'] = llamadas_por_hora.idxmax() if not llamadas_por_hora.empty else None
        kpis['llamadas_hora_pico'] = llamadas_por_hora.max() if not llamadas_por_hora.empty else 0

        dias_orden = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        llamadas_por_dia = df['dia_semana'].value_counts().reindex(dias_orden, fill_value=0)
        kpis['llamadas_por_dia_dict'] = llamadas_por_dia.to_dict()
        kpis['dia_mas_activo'] = llamadas_por_dia.idxmax() if not llamadas_por_dia.empty else None

        kpis['llamadas_fin_semana'] = df['es_fin_semana'].sum()
        kpis['llamadas_laborables'] = len(df) - kpis['llamadas_fin_semana']

    # Análisis de origen y destino
    if 'src' in df.columns:
        top_origen = df['src'].value_counts().head(10)
        kpis['top_origen_dict'] = top_origen.to_dict()
        kpis['extension_mas_activa'] = top_origen.index[0] if not top_origen.empty else None
        kpis['llamadas_extension_top'] = top_origen.iloc[0] if not top_origen.empty else 0

    if 'dst' in df.columns:
        top_destino = df['dst'].value_counts().head(10)
        kpis['top_destino_dict'] = top_destino.to_dict()
        kpis['destino_mas_frecuente'] = top_destino.index[0] if not top_destino.empty else None

        def es_extension(x):
            try:
                return str(x).isdigit() and 1000 <= int(x) <= 9999
            except:
                return False

        df['es_interna'] = df.apply(lambda fila: es_extension(fila.get('src')) and es_extension(fila.get('dst')), axis=1)
        kpis['llamadas_internas'] = df['es_interna'].sum()
        kpis['llamadas_externas'] = len(df) - kpis['llamadas_internas']

    # Facturación
    if 'billsec' in df.columns:
        kpis['minutos_facturables'] = df['billsec'].sum() / 60.0
        if 'duration' in df.columns:
            df_con_duracion = df[df['duration'] > 0]
            if not df_con_duracion.empty:
                kpis['ratio_facturacion_vs_duracion'] = (
                        df_con_duracion['billsec'].sum() / df_con_duracion['duration'].sum())

    # Análisis por departamento
    df['dept_origen'] = df['src'].apply(asignar_departamento)
    df['dept_destino'] = df['dst'].apply(asignar_departamento)

    actividad_por_depto = df['dept_origen'].value_counts()
    kpis['actividad_por_depto_dict'] = actividad_por_depto.to_dict()

    if 'duration' in df.columns:
        duracion_por_depto = df.groupby('dept_origen')['duration'].agg(['sum', 'mean', 'count'])
        kpis['duracion_por_depto_df'] = duracion_por_depto.reset_index().rename(
            columns={'sum': 'duracion_total_seg', 'mean': 'duracion_promedio_seg', 'count': 'llamadas'})

    if 'disposition' in df.columns:
        for dept in ['Administración', 'Comercial', 'Soporte Técnico']:
            df_dept = df[df['dept_origen'] == dept]
            if not df_dept.empty:
                total_dept = len(df_dept)
                contestadas_dept = len(df_dept[df_dept['disposition'] == 'ANSWERED'])
                kpis[f'tasa_respuesta_{dept.lower().replace(" ", "_")}'] = (
                        contestadas_dept / total_dept * 100) if total_dept > 0 else 0

    # Interacción entre departamentos
    if 'dept_origen' in df.columns and 'dept_destino' in df.columns:
        df['tipo_interaccion'] = df.apply(clasificar_interaccion, axis=1)
        kpis['interacciones_por_tipo_dict'] = df['tipo_interaccion'].value_counts().to_dict()

        colaboracion = df[
            (df['dept_origen'].isin(['Administración', 'Comercial', 'Soporte Técnico'])) &
            (df['dept_destino'].isin(['Administración', 'Comercial', 'Soporte Técnico']))
        ]
        if not colaboracion.empty:
            kpis['matriz_colaboracion_df'] = pd.crosstab(colaboracion['dept_origen'], colaboracion['dept_destino'])

    # DataFrame resumen de disposition
    if 'disposition' in df.columns and not df['disposition'].isnull().all():
        df_resumen = df['disposition'].value_counts().reset_index()
        df_resumen.columns = ['disposition', 'count']
        df_resumen['percentage'] = (df_resumen['count'] / df_resumen['count'].sum() * 100).round(1)
        kpis['df_resumen_disposition'] = df_resumen
    else:
        kpis['df_resumen_disposition'] = None

    return kpis


# ==================== VISUALIZACIÓN EN STREAMLIT ====================
def mostrar_cdrs():
    """Función principal: muestra toda la sección de CDRs en Streamlit."""

    # Inicializar session_state
    if 'pdf_generado' not in st.session_state:
        st.session_state.pdf_generado = False
    if 'pdf_bytes' not in st.session_state:
        st.session_state.pdf_bytes = None
    if 'pdf_filename' not in st.session_state:
        st.session_state.pdf_filename = None
    if 'datos_cargados' not in st.session_state:
        st.session_state.datos_cargados = False
    if 'df_cdr_original' not in st.session_state:
        st.session_state.df_cdr_original = None

    if st.button("Cargar y analizar CDR"):
        with st.spinner("Cargando datos desde Google Sheets..."):
            df_cdr, _ = cargar_y_procesar_cdr()

            if df_cdr is None:
                st.error("❌ No se pudieron cargar los datos. Verifica las credenciales y que la hoja exista.")
                return

            st.session_state.df_cdr_original = df_cdr.copy()

            # Filtrar llamadas con información útil
            mask = (
                (df_cdr['duration'].notna() & (df_cdr['duration'].astype(str).str.strip() != '')) |
                (df_cdr['billsec'].notna() & (df_cdr['billsec'].astype(str).str.strip() != '')) |
                (df_cdr['disposition'].notna() & (df_cdr['disposition'].astype(str).str.strip() != ''))
            )
            df_filtrado = df_cdr[mask].copy()

            for col in ['duration', 'billsec']:
                if col in df_filtrado.columns:
                    df_filtrado[col] = pd.to_numeric(df_filtrado[col].replace('', 0).fillna(0), errors='coerce')

            kpis = calcular_kpis_cdr_ampliada(df_filtrado)
            kpis['total_registros'] = len(df_cdr)
            kpis['llamadas_filtradas'] = len(df_filtrado)
            kpis['intentos_no_completados'] = len(df_cdr) - len(df_filtrado)

            st.session_state.df_cdr = df_filtrado
            st.session_state.kpis = kpis
            st.session_state.datos_cargados = True
            st.session_state.pdf_generado = False

            st.success(f"✅ Datos cargados. Total registros: {len(df_cdr)} | Llamadas analizadas: {len(df_filtrado)}")

    if st.session_state.get('datos_cargados', False) and 'df_cdr' in st.session_state:
        df_cdr = st.session_state.df_cdr
        df_cdr_original = st.session_state.df_cdr_original
        kpis = st.session_state.kpis

        with st.expander("ℹ️ Información sobre el filtrado de datos"):
            st.write(f"""
            **Total de registros en el CDR:** {kpis.get('total_registros', 0)}
            **Llamadas analizadas (con información):** {kpis.get('llamadas_filtradas', 0)}
            **Intentos/registros sin información completa:** {kpis.get('intentos_no_completados', 0)}
            """)

        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("📄 Generar PDF con Gráficos", use_container_width=True, key="generar_pdf_btn"):
                with st.spinner("Generando PDF..."):
                    pdf_bytes = generar_pdf_kpis_con_graficos(kpis, df_cdr)
                    st.session_state.pdf_bytes = pdf_bytes
                    st.session_state.pdf_filename = f"informe_cdr_con_graficos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                    st.session_state.pdf_generado = True
                    st.rerun()

        if st.session_state.pdf_generado and st.session_state.pdf_bytes:
            st.download_button(
                label="⬇️ Descargar PDF con Gráficos",
                data=st.session_state.pdf_bytes,
                file_name=st.session_state.pdf_filename,
                mime="application/pdf",
                use_container_width=True,
                key="descargar_pdf"
            )

        tab1, tab2, tab3, tab4, tab5 = st.tabs(
            ["📈 Resumen General", "🕒 Patrones", "📞 Origen y Destino", "🏢 Análisis por Departamento", "📋 Detalles"]
        )

        with tab1:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Llamadas Totales", kpis.get('total_llamadas', 0))
            with col2:
                st.metric("Llamadas Internas", kpis.get('llamadas_internas', 0))
            with col3:
                st.metric("Tasa de Respuesta", f"{kpis.get('tasa_respuesta', 0):.1f}%")
            with col4:
                st.metric("Duración Promedio", f"{kpis.get('duracion_promedio_segundos', 0):.1f} s")

            if 'llamadas_por_dia' in kpis:
                st.subheader("Llamadas por Día")
                df_por_dia = pd.DataFrame(list(kpis['llamadas_por_dia'].items()), columns=['Fecha', 'Llamadas'])
                st.bar_chart(df_por_dia.set_index('Fecha'))

        with tab2:
            # Gráfico por hora y departamento
            df_cdr['hora'] = pd.to_datetime(df_cdr['calldate']).dt.hour
            dept_internos = ['Administración', 'Comercial', 'Soporte Técnico']
            df_cdr['dept_categoria'] = df_cdr['dept_origen'].apply(
                lambda x: x if x in dept_internos else 'Otros / Sin Extensión'
            )
            chart_data = df_cdr.groupby(['hora', 'dept_categoria']).size().reset_index(name='count')
            orden_categorias = dept_internos + ['Otros / Sin Extensión']
            chart_data['dept_categoria'] = pd.Categorical(chart_data['dept_categoria'], categories=orden_categorias, ordered=True)

            chart = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X('hora:O', title='Hora del Día'),
                y=alt.Y('count:Q', title='Llamadas', stack='zero'),
                color=alt.Color('dept_categoria:N', title='Departamento',
                                scale=alt.Scale(domain=orden_categorias,
                                                range=['#1f77b4', '#ff7f0e', '#2ca02c', '#7f7f7f']),
                                sort=orden_categorias),
                tooltip=['hora:O', 'dept_categoria:N', 'count:Q']
            ).properties(width=700, height=400, title='Llamadas por Franja Horaria y Departamento')
            st.altair_chart(chart, use_container_width=True)

            # Gráfico por día de la semana
            dias_traduccion = {
                'Monday': 'Lunes', 'Tuesday': 'Martes', 'Wednesday': 'Miércoles',
                'Thursday': 'Jueves', 'Friday': 'Viernes', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
            }
            df_cdr['dia_semana'] = pd.to_datetime(df_cdr['calldate']).dt.strftime('%A').map(dias_traduccion)
            chart_data_dia = df_cdr.groupby(['dia_semana', 'dept_categoria']).size().reset_index(name='count')
            dias_orden = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
            chart_data_dia['dia_semana'] = pd.Categorical(chart_data_dia['dia_semana'], categories=dias_orden, ordered=True)
            chart_data_dia = chart_data_dia.sort_values('dia_semana')

            chart_dia = alt.Chart(chart_data_dia).mark_bar().encode(
                x=alt.X('dia_semana:N', title='Día de la Semana', sort=dias_orden),
                y=alt.Y('count:Q', title='Llamadas', stack='zero'),
                color=alt.Color('dept_categoria:N', title='Departamento',
                                scale=alt.Scale(domain=orden_categorias,
                                                range=['#1f77b4', '#ff7f0e', '#2ca02c', '#7f7f7f']),
                                sort=orden_categorias),
                tooltip=['dia_semana:N', 'dept_categoria:N', 'count:Q']
            ).properties(width=700, height=400, title='Llamadas por Día de la Semana y Departamento')
            st.altair_chart(chart_dia, use_container_width=True)

        with tab3:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Top 5 Extensiones de Origen")
                if 'top_origen_dict' in kpis:
                    df_origen = pd.DataFrame(list(kpis['top_origen_dict'].items()), columns=['Extensión', 'Llamadas']).head(5)
                    st.dataframe(df_origen, use_container_width=True)
            with col2:
                st.subheader("Top 5 Destinos")
                if 'top_destino_dict' in kpis:
                    df_destino = pd.DataFrame(list(kpis['top_destino_dict'].items()), columns=['Destino', 'Llamadas']).head(5)
                    st.dataframe(df_destino, use_container_width=True)

            st.subheader("Distribución Interna/Externa")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Llamadas Internas", kpis.get('llamadas_internas', 0))
            with col2:
                st.metric("Llamadas Externas", kpis.get('llamadas_externas', 0))
            with col3:
                total = kpis.get('llamadas_internas', 0) + kpis.get('llamadas_externas', 0)
                pct_internas = (kpis.get('llamadas_internas', 0) / total * 100) if total > 0 else 0
                st.metric("% Internas", f"{pct_internas:.1f}%")

        with tab4:
            st.subheader("Actividad por Departamento")
            if 'actividad_por_depto_dict' in kpis:
                df_dept = pd.DataFrame(list(kpis['actividad_por_depto_dict'].items()), columns=['Departamento', 'Llamadas'])
                dept_internos = ['Administración', 'Comercial', 'Soporte Técnico']
                df_dept_filtrado = df_dept[df_dept['Departamento'].isin(dept_internos)]
                if not df_dept_filtrado.empty:
                    st.bar_chart(df_dept_filtrado.set_index('Departamento'))

            st.subheader("Comparativa de Rendimiento")
            if 'duracion_por_depto_df' in kpis:
                df_duracion = kpis['duracion_por_depto_df']
                df_duracion_internos = df_duracion[df_duracion['dept_origen'].isin(dept_internos)]
                cols = st.columns(len(df_duracion_internos))
                for idx, (_, fila) in enumerate(df_duracion_internos.iterrows()):
                    with cols[idx]:
                        st.metric(
                            label=f"{fila['dept_origen']}",
                            value=f"{fila['llamadas']} llamadas",
                            delta=f"Prom: {fila['duracion_promedio_seg']:.0f}s"
                        )

            st.subheader("Interacción entre Departamentos")
            if 'matriz_colaboracion_df' in kpis:
                st.write("Origen → Destino")
                st.dataframe(kpis['matriz_colaboracion_df'].style.background_gradient(cmap='Blues'), use_container_width=True)

            st.subheader("Distribución por Tipo de Llamada")
            if 'interacciones_por_tipo_dict' in kpis:
                df_tipo = pd.DataFrame(list(kpis['interacciones_por_tipo_dict'].items()), columns=['Tipo', 'Cantidad'])
                st.bar_chart(df_tipo.set_index('Tipo'))

        with tab5:
            st.subheader("Estado de las Llamadas")
            if kpis.get('df_resumen_disposition') is not None:
                st.dataframe(kpis['df_resumen_disposition'], use_container_width=True)

            st.subheader("Muestra de Datos (primeras 20 llamadas con información)")
            st.dataframe(df_cdr.head(20), use_container_width=True)

            if df_cdr_original is not None:
                with st.expander("Ver registros completos (incluyendo intentos)"):
                    st.dataframe(df_cdr_original.head(50), use_container_width=True)
                    st.caption(f"Mostrando 50 de {len(df_cdr_original)} registros totales")
            else:
                st.info("No hay registros originales disponibles.")


# ==================== GENERACIÓN DE PDF ====================
def generar_pdf_kpis_con_graficos(kpis, df=None):
    """Genera un PDF con los KPIs, tablas y gráficos, y devuelve los bytes."""
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    elements = []

    styles = getSampleStyleSheet()
    estilo_titulo = ParagraphStyle(
        'CustomTitle', parent=styles['Heading1'], fontSize=18, spaceAfter=30,
        alignment=TA_CENTER, textColor=colors.HexColor('#2E86C1')
    )
    estilo_subtitulo = ParagraphStyle(
        'CustomSubtitle', parent=styles['Heading2'], fontSize=14, spaceAfter=12,
        alignment=TA_LEFT, textColor=colors.HexColor('#2E86C1')
    )
    estilo_kpi = ParagraphStyle(
        'KPI', parent=styles['Normal'], fontSize=12, spaceAfter=6,
        alignment=TA_CENTER, textColor=colors.black
    )
    estilo_nota = ParagraphStyle(
        'Nota', parent=styles['Normal'], fontSize=9, spaceAfter=6,
        alignment=TA_LEFT, textColor=colors.grey
    )

    elements.append(Paragraph("INFORME DE KPIs - CDR", estilo_titulo))
    elements.append(Paragraph(f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}", estilo_kpi))

    if 'total_registros' in kpis and 'llamadas_filtradas' in kpis:
        elements.append(Paragraph(
            f"Nota: Se analizaron {kpis['llamadas_filtradas']} de {kpis['total_registros']} registros (solo llamadas con información completa)",
            estilo_nota
        ))

    elements.append(Spacer(1, 0.5 * inch))

    # Tabla de KPIs principales
    elements.append(Paragraph("1. KPIs Principales", estilo_subtitulo))
    datos_kpis = [
        ["KPI", "Valor", "KPI", "Valor"],
        ["Total de llamadas", str(kpis.get('total_llamadas', 0)),
         "Llamadas contestadas", str(kpis.get('llamadas_contestadas', 0))],
        ["Tasa de respuesta", f"{kpis.get('tasa_respuesta', 0):.1f}%",
         "Duración total", f"{kpis.get('duracion_total_segundos', 0):.0f} s"],
        ["Duración promedio", f"{kpis.get('duracion_promedio_segundos', 0):.1f} s",
         "Minutos facturables", f"{kpis.get('minutos_facturables', 0):.1f}"],
        ["Llamadas internas", str(kpis.get('llamadas_internas', 0)),
         "Llamadas externas", str(kpis.get('llamadas_externas', 0))],
        ["Extensiones únicas", str(kpis.get('extensiones_unicas', 0)),
         "Tasa internas", f"{(kpis.get('llamadas_internas', 0) / kpis.get('total_llamadas', 1) * 100):.1f}%"],
    ]
    tabla_kpis = Table(datos_kpis, colWidths=[2 * inch, 1.5 * inch, 2 * inch, 1.5 * inch])
    tabla_kpis.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2E86C1')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F8F9F9')),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#D5D8DC')),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F2F3F4')]),
    ]))
    elements.append(tabla_kpis)
    elements.append(Spacer(1, 0.5 * inch))

    # Gráficos
    elements.append(Paragraph("2. Gráficos de Análisis", estilo_subtitulo))
    elements.append(Spacer(1, 0.25 * inch))

    temp_dir = tempfile.mkdtemp()
    img_paths = []

    # Gráfico de llamadas por día
    if 'llamadas_por_dia' in kpis and kpis['llamadas_por_dia']:
        try:
            fig, ax = plt.subplots(figsize=(10, 4))
            fechas = list(kpis['llamadas_por_dia'].keys())[-10:]
            llamadas = list(kpis['llamadas_por_dia'].values())[-10:]
            bars = ax.bar(fechas, llamadas, color=plt.cm.Blues(np.linspace(0.4, 0.8, len(fechas))))
            ax.set_xlabel('Fecha', fontsize=10)
            ax.set_ylabel('Llamadas', fontsize=10)
            ax.set_title('Llamadas por Día (Últimos 10 días)', fontsize=12, fontweight='bold')
            ax.tick_params(axis='x', rotation=45, labelsize=8)
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height + 0.1, f'{int(height)}',
                        ha='center', va='bottom', fontsize=8)
            plt.tight_layout()
            img_path = os.path.join(temp_dir, 'llamadas_por_dia.png')
            plt.savefig(img_path, dpi=150, bbox_inches='tight')
            plt.close(fig)
            img_paths.append(img_path)
            elements.append(Paragraph("Llamadas por Día", estilo_kpi))
            elements.append(Image(img_path, width=6 * inch, height=2.5 * inch))
            elements.append(Spacer(1, 0.25 * inch))
        except Exception as e:
            print(f"Error generando gráfico: {e}")

    # Gráfico de distribución horaria
    if 'llamadas_por_hora_dict' in kpis and kpis['llamadas_por_hora_dict']:
        try:
            fig, ax = plt.subplots(figsize=(10, 4))
            horas = [f"{h}:00" for h in range(24)]
            llamadas = [kpis['llamadas_por_hora_dict'].get(h, 0) for h in range(24)]
            bars = ax.bar(horas, llamadas, color=plt.cm.Greens(np.linspace(0.3, 0.7, 24)))
            ax.set_xlabel('Hora del Día', fontsize=10)
            ax.set_ylabel('Llamadas', fontsize=10)
            ax.set_title('Distribución por Franja Horaria', fontsize=12, fontweight='bold')
            ax.tick_params(axis='x', rotation=45, labelsize=7)
            hora_pico = kpis.get('hora_pico', 0)
            if hora_pico in range(24):
                bars[hora_pico].set_color(plt.cm.Reds(0.7))
                ax.text(hora_pico, llamadas[hora_pico] + 0.5, 'PICO', ha='center', va='bottom',
                        fontsize=8, fontweight='bold', color='red')
            plt.tight_layout()
            img_path = os.path.join(temp_dir, 'distribucion_horaria.png')
            plt.savefig(img_path, dpi=150, bbox_inches='tight')
            plt.close(fig)
            img_paths.append(img_path)
            elements.append(Paragraph(f"Distribución Horaria (Hora pico: {hora_pico}:00)", estilo_kpi))
            elements.append(Image(img_path, width=6 * inch, height=2.5 * inch))
            elements.append(Spacer(1, 0.25 * inch))
        except Exception as e:
            print(f"Error generando gráfico: {e}")

    # Gráfico de actividad por departamento
    if 'actividad_por_depto_dict' in kpis and kpis['actividad_por_depto_dict']:
        try:
            fig, ax = plt.subplots(figsize=(10, 4))
            dept_internos = ['Administración', 'Comercial', 'Soporte Técnico']
            dept_data = [(dept, llamadas) for dept, llamadas in kpis['actividad_por_depto_dict'].items()
                         if dept in dept_internos]
            dept_data.sort(key=lambda x: x[1], reverse=True)
            if dept_data:
                departamentos = [d[0] for d in dept_data]
                llamadas = [d[1] for d in dept_data]
                colors_dept = [plt.cm.Set2(i / len(departamentos)) for i in range(len(departamentos))]
                bars = ax.bar(departamentos, llamadas, color=colors_dept)
                ax.set_xlabel('Departamento', fontsize=10)
                ax.set_ylabel('Llamadas', fontsize=10)
                ax.set_title('Actividad por Departamento (Internos)', fontsize=12, fontweight='bold')
                for bar in bars:
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height + 0.1, f'{int(height)}',
                            ha='center', va='bottom', fontsize=8)
                plt.tight_layout()
                img_path = os.path.join(temp_dir, 'actividad_depto.png')
                plt.savefig(img_path, dpi=150, bbox_inches='tight')
                plt.close(fig)
                img_paths.append(img_path)
                elements.append(Paragraph("Actividad por Departamento", estilo_kpi))
                elements.append(Image(img_path, width=6 * inch, height=2.5 * inch))
                elements.append(Spacer(1, 0.25 * inch))
        except Exception as e:
            print(f"Error generando gráfico: {e}")

    # Gráfico de top extensiones y destinos
    if 'top_origen_dict' in kpis and 'top_destino_dict' in kpis:
        try:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
            top_origen = list(kpis['top_origen_dict'].items())[:5]
            if top_origen:
                extensiones = [str(ext) for ext, _ in top_origen]
                llamadas_origen = [llam for _, llam in top_origen]
                colors_origen = plt.cm.Blues(np.linspace(0.5, 0.9, len(extensiones)))
                ax1.bar(extensiones, llamadas_origen, color=colors_origen)
                ax1.set_xlabel('Extensión', fontsize=10)
                ax1.set_ylabel('Llamadas', fontsize=10)
                ax1.set_title('Top 5 Extensiones de Origen', fontsize=12, fontweight='bold')
                ax1.tick_params(axis='x', rotation=45, labelsize=8)
                for i, v in enumerate(llamadas_origen):
                    ax1.text(i, v + 0.1, str(v), ha='center', va='bottom', fontsize=8)

            top_destino = list(kpis['top_destino_dict'].items())[:5]
            if top_destino:
                destinos = [str(dst) if len(str(dst)) < 15 else str(dst)[:12] + '...' for dst, _ in top_destino]
                llamadas_destino = [llam for _, llam in top_destino]
                colors_destino = plt.cm.Greens(np.linspace(0.5, 0.9, len(destinos)))
                ax2.bar(destinos, llamadas_destino, color=colors_destino)
                ax2.set_xlabel('Destino', fontsize=10)
                ax2.set_ylabel('Llamadas', fontsize=10)
                ax2.set_title('Top 5 Destinos', fontsize=12, fontweight='bold')
                ax2.tick_params(axis='x', rotation=45, labelsize=8)
                for i, v in enumerate(llamadas_destino):
                    ax2.text(i, v + 0.1, str(v), ha='center', va='bottom', fontsize=8)

            plt.tight_layout()
            img_path = os.path.join(temp_dir, 'top_ext_dest.png')
            plt.savefig(img_path, dpi=150, bbox_inches='tight')
            plt.close(fig)
            img_paths.append(img_path)
            elements.append(Paragraph("Top Extensiones y Destinos", estilo_kpi))
            elements.append(Image(img_path, width=6 * inch, height=2.5 * inch))
            elements.append(Spacer(1, 0.25 * inch))
        except Exception as e:
            print(f"Error generando gráfico: {e}")

    # Tablas detalladas
    elements.append(PageBreak())
    elements.append(Paragraph("3. Tablas Detalladas", estilo_subtitulo))
    elements.append(Spacer(1, 0.25 * inch))

    # Actividad por departamento (tabla)
    if 'actividad_por_depto_dict' in kpis:
        elements.append(Paragraph("3.1 Actividad por Departamento", estilo_kpi))
        datos_dept = [["Departamento", "Llamadas", "% del Total"]]
        total_llamadas = kpis.get('total_llamadas', 1)
        for dept, llamadas in sorted(kpis['actividad_por_depto_dict'].items(), key=lambda x: x[1], reverse=True):
            if llamadas > 0:
                porcentaje = (llamadas / total_llamadas * 100)
                datos_dept.append([dept, str(llamadas), f"{porcentaje:.1f}%"])
        if len(datos_dept) > 1:
            tabla_dept = Table(datos_dept, colWidths=[3 * inch, 1.5 * inch, 1.5 * inch])
            tabla_dept.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2E86C1')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F8F9F9')),
                ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#D5D8DC')),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F2F3F4')]),
            ]))
            elements.append(tabla_dept)
            elements.append(Spacer(1, 0.25 * inch))

    # Estado de llamadas
    if kpis.get('df_resumen_disposition') is not None:
        elements.append(Paragraph("3.2 Estado de las Llamadas", estilo_kpi))
        df_disposition = kpis['df_resumen_disposition']
        if all(col in df_disposition.columns for col in ['disposition', 'count', 'percentage']):
            datos_disposition = [["Estado", "Cantidad", "%"]]
            for _, row in df_disposition.iterrows():
                datos_disposition.append([str(row['disposition']), str(row['count']), f"{row['percentage']:.1f}%"])
            tabla_disposition = Table(datos_disposition, colWidths=[2.5 * inch, 1.5 * inch, 2 * inch])
            tabla_disposition.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#27AE60')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F8F9F9')),
                ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#D5D8DC')),
            ]))
            elements.append(tabla_disposition)
            elements.append(Spacer(1, 0.25 * inch))

    # Muestra de datos
    if df is not None and not df.empty:
        elements.append(Paragraph("4. Muestra de Datos (primeras 10 llamadas)", estilo_kpi))
        columnas_interes = ['calldate', 'clid', 'src', 'dst', 'duration', 'disposition', 'billsec', 'lastapp']
        columnas_disponibles = [col for col in columnas_interes if col in df.columns]
        if not columnas_disponibles:
            columnas_disponibles = df.columns.tolist()[:5]

        if columnas_disponibles:
            df_muestra = df[columnas_disponibles].head(10)
            datos_muestra = [columnas_disponibles]
            for _, fila in df_muestra.iterrows():
                datos_muestra.append([str(fila[col])[:30] for col in columnas_disponibles])

            num_cols = len(columnas_disponibles)
            ancho_col = 6 * inch / num_cols if num_cols > 0 else 1 * inch
            tabla_muestra = Table(datos_muestra, colWidths=[ancho_col] * num_cols)
            tabla_muestra.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#7D3C98')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 9),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F8F9F9')),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#D5D8DC')),
                ('FONTSIZE', (0, 1), (-1, -1), 8),
            ]))
            elements.append(tabla_muestra)
            if 'llamadas_filtradas' in kpis:
                elements.append(Spacer(1, 0.1 * inch))
                elements.append(Paragraph(
                    f"Nota: Muestra de 10 llamadas de {kpis.get('llamadas_filtradas', 0)} analizadas.",
                    estilo_nota
                ))

    # Pie de página
    elements.append(Spacer(1, 0.5 * inch))
    elements.append(Paragraph(
        f"Resumen: {kpis.get('total_llamadas', 0)} llamadas analizadas | "
        f"Duración total: {kpis.get('duracion_total_segundos', 0) / 60:.1f} minutos | "
        f"Generado el {datetime.now().strftime('%d/%m/%Y a las %H:%M')}",
        ParagraphStyle('Footer', parent=styles['Normal'], fontSize=9,
                       alignment=TA_CENTER, textColor=colors.grey)
    ))

    doc.build(elements)

    for img_path in img_paths:
        try:
            os.remove(img_path)
        except:
            pass
    try:
        os.rmdir(temp_dir)
    except:
        pass

    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes