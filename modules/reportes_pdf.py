# reportes_pdf_reportlab.py - VERSIÓN COMPLETA CON TODOS LOS KPIs
import tempfile
import os
from datetime import datetime, timedelta
from io import BytesIO
import pandas as pd
import streamlit as st

# Importar Plotly (instalar con: pip install plotly kaleido)
try:
    import plotly.express as px
    import plotly.graph_objects as go

    PLOTLY_DISPONIBLE = True
except ImportError:
    PLOTLY_DISPONIBLE = False
    st.warning("Plotly no está instalado. Los gráficos no se generarán. Instala con: pip install plotly kaleido")

# Importar ReportLab
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors


def guardar_grafico_plotly(fig, nombre="grafico"):
    """Guarda un gráfico de Plotly como imagen temporal y devuelve la ruta"""
    if not PLOTLY_DISPONIBLE:
        return None

    try:
        # Crear un archivo temporal para la imagen
        with tempfile.NamedTemporaryFile(delete=False, suffix='.png', prefix=nombre + '_') as tmpfile:
            # Guardar el gráfico como imagen
            fig.write_image(tmpfile.name, width=800, height=500, scale=2)
            return tmpfile.name
    except Exception as e:
        print(f"Error guardando gráfico {nombre}: {e}")
        return None


def preparar_datos_para_pdf(df_contratos):
    """
    Prepara todos los datos necesarios para el informe PDF, incluyendo gráficos
    Retorna: {'datos': {...}, 'graficos': {...}}
    """
    resultado = {'datos': {}, 'graficos': {}}

    try:
        # ========================================
        # 1. PREPARAR DATOS ESTADÍSTICOS
        # ========================================
        datos = {}

        # 1.1 KPIs principales
        datos['kpis_principales'] = {
            'Total Contratos': len(df_contratos),
            'Comerciales Únicos': df_contratos['comercial'].nunique() if 'comercial' in df_contratos.columns else 0,
            'Técnicos Únicos': df_contratos['tecnico'].nunique() if 'tecnico' in df_contratos.columns else 0,
            'Estados Diferentes': df_contratos['estado'].nunique() if 'estado' in df_contratos.columns else 0,
            'SAT Únicos': df_contratos['SAT'].nunique() if 'SAT' in df_contratos.columns else 0,
            'Tipos Cliente': df_contratos['Tipo_cliente'].nunique() if 'Tipo_cliente' in df_contratos.columns else 0,
            'Contratos últimos 30 días': 0  # Se calculará después
        }

        # 1.2 Análisis por estado
        if 'estado' in df_contratos.columns:
            estado_stats = df_contratos['estado'].value_counts().reset_index()
            estado_stats.columns = ['Estado', 'Cantidad']
            estado_stats['Porcentaje'] = (estado_stats['Cantidad'] / len(df_contratos) * 100).round(2)
            datos['estado_stats'] = estado_stats

        # 1.3 Análisis por método de entrada
        if 'metodo_entrada' in df_contratos.columns:
            metodo_stats = df_contratos['metodo_entrada'].value_counts().reset_index()
            metodo_stats.columns = ['Método de Entrada', 'Cantidad']
            metodo_stats['Porcentaje'] = (metodo_stats['Cantidad'] / len(df_contratos) * 100).round(2)
            datos['metodo_entrada_stats'] = metodo_stats

        # 1.4 Análisis por comercial
        if 'comercial' in df_contratos.columns:
            comercial_stats = df_contratos['comercial'].value_counts().reset_index()
            comercial_stats.columns = ['Comercial', 'Cantidad']
            datos['comercial_stats'] = comercial_stats

        # 1.5 Análisis por técnico
        if 'tecnico' in df_contratos.columns:
            tecnico_stats = df_contratos['tecnico'].value_counts().reset_index()
            tecnico_stats.columns = ['Técnico', 'Cantidad']
            datos['tecnico_stats'] = tecnico_stats

        # 1.6 Análisis por SAT
        if 'SAT' in df_contratos.columns:
            sat_stats = df_contratos['SAT'].value_counts().reset_index()
            sat_stats.columns = ['SAT', 'Cantidad']
            datos['sat_stats'] = sat_stats

        # 1.7 Análisis por tipo de cliente
        if 'Tipo_cliente' in df_contratos.columns:
            tipo_cliente_stats = df_contratos['Tipo_cliente'].value_counts().reset_index()
            tipo_cliente_stats.columns = ['Tipo Cliente', 'Cantidad']
            datos['tipo_cliente_stats'] = tipo_cliente_stats

        # 1.8 Últimos contratos
        if 'fecha_inicio_contrato' in df_contratos.columns:
            try:
                df_contratos_copy = df_contratos.copy()
                df_contratos_copy['fecha_inicio_contrato'] = pd.to_datetime(
                    df_contratos_copy['fecha_inicio_contrato'], errors='coerce')
                datos['ultimos_contratos'] = df_contratos_copy.sort_values(
                    'fecha_inicio_contrato', ascending=False).head(20)

                # Calcular contratos últimos 30 días
                hace_30_dias = datetime.now() - timedelta(days=30)
                contratos_30_dias = df_contratos_copy[
                    df_contratos_copy['fecha_inicio_contrato'] >= hace_30_dias
                    ].shape[0]
                datos['kpis_principales']['Contratos últimos 30 días'] = contratos_30_dias

            except:
                datos['ultimos_contratos'] = df_contratos.head(20)

        # 1.9 Tiempos de respuesta
        if all(col in df_contratos.columns for col in ['fecha_ingreso', 'fecha_instalacion']):
            try:
                df_tiempos = df_contratos.copy()
                df_tiempos['fecha_ingreso'] = pd.to_datetime(df_tiempos['fecha_ingreso'], errors='coerce')
                df_tiempos['fecha_instalacion'] = pd.to_datetime(df_tiempos['fecha_instalacion'], errors='coerce')
                df_tiempos['tiempo_respuesta'] = (df_tiempos['fecha_instalacion'] - df_tiempos['fecha_ingreso']).dt.days
                df_tiempos = df_tiempos[df_tiempos['tiempo_respuesta'].between(0, 365)]

                if not df_tiempos.empty:
                    datos['tiempos_respuesta'] = {
                        'promedio': df_tiempos['tiempo_respuesta'].mean(),
                        'mediana': df_tiempos['tiempo_respuesta'].median(),
                        'menos_7_dias': (df_tiempos['tiempo_respuesta'] <= 7).mean() * 100,
                        'mas_30_dias': (df_tiempos['tiempo_respuesta'] > 30).mean() * 100,
                        'muestra': len(df_tiempos)
                    }

                    # Guardar datos de tiempos para gráfico
                    datos['df_tiempos_muestra'] = df_tiempos
            except:
                pass

        # 1.10 NUEVO: Datos para evolución temporal (mensual y semanal)
        if 'fecha_inicio_contrato' in df_contratos.columns:
            try:
                df_temporal = df_contratos.copy()
                df_temporal['fecha_inicio_contrato'] = pd.to_datetime(
                    df_temporal['fecha_inicio_contrato'], errors='coerce')

                # Evolución mensual
                df_temporal['mes'] = df_temporal['fecha_inicio_contrato'].dt.to_period('M')
                evolucion_mensual = df_temporal.groupby('mes').size().reset_index()
                evolucion_mensual.columns = ['Mes', 'Contratos']
                evolucion_mensual['Mes'] = evolucion_mensual['Mes'].astype(str)
                datos['evolucion_mensual'] = evolucion_mensual

                # Evolución semanal
                df_temporal['semana'] = df_temporal['fecha_inicio_contrato'].dt.strftime('%Y-W%U')
                evolucion_semanal = df_temporal.groupby('semana').size().reset_index()
                evolucion_semanal.columns = ['Semana', 'Contratos']
                evolucion_semanal = evolucion_semanal.sort_values('Semana')
                datos['evolucion_semanal'] = evolucion_semanal

                # Método de entrada por mes
                if 'metodo_entrada' in df_temporal.columns:
                    df_temporal['metodo_entrada'] = df_temporal['metodo_entrada'].fillna('No especificado')
                    metodo_mes = pd.crosstab(
                        df_temporal['mes'].astype(str),
                        df_temporal['metodo_entrada']
                    ).reset_index()
                    datos['metodo_mes'] = metodo_mes

                # Comercial por mes
                if 'comercial' in df_temporal.columns:
                    df_temporal['comercial'] = df_temporal['comercial'].fillna('No especificado')
                    comercial_mes = pd.crosstab(
                        df_temporal['mes'].astype(str),
                        df_temporal['comercial']
                    ).reset_index()
                    datos['comercial_mes'] = comercial_mes

            except Exception as e:
                st.warning(f"Error preparando datos temporales: {e}")

        # 1.11 NUEVO: Datos para mapa geográfico
        if 'coordenadas' in df_contratos.columns:
            try:
                df_geo = df_contratos.copy()
                df_geo = df_geo.dropna(subset=['coordenadas'])
                df_geo['coordenadas'] = df_geo['coordenadas'].astype(str).str.strip()
                df_geo = df_geo[df_geo['coordenadas'] != '']

                coords_list = []
                valid_coords = []
                estados_list = []

                for idx, row in df_geo.iterrows():
                    coord_str = row['coordenadas']
                    try:
                        parts = coord_str.split(',')
                        if len(parts) == 2:
                            lat = float(parts[0].strip())
                            lon = float(parts[1].strip())
                            if -90 <= lat <= 90 and -180 <= lon <= 180:
                                coords_list.append((lat, lon))
                                valid_coords.append(row)
                                estados_list.append(row.get('estado', 'DESCONOCIDO'))
                    except:
                        continue

                if coords_list:
                    df_valid = pd.DataFrame(valid_coords)
                    df_valid[['lat', 'lon']] = pd.DataFrame(coords_list, index=df_valid.index)
                    df_valid['estado'] = estados_list

                    # Estadísticas geográficas
                    datos['geograficos'] = {
                        'coordenadas_validas': len(df_valid),
                        'coordenadas_totales': len(df_geo),
                        'df_mapa': df_valid,
                        'centro_lat': df_valid['lat'].mean() if not df_valid.empty else 0,
                        'centro_lon': df_valid['lon'].mean() if not df_valid.empty else 0
                    }

            except Exception as e:
                st.warning(f"Error preparando datos geográficos: {e}")

        resultado['datos'] = datos

        # ========================================
        # 2. GENERAR GRÁFICOS (solo si Plotly está disponible)
        # ========================================
        graficos = {}

        if PLOTLY_DISPONIBLE:
            # 2.1 Gráfico de Distribución por Estado
            if 'estado_stats' in datos and not datos['estado_stats'].empty:
                try:
                    df_top_estados = datos['estado_stats'].head(10)
                    fig_estados = px.bar(
                        df_top_estados,
                        x='Estado',
                        y='Cantidad',
                        title='Top 10 Estados de Contratos',
                        color='Cantidad',
                        text='Cantidad',
                        color_continuous_scale='Viridis'
                    )
                    fig_estados.update_layout(
                        height=500,
                        showlegend=False,
                        xaxis_tickangle=45 if len(df_top_estados) > 5 else 0
                    )
                    ruta = guardar_grafico_plotly(fig_estados, 'estados')
                    if ruta:
                        graficos['estados'] = ruta
                except Exception as e:
                    st.warning(f"No se pudo generar gráfico de estados: {e}")

            # 2.2 Gráfico de Métodos de Entrada
            if 'metodo_entrada_stats' in datos and not datos['metodo_entrada_stats'].empty:
                try:
                    df_metodos = datos['metodo_entrada_stats'].head(8)
                    fig_metodos = px.bar(
                        df_metodos,
                        x='Método de Entrada',
                        y='Cantidad',
                        title='Distribución por Método de Entrada',
                        color='Cantidad',
                        color_continuous_scale='Blues'
                    )
                    fig_metodos.update_layout(
                        height=500,
                        showlegend=False,
                        xaxis_tickangle=45 if len(df_metodos) > 3 else 0
                    )
                    ruta = guardar_grafico_plotly(fig_metodos, 'metodos_entrada')
                    if ruta:
                        graficos['metodos_entrada'] = ruta
                except Exception as e:
                    st.warning(f"No se pudo generar gráfico de métodos: {e}")

            # 2.3 Gráfico de Comerciales
            if 'comercial_stats' in datos and not datos['comercial_stats'].empty:
                try:
                    df_comerciales = datos['comercial_stats'].head(10)
                    fig_comerciales = px.bar(
                        df_comerciales,
                        x='Comercial',
                        y='Cantidad',
                        title='Top 10 Comerciales',
                        color='Cantidad',
                        color_continuous_scale='Greens'
                    )
                    fig_comerciales.update_layout(
                        height=500,
                        showlegend=False,
                        xaxis_tickangle=45
                    )
                    ruta = guardar_grafico_plotly(fig_comerciales, 'comerciales')
                    if ruta:
                        graficos['comerciales'] = ruta
                except Exception as e:
                    st.warning(f"No se pudo generar gráfico de comerciales: {e}")

            # 2.4 Gráfico de Técnicos
            if 'tecnico_stats' in datos and not datos['tecnico_stats'].empty:
                try:
                    df_tecnicos = datos['tecnico_stats'].head(10)
                    fig_tecnicos = px.bar(
                        df_tecnicos,
                        x='Técnico',
                        y='Cantidad',
                        title='Top 10 Técnicos',
                        color='Cantidad',
                        color_continuous_scale='Reds'
                    )
                    fig_tecnicos.update_layout(
                        height=500,
                        showlegend=False,
                        xaxis_tickangle=45
                    )
                    ruta = guardar_grafico_plotly(fig_tecnicos, 'tecnicos')
                    if ruta:
                        graficos['tecnicos'] = ruta
                except Exception as e:
                    st.warning(f"No se pudo generar gráfico de técnicos: {e}")

            # 2.5 Gráfico de Tipos de Cliente (Torta)
            if 'tipo_cliente_stats' in datos and not datos['tipo_cliente_stats'].empty:
                try:
                    fig_tipo_cliente = px.pie(
                        datos['tipo_cliente_stats'],
                        values='Cantidad',
                        names='Tipo Cliente',
                        title='Distribución por Tipo de Cliente',
                        hole=0.3,
                        color_discrete_sequence=['#FF0000', '#0000FF']
                    )
                    fig_tipo_cliente.update_layout(height=500)
                    ruta = guardar_grafico_plotly(fig_tipo_cliente, 'tipo_cliente')
                    if ruta:
                        graficos['tipo_cliente'] = ruta
                except Exception as e:
                    st.warning(f"No se pudo generar gráfico de tipos de cliente: {e}")

            # 2.6 Gráfico de Histograma de Tiempos de Respuesta
            if 'df_tiempos_muestra' in datos and not datos['df_tiempos_muestra'].empty:
                try:
                    df_tiempos_hist = datos['df_tiempos_muestra']
                    fig_tiempos = px.histogram(
                        df_tiempos_hist,
                        x='tiempo_respuesta',
                        nbins=20,
                        title='Distribución de Tiempos de Respuesta',
                        labels={'tiempo_respuesta': 'Días'},
                        color_discrete_sequence=['#2E86AB']
                    )
                    fig_tiempos.update_layout(height=500)
                    ruta = guardar_grafico_plotly(fig_tiempos, 'tiempos_respuesta')
                    if ruta:
                        graficos['tiempos_respuesta'] = ruta
                except Exception as e:
                    st.warning(f"No se pudo generar gráfico de tiempos: {e}")

            # 2.7 Gráfico de SAT
            if 'sat_stats' in datos and not datos['sat_stats'].empty:
                try:
                    df_sat = datos['sat_stats'].head(10)
                    fig_sat = px.bar(
                        df_sat,
                        x='SAT',
                        y='Cantidad',
                        title='Top 10 SAT',
                        color='Cantidad',
                        color_continuous_scale='Purples'
                    )
                    fig_sat.update_layout(
                        height=500,
                        showlegend=False,
                        xaxis_tickangle=45 if len(df_sat) > 5 else 0
                    )
                    ruta = guardar_grafico_plotly(fig_sat, 'sat')
                    if ruta:
                        graficos['sat'] = ruta
                except Exception as e:
                    st.warning(f"No se pudo generar gráfico de SAT: {e}")

            # 2.8 NUEVO: Gráfico de Evolución Mensual
            if 'evolucion_mensual' in datos and not datos['evolucion_mensual'].empty:
                try:
                    fig_evolucion_mensual = px.line(
                        datos['evolucion_mensual'],
                        x='Mes',
                        y='Contratos',
                        title='Evolución Mensual de Contratos',
                        markers=True
                    )
                    fig_evolucion_mensual.update_layout(height=500)
                    ruta = guardar_grafico_plotly(fig_evolucion_mensual, 'evolucion_mensual')
                    if ruta:
                        graficos['evolucion_mensual'] = ruta
                except Exception as e:
                    st.warning(f"No se pudo generar gráfico de evolución mensual: {e}")

            # 2.9 NUEVO: Gráfico de Evolución Semanal
            if 'evolucion_semanal' in datos and not datos['evolucion_semanal'].empty:
                try:
                    # Tomar solo las últimas 12 semanas para mejor visualización
                    df_semanal_recente = datos['evolucion_semanal'].tail(12)
                    fig_evolucion_semanal = px.line(
                        df_semanal_recente,
                        x='Semana',
                        y='Contratos',
                        title='Evolución Semanal de Contratos (Últimas 12 semanas)',
                        markers=True
                    )
                    fig_evolucion_semanal.update_layout(height=500, xaxis_tickangle=45)
                    ruta = guardar_grafico_plotly(fig_evolucion_semanal, 'evolucion_semanal')
                    if ruta:
                        graficos['evolucion_semanal'] = ruta
                except Exception as e:
                    st.warning(f"No se pudo generar gráfico de evolución semanal: {e}")

            # 2.10 NUEVO: Gráfico de Método de Entrada por Mes
            if 'metodo_mes' in datos and not datos['metodo_mes'].empty:
                try:
                    # Preparar datos para gráfico de área apilada
                    df_metodo_mes = datos['metodo_mes'].copy()
                    # Tomar solo los 3 métodos principales
                    columnas_metodos = df_metodo_mes.columns[1:]  # Excluir columna 'mes'
                    if len(columnas_metodos) > 3:
                        totales_metodos = df_metodo_mes[columnas_metodos].sum()
                        top_metodos = totales_metodos.nlargest(3).index.tolist()
                        df_metodo_mes = df_metodo_mes[['mes'] + top_metodos]

                    # Crear gráfico de área
                    fig_metodo_mes = px.area(
                        df_metodo_mes,
                        x='mes',
                        y=df_metodo_mes.columns[1:].tolist(),
                        title='Evolución de Métodos de Entrada por Mes',
                        labels={'value': 'Contratos', 'variable': 'Método de Entrada'},
                        color_discrete_sequence=['#FF0000', '#0000FF']
                    )
                    fig_metodo_mes.update_layout(height=500, xaxis_tickangle=45)
                    ruta = guardar_grafico_plotly(fig_metodo_mes, 'metodo_mes')
                    if ruta:
                        graficos['metodo_mes'] = ruta
                except Exception as e:
                    st.warning(f"No se pudo generar gráfico de método por mes: {e}")

            # 2.11 NUEVO: Gráfico de Mapa Geográfico
            if 'geograficos' in datos and 'df_mapa' in datos['geograficos']:
                try:
                    df_mapa = datos['geograficos']['df_mapa']
                    if not df_mapa.empty and len(df_mapa) > 0:
                        # Crear columna de color basada en el estado
                        df_mapa['color_mapa'] = df_mapa['estado'].apply(
                            lambda x: '#00FF00' if 'FINALIZADO' in str(x).upper() else '#FF0000'
                        )

                        # Crear texto para los marcadores
                        df_mapa['texto_marcador'] = df_mapa.apply(
                            lambda row: f"Contrato: {row.get('num_contrato', 'N/A')}<br>" +
                                        f"Cliente: {row.get('cliente', 'N/A')}<br>" +
                                        f"Estado: {row.get('estado', 'N/A')}",
                            axis=1
                        )

                        # Crear mapa
                        fig_mapa = go.Figure()

                        # Puntos FINALIZADOS (verdes)
                        df_finalizados = df_mapa[df_mapa['color_mapa'] == '#00FF00']
                        if not df_finalizados.empty:
                            fig_mapa.add_trace(go.Scattermapbox(
                                lat=df_finalizados['lat'],
                                lon=df_finalizados['lon'],
                                mode='markers',
                                marker=dict(size=12, color='#00FF00'),
                                text=df_finalizados['texto_marcador'],
                                name='FINALIZADO',
                                hovertemplate='%{text}<extra></extra>'
                            ))

                        # Puntos OTROS ESTADOS (rojos)
                        df_otros = df_mapa[df_mapa['color_mapa'] == '#FF0000']
                        if not df_otros.empty:
                            fig_mapa.add_trace(go.Scattermapbox(
                                lat=df_otros['lat'],
                                lon=df_otros['lon'],
                                mode='markers',
                                marker=dict(size=5, color='#FF0000'),
                                text=df_otros['texto_marcador'],
                                name='Otros estados',
                                hovertemplate='%{text}<extra></extra>'
                            ))

                        # Configurar el layout del mapa
                        centro_lat = datos['geograficos'].get('centro_lat', df_mapa['lat'].mean())
                        centro_lon = datos['geograficos'].get('centro_lon', df_mapa['lon'].mean())

                        fig_mapa.update_layout(
                            mapbox=dict(
                                style="open-street-map",
                                center=dict(lat=centro_lat, lon=centro_lon),
                                zoom=7
                            ),
                            height=600,
                            title="Mapa de Contratos por Estado",
                            legend=dict(
                                yanchor="top",
                                y=0.99,
                                xanchor="left",
                                x=0.01
                            ),
                            margin={"r": 0, "t": 30, "l": 0, "b": 0}
                        )

                        ruta = guardar_grafico_plotly(fig_mapa, 'mapa_contratos')
                        if ruta:
                            graficos['mapa'] = ruta
                except Exception as e:
                    st.warning(f"No se pudo generar gráfico de mapa: {e}")

        else:
            st.warning("⚠️ Plotly no está disponible. Los gráficos no se generarán.")

        resultado['graficos'] = graficos

        # Información de depuración
        st.info(f"✅ Datos preparados. Gráficos generados: {len(graficos)}")

    except Exception as e:
        st.error(f"❌ Error preparando datos para PDF: {str(e)}")
        import traceback
        with st.expander("🔍 Ver detalles del error", expanded=False):
            st.code(traceback.format_exc())

    return resultado


def generar_pdf_reportlab(df_contratos, datos_completos):
    """Genera PDF usando ReportLab incluyendo gráficos"""

    # Extraer datos y gráficos del diccionario completo
    kpis_data = datos_completos.get('datos', {})
    graficos = datos_completos.get('graficos', {})

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=30, bottomMargin=30)
    story = []
    styles = getSampleStyleSheet()

    # Título principal
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=20,
        spaceAfter=20,
        alignment=1,  # Centrado
        textColor=colors.HexColor('#2C3E50')
    )
    story.append(Paragraph("INFORME DE KPIs - SEGUIMIENTO DE CONTRATOS", title_style))

    # Fecha y metadatos
    date_style = ParagraphStyle(
        'CustomDate',
        parent=styles['Normal'],
        fontSize=10,
        alignment=1,
        textColor=colors.grey
    )
    story.append(Paragraph(f"Generado el {datetime.now().strftime('%d/%m/%Y %H:%M')}", date_style))
    story.append(Paragraph(f"Total contratos analizados: {len(df_contratos)}", date_style))
    story.append(Spacer(1, 20))

    # 1. KPIs PRINCIPALES
    story.append(Paragraph("1. KPIs PRINCIPALES", styles['Heading2']))

    if 'kpis_principales' in kpis_data:
        kpi_data = [['KPI', 'Valor']]
        for key, value in kpis_data['kpis_principales'].items():
            if isinstance(value, (int, float)):
                formatted_value = f"{value:,}" if isinstance(value, int) else f"{value:.1f}"
            else:
                formatted_value = str(value)
            kpi_data.append([key, formatted_value])

        if len(kpi_data) > 1:
            table = Table(kpi_data, colWidths=[250, 100])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498DB')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 11),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F8F9FA')),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F8F9FA')])
            ]))
            story.append(table)
            story.append(Spacer(1, 20))

    # 2. ANÁLISIS POR ESTADO
    if 'estado_stats' in kpis_data and not kpis_data['estado_stats'].empty:
        story.append(Paragraph("2. ANÁLISIS POR ESTADO", styles['Heading2']))

        estado_data = [['Estado', 'Cantidad', 'Porcentaje']]
        for _, row in kpis_data['estado_stats'].head(10).iterrows():
            estado_data.append([
                str(row['Estado']),
                str(row['Cantidad']),
                f"{row['Porcentaje']:.1f}%"
            ])

        table = Table(estado_data, colWidths=[200, 80, 80])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2ECC71')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F8F9FA')])
        ]))
        story.append(table)
        story.append(Spacer(1, 10))

        # Gráfico de estados si existe
        if 'estados' in graficos:
            try:
                story.append(Paragraph("Gráfico: Distribución por Estado", styles['Heading3']))
                img = Image(graficos['estados'], width=400, height=250)
                story.append(img)
                story.append(Spacer(1, 10))
            except Exception as e:
                story.append(Paragraph(f"(Error cargando gráfico: {e})", styles['Italic']))
                story.append(Spacer(1, 10))
        else:
            story.append(Paragraph("(No hay gráfico disponible para esta sección)", styles['Italic']))
            story.append(Spacer(1, 10))

    # 3. ANÁLISIS POR MÉTODO DE ENTRADA
    if 'metodo_entrada_stats' in kpis_data and not kpis_data['metodo_entrada_stats'].empty:
        #story.append(PageBreak())
        story.append(Paragraph("3. ANÁLISIS POR MÉTODO DE ENTRADA", styles['Heading2']))

        metodo_data = [['Método de Entrada', 'Cantidad', 'Porcentaje']]
        for _, row in kpis_data['metodo_entrada_stats'].head(10).iterrows():
            metodo_data.append([
                str(row['Método de Entrada']),
                str(row['Cantidad']),
                f"{row['Porcentaje']:.1f}%"
            ])

        table = Table(metodo_data, colWidths=[200, 80, 80])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E74C3C')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        story.append(table)
        story.append(Spacer(1, 10))

        # Gráfico de métodos si existe
        if 'metodos_entrada' in graficos:
            try:
                story.append(Paragraph("Gráfico: Métodos de Entrada", styles['Heading3']))
                img = Image(graficos['metodos_entrada'], width=400, height=250)
                story.append(img)
                story.append(Spacer(1, 20))
            except Exception as e:
                story.append(Paragraph(f"(Error cargando gráfico: {e})", styles['Italic']))
                story.append(Spacer(1, 20))
        else:
            story.append(Paragraph("(No hay gráfico disponible para esta sección)", styles['Italic']))
            story.append(Spacer(1, 20))

    # 4. ANÁLISIS POR COMERCIAL
    if 'comercial_stats' in kpis_data and not kpis_data['comercial_stats'].empty:
        story.append(Paragraph("4. ANÁLISIS POR COMERCIAL", styles['Heading2']))

        comercial_data = [['Comercial', 'Contratos']]
        for _, row in kpis_data['comercial_stats'].head(10).iterrows():
            comercial_data.append([
                str(row['Comercial']),
                str(row['Cantidad'])
            ])

        table = Table(comercial_data, colWidths=[250, 80])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#9B59B6')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        story.append(table)
        story.append(Spacer(1, 10))

        # Gráfico de comerciales si existe
        if 'comerciales' in graficos:
            try:
                story.append(Paragraph("Gráfico: Top Comerciales", styles['Heading3']))
                img = Image(graficos['comerciales'], width=400, height=250)
                story.append(img)
                story.append(Spacer(1, 20))
            except Exception as e:
                story.append(Paragraph(f"(Error cargando gráfico: {e})", styles['Italic']))
                story.append(Spacer(1, 20))
        else:
            story.append(Paragraph("(No hay gráfico disponible para esta sección)", styles['Italic']))
            story.append(Spacer(1, 20))

    # 5. ANÁLISIS POR TÉCNICO
    if 'tecnico_stats' in kpis_data and not kpis_data['tecnico_stats'].empty:
        story.append(Paragraph("5. ANÁLISIS POR TÉCNICO", styles['Heading2']))

        tecnico_data = [['Técnico', 'Instalaciones']]
        for _, row in kpis_data['tecnico_stats'].head(10).iterrows():
            tecnico_data.append([
                str(row['Técnico']),
                str(row['Cantidad'])
            ])

        table = Table(tecnico_data, colWidths=[250, 80])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1ABC9C')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        story.append(table)
        story.append(Spacer(1, 10))

        # Gráfico de técnicos si existe
        if 'tecnicos' in graficos:
            try:
                story.append(Paragraph("Gráfico: Top Técnicos", styles['Heading3']))
                img = Image(graficos['tecnicos'], width=400, height=250)
                story.append(img)
                story.append(Spacer(1, 20))
            except Exception as e:
                story.append(Paragraph(f"(Error cargando gráfico: {e})", styles['Italic']))
                story.append(Spacer(1, 20))
        else:
            story.append(Paragraph("(No hay gráfico disponible para esta sección)", styles['Italic']))
            story.append(Spacer(1, 20))

    # 6. ANÁLISIS POR SAT
    if 'sat_stats' in kpis_data and not kpis_data['sat_stats'].empty:
        #story.append(PageBreak())
        story.append(Paragraph("6. ANÁLISIS POR SAT", styles['Heading2']))

        sat_data = [['SAT', 'Contratos']]
        for _, row in kpis_data['sat_stats'].head(10).iterrows():
            sat_data.append([
                str(row['SAT']),
                str(row['Cantidad'])
            ])

        table = Table(sat_data, colWidths=[250, 80])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#F39C12')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        story.append(table)
        story.append(Spacer(1, 10))

        # Gráfico de SAT si existe
        if 'sat' in graficos:
            try:
                story.append(Paragraph("Gráfico: Top SAT", styles['Heading3']))
                img = Image(graficos['sat'], width=400, height=250)
                story.append(img)
                story.append(Spacer(1, 20))
            except Exception as e:
                story.append(Paragraph(f"(Error cargando gráfico: {e})", styles['Italic']))
                story.append(Spacer(1, 20))
        else:
            story.append(Paragraph("(No hay gráfico disponible para esta sección)", styles['Italic']))
            story.append(Spacer(1, 20))

    # 7. ANÁLISIS POR TIPO DE CLIENTE
    # 7. ANÁLISIS POR TIPO DE CLIENTE
    if 'tipo_cliente_stats' in kpis_data and not kpis_data['tipo_cliente_stats'].empty:
        story.append(Paragraph("7. ANÁLISIS POR TIPO DE CLIENTE", styles['Heading2']))

        tipo_cliente_data = [['Tipo de Cliente', 'Cantidad']]
        for _, row in kpis_data['tipo_cliente_stats'].iterrows():
            tipo_cliente_data.append([
                str(row['Tipo Cliente']),
                str(row['Cantidad'])
            ])

        table = Table(tipo_cliente_data, colWidths=[250, 80])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#34495E')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        story.append(table)
        story.append(Spacer(1, 10))

        # Gráfico de tipo de cliente si existe
        if 'tipo_cliente' in graficos:
            try:
                story.append(Paragraph("Gráfico: Distribución por Tipo de Cliente", styles['Heading3']))

                # **SOLUCIÓN: Usar solo dos colores básicos y tamaño reducido**
                try:
                    # Primero intentar con colores básicos y tamaño mediano
                    img = Image(graficos['tipo_cliente'], width=280, height=190)
                    story.append(img)
                except Exception as img_error:
                    # Si falla, mostrar solo la tabla sin gráfico
                    story.append(Paragraph("(El gráfico no se pudo mostrar correctamente)", styles['Italic']))
                    story.append(Spacer(1, 10))

                    # **ALTERNATIVA: Mostrar tabla de colores manual**
                    story.append(Paragraph("Distribución visual por colores:", styles['Normal']))

                    # Crear una tabla simple con colores básicos
                    colores_basicos = [['🔵', 'Rojo/Naranja'], ['🟢', 'Verde/Azul']]
                    table_colores = Table(colores_basicos, colWidths=[50, 200])
                    table_colores.setStyle(TableStyle([
                        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                        ('FONTSIZE', (0, 0), (-1, -1), 10),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ]))
                    story.append(table_colores)

                story.append(Spacer(1, 20))
            except Exception as e:
                story.append(Paragraph(f"(Error técnico en el gráfico)", styles['Italic']))
                story.append(Spacer(1, 20))
        else:
            story.append(Paragraph("(No hay gráfico disponible para esta sección)", styles['Italic']))
            story.append(Spacer(1, 20))

    # 8. TIEMPOS DE RESPUESTA
    if 'tiempos_respuesta' in kpis_data:
        story.append(Paragraph("8. TIEMPOS DE RESPUESTA", styles['Heading2']))

        tiempos_data = [['Métrica', 'Valor']]
        tiempos = kpis_data['tiempos_respuesta']

        metricas = [
            ('Tiempo promedio', f"{tiempos.get('promedio', 0):.1f} días"),
            ('Mediana', f"{tiempos.get('mediana', 0):.1f} días"),
            ('Instalaciones < 7 días', f"{tiempos.get('menos_7_dias', 0):.1f}%"),
            ('Instalaciones > 30 días', f"{tiempos.get('mas_30_dias', 0):.1f}%"),
            ('Muestra analizada', f"{tiempos.get('muestra', 0)} contratos")
        ]

        for nombre, valor in metricas:
            tiempos_data.append([nombre, valor])

        table = Table(tiempos_data, colWidths=[250, 100])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E67E22')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        story.append(table)
        story.append(Spacer(1, 10))

        # Gráfico de tiempos de respuesta si existe
        if 'tiempos_respuesta' in graficos:
            try:
                story.append(Paragraph("Gráfico: Distribución de Tiempos de Respuesta", styles['Heading3']))
                img = Image(graficos['tiempos_respuesta'], width=400, height=250)
                story.append(img)
                story.append(Spacer(1, 20))
            except Exception as e:
                story.append(Paragraph(f"(Error cargando gráfico: {e})", styles['Italic']))
                story.append(Spacer(1, 20))
        else:
            story.append(Paragraph("(No hay gráfico disponible para esta sección)", styles['Italic']))
            story.append(Spacer(1, 20))

    # 9. NUEVA SECCIÓN: EVOLUCIÓN TEMPORAL
    story.append(PageBreak())
    story.append(Paragraph("9. EVOLUCIÓN TEMPORAL", styles['Heading2']))

    # 9.1 Evolución Mensual
    if 'evolucion_mensual' in kpis_data and not kpis_data['evolucion_mensual'].empty:
        story.append(Paragraph("9.1 Evolución Mensual de Contratos", styles['Heading3']))

        # Tabla de datos
        evolucion_data = [['Mes', 'Contratos']]
        for _, row in kpis_data['evolucion_mensual'].iterrows():
            evolucion_data.append([
                str(row['Mes']),
                str(row['Contratos'])
            ])

        table = Table(evolucion_data, colWidths=[200, 80])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498DB')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        story.append(table)
        story.append(Spacer(1, 10))

        # Gráfico de evolución mensual si existe
        if 'evolucion_mensual' in graficos:
            try:
                story.append(Paragraph("Gráfico: Evolución Mensual", styles['Heading4']))
                img = Image(graficos['evolucion_mensual'], width=400, height=250)
                story.append(img)
                story.append(Spacer(1, 20))
            except Exception as e:
                story.append(Paragraph(f"(Error cargando gráfico: {e})", styles['Italic']))
                story.append(Spacer(1, 20))

    # 9.2 Evolución Semanal
    if 'evolucion_semanal' in kpis_data and not kpis_data['evolucion_semanal'].empty:
        story.append(Paragraph("9.2 Evolución Semanal de Contratos", styles['Heading3']))

        # Tabla de datos (últimas 8 semanas)
        evolucion_semanal_data = [['Semana', 'Contratos']]
        for _, row in kpis_data['evolucion_semanal'].tail(8).iterrows():
            evolucion_semanal_data.append([
                str(row['Semana']),
                str(row['Contratos'])
            ])

        table = Table(evolucion_semanal_data, colWidths=[150, 80])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#9B59B6')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        story.append(table)
        story.append(Spacer(1, 10))

        # Gráfico de evolución semanal si existe
        if 'evolucion_semanal' in graficos:
            try:
                story.append(Paragraph("Gráfico: Evolución Semanal", styles['Heading4']))
                img = Image(graficos['evolucion_semanal'], width=400, height=250)
                story.append(img)
                story.append(Spacer(1, 20))
            except Exception as e:
                story.append(Paragraph(f"(Error cargando gráfico: {e})", styles['Italic']))
                story.append(Spacer(1, 20))

    # 9.3 Método de Entrada por Mes
    if 'metodo_mes' in kpis_data and not kpis_data['metodo_mes'].empty:
        story.append(Paragraph("9.3 Evolución de Métodos de Entrada por Mes", styles['Heading3']))

        # Tabla resumen (últimos 3 meses)
        if 'metodo_mes' in graficos:
            try:
                story.append(Paragraph("Gráfico: Métodos de Entrada por Mes", styles['Heading4']))
                img = Image(graficos['metodo_mes'], width=400, height=250)
                story.append(img)
                story.append(Spacer(1, 20))
            except Exception as e:
                story.append(Paragraph(f"(Error cargando gráfico: {e})", styles['Italic']))
                story.append(Spacer(1, 20))

    # 10. NUEVA SECCIÓN: ANÁLISIS GEOGRÁFICO
    if 'geograficos' in kpis_data:
        story.append(PageBreak())
        story.append(Paragraph("10. ANÁLISIS GEOGRÁFICO", styles['Heading2']))

        geograficos = kpis_data['geograficos']
        story.append(Paragraph(
            f"Coordenadas válidas: {geograficos.get('coordenadas_validas', 0)} de {geograficos.get('coordenadas_totales', 0)}",
            styles['Normal']))
        story.append(Spacer(1, 10))

        # Gráfico de mapa si existe
        if 'mapa' in graficos:
            try:
                story.append(Paragraph("Mapa de Distribución de Contratos", styles['Heading3']))
                img = Image(graficos['mapa'], width=500, height=350)
                story.append(img)
                story.append(Spacer(1, 10))

                # Leyenda
                legend_style = ParagraphStyle(
                    'LegendStyle',
                    parent=styles['Normal'],
                    fontSize=9,
                    textColor=colors.black
                )

                story.append(Paragraph("Leyenda:", styles['Heading4']))
                story.append(
                    Paragraph("• <font color='#00FF00'>🟢 Puntos VERDES</font>: Contratos FINALIZADOS", legend_style))
                story.append(Paragraph("• <font color='#FF0000'>🔴 Puntos ROJOS</font>: Otros estados de contratos",
                                       legend_style))
                story.append(Spacer(1, 20))

            except Exception as e:
                story.append(Paragraph(f"(Error cargando mapa: {e})", styles['Italic']))
                story.append(Spacer(1, 20))
        else:
            story.append(Paragraph("(No hay datos geográficos suficientes para generar el mapa)", styles['Italic']))
            story.append(Spacer(1, 20))

    # 11. ÚLTIMOS CONTRATOS
    if 'ultimos_contratos' in kpis_data and not kpis_data['ultimos_contratos'].empty:
        #story.append(PageBreak())
        story.append(Paragraph("11. ÚLTIMOS CONTRATOS REGISTRADOS", styles['Heading2']))

        # Seleccionar columnas para mostrar
        columnas_disponibles = kpis_data['ultimos_contratos'].columns.tolist()
        columnas_mostrar = ['num_contrato', 'cliente', 'estado', 'fecha_inicio_contrato']
        columnas_mostrar = [c for c in columnas_mostrar if c in columnas_disponibles]

        if columnas_mostrar:
            ultimos_data = [columnas_mostrar]

            for _, row in kpis_data['ultimos_contratos'].head(15).iterrows():
                fila = []
                for col in columnas_mostrar:
                    valor = row[col]
                    if pd.isna(valor):
                        fila.append('')
                    elif 'fecha' in col.lower() and hasattr(valor, 'strftime'):
                        fila.append(valor.strftime('%d/%m/%Y'))
                    else:
                        fila.append(str(valor)[:30])  # Limitar longitud
                ultimos_data.append(fila)

            # Calcular anchos de columna
            col_widths = [80, 150, 80, 80][:len(columnas_mostrar)]

            table = Table(ultimos_data, colWidths=col_widths)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2C3E50')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 9),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('FONTSIZE', (0, 1), (-1, -1), 8),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F8F9FA')])
            ]))
            story.append(table)

    # 12. RESUMEN EJECUTIVO
    #story.append(PageBreak())
    story.append(Paragraph("12. RESUMEN EJECUTIVO", styles['Heading2']))

    # Calcular algunos KPIs adicionales para el resumen
    contratos_30_dias = kpis_data['kpis_principales'].get('Contratos últimos 30 días',
                                                          0) if 'kpis_principales' in kpis_data else 0

    resumen_text = f"""
    Este informe presenta un análisis completo de los contratos de la empresa.
    Se han analizado un total de <b>{len(df_contratos)}</b> contratos, con datos recopilados de múltiples dimensiones.

    <b>Principales hallazgos:</b>
    <br/>• Se identificaron <b>{df_contratos['estado'].nunique() if 'estado' in df_contratos.columns else 0}</b> estados diferentes de contratos
    <br/>• <b>{df_contratos['comercial'].nunique() if 'comercial' in df_contratos.columns else 0}</b> comerciales activos
    <br/>• <b>{df_contratos['tecnico'].nunique() if 'tecnico' in df_contratos.columns else 0}</b> técnicos realizando instalaciones
    <br/>• <b>{df_contratos['Tipo_cliente'].nunique() if 'Tipo_cliente' in df_contratos.columns else 0}</b> tipos de clientes diferentes
    <br/>• <b>{contratos_30_dias}</b> contratos registrados en los últimos 30 días

    <b>Análisis temporal:</b>
    <br/>• La evolución mensual muestra las tendencias de contratación
    <br/>• El análisis semanal permite identificar patrones de actividad
    <br/>• Los métodos de entrada por mes ayudan a optimizar canales

    <b>Distribución geográfica:</b>
    <br/>• Se han mapeado <b>{kpis_data.get('geograficos', {}).get('coordenadas_validas', 0) if 'geograficos' in kpis_data else 0}</b> ubicaciones
    <br/>• Permite identificar concentraciones y áreas de oportunidad

    <b>Recomendaciones:</b>
    <br/>• Revisar los contratos con tiempos de respuesta elevados
    <br/>• Analizar la distribución por método de entrada para optimizar canales
    <br/>• Evaluar el rendimiento por comercial y técnico
    <br/>• Expandir operaciones en áreas con alta concentración de contratos
    <br/>• Monitorear la evolución mensual para anticipar tendencias
    """

    # Definir estilo para el resumen
    resumen_style = ParagraphStyle(
        'ResumenStyle',
        parent=styles['Normal'],
        fontSize=10,
        leading=14,  # Espaciado entre líneas
        spaceAfter=20
    )

    story.append(Paragraph(resumen_text, resumen_style))

    # Pie de página con metadatos
    metadata_style = ParagraphStyle(
        'MetadataStyle',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.grey,
        alignment=1
    )

    story.append(Spacer(1, 20))
    story.append(Paragraph("---", metadata_style))
    story.append(Paragraph(f"Informe generado automáticamente por Sistema de KPIs", metadata_style))
    story.append(
        Paragraph(f"Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} | Total páginas: [PÁGINA]", metadata_style))

    # Construir el PDF
    doc.build(story)
    buffer.seek(0)

    # Limpiar archivos temporales de gráficos
    for ruta_grafico in graficos.values():
        try:
            if os.path.exists(ruta_grafico):
                os.remove(ruta_grafico)
        except:
            pass  # Ignorar errores al eliminar

    return buffer