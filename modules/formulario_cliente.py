import streamlit as st
import os
from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from modules.plantilla_email import generar_html
import smtplib
from email.message import EmailMessage
import base64
from PIL import Image as PILImage
import numpy as np
import json
from datetime import datetime

# Añadir el import para el canvas de firma
try:
    from streamlit_drawable_canvas import st_canvas

    CANVAS_AVAILABLE = True
except ImportError:
    st.error(
        "❌ El componente streamlit-drawable-canvas no está instalado. Ejecuta: pip install streamlit-drawable-canvas")
    CANVAS_AVAILABLE = False

from modules.db import get_db_connection
# -------------------- GEOLOCALIZACIÓN SIMPLE --------------------
def obtener_coordenadas_cartociudad(direccion, cp, poblacion, provincia):
    """
    Obtiene coordenadas usando CartoCiudad.
    VERSIÓN OPTIMIZADA: Maneja direcciones sin número, busca portal más cercano y marca precisión.
    """
    import re
    import requests
    import json
    from urllib.parse import quote
    from datetime import datetime

    # 1. Análisis inteligente de la dirección
    direccion_limpia = direccion.strip()

    # Buscar el último número en la dirección
    match_numero = re.search(r'(\d+)\s*$', direccion_limpia)
    numero_buscado = int(match_numero.group(1)) if match_numero else None

    # Extraer el nombre de la vía (inteligente: elimina números y caracteres extraños)
    if numero_buscado:
        # Elimina solo el número final
        nombre_via = re.sub(r'\s*\d+\s*$', '', direccion_limpia).strip()
        # Limpia espacios extra y comas al final
        nombre_via = re.sub(r'[\s,]+$', '', nombre_via)
    else:
        nombre_via = direccion_limpia

    print(f"🔍 Análisis de dirección:")
    print(f"   - Dirección original: '{direccion}'")
    print(f"   - Nombre de vía: '{nombre_via}'")
    print(f"   - Número buscado: {numero_buscado}")
    print(f"   - Población: {poblacion}")
    print(f"   - CP: {cp}")

    # 2. Construcción inteligente de variantes (optimizado)
    variantes_a_probar = []

    # Componentes para construir variantes
    componentes = {
        'via': nombre_via,
        'via_upper': nombre_via.upper(),
        'num': f" {numero_buscado}" if numero_buscado else "",
        'pob': poblacion,
        'pob_upper': poblacion.upper(),
        'cp': cp,
        'prov': provincia,
        'prov_upper': provincia.upper()
    }

    # REGLA: Construir variantes según si tenemos número o no
    if numero_buscado:
        # Con número - variantes más específicas primero
        variantes_a_probar.extend([
            f"{componentes['via']}{componentes['num']}, {componentes['pob']}",
            f"{componentes['via']}{componentes['num']}, {componentes['cp']} {componentes['pob']}",
            f"{componentes['via_upper']}{componentes['num']}, {componentes['pob_upper']}",
            f"{componentes['via']}{componentes['num']}, {componentes['pob']}, {componentes['prov']}",
            f"{componentes['via']}, {componentes['pob']}",  # Sin número como fallback
        ])
    else:
        # Sin número - variantes más genéricas
        variantes_a_probar.extend([
            f"{componentes['via']}, {componentes['pob']}",
            f"{componentes['via']}, {componentes['cp']} {componentes['pob']}",
            f"{componentes['via_upper']}, {componentes['pob_upper']}",
            f"{componentes['via']}, {componentes['pob']}, {componentes['prov']}",
            componentes['via'],  # Solo la vía
        ])

    print(f"🔢 Se probarán {len(variantes_a_probar)} variantes")

    # 3. Búsqueda principal en la API
    mejores_candidatos = []
    mejor_variante = None

    for i, direccion_consulta in enumerate(variantes_a_probar):
        try:
            print(f"\n🔄 Intento {i + 1}: '{direccion_consulta}'")

            direccion_codificada = quote(direccion_consulta)
            url = f"https://www.cartociudad.es/geocoder/api/geocoder/candidatesJsonp?q={direccion_codificada}&limit=15"
            response = requests.get(url, timeout=8)

            if response.status_code != 200:
                continue

            contenido = response.text.strip()
            if contenido.startswith('callback(') and contenido.endswith(')'):
                json_str = contenido[9:-1]
                candidatos = json.loads(json_str)
            else:
                candidatos = json.loads(contenido)

            if not candidatos:
                continue

            print(f"   ✅ {len(candidatos)} candidatos encontrados")

            # Guardar los mejores candidatos de esta variante
            mejores_candidatos.extend(candidatos)
            mejor_variante = direccion_consulta

            # Si tenemos suficientes candidatos, podemos parar
            if len(mejores_candidatos) >= 10:
                break

        except Exception:
            continue

    # 4. Análisis de resultados y selección FINAL
    if not mejores_candidatos:
        print(f"\n❌ No se encontraron resultados para ninguna variante")
        return None

    print(f"\n📊 ANÁLISIS FINAL: {len(mejores_candidatos)} candidatos para evaluar")

    # Preparar lista de portales disponibles
    portales_disponibles = []
    for cand in mejores_candidatos:
        portal_num = cand.get('portalNumber')
        if portal_num is not None:
            portales_disponibles.append((portal_num, cand))

    # Caso 1: No buscamos número específico
    if not numero_buscado:
        portal_encontrado = mejores_candidatos[0]
        precision = "sin_numero"
        print(f"   ⭐ Dirección sin número - usando primer resultado")

    # Caso 2: Buscamos número y existe exactamente
    elif any(portal_num == numero_buscado for portal_num, _ in portales_disponibles):
        for portal_num, cand in portales_disponibles:
            if portal_num == numero_buscado:
                portal_encontrado = cand
                precision = "exacta"
                print(f"   🎯 ENCONTRADO portal {numero_buscado} (coincidencia exacta)")
                break

    # Caso 3: Buscamos número pero no existe - encontrar el MÁS CERCANO
    else:
        # Encontrar el portal con número más cercano
        portal_mas_cercano = None
        menor_diferencia = float('inf')

        for portal_num, cand in portales_disponibles:
            diferencia = abs(portal_num - numero_buscado)
            if diferencia < menor_diferencia:
                menor_diferencia = diferencia
                portal_mas_cercano = cand
                portal_num_cercano = portal_num

        if portal_mas_cercano:
            portal_encontrado = portal_mas_cercano
            precision = "aproximada"
            print(f"   🔍 APROXIMACIÓN: portal {numero_buscado} no existe")
            print(f"   📍 Usando portal {portal_num_cercano} (diferencia: {menor_diferencia})")
        else:
            # Fallback: usar primer resultado
            portal_encontrado = mejores_candidatos[0]
            precision = "aproximada_general"
            print(f"   ⚠️  No hay portales numerados - usando mejor candidato")

    # 5. Construir resultado FINAL
    resultado = {
        "lat": float(portal_encontrado['lat']),
        "lon": float(portal_encontrado['lng']),
        "direccion_normalizada": portal_encontrado.get('address', mejor_variante or direccion_limpia),
        "portal_original": numero_buscado,
        "portal_encontrado": portal_encontrado.get('portalNumber'),
        "codigo_postal": portal_encontrado.get('postalCode', cp),
        "precision": precision,
        "fuente": "CartoCiudad",
        "timestamp": datetime.now().isoformat(),
        "notas": ""
    }

    # Añadir notas según la precisión
    if precision == "exacta":
        resultado["notas"] = "Ubicación exacta del portal"
    elif precision == "aproximada":
        dif = abs(numero_buscado - resultado["portal_encontrado"])
        resultado[
            "notas"] = f"Portal {numero_buscado} no encontrado. Usando portal {resultado['portal_encontrado']} (diferencia: {dif})"
    elif precision == "sin_numero":
        resultado["notas"] = "Dirección sin número específico. Usando ubicación aproximada de la vía."
    else:
        resultado["notas"] = "Ubicación aproximada basada en la dirección proporcionada."

    print(f"\n✅ RESULTADO FINAL: {resultado['direccion_normalizada']}")
    print(f"   📍 Coordenadas: {resultado['lat']:.6f}, {resultado['lon']:.6f}")
    print(f"   🎯 Precisión: {precision}")
    print(f"   📝 Notas: {resultado['notas']}")

    return resultado


def guardar_coordenadas_en_db(precontrato_id, coordenadas):
    """Guarda las coordenadas en la base de datos"""
    if not coordenadas:
        return False

    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE precontratos 
            SET coordenadas = %s 
            WHERE id = %s
        """, (json.dumps(coordenadas), precontrato_id))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Error guardando coordenadas: {e}")
        return False
    finally:
        conn.close()
#####


# -------------------- VALIDAR TOKEN (CON DEPURACIÓN) --------------------
def validar_token(precontrato_id, token):
    # st.write(f"🔍 Depuración: Validando token - precontrato_id: {precontrato_id}, token: {token}")

    conn = get_db_connection()
    if not conn:
        return False, "Error de conexión a la base de datos"

    try:
        # Convertir a int porque viene como string desde la URL
        precontrato_id = int(precontrato_id)

        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM precontrato_links
            WHERE precontrato_id = %s AND token = %s
        """, (precontrato_id, token))
        link = cursor.fetchone()

        # st.write(f"🔍 Resultado de la consulta: {link}")

        if not link:
            return False, "❌ Enlace no válido o ya utilizado."

        # Asumiendo la estructura de la tabla:
        # [0]: id, [1]: precontrato_id, [2]: token, [3]: expiracion, [4]: usado
        expiracion = datetime.fromisoformat(link[3])
        usado = link[4]

        if usado:
            return False, "❌ Este enlace ya ha sido utilizado."

        if datetime.now() > expiracion:
            return False, "⚠️ El enlace ha caducado. Solicita uno nuevo."

        return True, None

    except Exception as e:
        st.error(f"❌ Error en validación: {e}")
        return False, f"Error en validación: {e}"
    finally:
        conn.close()


# ------FUNCIONES DE VALIDACION------#
# Funciones de validación
def validar_dni(dni):
    """Validar DNI/NIF español incluyendo la letra de control"""
    if not dni:
        return False, "El DNI no puede estar vacío"

    # Limpiar y normalizar el DNI
    dni_upper = dni.upper().replace(' ', '').replace('-', '')

    # Patrón para DNI (8 números + letra) o NIE (X/Y/Z + 7 números + letra)
    patron_dni = r'^[0-9]{8}[A-Z]$'
    patron_nie = r'^[XYZ][0-9]{7}[A-Z]$'

    if not (re.match(patron_dni, dni_upper) or re.match(patron_nie, dni_upper)):
        return False, "Formato de DNI/NIE inválido. Use: 12345678A o X1234567A"

    # Validar la letra de control
    letras_validas = "TRWAGMYFPDXBNJZSQVHLCKE"

    if re.match(patron_dni, dni_upper):
        # Validar DNI español
        numero = int(dni_upper[:8])
        letra_correcta = letras_validas[numero % 23]
        if dni_upper[8] != letra_correcta:
            return False, f"Letra del DNI incorrecta. Debería ser: {letra_correcta}"
    else:
        # Validar NIE (Número de Identificación de Extranjero)
        # Mapear letra inicial a número: X=0, Y=1, Z=2
        mapa_nie = {'X': '0', 'Y': '1', 'Z': '2'}
        numero_str = mapa_nie[dni_upper[0]] + dni_upper[1:8]
        numero = int(numero_str)
        letra_correcta = letras_validas[numero % 23]
        if dni_upper[8] != letra_correcta:
            return False, f"Letra del NIE incorrecta. Debería ser: {letra_correcta}"

    return True, "DNI/NIE válido"


def validar_email(email):
    """Validar formato de email"""
    if not email:
        return False, "El email no puede estar vacío"

    patron_email = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

    if not re.match(patron_email, email):
        return False, "Formato de email inválido. Use: ejemplo@dominio.com"

    return True, "Email válido"


import re


def validar_codigo_postal(cp):
    """Validar código postal español con mejores verificaciones y retrocompatibilidad"""
    if not cp:
        return False, "El código postal no puede estar vacío"

    # Limpiar y normalizar
    cp_limpio = cp.strip().upper().replace(' ', '').replace('-', '')

    # Verificar formato básico
    if not re.match(r'^[0-9]{5}$', cp_limpio):
        # Intentar corregir formatos como "28 001" o "28-001"
        match = re.match(r'^([0-9]{2})[ -]?([0-9]{3})$', cp.strip())
        if match:
            cp_limpio = match.group(1) + match.group(2)
        else:
            return False, "Formato inválido. Debe tener 5 dígitos (ej: 28001)"

    # Validar rango general
    codigo_num = int(cp_limpio)
    if codigo_num < 1000 or codigo_num > 52999:
        return False, "Código postal fuera del rango válido (01000-52999)"

    primer_digito = cp_limpio[0]
    codigo_provincia = cp_limpio[:2]

    # Diccionario de provincias
    provincias = {
        '01': 'Álava', '02': 'Albacete', '03': 'Alicante', '04': 'Almería', '05': 'Ávila',
        '06': 'Badajoz', '07': 'Baleares', '08': 'Barcelona', '09': 'Burgos', '10': 'Cáceres',
        '11': 'Cádiz', '12': 'Castellón', '13': 'Ciudad Real', '14': 'Córdoba', '15': 'Coruña',
        '16': 'Cuenca', '17': 'Girona', '18': 'Granada', '19': 'Guadalajara', '20': 'Guipúzcoa',
        '21': 'Huelva', '22': 'Huesca', '23': 'Jaén', '24': 'León', '25': 'Lleida',
        '26': 'La Rioja', '27': 'Lugo', '28': 'Madrid', '29': 'Málaga', '30': 'Murcia',
        '31': 'Navarra', '32': 'Ourense', '33': 'Asturias', '34': 'Palencia', '35': 'Las Palmas',
        '36': 'Pontevedra', '37': 'Salamanca', '38': 'Santa Cruz de Tenerife', '39': 'Cantabria',
        '40': 'Segovia', '41': 'Sevilla', '42': 'Soria', '43': 'Tarragona', '44': 'Teruel',
        '45': 'Toledo', '46': 'Valencia', '47': 'Valladolid', '48': 'Vizcaya', '49': 'Zamora',
        '50': 'Zaragoza', '51': 'Ceuta', '52': 'Melilla'
    }

    # Validar provincia
    if codigo_provincia not in provincias:
        # Detectar errores comunes
        error_msg = f"Código postal inválido"

        if codigo_provincia in ['00', '53', '54', '55', '56', '57', '58', '59']:
            error_msg += ". Los códigos empiezan por 01-52"
        elif codigo_provincia[0] in '6789':
            error_msg += ". Los códigos españoles empiezan por 0-5"
        elif codigo_provincia in ['60', '61', '62', '63', '64', '65', '66', '67', '68', '69']:
            error_msg += ". ¿Quizás es un código de otro país?"
        elif codigo_provincia in ['00', '0O', 'O0', 'O1', 'O2', 'O3', 'O4', 'O5']:
            error_msg += ". ¿Ha confundido el número 0 con la letra O?"

        return False, error_msg

    # Validaciones específicas por zona
    provincia = provincias[codigo_provincia]
    primer_digito_int = int(primer_digito)

    # Mapa de consistencia entre primer dígito y provincia
    if primer_digito_int == 0:
        if not ('01' <= codigo_provincia <= '09'):
            return False, f"Inconsistencia. Códigos que empiezan por 0 son para provincias 01-09, no {codigo_provincia}"

    elif primer_digito_int == 1:
        provincias_1 = ['10', '11', '12', '13', '14', '15', '16', '17', '28', '29']
        if codigo_provincia not in provincias_1:
            return False, f"Inconsistencia. Para {provincia} el código debería empezar por otro dígito"

    elif primer_digito_int == 2:
        provincias_2 = ['20', '21', '22', '23', '24', '25', '26', '27', '28', '29']
        if codigo_provincia not in provincias_2:
            return False, f"Inconsistencia. Para {provincia} el código debería empezar por otro dígito"

    elif primer_digito_int == 3:
        provincias_3 = ['30', '31', '32', '33', '34', '35', '36', '37', '38', '39']
        if codigo_provincia not in provincias_3:
            return False, f"Inconsistencia. Para {provincia} el código debería empezar por otro dígito"

    elif primer_digito_int == 4:
        provincias_4 = ['40', '41', '42', '43', '44', '45', '46', '47', '48', '49']
        if codigo_provincia not in provincias_4:
            return False, f"Inconsistencia. Para {provincia} el código debería empezar por otro dígito"

    elif primer_digito_int == 5:
        provincias_5 = ['50', '51', '52']
        if codigo_provincia not in provincias_5:
            return False, f"Inconsistencia. Para {provincia} el código debería empezar por otro dígito"

    # Validaciones adicionales útiles
    if codigo_provincia == '28':  # Madrid
        if not (28001 <= codigo_num <= 28999):
            return False, "Código postal de Madrid fuera de rango (28001-28999)"

    elif codigo_provincia == '08':  # Barcelona
        if not (8001 <= int(cp_limpio[2:]) <= 8999):
            return False, "Código postal de Barcelona fuera de rango (08001-08999)"

    elif codigo_provincia in ['35', '38']:  # Canarias
        if codigo_provincia == '35' and not (35000 <= codigo_num <= 35999):
            return False, "Las Palmas: rango 35000-35999"
        elif codigo_provincia == '38' and not (38000 <= codigo_num <= 38999):
            return False, "Santa Cruz de Tenerife: rango 38000-38999"

    elif codigo_provincia in ['51', '52']:  # Ceuta y Melilla
        if codigo_provincia == '51' and not (51000 <= codigo_num <= 51099):
            return False, "Ceuta: rango 51000-51099"
        elif codigo_provincia == '52' and not (52000 <= codigo_num <= 52099):
            return False, "Melilla: rango 52000-52099"

    # Éxito - devolver mensaje informativo
    cp_formateado = f"{cp_limpio[:2]} {cp_limpio[2:]}"
    return True, f"Válido ({cp_formateado}) - {provincia}"


# Función opcional para validación más estricta (mantener retrocompatibilidad)
def validar_cp_con_provincia(cp, provincia_usuario=None):
    """
    Validar CP y además verificar coincidencia con provincia proporcionada
    """
    es_valido, mensaje = validar_codigo_postal(cp)

    if es_valido and provincia_usuario:
        # Extraer provincia del mensaje de éxito
        if " - " in mensaje:
            provincia_cp = mensaje.split(" - ")[-1]

            # Normalizar nombres para comparación
            def normalizar(texto):
                return texto.lower().replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace(
                    'ú', 'u')

            if normalizar(provincia_cp) != normalizar(provincia_usuario):
                return False, f"El CP no corresponde a {provincia_usuario}. Es de {provincia_cp}"

    return es_valido, mensaje


def validar_iban(iban):
    """Validar IBAN español incluyendo verificación de dígitos de control"""
    if not iban:
        return False, "El IBAN no puede estar vacío"

    iban_limpio = iban.upper().replace(' ', '').replace('-', '')

    # Formato IBAN español: ES + 22 dígitos
    if not re.match(r'^ES[0-9]{22}$', iban_limpio):
        return False, "Formato IBAN inválido. Use: ES + 22 dígitos (Ej: ES9121000418450200051332)"

    # Validar dígitos de control usando el algoritmo oficial
    # Reordenar: mover los 4 primeros caracteres al final
    iban_reordenado = iban_limpio[4:] + iban_limpio[:4]

    # Convertir letras a números (A=10, B=11, ..., Z=35)
    iban_numerico = ''
    for caracter in iban_reordenado:
        if caracter.isdigit():
            iban_numerico += caracter
        else:
            iban_numerico += str(10 + ord(caracter) - ord('A'))

    # Calcular módulo 97 (debe ser 1 para ser válido)
    # Usamos cálculo modular para números grandes
    resto = 0
    for i in range(0, len(iban_numerico), 7):
        segmento = str(resto) + iban_numerico[i:i + 7]
        resto = int(segmento) % 97

    if resto != 1:
        return False, "IBAN inválido: dígitos de control incorrectos"

    return True, "IBAN válido"


def validar_telefono(telefono):
    """Validar número de teléfono español"""
    if not telefono:
        return False, "El teléfono no puede estar vacío"

    telefono_limpio = telefono.replace(' ', '').replace('-', '')

    # Teléfono español: 9 dígitos, puede empezar con 6,7,8,9
    if not re.match(r'^[6789][0-9]{8}$', telefono_limpio):
        return False, "Teléfono inválido. Debe tener 9 dígitos y empezar por 6,7,8,9"

    return True, "Teléfono válido"


# -----------------------------------#

# -------------------- FUNCIONES DE FIRMA --------------------
def procesar_firma(canvas_result):
    """Convierte el canvas de firma a base64 para almacenamiento"""
    if canvas_result is not None and canvas_result.image_data is not None:
        try:
            # Convertir a imagen PIL
            img_array = np.array(canvas_result.image_data)

            # Verificar si hay algún trazo no transparente
            if np.any(img_array[:, :, 3] > 0):  # Verificar canal alpha
                img_pil = PILImage.fromarray(img_array.astype('uint8'), 'RGBA')

                # Crear fondo blanco para la firma
                background = PILImage.new('RGBA', img_pil.size, (255, 255, 255, 255))
                # Combinar la firma con fondo blanco
                firma_con_fondo = PILImage.alpha_composite(background, img_pil)

                # Convertir a RGB para mejor compatibilidad
                firma_rgb = firma_con_fondo.convert('RGB')

                # Convertir a base64
                buffered = BytesIO()
                firma_rgb.save(buffered, format="PNG", optimize=True)
                img_str = base64.b64encode(buffered.getvalue()).decode()
                return img_str
        except Exception as e:
            st.error(f"Error procesando firma: {e}")
            return None
    return None


def firma_para_pdf(firma_base64):
    """Convierte la firma base64 a imagen para PDF"""
    if not firma_base64:
        return None

    try:
        firma_data = base64.b64decode(firma_base64)
        return BytesIO(firma_data)
    except Exception as e:
        st.error(f"Error procesando firma para PDF: {e}")
        return None


# -------------------- GENERAR PDF --------------------
def generar_pdf(precontrato_datos, lineas=[]):
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter,
                            rightMargin=50, leftMargin=50,
                            topMargin=50, bottomMargin=50)
    elements = []
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='CustomTitle', fontSize=18, leading=22, alignment=1,
                              textColor=colors.darkgreen, spaceAfter=20))
    styles.add(ParagraphStyle(name='CustomHeading', fontSize=14, leading=18,
                              textColor=colors.darkblue, spaceAfter=10))
    styles.add(ParagraphStyle(name='NormalBold', parent=styles['Normal'], fontName='Helvetica-Bold'))

    elements.append(Paragraph(f"Precontrato {precontrato_datos['precontrato_id']}", styles['CustomTitle']))
    elements.append(Spacer(1, 12))

    def tabla_seccion(titulo, datos):
        elements.append(Paragraph(titulo, styles['CustomHeading']))
        data = [[Paragraph(f"<b>{k}</b>", styles['Normal']), Paragraph(str(v or ''), styles['Normal'])] for k, v in
                datos.items()]
        table = Table(data, colWidths=[150, 350])
        table.setStyle(TableStyle([
            ('BOX', (0, 0), (-1, -1), 0.5, colors.grey),
            ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.grey),
            ('BACKGROUND', (0, 0), (-1, 0), colors.whitesmoke),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT')
        ]))
        elements.append(table)
        elements.append(Spacer(1, 12))

    # Datos del Precontrato
    tabla_seccion("Datos del Precontrato", {
        "Apartment ID": precontrato_datos["apartment_id"],
        "Tarifa": precontrato_datos["tarifa"],
        "Comercial": precontrato_datos["comercial"],
        "Observaciones": precontrato_datos["observaciones"],
        "Precio (€ I.V.A Incluido)": precontrato_datos["precio"],
        "Fecha": precontrato_datos["fecha"],
        "Permanencia": precontrato_datos["permanencia"],
        "Servicio Adicional": precontrato_datos["servicio_adicional"]
    })

    # Datos del Cliente
    datos_cliente = {
        "Nombre": precontrato_datos["nombre"],
        "Nombre legal / Razón social": precontrato_datos["nombre_legal"],
        "NIF": precontrato_datos["nif"],
        "CIF": precontrato_datos["cif"],
        "Teléfono 1": precontrato_datos["telefono1"],
        "Teléfono 2": precontrato_datos["telefono2"],
        "Email": precontrato_datos["mail"],
        "Dirección": precontrato_datos["direccion"],
        "CP": precontrato_datos["cp"],
        "Población": precontrato_datos["poblacion"],
        "Provincia": precontrato_datos["provincia"],
        "IBAN": precontrato_datos["iban"],
        "BIC": precontrato_datos["bic"]
    }

    # Añadir sección de firma si existe
    if precontrato_datos.get("firma_base64"):
        datos_cliente["Firma"] = "✓ FIRMA ADJUNTADA (Ver imagen abajo)"

    tabla_seccion("Datos del Cliente", datos_cliente)

    # ===========================================================================
    # AQUÍ VA LA NUEVA SECCIÓN DE GEOLOCALIZACIÓN - DESPUÉS DE DATOS CLIENTE
    # ===========================================================================
    # Añadir coordenadas al PDF si existen
    if precontrato_datos.get("coordenadas"):
        try:
            coords = json.loads(precontrato_datos["coordenadas"])
            elements.append(Paragraph("📍 Geolocalización", styles['CustomHeading']))

            # Determinar icono y texto según precisión
            precision = coords.get('precision', 'desconocida')
            if precision == "exacta":
                icono_texto = "✅ UBICACIÓN EXACTA"
                color = colors.darkgreen
            elif precision == "aproximada":
                icono_texto = "⚠️ UBICACIÓN APROXIMADA"
                color = colors.orange
            else:
                icono_texto = "📍 UBICACIÓN DE REFERENCIA"
                color = colors.blue

            elements.append(Paragraph(f"<b>{icono_texto}</b>",
                                      ParagraphStyle(name='PrecisionStyle',
                                                     textColor=color,
                                                     fontSize=10,
                                                     spaceAfter=6)))

            coord_data = [
                ["Latitud", f"{coords.get('lat', 'N/A'):.6f}"],
                ["Longitud", f"{coords.get('lon', 'N/A'):.6f}"],
                ["Portal buscado", str(coords.get('portal_original', 'N/A'))],
                ["Portal encontrado", str(coords.get('portal_encontrado', 'N/A'))],
                ["Precisión", precision.upper()],
                ["Notas", coords.get('notas', '')],
                ["Google Maps", f"https://maps.google.com/?q={coords.get('lat')},{coords.get('lon')}"]
            ]

            coord_table = Table(coord_data, colWidths=[150, 350])
            coord_table.setStyle(TableStyle([
                ('BOX', (0, 0), (-1, -1), 0.5, colors.lightgrey),
                ('BACKGROUND', (0, 0), (-1, 0), colors.whitesmoke),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ]))
            elements.append(coord_table)
            elements.append(Spacer(1, 12))
        except Exception as e:
            print(f"Error al procesar coordenadas en PDF: {e}")
            pass  # Si hay error, simplemente no mostrar
    # ===========================================================================

    # Añadir imagen de la firma si existe
    if precontrato_datos.get("firma_base64"):
        elements.append(Spacer(1, 12))
        elements.append(Paragraph("Firma del Cliente", styles['CustomHeading']))
        try:
            firma_buffer = firma_para_pdf(precontrato_datos["firma_base64"])
            if firma_buffer:
                firma_img = Image(firma_buffer, width=200, height=80)
                elements.append(firma_img)
                elements.append(Spacer(1, 12))
        except Exception as e:
            elements.append(Paragraph(f"Error cargando firma: {e}", styles['Normal']))

    # Líneas adicionales
    if lineas:
        for i, l in enumerate(lineas, start=1):
            tabla_seccion(f"Línea adicional {i}", {
                "Tipo": l.get('tipo', ''),
                "Número nuevo": l.get('numero_nuevo_portabilidad', ''),
                "Número a portar": l.get('numero_a_portar', ''),
                "Titular": l.get('titular', ''),
                "DNI": l.get('dni', ''),
                "Operador donante": l.get('operador_donante', ''),
            })

    elements.append(Spacer(1, 24))
    elements.append(Paragraph("Verdetuoperador.com · atencioncliente@verdetuoperador.com", styles['Normal']))

    doc.build(elements)
    buffer.seek(0)
    return buffer


# -------------------- ENVIAR CORREO --------------------
def enviar_correo_pdf(precontrato_datos, archivos=[], lineas=[]):
    pdf_buffer = generar_pdf(precontrato_datos, lineas=lineas)
    asunto = f"Precontrato completado: {precontrato_datos['precontrato_id']}"
    contenido = {"mensaje": "El cliente ha completado el formulario de precontrato.",
                 "Precontrato": precontrato_datos,
                 "Líneas adicionales": lineas}
    html_body = generar_html(asunto, contenido)

    msg = EmailMessage()
    msg['Subject'] = asunto
    msg['From'] = "noreply.verdetuoperador@gmail.com"
    destinatarios = ["patricia@verdetuoperador.com", "bo@verdetuoperador.com", "jpterrel@verdetuoperador.com"]
    msg['To'] = ", ".join(destinatarios)
    msg.set_content("Tu cliente ha completado el formulario. Ver versión HTML para más detalles.")
    msg.add_alternative(html_body, subtype='html')

    msg.add_attachment(pdf_buffer.read(), maintype='application', subtype='pdf',
                       filename=f"Precontrato_{precontrato_datos['precontrato_id']}.pdf")

    for archivo in archivos:
        nombre = archivo.name
        tipo = archivo.type.split('/')
        maintype = tipo[0]
        subtype = tipo[1] if len(tipo) > 1 else 'octet-stream'
        msg.add_attachment(archivo.getvalue(), maintype=maintype, subtype=subtype, filename=nombre)

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login("noreply.verdetuoperador@gmail.com", "mwht uuwd slzc renq")
            smtp.send_message(msg)
        return True, "✅ Correo enviado correctamente"
    except Exception as e:
        return False, f"❌ Error al enviar correo: {e}"


# -------------------- INTERFAZ STREAMLIT --------------------
def formulario_cliente(precontrato_id=None, token=None):
    st.title("Formulario de Cliente - Precontrato")

    # Verificar disponibilidad del canvas
    if not CANVAS_AVAILABLE:
        st.error("""
        ❌ El componente de firma no está disponible. 
        Por favor, instala el paquete requerido:
        ```bash
        pip install streamlit-drawable-canvas
        ```
        """)
        return

    # Inicializar estado de sesión
    if 'validado' not in st.session_state:
        st.session_state.validado = False
    if 'precontrato_id' not in st.session_state:
        st.session_state.precontrato_id = ""
    if 'token' not in st.session_state:
        st.session_state.token = ""
    if 'precontrato_data' not in st.session_state:
        st.session_state.precontrato_data = None
    if 'firma_base64' not in st.session_state:
        st.session_state.firma_base64 = None
    if 'formulario_completado' not in st.session_state:
        st.session_state.formulario_completado = False

    # Si se pasan parámetros desde app.py, usarlos para validación automática
    if precontrato_id and token and not st.session_state.validado:
        valido, mensaje = validar_token(precontrato_id, token)
        if not valido:
            st.error(mensaje)
            # Mostrar opción para validación manual
            st.info("💡 Si crees que esto es un error, intenta validar manualmente:")
            precontrato_id_manual = st.text_input("ID del precontrato (manual)", key="manual_precontrato_id")
            token_manual = st.text_input("Token de acceso (manual)", key="manual_token")
            if st.button("Validar manualmente"):
                if precontrato_id_manual and token_manual:
                    valido_manual, mensaje_manual = validar_token(precontrato_id_manual, token_manual)
                    if valido_manual:
                        st.session_state.validado = True
                        st.session_state.precontrato_id = precontrato_id_manual
                        st.session_state.token = token_manual
                        # Cargar datos del precontrato
                        conn = get_db_connection()
                        if conn:
                            cursor = conn.cursor()
                            cursor.execute("SELECT * FROM precontratos WHERE id = %s", (int(precontrato_id_manual),))
                            st.session_state.precontrato_data = cursor.fetchone()
                            conn.close()
                            st.rerun()
                    else:
                        st.error(mensaje_manual)
            return

        # Si la validación fue exitosa, cargar datos del precontrato
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM precontratos WHERE id = %s", (int(precontrato_id),))
            precontrato = cursor.fetchone()
            conn.close()

            if not precontrato:
                st.error("❌ No se encontró el precontrato asociado a este enlace.")
                return

            # Guardar en estado de sesión
            st.session_state.validado = True
            st.session_state.precontrato_id = precontrato_id
            st.session_state.token = token
            st.session_state.precontrato_data = precontrato
            st.rerun()

    # Si no está validado, mostrar formulario de validación
    if not st.session_state.validado:
        st.info("🔐 Introduce tus credenciales de acceso")
        precontrato_id_input = st.text_input("ID del precontrato", key="input_precontrato_id")
        token_input = st.text_input("Token de acceso", key="input_token")

        if st.button("Validar enlace"):
            if not precontrato_id_input or not token_input:
                st.error("❌ Faltan parámetros.")
                return

            valido, mensaje = validar_token(precontrato_id_input, token_input)
            if not valido:
                st.error(mensaje)
                return

            # Cargar datos de precontrato
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM precontratos WHERE id = %s", (int(precontrato_id_input),))
                precontrato = cursor.fetchone()
                conn.close()

                if not precontrato:
                    st.toast("❌ No se encontró el precontrato asociado a este enlace.")
                    return

                # Guardar en estado de sesión
                st.session_state.validado = True
                st.session_state.precontrato_id = precontrato_id_input
                st.session_state.token = token_input
                st.session_state.precontrato_data = precontrato
                st.rerun()

    else:
        # Mostrar formulario principal
        precontrato = st.session_state.precontrato_data
        st.toast("✅ Enlace válido. Completa el formulario correctamente.")

        # Crear una tabla visual con los datos
        with st.container():
            st.markdown("### Condiciones del Servicio")

            # Usar columnas para mejor organización
            col1, col2 = st.columns(2)

            with col1:
                st.info(f"**🗳️ Tarifa:** {precontrato[2] or 'No especificada'}")
                st.info(f"**💰 Precio:** {precontrato[4] or 'No especificado'}")

            with col2:
                st.info(f"**⏱️ Permanencia:** {precontrato[21] or 'No especificada'}")
                st.info(f"**🛠️ Servicios Adicionales:** {precontrato[22] or 'Ninguno'}")

            if precontrato[3]:  # Observaciones
                st.warning(f"**📝 Observaciones:** {precontrato[3]}")

        # FORMULARIO PRINCIPAL
        with st.form(key="formulario_cliente"):
            st.subheader("👤 Datos Personales")

            # Mostrar información del precontrato para referencia
            st.info(f"**Precontrato ID:** {st.session_state.precontrato_id}")

            # PRIMERA FILA: Datos básicos
            col1, col2 = st.columns(2)

            with col1:
                nombre = st.text_input("Nombre completo*", precontrato[6] or "")
                nif = st.text_input("NIF / DNI*", precontrato[9] or "")
                telefono1 = st.text_input("Teléfono principal*", precontrato[10] or "")
                telefono2 = st.text_input("Teléfono alternativo", precontrato[11] or "")
                mail = st.text_input("Email*", precontrato[12] or "")

            with col2:
                nombre_legal = st.text_input("Nombre legal / Razón social", precontrato[7] or "")
                cif = st.text_input("CIF", precontrato[8] or "")
                iban = st.text_input("IBAN*", precontrato[17] or "")
                bic = st.text_input("BIC", precontrato[18] or "")

            # SEGUNDA FILA: Dirección
            col3, col4, col5 = st.columns([3, 1, 2])

            with col3:
                direccion = st.text_input("Dirección*", precontrato[13] or "")
            with col4:
                cp = st.text_input("CP*", precontrato[14] or "")
            with col5:
                poblacion = st.text_input("Población*", precontrato[15] or "")

            # TERCERA FILA: Provincia y documentos
            col6, col7 = st.columns([2, 3])

            with col6:
                provincia = st.text_input("Provincia*", precontrato[16] or "")

            with col7:
                archivos = st.file_uploader("📎 Adjuntar documentos* (OBLIGATORIO)",
                                            accept_multiple_files=True,
                                            type=['pdf', 'png', 'jpg', 'jpeg'],
                                            help="Debe adjuntar al menos un documento para continuar")

            # Mostrar ejemplos de formato válido
            with st.expander("ℹ️ Formatos válidos esperados"):
                st.write("""
                        - **DNI/NIF**: 12345678A o X1234567A
                        - **Email**: ejemplo@dominio.com
                        - **Código Postal**: 5 dígitos (28001)
                        - **IBAN**: ES + 22 dígitos (ES9121000418450200051332)
                        - **Teléfono**: 9 dígitos empezando por 6,7,8,9 (612345678)
                        - **Documentos**: Formatos aceptados: PDF, PNG, JPG, JPEG. Mínimo 1 archivo obligatorio.
                        """)

            # LÍNEAS DE SERVICIO
            st.subheader("📞 Líneas de Servicio")

            # Línea Móvil y Fija en columnas
            col_movil, col_fija = st.columns(2)

            with col_movil:
                st.markdown("**📱 Línea Móvil principal**")
                movil_numero_nuevo_portabilidad = st.text_input("Número nuevo / portabilidad", key="movil_numero")
                movil_numero_a_portar = st.text_input("Número a portar", key="movil_portar")
                movil_titular = st.text_input("Titular", key="movil_titular")
                movil_dni = st.text_input("DNI", key="movil_dni")
                movil_operador_donante = st.text_input("Operador donante", key="movil_operador")

            with col_fija:
                st.markdown("**🏠 Línea Fija principal**")
                fija_numero_nuevo_portabilidad = st.text_input("Número nuevo / portabilidad", key="fija_numero")
                fija_numero_a_portar = st.text_input("Número a portar", key="fija_portar")
                fija_titular = st.text_input("Titular", key="fija_titular")
                fija_dni = st.text_input("DNI", key="fija_dni")
                fija_operador_donante = st.text_input("Operador donante", key="fija_operador")

            # Líneas adicionales
            st.subheader("📲 Líneas adicionales")
            lineas_adicionales = []
            for i in range(1, 6):
                with st.expander(f"Línea adicional {i}", expanded=False):
                    # Organizar cada línea adicional en columnas
                    col_linea1, col_linea2 = st.columns(2)

                    with col_linea1:
                        tipo = st.selectbox("Tipo de línea", ["", "movil_adicional", "fijo_adicional"],
                                            key=f"tipo_{i}")
                        numero_nuevo = st.text_input("Número nuevo / portabilidad", key=f"numero_nuevo_{i}")
                        numero_a_portar = st.text_input("Número a portar", key=f"numero_a_portar_{i}")
                        titular = st.text_input("Titular", key=f"titular_{i}")

                    with col_linea2:
                        dni_l = st.text_input("DNI", key=f"dni_{i}")
                        operador = st.text_input("Operador donante", key=f"operador_donante_{i}")

                    # Solo agregar a la lista si hay algún dato completado
                    if tipo or numero_nuevo or numero_a_portar or titular or dni_l or operador:
                        lineas_adicionales.append({
                            "tipo": tipo,
                            "numero_nuevo_portabilidad": numero_nuevo,
                            "numero_a_portar": numero_a_portar,
                            "titular": titular,
                            "dni": dni_l,
                            "operador_donante": operador,
                        })

            # Botón para completar formulario (sin firma aún)
            col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
            with col_btn2:
                submitted = st.form_submit_button("✅ Completar formulario y proceder a firma", use_container_width=True)

            if submitted:
                # Validaciones básicas de campos obligatorios (sin firma por ahora)
                campos_obligatorios = [
                    (nombre, "Nombre completo"),
                    (nif, "NIF/DNI"),
                    (telefono1, "Teléfono principal"),
                    (mail, "Email"),
                    (direccion, "Dirección"),
                    (cp, "Código Postal"),
                    (poblacion, "Población"),
                    (provincia, "Provincia"),
                    (iban, "IBAN"),
                    (archivos, "Adjuntar documentos")
                ]

                #campos_faltantes = [campo[1] for campo in campos_obligatorios if not campo[0]]
                campos_faltantes = []
                for campo, nombre_campo in campos_obligatorios:
                    if not campo:
                        campos_faltantes.append(nombre_campo)
                    # Validación específica para archivos (debe ser una lista no vacía)
                    elif nombre_campo == "Adjuntar documentos" and (not archivos or len(archivos) == 0):
                        campos_faltantes.append(nombre_campo)

                if campos_faltantes:
                    st.toast(f"❌ **Campos obligatorios faltantes:** {', '.join(campos_faltantes)}")
                    st.stop()

                # Validaciones de formato
                errores_validacion = []

                # Validar DNI/NIF
                dni_valido, mensaje_dni = validar_dni(nif)
                if not dni_valido:
                    errores_validacion.append(f"**DNI/NIF**: {mensaje_dni}")

                # Validar Email
                email_valido, mensaje_email = validar_email(mail)
                if not email_valido:
                    errores_validacion.append(f"**Email**: {mensaje_email}")

                # Validar Código Postal
                cp_valido, mensaje_cp = validar_codigo_postal(cp)
                if not cp_valido:
                    errores_validacion.append(f"**Código Postal**: {mensaje_cp}")

                # Validar IBAN
                iban_valido, mensaje_iban = validar_iban(iban)
                if not iban_valido:
                    errores_validacion.append(f"**IBAN**: {mensaje_iban}")

                # Validar Teléfono principal
                telefono_valido, mensaje_telefono = validar_telefono(telefono1)
                if not telefono_valido:
                    errores_validacion.append(f"**Teléfono principal**: {mensaje_telefono}")

                # Validar Teléfono alternativo (si está completado)
                if telefono2:
                    telefono2_valido, mensaje_telefono2 = validar_telefono(telefono2)
                    if not telefono2_valido:
                        errores_validacion.append(f"**Teléfono alternativo**: {mensaje_telefono2}")

                # Mostrar errores de validación
                if errores_validacion:
                    st.toast("❌ **Errores de validación:**")
                    for error in errores_validacion:
                        st.write(f"- {error}")
                    st.stop()

                # Si llegamos aquí, todas las validaciones pasaron
                st.session_state.formulario_completado = True
                st.success("✅ Formulario completado correctamente. Ahora procede a firmar.")
                st.rerun()

        # SECCIÓN DE FIRMA - SOLO SE MUESTRA SI EL FORMULARIO ESTÁ COMPLETADO
        if st.session_state.formulario_completado:
            st.subheader("✍️ Firma Digital Obligatoria")

            # Mensaje importante
            with st.container():
                st.warning("""
                **⚠️ ATENCIÓN: FIRMA REQUERIDA**

                Para completar el proceso, debe proporcionar su firma digital. 
                Sin la firma, el formulario no podrá ser enviado.
                """)

            # Canvas para la firma
            col_firma1, col_firma2, col_firma3 = st.columns([1, 2, 1])
            with col_firma2:
                canvas_result = st_canvas(
                    fill_color="rgba(255, 255, 255, 0)",
                    stroke_width=3,
                    stroke_color="#000000",
                    background_color="#FFFFFF",
                    background_image=None,
                    update_streamlit=True,
                    height=200,
                    width=500,
                    drawing_mode="freedraw",
                    point_display_radius=0,
                    key="signature_canvas_final",
                )

            # Botones para gestionar la firma - AHORA SOLO 2 BOTONES
            col_btns1, col_btns2 = st.columns([1, 2])

            with col_btns1:
                if st.button("🔄 Limpiar Firma", use_container_width=True, key="limpiar_firma_final"):
                    st.session_state.firma_base64 = None
                    st.rerun()

            with col_btns2:
                # BOTÓN ÚNICO QUE GUARDA LA FIRMA Y ENVÍA EL FORMULARIO
                if st.button("💾 Guardar Firma y Enviar Formulario",
                             use_container_width=True,
                             type="primary",
                             key="guardar_y_enviar"):

                    # Primero: procesar y guardar la firma
                    firma_base64 = procesar_firma(canvas_result)
                    if not firma_base64:
                        st.error("❌ Debe dibujar su firma antes de enviar el formulario.")
                        return

                    st.session_state.firma_base64 = firma_base64
                    st.success("✅ Firma guardada correctamente")

                    # Mostrar vista previa de la firma
                    try:
                        firma_data = base64.b64decode(firma_base64)
                        st.image(firma_data, width=200, caption="Su firma guardada")
                    except Exception as e:
                        st.error(f"Error mostrando firma: {e}")

                    # Segundo: proceder con el envío del formulario
                    with st.spinner("📤 Enviando formulario..."):
                        try:
                            #########################################PRUEBA#################################################
                            # ============================================
                            # VERIFICACIÓN PROACTIVA PARA MÓVILES
                            # ============================================
                            claves_requeridas = ['precontrato_id', 'token', 'precontrato_data']
                            faltan_claves = [k for k in claves_requeridas if not st.session_state.get(k)]

                            if faltan_claves:
                                st.error(f"⚠️ **Error de sesión:** Faltan datos: {', '.join(faltan_claves)}")

                                # Intentar recuperar de la URL
                                params = st.query_params
                                if 'precontrato_id' in params and 'token' in params:
                                    if st.button("🔗 Recuperar sesión desde el enlace", key="recuperar_desde_url"):
                                        st.session_state.precontrato_id = params['precontrato_id']
                                        st.session_state.token = params['token']
                                        st.toast("✅ Sesión recuperada. Continúa con el envío.")
                                        st.rerun()
                                else:
                                    st.error(
                                        "❌ No se puede recuperar la sesión. Vuelve a acceder con el enlace original.")
                                return
                            # ============================================
                            #####################################################PRUEBA###########################################
                            # Preparar datos de líneas
                            movil = {
                                "precontrato_id": int(st.session_state.precontrato_id),
                                "tipo": "movil",
                                "numero_nuevo_portabilidad": movil_numero_nuevo_portabilidad,
                                "numero_a_portar": movil_numero_a_portar,
                                "titular": movil_titular,
                                "dni": movil_dni,
                                "operador_donante": movil_operador_donante,
                            }

                            fija = {
                                "precontrato_id": int(st.session_state.precontrato_id),
                                "tipo": "fija",
                                "numero_nuevo_portabilidad": fija_numero_nuevo_portabilidad,
                                "numero_a_portar": fija_numero_a_portar,
                                "titular": fija_titular,
                                "dni": fija_dni,
                                "operador_donante": fija_operador_donante,
                            }

                            todas_lineas = [movil, fija] + lineas_adicionales

                            conn = get_db_connection()
                            if not conn:
                                st.error("❌ No se pudo conectar a la base de datos")
                                return

                            cursor = conn.cursor()

                            # Actualizar precontrato con firma
                            cursor.execute("""
                                UPDATE precontratos
                                SET nombre=%s, nombre_legal=%s, cif=%s, nif=%s, telefono1=%s, telefono2=%s, mail=%s, direccion=%s,
                                    cp=%s, poblacion=%s, provincia=%s, iban=%s, bic=%s, firma=%s
                                WHERE id=%s
                            """, (nombre, nombre_legal, cif, nif, telefono1, telefono2, mail, direccion,
                                  cp, poblacion, provincia, iban, bic, firma_base64,
                                  int(st.session_state.precontrato_id)))

                            # Marcar link como usado
                            cursor.execute("""
                                UPDATE precontrato_links
                                SET usado = true
                                WHERE precontrato_id = %s AND token = %s
                            """, (int(st.session_state.precontrato_id), st.session_state.token))

                            # Insertar líneas
                            pre_id = int(st.session_state.precontrato_id)
                            for linea in todas_lineas:
                                # Solo insertar si hay datos reales (tipo, numero, titular, etc.)
                                datos_linea = [linea.get("tipo",""), linea.get("numero_nuevo_portabilidad",""),
                                               linea.get("numero_a_portar",""), linea.get("titular",""),
                                               linea.get("dni",""), linea.get("operador_donante","")]
                                if any(datos_linea):
                                    cursor.execute("""
                                        INSERT INTO lineas (precontrato_id, tipo, numero_nuevo_portabilidad, numero_a_portar,
                                                            titular, dni, operador_donante)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    """, (pre_id, linea.get("tipo",""), linea.get("numero_nuevo_portabilidad",""),
                                          linea.get("numero_a_portar",""), linea.get("titular",""),
                                          linea.get("dni",""), linea.get("operador_donante","")))

                            conn.commit()
                            conn.close()

                            # Preparar datos para PDF
                            datos_pdf = {
                                "precontrato_id": precontrato[23],
                                "apartment_id": precontrato[1],
                                "tarifa": precontrato[2],
                                "comercial": precontrato[5],
                                "observaciones": precontrato[3],
                                "precio": precontrato[4],
                                "fecha": precontrato[19],
                                "permanencia": precontrato[21],
                                "servicio_adicional": precontrato[22],
                                "nombre": nombre,
                                "nombre_legal": nombre_legal,
                                "cif": cif,
                                "nif": nif,
                                "telefono1": telefono1,
                                "telefono2": telefono2,
                                "mail": mail,
                                "direccion": direccion,
                                "cp": cp,
                                "poblacion": poblacion,
                                "provincia": provincia,
                                "iban": iban,
                                "bic": bic,
                                "firma_base64": firma_base64
                            }

                            # ============================================
                            # GEOLOCALIZACIÓN AUTOMÁTICA (TRANSPARENTE PARA EL USUARIO)
                            # ============================================
                            with st.spinner("📍 Obteniendo coordenadas de ubicación..."):
                                coordenadas = obtener_coordenadas_cartociudad(direccion, cp, poblacion, provincia)

                                if coordenadas:
                                    # Guardar en la base de datos
                                    guardar_coordenadas_en_db(
                                        int(st.session_state.precontrato_id),
                                        coordenadas
                                    )

                                    # También añadir al PDF
                                    datos_pdf["coordenadas"] = json.dumps(coordenadas)

                                    st.toast("✅ Ubicación geolocalizada automáticamente")
                                else:
                                    st.toast("⚠️ No se pudo obtener ubicación exacta, continuando...")

                            # Enviar correo
                            success, message = enviar_correo_pdf(datos_pdf, archivos=archivos, lineas=todas_lineas)
                            if success:
                                st.balloons()
                                st.success("🎉 ¡Formulario enviado correctamente! Gracias por completar el proceso.")

                                # Limpiar estado
                                st.session_state.validado = False
                                st.session_state.precontrato_id = ""
                                st.session_state.token = ""
                                st.session_state.precontrato_data = None
                                st.session_state.firma_base64 = None
                                st.session_state.formulario_completado = False
                            else:
                                st.error(f"❌ Error al enviar: {message}")

                        except Exception as e:
                            st.error(f"❌ Error al guardar los datos: {str(e)}")
                            #####################################PRUEBA###############################
                            # Después del mensaje de error, añade:
                            if st.button("🔄 Intentar recuperar sesión", key="recuperar_sesion"):
                                # Intentar recuperar datos de la URL
                                params = st.query_params
                                if 'precontrato_id' in params and 'token' in params:
                                    st.session_state.precontrato_id = params['precontrato_id']
                                    st.session_state.token = params['token']
                                    st.toast("✅ Sesión recuperada. Intenta enviar nuevamente.")
                                    st.rerun()
                                else:
                                    st.error("No se pudo recuperar la sesión. Necesitas el enlace original.")
                                    #######################################PRUEBA#########################################