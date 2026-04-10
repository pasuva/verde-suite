# api_mapa.py — Micro-API para carga de puntos del mapa por viewport
# Corre junto a Streamlit en el mismo contenedor Docker
# El JavaScript del mapa llama directamente a este API

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
import psycopg
import os

app = FastAPI(title="Verde Suite Map API", default_response_class=ORJSONResponse)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Conexión: mismas variables de entorno que db.py
_CONNINFO = "host={h} port={p} dbname={d} user={u} password={pw}".format(
    h=os.getenv("DB_HOST", "srv-captain--pre-verde-suite"),
    p=os.getenv("DB_PORT", "5432"),
    d=os.getenv("DB_NAME", "verde_suite_pre"),
    u=os.getenv("DB_USER", "postgres"),
    pw=os.getenv("DB_PASSWORD", "7ee2db054df467f5"),
)


def _get_conn():
    return psycopg.connect(_CONNINFO)


@app.get("/api/points")
def get_points(
    south: float = Query(...),
    north: float = Query(...),
    west: float = Query(...),
    east: float = Query(...),
    limit: int = Query(5000, le=20000),
):
    """Devuelve puntos dentro del viewport (bounds) desde ambas tablas."""
    conn = _get_conn()
    try:
        cur = conn.cursor()

        # datos_uis tiene prioridad — se cargan primero
        cur.execute("""
            SELECT d.apartment_id, d.latitud AS lat, d.longitud AS lon,
                   d.provincia, d.municipio, d.poblacion, d.vial, d.numero,
                   d.parcela_catastral, d.cto_id, d.tipo_olt_rental,
                   a.apartment_sales_area,
                   'datos_uis' AS fuente
            FROM datos_uis d
            LEFT JOIN apartments a ON a.apartment_id::text = LTRIM(d.apartment_id, 'P0')
            WHERE d.latitud BETWEEN %s AND %s
            AND d.longitud BETWEEN %s AND %s
            AND d.latitud IS NOT NULL AND d.longitud IS NOT NULL
            AND d.latitud != 0 AND d.longitud != 0
        """, (south, north, west, east))

        cols = [desc[0] for desc in cur.description]
        rows_duis = [dict(zip(cols, row)) for row in cur.fetchall()]

        # IDs de datos_uis para excluir duplicados de apartments
        duis_ids = set()
        for r in rows_duis:
            aid = str(r['apartment_id'])
            if aid.startswith('P'):
                try:
                    duis_ids.add(int(aid.lstrip('P0') or '0'))
                except ValueError:
                    pass

        # apartments — solo los que NO están en datos_uis
        cur.execute("""
            SELECT apartment_id AS apt_num, lat, lng AS lon,
                   provincia, municipio, poblacion, vial, numero,
                   parcela_catastral, apartment_sales_area
            FROM apartments
            WHERE lat BETWEEN %s AND %s
            AND lng BETWEEN %s AND %s
            AND lat IS NOT NULL AND lng IS NOT NULL
            AND lat != 0 AND lng != 0
            LIMIT %s
        """, (south, north, west, east, limit))

        cols_apt = [desc[0] for desc in cur.description]
        rows_apt = []
        for row in cur.fetchall():
            r = dict(zip(cols_apt, row))
            # Excluir si ya está en datos_uis
            if r['apt_num'] in duis_ids:
                continue
            # Convertir a formato compatible
            rows_apt.append({
                'apartment_id': 'P' + str(r['apt_num']).zfill(10),
                'lat': r['lat'],
                'lon': r['lon'],
                'provincia': r['provincia'],
                'municipio': r['municipio'],
                'poblacion': r['poblacion'],
                'vial': r['vial'],
                'numero': r['numero'],
                'parcela_catastral': r['parcela_catastral'],
                'apartment_sales_area': r.get('apartment_sales_area'),
                'cto_id': None,
                'tipo_olt_rental': None,
                'fuente': 'apartments',
            })

        # Cargar datos comerciales para los puntos encontrados
        all_apt_ids = [r['apartment_id'] for r in rows_duis] + [r['apartment_id'] for r in rows_apt]

        comercial = {}
        if all_apt_ids:
            # Usar ANY para eficiencia
            cur.execute("""
                SELECT apartment_id, comercial, serviciable, motivo_serviciable,
                       incidencia, contrato, observaciones,
                       latitud AS lat_comercial, longitud AS lon_comercial
                FROM comercial_rafa
                WHERE apartment_id = ANY(%s)
            """, (all_apt_ids,))
            com_cols = [desc[0] for desc in cur.description]
            for row in cur.fetchall():
                r = dict(zip(com_cols, row))
                comercial[r['apartment_id']] = r

        # Construir respuesta final
        def clean(v):
            if v is None:
                return None
            s = str(v).strip()
            return None if s.lower() in ('nan', 'none', '') else s

        # Colores por área
        AREA_COLORS = {
            'AREA A36': '#27ae60', 'AREA A00': '#3498db', 'AREA D45': '#e67e22',
            'AREA A90': '#e74c3c', 'AREA A10': '#9b59b6', 'AREA B00': '#1abc9c',
            'AREA A60': '#f39c12', 'AREA A70': '#2980b9', 'AREA B90': '#d35400',
            'AREA C00': '#c0392b', 'AREA V30': '#16a085', 'AREA A80': '#8e44ad',
            'AREA A01': '#2ecc71', 'AREA A30': '#e91e63', 'AREA A': '#607d8b',
            'AREA D00': '#f1c40f', 'AREA C10': '#1a5276', 'AREA D10': '#cb4335',
            'AREA B10': '#117a65', 'AREA NOT ASSIGNED': '#bdc3c7',
            'AREA X33': '#a04000', 'AREA Y14': '#7d3c98', 'AREA C30': '#2e86c1',
            'AREA B30': '#d4ac0d', 'AREA Y13': '#a93226', 'AREA C01': '#148f77',
            'AREA B11': '#5b2c6f', 'AREA B': '#717d7e',
        }
        NO_AREA_COLOR = '#95a5a6'

        points = []
        for r in rows_duis + rows_apt:
            apt_id = r['apartment_id']
            com = comercial.get(apt_id, {})

            area = clean(r.get('apartment_sales_area'))
            color = AREA_COLORS.get(area, NO_AREA_COLOR) if area else NO_AREA_COLOR

            p = {
                'id': apt_id,
                'lat': float(r['lat']),
                'lon': float(r['lon']),
                'c': color,
                'area': area or 'Sin área',
                'prov': clean(r.get('provincia')) or '',
                'mun': clean(r.get('municipio')) or '',
                'pob': clean(r.get('poblacion')) or '',
                'via': clean(r.get('vial')) or '',
                'num': clean(r.get('numero')) or '',
                'fuente': r.get('fuente', ''),
            }

            # Datos técnicos
            pc = clean(r.get('parcela_catastral'))
            if pc:
                p['pc'] = pc
            cto = clean(r.get('cto_id'))
            if cto:
                p['cto'] = cto
            tor = clean(r.get('tipo_olt_rental'))
            if tor:
                p['tor'] = tor

            # Datos comerciales
            if com:
                c_com = clean(com.get('comercial'))
                if c_com:
                    p['com'] = c_com
                c_srv = clean(com.get('serviciable'))
                if c_srv:
                    p['srv'] = c_srv
                c_msrv = clean(com.get('motivo_serviciable'))
                if c_msrv:
                    p['msrv'] = c_msrv
                c_ctr = clean(com.get('contrato'))
                if c_ctr:
                    p['ctr'] = c_ctr
                c_obs = clean(com.get('observaciones'))
                if c_obs:
                    if len(c_obs) > 120:
                        c_obs = c_obs[:120] + '...'
                    p['obs'] = c_obs
                c_latc = clean(com.get('lat_comercial'))
                if c_latc:
                    p['latc'] = c_latc
                c_lonc = clean(com.get('lon_comercial'))
                if c_lonc:
                    p['lonc'] = c_lonc

            points.append(p)

        return {"points": points, "count": len(points)}

    finally:
        conn.close()


@app.get("/api/health")
def health():
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM datos_uis")
        duis = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM apartments")
        apts = cur.fetchone()[0]
        conn.close()
        return {"status": "ok", "datos_uis": duis, "apartments": apts}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
