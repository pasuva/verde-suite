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
            SELECT apartment_id, latitud AS lat, longitud AS lon,
                   provincia, municipio, poblacion, vial, numero,
                   parcela_catastral, cto_id, tipo_olt_rental,
                   'datos_uis' AS fuente
            FROM datos_uis
            WHERE latitud BETWEEN %s AND %s
            AND longitud BETWEEN %s AND %s
            AND latitud IS NOT NULL AND longitud IS NOT NULL
            AND latitud != 0 AND longitud != 0
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
                   parcela_catastral
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

        def color_estado(apt_id, serv_uis, com_data):
            inc = clean(com_data.get('incidencia', '')) if com_data else None
            serv_of = clean(com_data.get('serviciable', '')) if com_data else None
            contrato = clean(com_data.get('contrato', '')) if com_data else None
            su = clean(serv_uis)

            if inc and inc.lower() == 'sí':
                return '#8e44ad', 'incidencia'
            elif serv_of and serv_of.lower() == 'no':
                return '#e74c3c', 'no_serviciable'
            elif su and su.lower() == 'sí':
                return '#27ae60', 'serviciable'
            elif contrato and contrato.lower() == 'sí' and (not su or su.lower() != 'sí'):
                return '#f39c12', 'contratado'
            elif contrato and contrato.lower() == 'no interesado' and (not su or su.lower() != 'sí'):
                return '#95a5a6', 'no_interesado'
            else:
                return '#3498db', 'no_visitado'

        points = []
        for r in rows_duis + rows_apt:
            apt_id = r['apartment_id']
            com = comercial.get(apt_id, {})

            # Determinar serviciable de datos_uis (si viene)
            serv_uis = None
            # datos_uis rows don't have 'serviciable' in our SELECT but the field
            # exists in the table — we chose not to load it for performance.
            # We use comercial_rafa's serviciable instead.

            color, estado = color_estado(apt_id, serv_uis, com)

            p = {
                'id': apt_id,
                'lat': float(r['lat']),
                'lon': float(r['lon']),
                'c': color,
                'e': estado,
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
