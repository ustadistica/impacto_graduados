"""
dashboard_rues.py
=================
Dashboard RUES — Universidad Santo Tomás
Estética: minimalista, fondo beige, tipografía editorial.

Requisitos:
    pip install streamlit duckdb plotly pandas

Uso:
    streamlit run dashboard_rues.py
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, date
import os

st.set_page_config(
    page_title="RUES · Análisis Empresarial",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

C = {
    "bg":      "#0D1B2A",
    "surface": "#112240",
    "surface2":"#1A2F4A",
    "border":  "#1E3A5F",
    "ink":     "#E8F1F8",
    "ink2":    "#8BAFC8",
    "accent":  "#4DA6FF",
    "accent2": "#64B5F6",
    "green":   "#43D9A2",
    "red":     "#FF6B6B",
    "blue":    "#4DA6FF",
    "gold":    "#FFD166",
}

SECTORES_CIIU = {
    "01": "Agricultura y ganadería",
    "02": "Silvicultura y extracción de madera",
    "05": "Pesca",
    "10": "Explotación de minas de carbón",
    "11": "Extracción de petróleo y gas natural",
    "13": "Extracción de minerales metálicos",
    "14": "Explotación de otras minas y canteras",
    "15": "Elaboración de productos alimenticios y bebidas",
    "16": "Elaboración de productos de tabaco",
    "17": "Fabricación de productos textiles",
    "18": "Fabricación de prendas de vestir",
    "19": "Curtido y preparado de cueros y calzado",
    "20": "Producción de madera y productos de madera",
    "21": "Fabricación de papel y cartón",
    "22": "Actividades de edición e impresión",
    "23": "Fabricación de coque y derivados del petróleo",
    "24": "Fabricación de sustancias y productos químicos",
    "25": "Fabricación de productos de caucho y plástico",
    "26": "Fabricación de productos minerales no metálicos",
    "27": "Fabricación de metales comunes",
    "28": "Fabricación de productos elaborados de metal",
    "29": "Fabricación de maquinaria y equipo",
    "30": "Fabricación de maquinaria de oficina e informática",
    "31": "Fabricación de maquinaria y aparatos eléctricos",
    "32": "Fabricación de equipos de radio, TV y comunicaciones",
    "33": "Fabricación de instrumentos médicos y de precisión",
    "34": "Fabricación de vehículos automotores",
    "35": "Fabricación de otros equipos de transporte",
    "36": "Fabricación de muebles y colchones",
    "37": "Reciclaje",
    "40": "Suministro de electricidad, gas y vapor",
    "41": "Captación y distribución de agua",
    "45": "Construcción",
    "50": "Comercio de vehículos, accesorios y combustibles",
    "51": "Comercio al por mayor",
    "52": "Comercio al por menor",
    "55": "Hoteles, restaurantes y bares",
    "60": "Transporte terrestre",
    "61": "Transporte acuático",
    "62": "Transporte aéreo",
    "63": "Actividades complementarias al transporte y agencias de viaje",
    "64": "Correos y telecomunicaciones",
    "65": "Intermediación financiera y bancaria",
    "66": "Seguros y fondos de pensiones",
    "67": "Actividades auxiliares de la intermediación financiera",
    "70": "Actividades inmobiliarias",
    "71": "Alquiler de maquinaria, equipo y efectos personales",
    "72": "Informática y actividades conexas",
    "73": "Investigación y desarrollo",
    "74": "Otras actividades empresariales y de consultoría",
    "75": "Administración pública y defensa",
    "80": "Educación",
    "85": "Servicios sociales y de salud",
    "90": "Eliminación de desperdicios y saneamiento",
    "91": "Actividades de asociaciones y organizaciones",
    "92": "Actividades de entretenimiento, cultura y deporte",
    "93": "Otras actividades de servicios personales",
    "95": "Hogares privados con servicio doméstico",
    "99": "Organizaciones y organismos extraterritoriales",
    "9999": "Actividad no especificada",
}

def get_sector(ciiu):
    if not ciiu or str(ciiu) in ("nan", "None", "—", ""):
        return "No especificado"
    ciiu_str = str(ciiu).strip()
    ciiu_digits = ''.join(filter(str.isdigit, ciiu_str))
    if not ciiu_digits:
        return f"Sector {ciiu_str}"
    if ciiu_digits in SECTORES_CIIU:
        return SECTORES_CIIU[ciiu_digits]
    if ciiu_digits[:2] in SECTORES_CIIU:
        return SECTORES_CIIU[ciiu_digits[:2]]
    if ciiu_digits[:1] in SECTORES_CIIU:
        return SECTORES_CIIU[ciiu_digits[:1]]
    return f"Sector CIIU {ciiu_str}"

CIUDADES_COL = {
    "bogota": (4.7110, -74.0721), "bogotá": (4.7110, -74.0721),
    "medellin": (6.2442, -75.5812), "medellín": (6.2442, -75.5812),
    "cali": (3.4516, -76.5320), "barranquilla": (10.9685, -74.7813),
    "cartagena": (10.3910, -75.4794), "cucuta": (7.8939, -72.5078),
    "cúcuta": (7.8939, -72.5078), "bucaramanga": (7.1193, -73.1227),
    "pereira": (4.8133, -75.6961), "manizales": (5.0703, -75.5138),
    "armenia": (4.5339, -75.6811), "ibague": (4.4389, -75.2322),
    "ibagué": (4.4389, -75.2322), "villavicencio": (4.1420, -73.6266),
    "neiva": (2.9273, -75.2819), "santa marta": (11.2408, -74.1990),
    "pasto": (1.2136, -77.2811), "monteria": (8.7575, -75.8875),
    "montería": (8.7575, -75.8875), "sincelejo": (9.3047, -75.3978),
    "valledupar": (10.4631, -73.2532), "riohacha": (11.5444, -72.9072),
    "quibdo": (5.6919, -76.6583), "quibdó": (5.6919, -76.6583),
    "popayan": (2.4419, -76.6071), "popayán": (2.4419, -76.6071),
    "florencia": (1.6144, -75.6062), "mocoa": (1.1523, -76.6483),
    "yopal": (5.3378, -72.3959), "arauca": (7.0875, -70.7592),
    "tunja": (5.5353, -73.3678), "buenaventura": (3.8831, -77.0311),
    "palmira": (3.5394, -76.3036), "bello": (6.3372, -75.5578),
    "soledad": (10.9175, -74.7667), "soacha": (4.5797, -74.2170),
    "itagui": (6.1847, -75.5990), "dosquebradas": (4.8398, -75.6611),
    "floridablanca": (7.0649, -73.0876), "giron": (7.0731, -73.1697),
    "girón": (7.0731, -73.1697), "envigado": (6.1753, -75.5920),
    "leticia": (-4.2153, -69.9406), "inirida": (3.8653, -67.9239),
    "camara de comercio de bogota": (4.7110, -74.0721),
    "camara de comercio de medellin": (6.2442, -75.5812),
    "camara de comercio de cali": (3.4516, -76.5320),
    "camara de comercio de armenia": (4.5339, -75.6811),
    "camara de comercio del quindio": (4.5339, -75.6811),
    "camara de comercio de pereira": (4.8133, -75.6961),
    "camara de comercio de manizales": (5.0703, -75.5138),
    "camara de comercio de barranquilla": (10.9685, -74.7813),
    "camara de comercio de cartagena": (10.3910, -75.4794),
    "camara de comercio de bucaramanga": (7.1193, -73.1227),
    "camara de comercio de cucuta": (7.8939, -72.5078),
    "camara de comercio de ibague": (4.4389, -75.2322),
    "camara de comercio de neiva": (2.9273, -75.2819),
    "camara de comercio de pasto": (1.2136, -77.2811),
    "camara de comercio de santa marta": (11.2408, -74.1990),
    "camara de comercio de monteria": (8.7575, -75.8875),
    "camara de comercio de sincelejo": (9.3047, -75.3978),
    "camara de comercio de villavicencio": (4.1420, -73.6266),
    "camara de comercio de valledupar": (10.4631, -73.2532),
    "camara de comercio de popayan": (2.4419, -76.6071),
    "camara de comercio de tunja": (5.5353, -73.3678),
}

def geocode_camara(camara: str):
    if not camara:
        return None
    key = camara.lower().strip()
    if key in CIUDADES_COL:
        return CIUDADES_COL[key]
    for k, v in CIUDADES_COL.items():
        if k in key or key in k:
            return v
    return None

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html, body, [class*="css"] {{ font-family: 'Inter', -apple-system, sans-serif; background-color: {C["bg"]}; color: {C["ink"]}; }}
.stApp {{ background: {C["bg"]}; }}
.rues-header {{ padding: 2rem 0 1.2rem 0; border-bottom: 1px solid {C["border"]}; margin-bottom: 2rem; }}
.rues-header .title {{ font-family: 'Inter', sans-serif; font-size: 1.5rem; font-weight: 700; color: {C["ink"]}; letter-spacing: 0.06em; text-transform: uppercase; margin: 0; }}
.rues-header .subtitle {{ font-size: 0.72rem; color: {C["ink2"]}; letter-spacing: 0.14em; text-transform: uppercase; margin-top: 0.3rem; }}
.section-title {{ font-family: 'Inter', sans-serif; font-size: 0.72rem; font-weight: 600; color: {C["accent"]}; border-bottom: 1px solid {C["border"]}; padding-bottom: 0.5rem; margin: 2rem 0 0.6rem 0; letter-spacing: 0.12em; text-transform: uppercase; }}
.metric-card {{ background: {C["surface"]}; border: 1px solid {C["border"]}; border-radius: 8px; border-top: 3px solid {C["accent"]}; padding: 1.1rem 1.3rem; transition: border-color .2s, transform .15s; }}
.metric-card:hover {{ border-color: {C["accent"]}; transform: translateY(-2px); }}
.metric-card .val {{ font-family: 'Inter', sans-serif; font-size: 2rem; font-weight: 700; color: {C["accent"]}; line-height: 1; }}
.metric-card .lbl {{ font-size: 0.62rem; text-transform: uppercase; letter-spacing: 0.12em; color: {C["ink2"]}; margin-top: 0.4rem; }}
.metric-card .sub {{ font-size: 0.72rem; color: {C["ink2"]}; margin-top: 0.2rem; }}
.emp-card {{ background: {C["surface"]}; border: 1px solid {C["border"]}; border-radius: 8px; border-left: 3px solid {C["accent"]}; padding: 1.3rem 1.5rem; margin-bottom: 1rem; }}
.emp-card.activa {{ border-left-color: {C["green"]}; }}
.emp-card.cancelada {{ border-left-color: {C["red"]}; }}
.emp-nombre {{ font-family: 'Inter', sans-serif; font-size: 1rem; font-weight: 600; color: {C["ink"]}; }}
.badge {{ display: inline-block; padding: .15rem .6rem; font-size: .62rem; font-weight: 600; letter-spacing: .08em; text-transform: uppercase; border: 1px solid currentColor; border-radius: 4px; }}
.badge-activa {{ color: {C["green"]}; }}
.badge-cancelada {{ color: {C["red"]}; }}
.badge-otro {{ color: {C["blue"]}; }}
.search-box {{ background: {C["surface"]}; border: 1px solid {C["border"]}; border-radius: 10px; padding: 1.5rem 1.8rem; margin-bottom: 2rem; }}
.search-box h4 {{ font-family: 'Inter', sans-serif; font-size: 0.95rem; font-weight: 600; color: {C["ink"]}; margin: 0 0 .25rem 0; letter-spacing: 0.02em; }}
.search-box p {{ font-size: .76rem; color: {C["ink2"]}; margin: 0 0 1rem 0; }}
div[data-testid="stTextInput"] input {{ background: {C["bg"]} !important; border: 1.5px solid {C["border"]} !important; border-radius: 8px !important; color: {C["ink"]} !important; font-family: 'Inter', sans-serif !important; font-size: .9rem !important; box-shadow: none !important; }}
div[data-testid="stTextInput"] input:focus {{ border-color: {C["accent"]} !important; box-shadow: 0 0 0 3px {C["accent"]}22 !important; }}
div[data-testid="stTextInput"] input::placeholder {{ color: {C["ink2"]} !important; }}
.stButton > button {{ background: {C["accent"]} !important; color: {C["bg"]} !important; border: none !important; border-radius: 8px !important; font-family: 'Inter', sans-serif !important; font-size: .82rem !important; font-weight: 600 !important; letter-spacing: .05em !important; text-transform: uppercase !important; padding: .55rem 1.8rem !important; transition: all .2s !important; }}
.stButton > button:hover {{ background: {C["accent2"]} !important; transform: translateY(-1px) !important; box-shadow: 0 4px 12px {C["accent"]}44 !important; }}
div[data-baseweb="select"] > div {{ background: {C["surface"]} !important; border-color: {C["border"]} !important; border-radius: 8px !important; color: {C["ink"]} !important; }}
.stDataFrame {{ border: 1px solid {C["border"]}; border-radius: 8px; overflow: hidden; }}
hr {{ border-color: {C["border"]} !important; margin: 1.5rem 0 !important; }}
.stMultiSelect span[data-baseweb="tag"] {{ background: {C["accent"]}33 !important; color: {C["accent"]} !important; border-radius: 4px !important; }}
[data-testid="stMetricValue"] {{ color: {C["accent"]} !important; }}
::-webkit-scrollbar {{ width: 5px; }}
::-webkit-scrollbar-track {{ background: {C["bg"]}; }}
::-webkit-scrollbar-thumb {{ background: {C["border"]}; border-radius: 3px; }}
</style>
""", unsafe_allow_html=True)

PARQUET_FILE = "data/rues_data.parquet"

@st.cache_resource(show_spinner=False)
def get_connection():
    if not os.path.exists(PARQUET_FILE):
        return None
    con = duckdb.connect()
    con.execute(f"CREATE VIEW rues AS SELECT * FROM '{PARQUET_FILE}'")
    return con

def parse_fecha(v):
    if not v or pd.isna(v):
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(str(v).strip()[:10], fmt).date()
        except ValueError:
            continue
    return None

def antiguedad_str(years: float) -> str:
    return f"{int(years * 12)} meses" if years < 1 else f"{years:.1f} años"

@st.cache_data(show_spinner=False, ttl=300)
def buscar_por_documento(num_id: str) -> pd.DataFrame:
    con = get_connection()
    if con is None:
        return pd.DataFrame()
    num_clean = num_id.strip().lstrip("0")
    return con.execute("""
        SELECT * FROM rues
        WHERE CAST(numero_identificacion AS VARCHAR) = ?
           OR LTRIM(CAST(numero_identificacion AS VARCHAR), '0') = ?
           OR CAST(nit AS VARCHAR) = ?
           OR LTRIM(CAST(nit AS VARCHAR), '0') = ?
        LIMIT 500
    """, [num_id.strip(), num_clean, num_id.strip(), num_clean]).df()

def nombre_empresa(row) -> str:
    rs = str(row.get("razon_social") or "").strip()
    if rs and rs not in ("nan", "None"):
        return rs.title()
    partes = [str(row.get("primer_nombre") or ""), str(row.get("primer_apellido") or "")]
    return " ".join(p for p in partes if p.strip() and p not in ("nan","None")).strip().title() or "Sin nombre"

def badge_estado(estado: str) -> str:
    e = str(estado or "").upper()
    if "ACTIV" in e or "VIGENT" in e:
        return '<span class="badge badge-activa">● Activa</span>'
    if "CANCEL" in e or "DISUEL" in e or "LIQUID" in e:
        return f'<span class="badge badge-cancelada">✕ {estado}</span>'
    return f'<span class="badge badge-otro">{estado or "—"}</span>'

def antiguedad_empresa(row):
    fobj = parse_fecha(row.get("fecha_matricula"))
    if not fobj:
        return None
    fcan = parse_fecha(str(row.get("fecha_cancelacion") or ""))
    fin = fcan if fcan else date.today()
    return round((fin - fobj).days / 365.25, 1)

@st.cache_data(show_spinner=False, ttl=600)
def tasa_supervivencia_cohorte(anio_matricula: int) -> dict:
    """
    Tasa REAL: de todas las empresas matriculadas en `anio_matricula`,
    cuántas siguen activas hoy. Formato fecha en base: YYYYMMDD (ej: 19970825).
    """
    con = get_connection()
    if con is None or not anio_matricula:
        return {}
    try:
        result = con.execute("""
            SELECT
                COUNT(*) AS total_cohorte,
                SUM(CASE WHEN estado_matricula ILIKE '%ACTIV%'
                          OR estado_matricula ILIKE '%VIGENT%'
                     THEN 1 ELSE 0 END) AS activas
            FROM rues
            WHERE LEFT(CAST(fecha_matricula AS VARCHAR), 4) = CAST(? AS VARCHAR)
              AND CAST(fecha_matricula AS VARCHAR) NOT IN ('nan','None','','00000000')
              AND LENGTH(CAST(fecha_matricula AS VARCHAR)) >= 8
        """, [anio_matricula]).fetchone()
        if not result or not result[0]:
            return {}
        total, activas = int(result[0]), int(result[1])
        return {
            "anio":            anio_matricula,
            "total_cohorte":   total,
            "activas_cohorte": activas,
            "tasa":            round(activas / total * 100, 1),
        }
    except Exception:
        return {}


# Población Colombia por año (DANE - estimaciones intercensales)
POBLACION_COL = {
    1990: 33_149_000, 1991: 33_904_000, 1992: 34_650_000, 1993: 35_386_000,
    1994: 36_112_000, 1995: 36_828_000, 1996: 37_531_000, 1997: 38_221_000,
    1998: 38_898_000, 1999: 39_564_000, 2000: 40_282_000, 2001: 40_895_000,
    2002: 41_468_000, 2003: 42_054_000, 2004: 42_660_000, 2005: 43_281_000,
    2006: 43_906_000, 2007: 44_534_000, 2008: 45_161_000, 2009: 45_781_000,
    2010: 46_388_000, 2011: 46_975_000, 2012: 47_551_000, 2013: 48_116_000,
    2014: 48_674_000, 2015: 49_228_000, 2016: 49_765_000, 2017: 50_372_000,
    2018: 50_880_000, 2019: 51_265_000, 2020: 50_882_000, 2021: 51_049_000,
    2022: 51_682_000, 2023: 52_215_000, 2024: 52_886_000, 2025: 53_000_000,
}

@st.cache_data(show_spinner=False, ttl=3600)
def tasa_emprendimiento_nacional() -> list:
    """
    Nuevas matrículas por año / población Colombia ese año * 1000.
    Retorna lista de dicts {anio, matriculas, poblacion, tasa}.
    """
    con = get_connection()
    if con is None:
        return []
    try:
        rows = con.execute("""
            SELECT
                LEFT(CAST(fecha_matricula AS VARCHAR), 4) AS anio_str,
                COUNT(*) AS n
            FROM rues
            WHERE fecha_matricula IS NOT NULL
              AND CAST(fecha_matricula AS VARCHAR) NOT IN ('nan','None','','00000000')
              AND LENGTH(CAST(fecha_matricula AS VARCHAR)) >= 8
              AND LEFT(CAST(fecha_matricula AS VARCHAR), 4) BETWEEN '1990' AND '2025'
            GROUP BY anio_str
            ORDER BY anio_str
        """).fetchall()
        result = []
        for anio_str, n in rows:
            try:
                anio = int(anio_str)
            except Exception:
                continue
            pob = POBLACION_COL.get(anio)
            if not pob:
                continue
            result.append({
                "anio":       anio,
                "matriculas": int(n),
                "poblacion":  pob,
                "tasa":       round(int(n) / pob * 1000, 2),
            })
        return result
    except Exception:
        return []

@st.cache_data(show_spinner=False, ttl=3600)
def sector_en_anio(ciiu: str, anio: int) -> dict:
    """
    Para un CIIU y año dados, retorna:
    - total y activas del sector en ese año
    - total y activas nacional en ese año
    - tasas de supervivencia sector vs nacional
    """
    con = get_connection()
    if con is None or not ciiu or not anio:
        return {}
    ciiu_prefix = str(ciiu).strip()[:2]
    try:
        r_sec = con.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN estado_matricula ILIKE '%ACTIV%'
                          OR estado_matricula ILIKE '%VIGENT%'
                     THEN 1 ELSE 0 END) AS activas
            FROM rues
            WHERE LEFT(CAST(fecha_matricula AS VARCHAR), 4) = CAST(? AS VARCHAR)
              AND CAST(fecha_matricula AS VARCHAR) NOT IN ('nan','None','','00000000')
              AND LENGTH(CAST(fecha_matricula AS VARCHAR)) >= 8
              AND LEFT(CAST(cod_ciiu_act_econ_pri AS VARCHAR), 2) = ?
        """, [anio, ciiu_prefix]).fetchone()

        r_nac = con.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN estado_matricula ILIKE '%ACTIV%'
                          OR estado_matricula ILIKE '%VIGENT%'
                     THEN 1 ELSE 0 END) AS activas
            FROM rues
            WHERE LEFT(CAST(fecha_matricula AS VARCHAR), 4) = CAST(? AS VARCHAR)
              AND CAST(fecha_matricula AS VARCHAR) NOT IN ('nan','None','','00000000')
              AND LENGTH(CAST(fecha_matricula AS VARCHAR)) >= 8
        """, [anio]).fetchone()

        if not r_sec or not r_sec[0] or not r_nac or not r_nac[0]:
            return {}

        total_sec, act_sec = int(r_sec[0]), int(r_sec[1])
        total_nac, act_nac = int(r_nac[0]), int(r_nac[1])

        return {
            "total_sector":    total_sec,
            "activas_sector":  act_sec,
            "tasa_sector":     round(act_sec / total_sec * 100, 1) if total_sec else 0,
            "total_nacional":  total_nac,
            "activas_nacional": act_nac,
            "tasa_nacional":   round(act_nac / total_nac * 100, 1) if total_nac else 0,
        }
    except Exception:
        return {}

# ── PLOT HELPERS ───────────────────────────────────────────────────────────────
PLOT_BASE = dict(
    paper_bgcolor=C["surface"], plot_bgcolor=C["bg"],
    font=dict(family="Inter, -apple-system, sans-serif", color=C["ink2"]),
)

SECTION_DESCRIPTIONS = {
    "① Perfil de la empresa":
        "Información básica del registro mercantil: quién es el propietario, en qué sector opera, cuándo se matriculó y cuál es su estado actual.",
    "② Indicadores generales":
        "Resumen cuantitativo de todas las empresas encontradas para esta cédula: cuántas tiene, cuántas siguen activas y cuánto tiempo llevan en el mercado en promedio.",
    "③ Supervivencia empresarial":
        "Muestra cuánto tiempo duró o lleva cada empresa activa, y la compara con su cohorte: el grupo de todas las empresas que se crearon en Colombia ese mismo año.",
    "④ Contexto de cohorte y emprendimiento":
        "Analiza el entorno en que nació la empresa: qué tan activo estaba el emprendimiento ese año en Colombia, y cómo le ha ido históricamente a las empresas del mismo sector.",
    "⑤ La empresa en su contexto nacional":
        "Compara la duración de cada empresa con el porcentaje de empresas de su misma generación que aún sobreviven hoy.",
    "⑥ El sector en el año de matrícula":
        "Muestra cómo se comportó el sector económico de la empresa en el año en que se creó: cuántas empresas del mismo sector nacieron ese año y cuántas siguen activas.",
    "⑦ Datos completos":
        "Tabla con todos los campos del registro RUES. Puedes elegir qué columnas ver y descargar los datos en CSV.",
}

def section(title):
    desc = SECTION_DESCRIPTIONS.get(title, "")
    desc_html = (
        f'<p style="font-size:0.78rem;color:{C["ink2"]};font-style:italic;'
        f'margin:0.3rem 0 1rem 0;line-height:1.6;">{desc}</p>'
        if desc else ""
    )
    st.markdown(
        f'<div class="section-title">{title}</div>{desc_html}',
        unsafe_allow_html=True,
    )

def kpi_row(datos):
    cols = st.columns(len(datos))
    for col, (v, l, s, color) in zip(cols, datos):
        border = f"border-top-color:{color}" if color else ""
        val_color = f"color:{color}" if color else ""
        with col:
            st.markdown(f"""
            <div class="metric-card" style="{border}">
                <div class="val" style="{val_color}">{v}</div>
                <div class="lbl">{l}</div>
                {"<div class='sub'>" + s + "</div>" if s else ""}
            </div>""", unsafe_allow_html=True)

# ── MAPA EMPRESA ───────────────────────────────────────────────────────────────
def fig_mapa_empresa(camara: str):
    coords = geocode_camara(camara)
    if not coords:
        return None
    lat, lon = coords
    ciudad = camara.title()
    for k, v in CIUDADES_COL.items():
        if v == coords and not k.startswith("camara"):
            ciudad = k.title()
            break
    fig = go.Figure()
    fig.add_trace(go.Scattergeo(
        lat=[lat], lon=[lon], text=[ciudad],
        mode="markers+text", textposition="top right",
        textfont=dict(size=11, color=C["ink"], family="Inter, sans-serif"),
        marker=dict(size=12, color=C["accent"], line=dict(color=C["bg"], width=2.5)),
        hovertemplate=f"<b>{camara.title()}</b><extra></extra>",
    ))
    fig.update_layout(
        paper_bgcolor=C["surface"],
        geo=dict(scope="south america", showland=True, landcolor="#2A4A6B",
                 showocean=True, oceancolor="#0D1B2A", showcountries=True,
                 countrycolor="#4DA6FF", showcoastlines=True, coastlinecolor="#4DA6FF",
                 showrivers=False, showlakes=False,
                 center=dict(lat=4.5, lon=-74.0),
                 lonaxis=dict(range=[-80, -66]), lataxis=dict(range=[-5, 13]),
                 projection_type="mercator", bgcolor=C["surface"], framewidth=0),
        font=dict(family="Inter, sans-serif", color=C["ink2"]),
        title_text=f"Ubicación · {ciudad}",
        title_font=dict(color=C["ink"], size=13, family="Inter, sans-serif"),
        margin=dict(t=36, b=0, l=0, r=0), height=340,
    )
    return fig

# ── TARJETA EMPRESA ────────────────────────────────────────────────────────────
def render_empresa_card(row: pd.Series):
    nombre  = nombre_empresa(row)
    estado  = str(row.get("estado_matricula") or "")
    clase   = ("activa" if "ACTIV" in estado.upper() else
               "cancelada" if "CANCEL" in estado.upper() else "")
    mat     = row.get("matricula") or "—"
    ciiu    = row.get("cod_ciiu_act_econ_pri") or "—"
    org     = row.get("organizacion_juridica") or "—"
    camara  = row.get("camara_comercio") or "—"
    tipo_s  = row.get("tipo_sociedad") or "—"
    cat_mat = row.get("categoria_matricula") or "—"
    f_mat   = str(row.get("fecha_matricula") or "—")
    f_can   = str(row.get("fecha_cancelacion") or "")
    rep     = str(row.get("Representante Legal") or "")
    p_nom   = str(row.get("primer_nombre") or "")
    p_ape   = str(row.get("primer_apellido") or "")
    s_ape   = str(row.get("segundo_apellido") or "")

    dueno = rep.strip().title() if rep.strip() not in ("", "nan", "None") else ""
    if not dueno:
        dueno = " ".join(p for p in [p_nom, p_ape, s_ape]
                         if p.strip() not in ("", "nan", "None")).title()
    if not dueno:
        dueno = "No registrado"

    sector = get_sector(ciiu)
    f_can_clean = f_can.strip()

    fobj = parse_fecha(f_mat)
    if fobj:
        f_can_obj = parse_fecha(f_can_clean) if f_can_clean not in ("", "nan", "None", "—") else None
        fecha_fin = f_can_obj if f_can_obj else date.today()
        delta = (fecha_fin - fobj).days / 365.25
        ant_str = f"{delta:.1f} años"
    else:
        ant_str = ""

    f_mat_clean = f_mat[:10] if len(f_mat) >= 8 else f_mat

    if f_can_clean not in ("", "nan", "None", "—"):
        anio_can = f_can_clean[:4]
        estado_bloque = (
            f'<div style="margin-top:0.9rem;padding:0.6rem 0.9rem;background:#2B1010;border-left:3px solid {C["red"]};font-size:0.77rem;color:{C["red"]};line-height:1.8">'
            f'<b>✕ Empresa cancelada</b><br>'
            f'Año de cancelación: <b>{anio_can}</b> &nbsp;·&nbsp; Fecha exacta: <b>{f_can_clean[:10]}</b><br>'
            f'Duración en el mercado: <b>{ant_str or "—"}</b>'
            f'</div>'
        )
    elif "ACTIV" in estado.upper():
        estado_bloque = (
            f'<div style="margin-top:0.9rem;padding:0.6rem 0.9rem;background:#0D2B1F;border-left:3px solid {C["green"]};font-size:0.77rem;color:{C["green"]};line-height:1.8">'
            f'<b>● Empresa activa</b><br>'
            f'Lleva <b>{ant_str or "—"}</b> en el mercado'
            f'</div>'
        )
    else:
        estado_bloque = ""

    st.markdown(f"""
    <div class="emp-card {clase}">
      <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:0.85rem;padding-bottom:0.75rem;border-bottom:1px solid {C['border']}">
        <span class="emp-nombre">{nombre}</span>
        {badge_estado(estado)}
      </div>
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:0.9rem;padding:0.55rem 0.9rem;background:{C['bg']};border:1px solid {C['border']}">
        <span style="font-size:0.65rem;text-transform:uppercase;letter-spacing:0.09em;color:{C['ink2']};white-space:nowrap">Propietario / Rep. legal</span>
        <span style="font-family:'Inter',sans-serif;font-size:0.95rem;font-weight:600;color:{C['ink']}">{dueno}</span>
      </div>
      <div style="margin-bottom:0.9rem;padding:0.5rem 0.9rem;background:{C['surface']};border:1px solid {C['border']}">
        <span style="font-size:0.65rem;text-transform:uppercase;letter-spacing:0.09em;color:{C['ink2']}">Sector económico</span>
        <div style="font-size:0.9rem;color:{C['ink']};margin-top:2px;font-weight:500">
          🏭 &nbsp;{sector} <span style="color:{C['ink2']};font-size:0.75rem">&nbsp;(CIIU {ciiu})</span>
        </div>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:0.4rem 2rem;font-size:0.78rem;color:{C['ink2']};line-height:2">
        <div>📋 <b style="color:{C['ink']}">Tipo de organización:</b> {org}</div>
        <div>🏛️ <b style="color:{C['ink']}">Tipo de sociedad:</b> {tipo_s}</div>
        <div>🗂️ <b style="color:{C['ink']}">Categoría matrícula:</b> {cat_mat}</div>
        <div>📍 <b style="color:{C['ink']}">Cámara de comercio:</b> {str(camara).title()}</div>
        <div>📅 <b style="color:{C['ink']}">Fecha de matrícula:</b> {f_mat_clean}</div>
        <div>🪪 <b style="color:{C['ink']}">N° matrícula:</b> {mat}</div>
      </div>
      {estado_bloque}
    </div>
    """, unsafe_allow_html=True)
    return str(camara)

# ── TIMELINE ───────────────────────────────────────────────────────────────────
def render_timeline_empresas(df):
    df2 = df.copy()
    df2["inicio"] = df2["fecha_matricula"].apply(parse_fecha)
    hoy = date.today()
    def _fin(row):
        f = parse_fecha(str(row.get("fecha_cancelacion") or ""))
        return f if f else hoy
    df2["fin"] = df2.apply(_fin, axis=1)
    df2 = df2.dropna(subset=["inicio"]).sort_values("inicio")
    if df2.empty:
        st.info("No hay fechas de matrícula disponibles.")
        return

    max_dias = max((df2["fin"] - df2["inicio"]).apply(lambda x: x.days).max(), 1)
    n = len(df2)
    PD = "Playfair Display"
    BG = C["bg"]; SRF = C["surface"]; BRD = C["border"]
    INK = C["ink"]; INK2 = C["ink2"]; ACC = C["accent"]
    GRN = C["green"]; RED = C["red"]; BLU = C["blue"]

    def sp(k, v): return k + ":" + v + ";"

    html = []
    html.append("<div style='" + sp("background", SRF) + sp("border", "1px solid " + BRD) + sp("padding", "1.4rem 1.6rem") + "'>")
    html.append("<div style='" + sp("font-family", PD+",serif") + sp("font-size","1rem") + sp("font-weight","600") + sp("color",INK) + sp("margin-bottom","1rem") + sp("border-bottom","1px solid "+BRD) + sp("padding-bottom","0.5rem") + "'>Línea de tiempo empresarial</div>")

    for i, (_, row) in enumerate(df2.iterrows()):
        nombre  = nombre_empresa(row)[:50]
        estado  = str(row.get("estado_matricula") or "")
        inicio  = row["inicio"]
        fin     = row["fin"]
        d_dias  = (fin - inicio).days
        d_str   = antiguedad_str(d_dias / 365.25)
        camara  = str(row.get("camara_comercio") or "-")
        ciiu    = str(row.get("cod_ciiu_act_econ_pri") or "-")
        sector  = get_sector(ciiu)
        f_can   = str(row.get("fecha_cancelacion") or "")
        activa  = "ACTIV" in estado.upper() or "VIGENT" in estado.upper()
        cancel  = "CANCEL" in estado.upper() or "DISUEL" in estado.upper()
        dot     = GRN if activa else (RED if cancel else BLU)
        tc      = GRN if activa else (RED if cancel else INK2)
        tbg     = "#0D2B1F" if activa else ("#2B1010" if cancel else "#1A1F2A")
        tlbl    = "Activa" if activa else estado.title()
        pct     = min(100, int(d_dias / max_dias * 100))
        last    = (i == n - 1)
        line_bg = "linear-gradient(to bottom,"+BRD+",transparent)" if last else BRD
        fc      = f_can.strip()
        fcan_html = ""
        if fc and fc not in ("nan","None"):
            fcan_html = "<div style='" + sp("margin-top","3px") + sp("font-size","0.73rem") + sp("color",RED) + "'>Cancelada: <b>" + fc[:10] + "</b></div>"

        html.append("<div style='" + sp("display","flex") + sp("gap","0") + sp("align-items","stretch") + "'>")
        html.append("<div style='" + sp("width","62px") + sp("min-width","62px") + sp("display","flex") + sp("flex-direction","column") + sp("align-items","center") + "'><div style='" + sp("font-family",PD+",serif") + sp("font-size","0.82rem") + sp("font-weight","700") + sp("color",ACC) + sp("line-height","1") + sp("margin-top","16px") + sp("white-space","nowrap") + "'>" + str(inicio.year) + "</div><div style='" + sp("width","2px") + sp("background",line_bg) + sp("flex","1") + sp("margin-top","5px") + sp("min-height","20px") + "'></div></div>")
        html.append("<div style='" + sp("display","flex") + sp("align-items","flex-start") + sp("padding-top","16px") + sp("margin","0 -7px") + sp("z-index","2") + "'><div style='" + sp("width","13px") + sp("height","13px") + sp("border-radius","50%") + sp("background",dot) + "border:2.5px solid " + BG + ";box-shadow:0 0 0 2px " + dot + "55;" + sp("flex-shrink","0") + "'></div></div>")
        html.append("<div style='" + sp("flex","1") + sp("padding","12px 0 22px 16px") + "'>")
        html.append("<div style='" + sp("display","flex") + sp("align-items","center") + sp("gap","8px") + sp("flex-wrap","wrap") + "'><span style='" + sp("font-family",PD+",serif") + sp("font-size","0.96rem") + sp("font-weight","600") + sp("color",INK) + "'>" + nombre + "</span><span style='" + sp("font-size","0.64rem") + sp("font-weight","600") + sp("letter-spacing","0.07em") + sp("text-transform","uppercase") + sp("color",tc) + sp("background",tbg) + "border:1px solid " + tc + "40;" + sp("padding","2px 7px") + "'>" + tlbl + "</span></div>")
        html.append("<div style='" + sp("font-size","0.74rem") + sp("color",INK2) + sp("margin-top","4px") + sp("line-height","1.85") + "'>Inscrita: <b>" + str(inicio) + "</b> &nbsp;·&nbsp; Duración: <b>" + d_str + "</b> &nbsp;·&nbsp; " + camara + fcan_html + "<div style='" + sp("margin-top","2px") + "'>🏭 Sector: <b>" + sector + "</b> &nbsp;·&nbsp; CIIU: <b>" + ciiu + "</b></div></div>")
        html.append("<div style='" + sp("margin-top","7px") + sp("height","3px") + sp("background",BRD) + sp("border-radius","2px") + sp("max-width","300px") + "'><div style='" + sp("height","3px") + "width:" + str(pct) + "%;" + sp("background",dot) + sp("border-radius","2px") + sp("opacity","0.65") + "'></div></div>")
        html.append("<div style='" + sp("font-size","0.64rem") + sp("color",INK2) + sp("margin-top","2px") + "'>{:,} días de actividad</div>".format(d_dias))
        html.append("</div></div>")

    html.append("</div>")
    st.markdown("".join(html), unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# RENDER PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════
def render_perfil(df: pd.DataFrame):

    df2 = df.copy()
    df2["antiguedad"] = df2.apply(antiguedad_empresa, axis=1)
    e = df2["estado_matricula"].fillna("").str.upper()
    total      = len(df2)
    activas    = int(e.str.contains("ACTIV|VIGENT").sum())
    canceladas = int(e.str.contains("CANCEL|DISUEL|LIQUID").sum())
    tasa       = round(activas / total * 100, 1) if total else 0
    df_validas = df2.dropna(subset=["antiguedad"])
    ant_prom   = round(df_validas["antiguedad"].mean(), 1) if not df_validas.empty else 0
    ant_max    = round(df_validas["antiguedad"].max(), 1) if not df_validas.empty else 0

    # ── 1. PERFIL ──────────────────────────────────────────────────────────────
    section("① Perfil de la empresa")
    camaras_vistas = []
    for _, row in df2.iterrows():
        cam = render_empresa_card(row)
        camaras_vistas.append(cam)

    col_mapa, col_tl = st.columns([1, 1], gap="medium")
    with col_mapa:
        for cam in camaras_vistas:
            fig = fig_mapa_empresa(cam)
            if fig:
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
                break
    with col_tl:
        render_timeline_empresas(df2)

    # ── 2. INDICADORES ─────────────────────────────────────────────────────────
    section("② Indicadores generales")
    kpi_row([
        (total,      "Empresas registradas",       "",                          C["accent"]),
        (activas,    "Activas",                    f"{tasa}% del total",        C["green"]),
        (canceladas, "Canceladas / disueltas",     "",                          C["red"]),
        (f"{ant_prom} años", "Antigüedad promedio", f"Máxima: {ant_max} años", C["gold"]),
    ])

    # ── 3. SUPERVIVENCIA ───────────────────────────────────────────────────────
    section("③ Supervivencia empresarial")

    def _vida(row):
        fi = parse_fecha(row.get("fecha_matricula"))
        if not fi:
            return None, None, None
        fc = parse_fecha(str(row.get("fecha_cancelacion") or ""))
        fin = fc if fc else date.today()
        dias = (fin - fi).days
        return fi, fin, dias

    cards_html = []
    for _, row in df2.iterrows():
        nombre = nombre_empresa(row)
        estado = str(row.get("estado_matricula") or "")
        activa = "ACTIV" in estado.upper() or "VIGENT" in estado.upper()
        cancel = "CANCEL" in estado.upper() or "DISUEL" in estado.upper()
        fi, fin, dias = _vida(row)
        sector = get_sector(row.get("cod_ciiu_act_econ_pri"))
        camara = str(row.get("camara_comercio") or "—").title()

        dot_c = C["green"] if activa else (C["red"] if cancel else C["blue"])
        bg_c  = "#0D2B1F" if activa else ("#2B0D0D" if cancel else "#0D1B2A")
        lbl_e = "Activa" if activa else ("Cancelada" if cancel else estado.title() or "—")

        if dias is not None:
            anios  = dias / 365.25
            pct    = min(100, int(anios / 20 * 100))
            dur    = f"{anios:.1f} años" if anios >= 1 else f"{dias} días"
            fi_s   = str(fi)
            fin_s  = str(fin) if cancel else "Hoy"
        else:
            pct, dur, fi_s, fin_s = 0, "—", "—", "—"

        bar_fill = dot_c + "BB"

        # ── Tasa de supervivencia REAL de la cohorte ──────────────────────────
        tasa_data  = tasa_supervivencia_cohorte(fi.year) if fi else {}
        tasa_pct   = tasa_data.get("tasa", None)
        total_coh  = tasa_data.get("total_cohorte", 0)
        activas_c  = tasa_data.get("activas_cohorte", 0)
        tasa_color = C["green"] if (tasa_pct is not None and tasa_pct >= 50) else C["red"]
        tasa_str   = f"{tasa_pct}%" if tasa_pct is not None else "—"
        anio_label = fi.year if fi else "—"

        # frase interpretativa
        if tasa_pct is not None:
            empresa_estado_frase = (
                f"Esta empresa <b style='color:{C['green']}'>es parte de ese grupo</b>."
                if activa else
                f"Esta empresa <b style='color:{C['red']}'>no sobrevivió</b>."
            )
            tasa_bloque = f"""
          <div style="margin-top:1rem;padding:0.75rem 1rem;background:#1A2F4A;
                      border:1px solid {C['border']};display:flex;align-items:center;
                      gap:1.5rem;flex-wrap:wrap;">
            <div style="min-width:90px;flex-shrink:0;">
              <div style="font-size:0.62rem;text-transform:uppercase;letter-spacing:0.09em;
                          color:{C['ink2']};margin-bottom:3px;">Tasa de supervivencia</div>
              <div style="font-family:'Inter',sans-serif;font-size:1.5rem;
                          font-weight:700;color:{tasa_color};line-height:1;">{tasa_str}</div>
              <div style="font-size:0.65rem;color:{C['ink2']};margin-top:3px;">
                cohorte {anio_label}
              </div>
            </div>
            <div style="width:1px;height:44px;background:{C['border']};flex-shrink:0;"></div>
            <div style="font-size:0.76rem;color:{C['ink2']};line-height:1.85;flex:1;">
              De las <b style="color:{C['ink']};">{total_coh:,}</b> empresas creadas en
              <b style="color:{C['ink']};">{anio_label}</b>,
              solo <b style="color:{tasa_color};">{activas_c:,} ({tasa_str})</b> siguen activas hoy.
              {empresa_estado_frase}
            </div>
          </div>"""
        else:
            tasa_bloque = ""

        cards_html.append(f"""
        <div style="background:{bg_c};border:1px solid {C['border']};border-left:4px solid {dot_c};
                    padding:1.4rem 1.6rem;margin-bottom:1rem;">

          <div style="display:flex;align-items:center;justify-content:space-between;
                      flex-wrap:wrap;gap:8px;margin-bottom:1rem;">
            <span style="font-family:'Inter',sans-serif;font-size:1rem;
                         font-weight:600;color:{C['ink']};">{nombre}</span>
            <span style="font-size:0.68rem;font-weight:600;letter-spacing:0.08em;
                         text-transform:uppercase;color:{dot_c};background:#1A2F4A;
                         border:1px solid {dot_c};padding:3px 10px;">{lbl_e}</span>
          </div>

          <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:1rem;margin-bottom:1.1rem;">
            <div>
              <div style="font-size:0.62rem;text-transform:uppercase;letter-spacing:0.09em;
                          color:{C['ink2']};margin-bottom:3px;">Duración en el mercado</div>
              <div style="font-family:'Inter',sans-serif;font-size:1.6rem;
                          font-weight:700;color:{dot_c};line-height:1;">{dur}</div>
            </div>
            <div>
              <div style="font-size:0.62rem;text-transform:uppercase;letter-spacing:0.09em;
                          color:{C['ink2']};margin-bottom:3px;">Fecha matrícula</div>
              <div style="font-size:0.88rem;font-weight:500;color:{C['ink']};">{fi_s}</div>
            </div>
            <div>
              <div style="font-size:0.62rem;text-transform:uppercase;letter-spacing:0.09em;
                          color:{C['ink2']};margin-bottom:3px;">{"Cancelada" if cancel else "Activa hasta"}</div>
              <div style="font-size:0.88rem;font-weight:500;color:{C['ink']};">{fin_s}</div>
            </div>
          </div>

          <div style="font-size:0.62rem;text-transform:uppercase;letter-spacing:0.08em;
                      color:{C['ink2']};margin-bottom:5px;">Línea de vida &nbsp;
            <span style="color:{C['ink']};font-style:italic;font-size:0.7rem;">(referencia: 20 años)</span>
          </div>
          <div style="background:{C['border']};height:8px;border-radius:4px;overflow:hidden;">
            <div style="width:{pct}%;height:8px;background:{bar_fill};border-radius:4px;"></div>
          </div>
          <div style="display:flex;justify-content:space-between;font-size:0.65rem;
                      color:{C['ink2']};margin-top:3px;">
            <span>{fi_s}</span>
            <span style="font-weight:500;color:{dot_c};">{pct}% de 20 años</span>
            <span>{fin_s}</span>
          </div>

          {tasa_bloque}

          <div style="margin-top:1rem;padding-top:0.75rem;border-top:1px solid {C['border']};
                      display:flex;gap:2rem;flex-wrap:wrap;font-size:0.76rem;color:{C['ink2']};">
            <span>🏭 <b style="color:{C['ink']};">{sector}</b></span>
            <span>📍 <b style="color:{C['ink']};">{camara}</b></span>
          </div>
        </div>
        """)

    st.markdown("".join(cards_html), unsafe_allow_html=True)

    # ── Donut + Scatter (solo si hay más de 1 empresa) ─────────────────────────
    if total > 1:
        col_d1, col_d2 = st.columns([1, 1], gap="large")

        with col_d1:
            otras    = total - activas - canceladas
            lv_raw   = [("Activas", activas, C["green"]),
                        ("Canceladas", canceladas, C["red"]),
                        ("Otras", otras, C["blue"])]
            lv       = [(l, v, c) for l, v, c in lv_raw if v > 0]
            labels_d, values_d, colors_d = zip(*lv) if lv else ([], [], [])

            fig_donut = go.Figure(go.Pie(
                labels=labels_d, values=values_d, hole=0.68,
                marker=dict(colors=list(colors_d), line=dict(color=C["bg"], width=3)),
                textinfo="none",
                hovertemplate="<b>%{label}</b><br>%{value} empresas · %{percent}<extra></extra>",
            ))
            fig_donut.add_annotation(
                text=f"<b>{total}</b><br><span style='font-size:11px'>empresas</span>",
                x=0.5, y=0.5, showarrow=False,
                font=dict(family="Inter, sans-serif", color=C["ink"], size=16),
            )
            fig_donut.update_layout(
                **PLOT_BASE, height=260,
                title_text="Distribución por estado",
                title_font=dict(color=C["ink"], size=13, family="Inter, sans-serif"),
                showlegend=True,
                legend=dict(orientation="v", x=1, y=0.5,
                            font=dict(color=C["ink2"], size=11), bgcolor="rgba(0,0,0,0)"),
                margin=dict(t=50, b=20, l=20, r=80),
            )
            st.plotly_chart(fig_donut, use_container_width=True, config={"displayModeBar": False})

        with col_d2:
            df_plot = df_validas.copy()
            df_plot["sector_n"] = df_plot["cod_ciiu_act_econ_pri"].apply(get_sector)
            df_plot["nombre_n"] = df_plot.apply(nombre_empresa, axis=1)

            fig_sc = go.Figure()
            for estado_val, color_val, lbl_val in [
                ("ACTIV", C["green"], "Activa"),
                ("CANCEL", C["red"], "Cancelada"),
            ]:
                sub = df_plot[df_plot["estado_matricula"].str.upper().str.contains(estado_val, na=False)]
                if sub.empty:
                    continue
                fig_sc.add_trace(go.Scatter(
                    x=sub["antiguedad"], y=sub["sector_n"], mode="markers",
                    marker=dict(size=10, color=color_val, opacity=0.75,
                                line=dict(color=C["bg"], width=1.5)),
                    name=lbl_val, text=sub["nombre_n"],
                    hovertemplate="<b>%{text}</b><br>%{x:.1f} años<extra></extra>",
                ))
            fig_sc.update_layout(
                **PLOT_BASE, height=260,
                title_text="Antigüedad por sector",
                title_font=dict(color=C["ink"], size=13, family="Inter, sans-serif"),
                xaxis=dict(title="Años en el mercado", gridcolor=C["border"],
                           linecolor=C["border"], tickfont=dict(color=C["ink2"])),
                yaxis=dict(showgrid=False, linecolor=C["border"],
                           tickfont=dict(color=C["ink"], size=9)),
                legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=C["ink2"], size=10)),
                margin=dict(t=50, b=30, l=200, r=20),
            )
            st.plotly_chart(fig_sc, use_container_width=True, config={"displayModeBar": False})

    # ── 4. TASA DE EMPRENDIMIENTO ──────────────────────────────────────────────
    section("④ Tasa de emprendimiento por año")

    st.markdown(
        f'<p style="font-size:0.8rem;color:{C["ink2"]};font-style:italic;margin-bottom:1rem;">'
        f'Nuevas matrículas por cada 100,000 habitantes · Fuente población: DANE estimaciones intercensales</p>',
        unsafe_allow_html=True,
    )

    emp_data = tasa_emprendimiento_nacional()
    if emp_data:
        df_emp = pd.DataFrame(emp_data).sort_values("anio")

        # KPIs del año de matrícula de la primera empresa del perfil
        anio_empresa = None
        for _, row in df2.iterrows():
            fi_tmp = parse_fecha(row.get("fecha_matricula"))
            if fi_tmp:
                anio_empresa = fi_tmp.year
                break

        col_e1, col_e2, col_e3 = st.columns(3)
        if anio_empresa and anio_empresa in df_emp["anio"].values:
            fila_emp = df_emp[df_emp["anio"] == anio_empresa].iloc[0]
            fila_max = df_emp.loc[df_emp["tasa"].idxmax()]
            fila_rec = df_emp.iloc[-1]
            with col_e1:
                st.markdown(f"""
                <div class="metric-card" style="border-top-color:{C['accent']}">
                  <div class="val" style="color:{C['accent']}">{fila_emp['tasa']:.1f}</div>
                  <div class="lbl">Tasa en {anio_empresa}</div>
                  <div class="sub">por 100,000 hab · {fila_emp['matriculas']:,} matrículas</div>
                </div>""", unsafe_allow_html=True)
            with col_e2:
                st.markdown(f"""
                <div class="metric-card" style="border-top-color:{C['gold']}">
                  <div class="val" style="color:{C['gold']}">{fila_max['tasa']:.1f}</div>
                  <div class="lbl">Pico histórico ({int(fila_max['anio'])})</div>
                  <div class="sub">{fila_max['matriculas']:,} matrículas ese año</div>
                </div>""", unsafe_allow_html=True)
            with col_e3:
                st.markdown(f"""
                <div class="metric-card" style="border-top-color:{C['blue']}">
                  <div class="val" style="color:{C['blue']}">{fila_rec['tasa']:.1f}</div>
                  <div class="lbl">Tasa más reciente ({int(fila_rec['anio'])})</div>
                  <div class="sub">{fila_rec['matriculas']:,} matrículas ese año</div>
                </div>""", unsafe_allow_html=True)

        st.markdown("<div style='margin-top:1rem'></div>", unsafe_allow_html=True)

        fig_emp = go.Figure()
        # Área de fondo
        fig_emp.add_trace(go.Scatter(
            x=df_emp["anio"], y=df_emp["tasa"], mode="none",
            fill="tozeroy", fillcolor="rgba(139,94,60,0.08)",
            showlegend=False, hoverinfo="skip",
        ))
        # Línea principal
        fig_emp.add_trace(go.Scatter(
            x=df_emp["anio"], y=df_emp["tasa"], mode="lines+markers",
            line=dict(color=C["accent"], width=2.5, shape="spline", smoothing=0.7),
            marker=dict(size=6, color=C["bg"], line=dict(color=C["accent"], width=2)),
            hovertemplate="<b>%{x}</b><br>Tasa: %{y:.1f} por 100k hab<extra></extra>",
            showlegend=False,
        ))
        # Marcar el año de la empresa
        if anio_empresa and anio_empresa in df_emp["anio"].values:
            fila_marca = df_emp[df_emp["anio"] == anio_empresa].iloc[0]
            fig_emp.add_trace(go.Scatter(
                x=[fila_marca["anio"]], y=[fila_marca["tasa"]],
                mode="markers+text",
                marker=dict(size=13, color=C["accent2"], symbol="diamond",
                            line=dict(color=C["bg"], width=2)),
                text=[f"  Año de matrícula"], textposition="top right",
                textfont=dict(size=10, color=C["accent2"], family="Inter, sans-serif"),
                hovertemplate=f"<b>{anio_empresa}</b><br>Tasa: {fila_marca['tasa']:.1f}<extra></extra>",
                showlegend=False,
            ))
        # Pico histórico
        idx_max_emp = df_emp["tasa"].idxmax()
        fig_emp.add_annotation(
            x=df_emp.loc[idx_max_emp, "anio"], y=df_emp.loc[idx_max_emp, "tasa"],
            text=f"Pico: {df_emp.loc[idx_max_emp,'tasa']:.1f}",
            showarrow=True, arrowhead=2, arrowcolor=C["gold"],
            font=dict(color=C["gold"], size=10, family="Inter, sans-serif"),
            bgcolor=C["surface"], bordercolor=C["border"], borderwidth=1, ay=-38,
        )
        fig_emp.update_layout(
            **PLOT_BASE, height=300,
            title_text="Nuevas empresas por cada 100,000 habitantes",
            title_font=dict(color=C["ink"], size=13, family="Inter, sans-serif"),
            xaxis=dict(title="Año", gridcolor=C["border"], linecolor=C["border"],
                       tickfont=dict(color=C["ink2"]), dtick=5),
            yaxis=dict(title="Tasa por 100k hab.", gridcolor=C["border"],
                       linecolor=C["border"], tickfont=dict(color=C["ink2"])),
            hovermode="x unified",
        )
        st.plotly_chart(fig_emp, use_container_width=True, config={"displayModeBar": False})
    else:
        st.info("No se pudo calcular la tasa de emprendimiento. Verifica la conexión al parquet.")

    # ── 5. COMPARACIÓN CON COHORTE NACIONAL ────────────────────────────────────
    section("⑤ La empresa en su contexto nacional")

    st.markdown(
        f'<p style="font-size:0.8rem;color:{C["ink2"]};font-style:italic;margin-bottom:1rem;">'
        f'Comparación de cada empresa del perfil con el promedio de su cohorte (año de matrícula)</p>',
        unsafe_allow_html=True,
    )

    cohortes_comparadas = []
    for _, row in df2.iterrows():
        fi_c = parse_fecha(row.get("fecha_matricula"))
        if not fi_c:
            continue
        td = tasa_supervivencia_cohorte(fi_c.year)
        if not td:
            continue
        ant_c = antiguedad_empresa(row)
        cohortes_comparadas.append({
            "nombre":        nombre_empresa(row),
            "anio":          fi_c.year,
            "ant_empresa":   ant_c or 0,
            "ant_cohorte":   round(td["activas_cohorte"] / max(td["total_cohorte"], 1) * 20, 1),
            "tasa_cohorte":  td["tasa"],
            "total_cohorte": td["total_cohorte"],
            "activas_c":     td["activas_cohorte"],
            "estado":        str(row.get("estado_matricula") or ""),
        })

    if cohortes_comparadas:
        for comp in cohortes_comparadas:
            activa_comp  = "ACTIV" in comp["estado"].upper() or "VIGENT" in comp["estado"].upper()
            cancel_comp  = "CANCEL" in comp["estado"].upper()
            color_emp    = C["green"] if activa_comp else (C["red"] if cancel_comp else C["blue"])
            tasa_c       = comp["tasa_cohorte"]
            tasa_color_c = C["green"] if tasa_c >= 50 else C["red"]

            # Barra empresa vs promedio cohorte
            pct_emp_bar  = min(100, int(comp["ant_empresa"] / 20 * 100))
            # Antigüedad promedio de activas ≈ (activas / total) * 20 años referencia
            pct_coh_bar  = min(100, int(tasa_c))  # tasa = % activas de la cohorte

            diff = comp["ant_empresa"] - comp["ant_cohorte"]
            diff_str = (f"+{diff:.1f} años sobre el promedio de su cohorte"
                        if diff >= 0 else f"{diff:.1f} años bajo el promedio de su cohorte")
            diff_color = C["green"] if diff >= 0 else C["red"]

            st.markdown(f"""
            <div style="background:{C['surface']};border:1px solid {C['border']};
                        border-left:4px solid {color_emp};padding:1.3rem 1.6rem;margin-bottom:1rem;">

              <div style="font-family:'Inter',sans-serif;font-size:0.98rem;
                          font-weight:600;color:{C['ink']};margin-bottom:1rem;">
                {comp['nombre']}
                <span style="font-size:0.7rem;font-weight:400;color:{C['ink2']};
                             margin-left:10px;">cohorte {comp['anio']}</span>
              </div>

              <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:1.2rem;margin-bottom:1.2rem;">
                <div>
                  <div style="font-size:0.62rem;text-transform:uppercase;letter-spacing:0.08em;
                              color:{C['ink2']};margin-bottom:3px;">Duración de esta empresa</div>
                  <div style="font-family:'Inter',sans-serif;font-size:1.4rem;
                              font-weight:700;color:{color_emp};">{comp['ant_empresa']:.1f} años</div>
                </div>
                <div>
                  <div style="font-size:0.62rem;text-transform:uppercase;letter-spacing:0.08em;
                              color:{C['ink2']};margin-bottom:3px;">Tasa supervivencia cohorte</div>
                  <div style="font-family:'Inter',sans-serif;font-size:1.4rem;
                              font-weight:700;color:{tasa_color_c};">{tasa_c}%</div>
                  <div style="font-size:0.68rem;color:{C['ink2']};">{comp['activas_c']:,} de {comp['total_cohorte']:,} activas</div>
                </div>
                <div>
                  <div style="font-size:0.62rem;text-transform:uppercase;letter-spacing:0.08em;
                              color:{C['ink2']};margin-bottom:3px;">Vs. cohorte</div>
                  <div style="font-family:'Inter',sans-serif;font-size:1rem;
                              font-weight:600;color:{diff_color};">{diff_str}</div>
                </div>
              </div>

              <!-- Barra empresa -->
              <div style="font-size:0.62rem;text-transform:uppercase;letter-spacing:0.07em;
                          color:{C['ink2']};margin-bottom:4px;">Esta empresa · {comp['ant_empresa']:.1f} años</div>
              <div style="background:{C['border']};height:7px;border-radius:4px;overflow:hidden;margin-bottom:6px;">
                <div style="width:{pct_emp_bar}%;height:7px;background:{color_emp};border-radius:4px;"></div>
              </div>

              <!-- Barra cohorte -->
              <div style="font-size:0.62rem;text-transform:uppercase;letter-spacing:0.07em;
                          color:{C['ink2']};margin-bottom:4px;">% activas de su cohorte · {tasa_c}%</div>
              <div style="background:{C['border']};height:7px;border-radius:4px;overflow:hidden;">
                <div style="width:{pct_coh_bar}%;height:7px;background:{tasa_color_c};
                            border-radius:4px;opacity:0.7;"></div>
              </div>
              <div style="display:flex;justify-content:space-between;font-size:0.62rem;
                          color:{C['ink2']};margin-top:3px;"><span>0%</span><span>50%</span><span>100%</span></div>
            </div>
            """, unsafe_allow_html=True)

        # Gráfico de todas las cohortes juntas si hay más de 1 empresa
        if len(cohortes_comparadas) > 1:
            df_comp = pd.DataFrame(cohortes_comparadas)
            fig_comp = go.Figure()
            fig_comp.add_trace(go.Bar(
                name="Duración empresa (años)",
                x=df_comp["nombre"], y=df_comp["ant_empresa"],
                marker_color=C["accent"], text=df_comp["ant_empresa"].apply(lambda v: f"{v:.1f}a"),
                textposition="outside", textfont=dict(size=10, color=C["ink2"]),
            ))
            fig_comp.add_trace(go.Scatter(
                name="% activas de cohorte",
                x=df_comp["nombre"], y=df_comp["tasa_cohorte"],
                mode="markers+lines",
                marker=dict(size=10, color=C["gold"], symbol="diamond",
                            line=dict(color=C["bg"], width=2)),
                line=dict(color=C["gold"], width=1.5, dash="dot"),
                yaxis="y2",
                hovertemplate="Cohorte: %{y}%<extra></extra>",
            ))
            fig_comp.update_layout(
                **PLOT_BASE, height=320, barmode="group",
                title_text="Empresa vs. tasa de supervivencia de su cohorte",
                title_font=dict(color=C["ink"], size=13, family="Inter, sans-serif"),
                xaxis=dict(tickfont=dict(color=C["ink"], size=10), linecolor=C["border"]),
                yaxis=dict(title="Años en el mercado", gridcolor=C["border"],
                           linecolor=C["border"], tickfont=dict(color=C["ink2"])),
                yaxis2=dict(title="% activas cohorte", overlaying="y", side="right",
                            range=[0, 110], ticksuffix="%",
                            tickfont=dict(color=C["gold"]), showgrid=False),
                legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=C["ink2"], size=10),
                            orientation="h", x=0, y=1.12),
                margin=dict(t=60, b=30, l=60, r=60),
            )
            st.plotly_chart(fig_comp, use_container_width=True, config={"displayModeBar": False})
    else:
        st.info("No hay datos de cohorte disponibles para las empresas del perfil.")

    # ── 6. COMPORTAMIENTO DEL SECTOR EN EL AÑO DE MATRÍCULA ───────────────────
    section("⑥ El sector en el año de matrícula")

    st.markdown(
        f'<p style="font-size:0.8rem;color:{C["ink2"]};font-style:italic;margin-bottom:1rem;">'
        f'Cómo se comportó el sector CIIU de la empresa en el año en que se matriculó</p>',
        unsafe_allow_html=True,
    )

    for _, row in df2.iterrows():
        fi_s = parse_fecha(row.get("fecha_matricula"))
        ciiu_s = str(row.get("cod_ciiu_act_econ_pri") or "").strip()
        if not fi_s or not ciiu_s or ciiu_s in ("nan", "None", ""):
            continue
        sector_s = get_sector(ciiu_s)
        anio_s   = fi_s.year
        sd = sector_en_anio(ciiu_s, anio_s)
        if not sd:
            continue

        tasa_sec  = sd["tasa_sector"]
        tasa_nac  = sd["tasa_nacional"]
        total_sec = sd["total_sector"]
        act_sec   = sd["activas_sector"]
        tot_nac   = sd["total_nacional"]
        dif_sec   = round(tasa_sec - tasa_nac, 1)
        dif_color = C["green"] if dif_sec >= 0 else C["red"]
        dif_str   = f"+{dif_sec}pp vs. promedio nacional" if dif_sec >= 0 else f"{dif_sec}pp vs. promedio nacional"
        ts_color  = C["green"] if tasa_sec >= 50 else C["red"]
        tn_color  = C["green"] if tasa_nac >= 50 else C["red"]

        pct_sec_bar = min(100, int(tasa_sec))
        pct_nac_bar = min(100, int(tasa_nac))

        nombre_e = nombre_empresa(row)

        st.markdown(f"""
        <div style="background:{C['surface']};border:1px solid {C['border']};
                    padding:1.3rem 1.6rem;margin-bottom:1rem;">

          <div style="font-family:'Inter',sans-serif;font-size:0.95rem;font-weight:600;
                      color:{C['ink']};margin-bottom:0.3rem;">{nombre_e}</div>
          <div style="font-size:0.75rem;color:{C['ink2']};margin-bottom:1.1rem;">
            Sector: <b style="color:{C['ink']};">{sector_s}</b>
            &nbsp;·&nbsp; CIIU: <b>{ciiu_s}</b>
            &nbsp;·&nbsp; Año de matrícula: <b>{anio_s}</b>
          </div>

          <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:1.2rem;margin-bottom:1.2rem;">
            <div>
              <div style="font-size:0.62rem;text-transform:uppercase;letter-spacing:0.08em;
                          color:{C['ink2']};margin-bottom:3px;">Empresas del sector en {anio_s}</div>
              <div style="font-family:'Inter',sans-serif;font-size:1.4rem;
                          font-weight:700;color:{C['accent']};">{total_sec:,}</div>
              <div style="font-size:0.68rem;color:{C['ink2']};">{act_sec:,} siguen activas hoy</div>
            </div>
            <div>
              <div style="font-size:0.62rem;text-transform:uppercase;letter-spacing:0.08em;
                          color:{C['ink2']};margin-bottom:3px;">Supervivencia del sector</div>
              <div style="font-family:'Inter',sans-serif;font-size:1.4rem;
                          font-weight:700;color:{ts_color};">{tasa_sec}%</div>
            </div>
            <div>
              <div style="font-size:0.62rem;text-transform:uppercase;letter-spacing:0.08em;
                          color:{C['ink2']};margin-bottom:3px;">Promedio nacional {anio_s}</div>
              <div style="font-family:'Inter',sans-serif;font-size:1.4rem;
                          font-weight:700;color:{tn_color};">{tasa_nac}%</div>
              <div style="font-size:0.68rem;color:{dif_color};font-weight:600;">{dif_str}</div>
            </div>
          </div>

          <!-- Barra sector -->
          <div style="font-size:0.62rem;text-transform:uppercase;letter-spacing:0.07em;
                      color:{C['ink2']};margin-bottom:4px;">Supervivencia sector · {tasa_sec}%</div>
          <div style="background:{C['border']};height:7px;border-radius:4px;overflow:hidden;margin-bottom:6px;">
            <div style="width:{pct_sec_bar}%;height:7px;background:{ts_color};border-radius:4px;"></div>
          </div>

          <!-- Barra nacional -->
          <div style="font-size:0.62rem;text-transform:uppercase;letter-spacing:0.07em;
                      color:{C['ink2']};margin-bottom:4px;">Promedio nacional · {tasa_nac}%</div>
          <div style="background:{C['border']};height:7px;border-radius:4px;overflow:hidden;">
            <div style="width:{pct_nac_bar}%;height:7px;background:{tn_color};
                        border-radius:4px;opacity:0.55;"></div>
          </div>
          <div style="display:flex;justify-content:space-between;font-size:0.62rem;
                      color:{C['ink2']};margin-top:3px;"><span>0%</span><span>50%</span><span>100%</span></div>
        </div>
        """, unsafe_allow_html=True)

    # ── 7. DATOS COMPLETOS ─────────────────────────────────────────────────────
    section("⑦ Datos completos")

    col_def = ["razon_social","primer_nombre","primer_apellido","camara_comercio",
               "organizacion_juridica","estado_matricula","fecha_matricula",
               "fecha_cancelacion","cod_ciiu_act_econ_pri","matricula"]
    col_sel = st.multiselect("Columnas a mostrar", options=df.columns.tolist(),
                             default=[c for c in col_def if c in df.columns])
    if col_sel:
        st.dataframe(df[col_sel].reset_index(drop=True),
                     use_container_width=True, height=380)
        st.download_button(
            "↓ Descargar CSV",
            data=df[col_sel].to_csv(index=False).encode("utf-8"),
            file_name=f"empresas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    st.markdown("""
    <div class="rues-header">
        <div class="title">◈ &nbsp;Dashboard RUES · Análisis Empresarial</div>
        <div class="subtitle">Registro Único Empresarial · Universidad Santo Tomás · Colombia</div>
    </div>
    """, unsafe_allow_html=True)

    if get_connection() is None:
        st.error(f"No se encontró `{PARQUET_FILE}`. Ejecuta primero la conversión del CSV.")
        st.stop()

    st.markdown("""
    <div class="search-box">
        <h4>🔍 &nbsp;Consulta por número de identificación</h4>
        <p>Ingresa una cédula de ciudadanía o NIT para ver el perfil empresarial completo en el RUES.</p>
    </div>
    """, unsafe_allow_html=True)

    col_inp, col_btn = st.columns([5, 1])
    with col_inp:
        num_id = st.text_input("Número", placeholder="Ej: 79500000  ·  900123456",
                               label_visibility="collapsed", key="num_id")
    with col_btn:
        buscar = st.button("Buscar", type="primary", use_container_width=True)

    if buscar or (num_id and st.session_state.get("_last") != num_id):
        if not num_id.strip():
            st.warning("Ingresa un número de identificación.")
            return
        st.session_state["_last"] = num_id

        with st.spinner("Consultando el RUES…"):
            df = buscar_por_documento(num_id)

        if df.empty:
            st.markdown(f"""
            <div style="background:#1A1510;border:1px solid {C['border']};border-left:4px solid {C['accent2']};
                        padding:1.4rem 1.8rem;margin-top:1rem;">
              <div style="font-family:'Inter',sans-serif;font-size:1rem;font-weight:600;
                          color:{C['ink']};margin-bottom:0.5rem;">
                No encontramos empresas registradas
              </div>
              <div style="font-size:0.82rem;color:{C['ink2']};line-height:1.9;">
                No hay matrícula mercantil en el RUES para la cédula
                <b style="color:{C['ink']}">{num_id}</b>.<br>
                Esto puede deberse a alguna de estas razones:
                <ul style="margin-top:0.5rem;margin-bottom:0.5rem;padding-left:1.2rem;">
                  <li>Nunca has constituido una empresa o negocio formal en Colombia.</li>
                  <li>Tu actividad económica está registrada bajo un NIT diferente al de tu cédula.</li>
                  <li>El registro puede estar en proceso de actualización en la base del RUES.</li>
                </ul>
                Si crees que es un error, puedes verificar directamente en
                <b>www.rues.org.co</b> con tu número de identificación.
              </div>
            </div>
            """, unsafe_allow_html=True)
            return

        st.success(f"{len(df)} registro(s) encontrado(s) para **{num_id}**")
        st.markdown("---")
        render_perfil(df)

        # ── Acumular historial de sesión ──────────────────────────────────────
        if "historial" not in st.session_state:
            st.session_state["historial"] = pd.DataFrame()
        if "cedulas_consultadas" not in st.session_state:
            st.session_state["cedulas_consultadas"] = set()

        if num_id not in st.session_state["cedulas_consultadas"]:
            st.session_state["cedulas_consultadas"].add(num_id)
            df_tag = df.copy()
            df_tag.insert(0, "cedula_consultada", num_id)
            st.session_state["historial"] = pd.concat(
                [st.session_state["historial"], df_tag], ignore_index=True
            )

    # ── Historial de sesión ──────────────────────────────────────────────────
    hist = st.session_state.get("historial", pd.DataFrame())
    cedulas = st.session_state.get("cedulas_consultadas", set())
    if not hist.empty:
        st.markdown("---")
        st.markdown(
            f'<div class="section-title">📋 Historial de sesión</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'''<div style="background:{C["surface2"]};border:1px solid {C["border"]};
                        border-left:3px solid {C["gold"]};padding:0.75rem 1rem;
                        font-size:0.8rem;color:{C["ink2"]};margin-bottom:1rem;">
              📑 Historial de sesión: <b style="color:{C["ink"]}">{len(cedulas)}</b> cédula(s) consultada(s)
              · <b style="color:{C["ink"]}">{len(hist)}</b> registro(s) acumulados.
              El CSV descargable incluye todas las consultas de esta sesión.
            </div>''',
            unsafe_allow_html=True,
        )
        col_def_h = ["cedula_consultada","razon_social","primer_nombre","primer_apellido",
                     "camara_comercio","organizacion_juridica","estado_matricula",
                     "fecha_matricula","fecha_cancelacion","cod_ciiu_act_econ_pri","matricula"]
        cols_h = [c for c in col_def_h if c in hist.columns]
        st.dataframe(hist[cols_h].reset_index(drop=True), use_container_width=True, height=220)

        col_dl1, col_dl2, col_dl3 = st.columns([2, 2, 1])
        with col_dl1:
            st.download_button(
                "↓ Descargar historial completo (CSV)",
                data=hist[cols_h].to_csv(index=False).encode("utf-8"),
                file_name=f"historial_rues_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
            )
        with col_dl3:
            if st.button("🗑 Limpiar historial"):
                st.session_state["historial"] = pd.DataFrame()
                st.session_state["cedulas_consultadas"] = set()
                st.rerun()

    else:
        st.markdown("<br>", unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        guia = [
            ("01", "Ingresa tu número", "Cédula de ciudadanía o NIT en el campo de búsqueda."),
            ("02", "Consulta el RUES", "Se buscará en los 9 millones de registros del Registro Mercantil."),
            ("03", "Explora tu perfil", "Empresas, supervivencia, actividad económica y más."),
        ]
        for col, (num, tit, desc) in zip([c1, c2, c3], guia):
            with col:
                st.markdown(f"""
                <div class="metric-card" style="text-align:left">
                    <div style="font-family:'Inter',sans-serif;font-size:0.65rem;font-weight:700;
                                letter-spacing:0.15em;text-transform:uppercase;
                                color:{C['accent']};margin-bottom:.6rem;opacity:0.7">{num}</div>
                    <div style="font-weight:600;font-size:0.95rem;margin-bottom:.3rem;
                                color:{C['ink']}">{tit}</div>
                    <div style="font-size:.76rem;color:{C['ink2']};line-height:1.5">{desc}</div>
                </div>""", unsafe_allow_html=True)

        st.markdown(f"""
        <div style="text-align:center;color:{C['ink2']};font-size:.68rem;letter-spacing:.10em;
                    text-transform:uppercase;margin-top:3rem;border-top:1px solid {C['border']};
                    padding-top:1rem;font-family:Inter,sans-serif;">
            CONFECAMARAS &nbsp;·&nbsp; RUES &nbsp;·&nbsp; datos.gov.co &nbsp;·&nbsp; Actualización: Mayo 2026
        </div>""", unsafe_allow_html=True)

if __name__ == "__main__":
    main()