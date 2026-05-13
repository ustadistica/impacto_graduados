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
import json

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN DE PÁGINA
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="RUES · Análisis Empresarial",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ══════════════════════════════════════════════════════════════════════════════
# PALETA — beige editorial
# ══════════════════════════════════════════════════════════════════════════════
C = {
    "bg":      "#F5F0E8",
    "surface": "#EDE8DF",
    "border":  "#D6CEBD",
    "ink":     "#2C2417",
    "ink2":    "#7A6E5F",
    "accent":  "#8B5E3C",
    "accent2": "#C4874A",
    "green":   "#4A7C59",
    "red":     "#A04040",
    "blue":    "#3A5F7A",
    "gold":    "#C9A84C",
}

# ══════════════════════════════════════════════════════════════════════════════
# COORDENADAS DE CIUDADES COLOMBIANAS (lat, lon)
# ══════════════════════════════════════════════════════════════════════════════
CIUDADES_COL = {
    # Capitales de departamento y ciudades principales
    "bogota":            (4.7110,  -74.0721),
    "bogotá":            (4.7110,  -74.0721),
    "medellin":          (6.2442,  -75.5812),
    "medellín":          (6.2442,  -75.5812),
    "cali":              (3.4516,  -76.5320),
    "barranquilla":      (10.9685, -74.7813),
    "cartagena":         (10.3910, -75.4794),
    "cucuta":            (7.8939,  -72.5078),
    "cúcuta":            (7.8939,  -72.5078),
    "bucaramanga":       (7.1193,  -73.1227),
    "pereira":           (4.8133,  -75.6961),
    "manizales":         (5.0703,  -75.5138),
    "armenia":           (4.5339,  -75.6811),
    "ibague":            (4.4389,  -75.2322),
    "ibagué":            (4.4389,  -75.2322),
    "villavicencio":     (4.1420,  -73.6266),
    "neiva":             (2.9273,  -75.2819),
    "santa marta":       (11.2408, -74.1990),
    "pasto":             (1.2136,  -77.2811),
    "monteria":          (8.7575,  -75.8875),
    "montería":          (8.7575,  -75.8875),
    "sincelejo":         (9.3047,  -75.3978),
    "valledupar":        (10.4631, -73.2532),
    "riohacha":          (11.5444, -72.9072),
    "quibdo":            (5.6919,  -76.6583),
    "quibdó":            (5.6919,  -76.6583),
    "popayan":           (2.4419,  -76.6071),
    "popayán":           (2.4419,  -76.6071),
    "florencia":         (1.6144,  -75.6062),
    "mocoa":             (1.1523,  -76.6483),
    "yopal":             (5.3378,  -72.3959),
    "arauca":            (7.0875,  -70.7592),
    "san jose del guaviare": (2.5668, -72.6406),
    "mitu":              (1.1983,  -70.1736),
    "mitú":              (1.1983,  -70.1736),
    "puerto carreno":    (6.1894,  -67.4842),
    "puerto carreño":    (6.1894,  -67.4842),
    "leticia":           (-4.2153, -69.9406),
    "inirida":           (3.8653,  -67.9239),
    "inírida":           (3.8653,  -67.9239),
    "tunja":             (5.5353,  -73.3678),
    "manizales":         (5.0703,  -75.5138),
    "buenaventura":      (3.8831,  -77.0311),
    "palmira":           (3.5394,  -76.3036),
    "bello":             (6.3372,  -75.5578),
    "soledad":           (10.9175, -74.7667),
    "soacha":            (4.5797,  -74.2170),
    "itagui":            (6.1847,  -75.5990),
    "dosquebradas":      (4.8398,  -75.6611),
    "floridablanca":     (7.0649,  -73.0876),
    "giron":             (7.0731,  -73.1697),
    "girón":             (7.0731,  -73.1697),
    "envigado":          (6.1753,  -75.5920),
    # Cámaras de comercio
    "camara de comercio de bogota":         (4.7110,  -74.0721),
    "camara de comercio de medellin":       (6.2442,  -75.5812),
    "camara de comercio de cali":           (3.4516,  -76.5320),
    "camara de comercio de armenia":        (4.5339,  -75.6811),
    "camara de comercio del quindio":       (4.5339,  -75.6811),
    "camara de comercio de pereira":        (4.8133,  -75.6961),
    "camara de comercio de manizales":      (5.0703,  -75.5138),
    "camara de comercio de barranquilla":   (10.9685, -74.7813),
    "camara de comercio de cartagena":      (10.3910, -75.4794),
    "camara de comercio de bucaramanga":    (7.1193,  -73.1227),
    "camara de comercio de cucuta":         (7.8939,  -72.5078),
    "camara de comercio de ibague":         (4.4389,  -75.2322),
    "camara de comercio de neiva":          (2.9273,  -75.2819),
    "camara de comercio de pasto":          (1.2136,  -77.2811),
    "camara de comercio de santa marta":    (11.2408, -74.1990),
    "camara de comercio de monteria":       (8.7575,  -75.8875),
    "camara de comercio de sincelejo":      (9.3047,  -75.3978),
    "camara de comercio de villavicencio":  (4.1420,  -73.6266),
    "camara de comercio de valledupar":     (10.4631, -73.2532),
    "camara de comercio de popayan":        (2.4419,  -76.6071),
    "camara de comercio de tunja":          (5.5353,  -73.3678),
}


def geocode_camara(camara: str):
    """Devuelve (lat, lon) o None para una cámara de comercio / ciudad."""
    if not camara:
        return None
    key = camara.lower().strip()
    # Coincidencia exacta
    if key in CIUDADES_COL:
        return CIUDADES_COL[key]
    # Búsqueda parcial
    for k, v in CIUDADES_COL.items():
        if k in key or key in k:
            return v
    return None


# ══════════════════════════════════════════════════════════════════════════════
# CSS — minimalista beige
# ══════════════════════════════════════════════════════════════════════════════
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700&family=Lora:wght@400;500&display=swap');

html, body, [class*="css"] {{
    font-family: 'Lora', Georgia, serif;
    background-color: {C["bg"]};
    color: {C["ink"]};
}}
.stApp {{ background: {C["bg"]}; }}

/* Header */
.rues-header {{
    padding: 2.5rem 0 1.5rem 0;
    border-bottom: 1.5px solid {C["border"]};
    margin-bottom: 2rem;
}}
.rues-header .title {{
    font-family: 'Playfair Display', Georgia, serif;
    font-size: 2.2rem; font-weight: 700;
    color: {C["ink"]}; letter-spacing: -0.02em; margin: 0;
}}
.rues-header .subtitle {{
    font-size: 0.78rem; color: {C["ink2"]};
    letter-spacing: 0.12em; text-transform: uppercase; margin-top: 0.4rem;
}}

/* Métrica */
.metric-card {{
    background: {C["surface"]}; border: 1px solid {C["border"]};
    border-top: 3px solid {C["accent"]}; padding: 1.2rem 1.4rem;
    transition: border-top-color .2s;
}}
.metric-card:hover {{ border-top-color: {C["accent2"]}; }}
.metric-card .val {{
    font-family: 'Playfair Display', serif; font-size: 1.9rem;
    font-weight: 700; color: {C["accent"]}; line-height: 1;
}}
.metric-card .lbl {{
    font-size: 0.68rem; text-transform: uppercase;
    letter-spacing: 0.10em; color: {C["ink2"]}; margin-top: 0.35rem;
}}
.metric-card .sub {{
    font-size: 0.76rem; color: {C["ink2"]}; margin-top: 0.2rem; font-style: italic;
}}

/* Empresa card */
.emp-card {{
    background: {C["surface"]}; border: 1px solid {C["border"]};
    border-left: 3px solid {C["accent"]}; padding: 1.1rem 1.4rem; margin-bottom: 0.9rem;
}}
.emp-card.activa    {{ border-left-color: {C["green"]}; }}
.emp-card.cancelada {{ border-left-color: {C["red"]}; }}
.emp-nombre {{
    font-family: 'Playfair Display', serif; font-size: 1rem;
    font-weight: 600; color: {C["ink"]};
}}
.emp-meta {{ font-size: 0.79rem; color: {C["ink2"]}; margin-top: 0.45rem; line-height: 1.7; }}

/* Badges */
.badge {{
    display: inline-block; padding: .15rem .55rem;
    font-size: .66rem; font-weight: 600;
    letter-spacing: .07em; text-transform: uppercase;
    border: 1px solid currentColor;
}}
.badge-activa    {{ color: {C["green"]}; }}
.badge-cancelada {{ color: {C["red"]}; }}
.badge-otro      {{ color: {C["blue"]}; }}

/* Search box */
.search-box {{
    background: {C["surface"]}; border: 1px solid {C["border"]};
    padding: 1.8rem 2rem; margin-bottom: 2rem;
}}
.search-box h4 {{
    font-family: 'Playfair Display', serif; font-size: 1.05rem;
    color: {C["ink"]}; margin: 0 0 .3rem 0;
}}
.search-box p {{
    font-size: .79rem; color: {C["ink2"]}; margin: 0 0 1rem 0; font-style: italic;
}}

/* Input */
div[data-testid="stTextInput"] input {{
    background: {C["bg"]} !important; border: 1px solid {C["border"]} !important;
    border-radius: 0 !important; color: {C["ink"]} !important;
    font-family: 'Lora', serif !important; font-size: .95rem !important;
    box-shadow: none !important;
}}
div[data-testid="stTextInput"] input:focus {{
    border-color: {C["accent"]} !important; box-shadow: none !important;
}}

/* Botón */
.stButton > button {{
    background: {C["accent"]} !important; color: {C["bg"]} !important;
    border: none !important; border-radius: 0 !important;
    font-family: 'Lora', serif !important; font-size: .82rem !important;
    font-weight: 500 !important; letter-spacing: .06em !important;
    text-transform: uppercase !important; padding: .55rem 1.8rem !important;
    transition: background .2s !important;
}}
.stButton > button:hover {{ background: {C["accent2"]} !important; }}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {{
    background: transparent !important;
    border-bottom: 1.5px solid {C["border"]} !important; gap: 0 !important;
}}
.stTabs [data-baseweb="tab"] {{
    background: transparent !important; color: {C["ink2"]} !important;
    font-family: 'Lora', serif !important; font-size: .80rem !important;
    letter-spacing: .07em !important; text-transform: uppercase !important;
    border-radius: 0 !important; padding: .55rem 1.2rem !important;
    border-bottom: 2px solid transparent !important;
}}
.stTabs [aria-selected="true"] {{
    color: {C["accent"]} !important;
    border-bottom: 2px solid {C["accent"]} !important;
    background: transparent !important;
}}

/* Select */
div[data-baseweb="select"] > div {{
    background: {C["bg"]} !important; border-color: {C["border"]} !important;
    border-radius: 0 !important; color: {C["ink"]} !important;
}}

/* Misc */
.stDataFrame  {{ border: 1px solid {C["border"]}; }}
.stAlert      {{ border-radius: 0 !important; border-left-width: 3px !important; }}
hr            {{ border-color: {C["border"]} !important; margin: 1.5rem 0 !important; }}
::-webkit-scrollbar       {{ width: 5px; }}
::-webkit-scrollbar-track {{ background: {C["bg"]}; }}
::-webkit-scrollbar-thumb {{ background: {C["border"]}; }}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTES
# ══════════════════════════════════════════════════════════════════════════════
PARQUET_FILE = "data/rues_data.parquet"


# ══════════════════════════════════════════════════════════════════════════════
# DATOS
# ══════════════════════════════════════════════════════════════════════════════
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
    if rs:
        return rs.title()
    partes = [str(row.get("primer_nombre") or ""),
              str(row.get("primer_apellido") or "")]
    return " ".join(p for p in partes if p.strip()).strip().title() or "Sin nombre"


def badge_estado(estado: str) -> str:
    e = str(estado or "").upper()
    if "ACTIV" in e or "VIGENT" in e:
        return '<span class="badge badge-activa">● Activa</span>'
    if "CANCEL" in e or "DISUEL" in e or "LIQUID" in e:
        return f'<span class="badge badge-cancelada">✕ {estado}</span>'
    return f'<span class="badge badge-otro">{estado or "—"}</span>'


# ══════════════════════════════════════════════════════════════════════════════
# GRÁFICOS MEJORADOS
# ══════════════════════════════════════════════════════════════════════════════

def fig_supervivencia(df: pd.DataFrame) -> go.Figure:
    """Dona elegante con anillos concéntricos y anotación central."""
    e = df["estado_matricula"].fillna("").str.upper()
    activas    = e.str.contains("ACTIV|VIGENT").sum()
    canceladas = e.str.contains("CANCEL|DISUEL|LIQUID").sum()
    otras      = len(df) - activas - canceladas

    data = [(l, v, c) for l, v, c in [
        ("Activas",              activas,    C["green"]),
        ("Canceladas/Disueltas", canceladas, C["red"]),
        ("Otro estado",          otras,      C["ink2"]),
    ] if v > 0]

    if not data:
        return go.Figure()

    labels, values, colors = zip(*data)
    tasa = round(activas / len(df) * 100, 1) if len(df) else 0

    fig = go.Figure()

    # Anillo exterior (datos reales)
    fig.add_trace(go.Pie(
        labels=labels,
        values=values,
        marker=dict(
            colors=list(colors),
            line=dict(color=C["bg"], width=3),
        ),
        hole=0.65,
        textfont=dict(size=11, family="Lora, serif"),
        textinfo="label+percent",
        hovertemplate="<b>%{label}</b><br>%{value} empresas (%{percent})<extra></extra>",
        sort=False,
        direction="clockwise",
        pull=[0.04 if v == max(values) else 0 for v in values],
    ))

    fig.update_layout(
        paper_bgcolor=C["surface"],
        plot_bgcolor=C["bg"],
        font=dict(family="Georgia, serif", color=C["ink2"]),
        title_text="Tasa de supervivencia empresarial",
        title_font=dict(color=C["ink"], size=15, family="Playfair Display, serif"),
        showlegend=True,
        legend=dict(
            bgcolor=C["surface"], bordercolor=C["border"], borderwidth=1,
            font=dict(color=C["ink2"], size=11),
            orientation="h", x=0.5, xanchor="center", y=-0.1,
        ),
        margin=dict(t=60, b=60, l=20, r=20),
        height=340,
        annotations=[
            dict(
                text=f"<b>{tasa}%</b><br><span style='font-size:10px'>activas</span>",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=20, color=C["accent"], family="Playfair Display, serif"),
                align="center",
            )
        ],
    )
    return fig


def fig_tipo_organizacion(df: pd.DataFrame) -> go.Figure:
    """Barras horizontales con degradado y etiquetas elegantes."""
    orgs = (df["organizacion_juridica"]
            .fillna("Sin información")
            .value_counts()
            .head(8)
            .reset_index())
    orgs.columns = ["org", "n"]
    orgs = orgs.sort_values("n", ascending=True)

    # Colores progresivos desde beige a acento
    n = len(orgs)
    colors = [
        f"rgba(139,94,60,{0.25 + 0.75 * i / max(n - 1, 1)})"
        for i in range(n)
    ]

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=orgs["n"],
        y=orgs["org"],
        orientation="h",
        marker=dict(
            color=colors,
            line=dict(width=0),
        ),
        text=orgs["n"],
        textposition="outside",
        textfont=dict(color=C["ink2"], size=11, family="Lora, serif"),
        hovertemplate="<b>%{y}</b><br>%{x} registros<extra></extra>",
    ))

    fig.update_layout(
        paper_bgcolor=C["surface"],
        plot_bgcolor=C["bg"],
        font=dict(family="Georgia, serif", color=C["ink2"]),
        title_text="Tipo de organización jurídica",
        title_font=dict(color=C["ink"], size=15, family="Playfair Display, serif"),
        xaxis=dict(
            showgrid=True,
            gridcolor=C["border"],
            linecolor=C["border"],
            tickfont=dict(color=C["ink2"], size=10),
            zeroline=False,
        ),
        yaxis=dict(
            showgrid=False,
            linecolor=C["border"],
            tickfont=dict(color=C["ink"], size=11),
        ),
        margin=dict(t=56, b=30, l=160, r=60),
        height=max(260, n * 42 + 80),
        bargap=0.3,
    )
    return fig


def fig_timeline(df: pd.DataFrame) -> go.Figure:
    """Línea de tiempo con área rellena, puntos anotados y estética editorial."""
    df2 = df.copy()
    df2["f"] = df2["fecha_matricula"].apply(parse_fecha)
    df2 = df2.dropna(subset=["f"])
    df2["anio"] = df2["f"].apply(lambda d: d.year)
    conteo = df2.groupby("anio").size().reset_index(name="n").sort_values("anio")

    if conteo.empty:
        return go.Figure()

    # Año pico
    idx_max = conteo["n"].idxmax()
    anio_pico = conteo.loc[idx_max, "anio"]
    n_pico = conteo.loc[idx_max, "n"]

    fig = go.Figure()

    # Área de fondo suave
    fig.add_trace(go.Scatter(
        x=conteo["anio"],
        y=conteo["n"],
        mode="none",
        fill="tozeroy",
        fillcolor=f"rgba(139,94,60,0.10)",
        showlegend=False,
        hoverinfo="skip",
    ))

    # Línea principal
    fig.add_trace(go.Scatter(
        x=conteo["anio"],
        y=conteo["n"],
        mode="lines+markers",
        line=dict(color=C["accent"], width=2.5, shape="spline", smoothing=0.8),
        marker=dict(
            size=8, color=C["bg"],
            line=dict(color=C["accent"], width=2),
        ),
        hovertemplate="<b>%{x}</b><br>%{y} matrículas<extra></extra>",
        showlegend=False,
    ))

    # Anotación del pico
    fig.add_annotation(
        x=anio_pico, y=n_pico,
        text=f"Pico: {n_pico}",
        showarrow=True, arrowhead=2,
        arrowcolor=C["accent2"],
        font=dict(color=C["accent"], size=11, family="Lora, serif"),
        bgcolor=C["surface"],
        bordercolor=C["border"],
        borderwidth=1,
        ay=-36,
    )

    fig.update_layout(
        paper_bgcolor=C["surface"],
        plot_bgcolor=C["bg"],
        font=dict(family="Georgia, serif", color=C["ink2"]),
        title_text="Matrículas por año",
        title_font=dict(color=C["ink"], size=15, family="Playfair Display, serif"),
        xaxis=dict(
            title="Año", gridcolor=C["border"],
            linecolor=C["border"], tickfont=dict(color=C["ink2"]),
            showspikes=True, spikecolor=C["border"],
            tickmode="linear" if len(conteo) < 30 else "auto",
        ),
        yaxis=dict(
            title="Empresas registradas",
            gridcolor=C["border"], linecolor=C["border"],
            tickfont=dict(color=C["ink2"]),
            zeroline=True, zerolinecolor=C["border"],
        ),
        hovermode="x unified",
        margin=dict(t=60, b=48, l=50, r=30),
        height=300,
    )
    return fig


def fig_ciiu(df: pd.DataFrame) -> go.Figure:
    """Barras horizontales CIIU con escala de color café-dorado y etiquetas."""
    ciiu = (df["cod_ciiu_act_econ_pri"]
            .fillna("Sin código")
            .value_counts()
            .head(10)
            .reset_index())
    ciiu.columns = ["ciiu", "n"]
    ciiu = ciiu.sort_values("n", ascending=True)

    fig = go.Figure(go.Bar(
        x=ciiu["n"],
        y=ciiu["ciiu"].astype(str),
        orientation="h",
        marker=dict(
            color=ciiu["n"],
            colorscale=[
                [0.0, C["surface"]],
                [0.4, "#C4874A"],
                [1.0, "#6B3A1F"],
            ],
            showscale=True,
            colorbar=dict(
                thickness=10,
                len=0.6,
                bgcolor=C["surface"],
                bordercolor=C["border"],
                tickfont=dict(color=C["ink2"], size=9),
                outlinecolor=C["border"],
            ),
            line=dict(width=0),
        ),
        text=ciiu["n"],
        textposition="outside",
        textfont=dict(color=C["ink2"], size=10),
        hovertemplate="<b>CIIU %{y}</b><br>%{x} empresas<extra></extra>",
    ))

    fig.update_layout(
        paper_bgcolor=C["surface"],
        plot_bgcolor=C["bg"],
        font=dict(family="Georgia, serif", color=C["ink2"]),
        title_text="Top 10 actividades económicas (CIIU)",
        title_font=dict(color=C["ink"], size=15, family="Playfair Display, serif"),
        xaxis=dict(
            title="Número de empresas",
            gridcolor=C["border"], linecolor=C["border"],
            tickfont=dict(color=C["ink2"]),
        ),
        yaxis=dict(
            gridcolor="rgba(0,0,0,0)", linecolor=C["border"],
            tickfont=dict(color=C["ink"], size=11),
        ),
        margin=dict(t=56, b=40, l=80, r=80),
        height=max(320, len(ciiu) * 38 + 100),
        bargap=0.28,
    )
    return fig


def fig_camaras_mapa(df: pd.DataFrame) -> go.Figure:
    """Mapa scatter de Colombia mostrando ubicación de cámaras de comercio."""
    cams = (df["camara_comercio"]
            .fillna("Sin información")
            .value_counts()
            .reset_index())
    cams.columns = ["camara", "n"]

    lats, lons, labels, sizes, colors_list = [], [], [], [], []

    for _, row in cams.iterrows():
        coords = geocode_camara(row["camara"])
        if coords:
            lats.append(coords[0])
            lons.append(coords[1])
            labels.append(row["camara"])
            sizes.append(max(12, min(50, row["n"] * 3 + 12)))
            colors_list.append(row["n"])

    if not lats:
        return fig_camaras_barras(df)

    fig = go.Figure()

    # Scatter sobre mapa
    fig.add_trace(go.Scattergeo(
        lat=lats,
        lon=lons,
        text=labels,
        customdata=[[n] for n in [cams.loc[cams["camara"] == l, "n"].values[0] for l in labels]],
        mode="markers+text",
        textposition="top center",
        textfont=dict(size=9, color=C["ink"], family="Lora, serif"),
        marker=dict(
            size=sizes,
            color=colors_list,
            colorscale=[
                [0.0, "#D6CEBD"],
                [0.5, C["accent2"]],
                [1.0, C["accent"]],
            ],
            showscale=True,
            colorbar=dict(
                title=dict(text="Empresas", font=dict(color=C["ink2"], size=10)),
                thickness=10,
                len=0.5,
                bgcolor=C["surface"],
                bordercolor=C["border"],
                tickfont=dict(color=C["ink2"], size=9),
            ),
            line=dict(color=C["bg"], width=1.5),
            opacity=0.85,
        ),
        hovertemplate="<b>%{text}</b><br>%{customdata[0]} empresas<extra></extra>",
    ))

    fig.update_layout(
        paper_bgcolor=C["surface"],
        geo=dict(
            scope="south america",
            showland=True,
            landcolor="#EDE8DF",
            showocean=True,
            oceancolor="#D6CEBD",
            showcountries=True,
            countrycolor=C["border"],
            showcoastlines=True,
            coastlinecolor=C["border"],
            showrivers=True,
            rivercolor="#C8D8E8",
            showlakes=True,
            lakecolor="#C8D8E8",
            center=dict(lat=4.5, lon=-74.0),
            projection_scale=8,
            lonaxis=dict(range=[-82, -66]),
            lataxis=dict(range=[-5, 14]),
            bgcolor=C["surface"],
        ),
        font=dict(family="Georgia, serif", color=C["ink2"]),
        title_text="Distribución geográfica por cámara de comercio",
        title_font=dict(color=C["ink"], size=15, family="Playfair Display, serif"),
        margin=dict(t=56, b=10, l=10, r=10),
        height=480,
    )
    return fig


def fig_camaras_barras(df: pd.DataFrame) -> go.Figure:
    """Fallback: barras horizontales de cámaras de comercio."""
    cams = (df["camara_comercio"]
            .fillna("Sin información")
            .value_counts()
            .head(12)
            .reset_index())
    cams.columns = ["camara", "n"]
    cams = cams.sort_values("n", ascending=True)

    n = len(cams)
    colors = [f"rgba(58,95,122,{0.3 + 0.7 * i / max(n-1, 1)})" for i in range(n)]

    fig = go.Figure(go.Bar(
        x=cams["n"], y=cams["camara"],
        orientation="h",
        marker=dict(color=colors, line=dict(width=0)),
        text=cams["n"], textposition="outside",
        textfont=dict(color=C["ink2"], size=10),
        hovertemplate="<b>%{y}</b><br>%{x} empresas<extra></extra>",
    ))
    fig.update_layout(
        paper_bgcolor=C["surface"], plot_bgcolor=C["bg"],
        font=dict(family="Georgia, serif", color=C["ink2"]),
        title_text="Cámaras de comercio",
        title_font=dict(color=C["ink"], size=15, family="Playfair Display, serif"),
        xaxis=dict(gridcolor=C["border"], linecolor=C["border"], tickfont=dict(color=C["ink2"])),
        yaxis=dict(gridcolor="rgba(0,0,0,0)", linecolor=C["border"], tickfont=dict(color=C["ink"], size=10)),
        margin=dict(t=56, b=30, l=200, r=60),
        height=max(320, n * 38 + 80),
        bargap=0.28,
    )
    return fig


def fig_heatmap(df: pd.DataFrame):
    """Heatmap cámara × estado con escala editorial."""
    pivot = pd.crosstab(
        df["camara_comercio"].fillna("Sin info"),
        df["estado_matricula"].fillna("Sin info"),
    )

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=[
            [0.0, C["surface"]],
            [0.3, "#D6B896"],
            [0.7, C["accent2"]],
            [1.0, C["accent"]],
        ],
        hovertemplate="<b>%{y}</b><br>Estado: %{x}<br>%{z} registros<extra></extra>",
        showscale=True,
        colorbar=dict(
            thickness=10,
            bgcolor=C["surface"],
            bordercolor=C["border"],
            tickfont=dict(color=C["ink2"], size=9),
        ),
        xgap=2, ygap=2,
    ))

    fig.update_layout(
        paper_bgcolor=C["surface"],
        plot_bgcolor=C["bg"],
        font=dict(family="Georgia, serif", color=C["ink2"]),
        title_text="Cámara de comercio × Estado de matrícula",
        title_font=dict(color=C["ink"], size=15, family="Playfair Display, serif"),
        xaxis=dict(
            tickangle=-30, side="bottom",
            tickfont=dict(color=C["ink2"], size=10),
        ),
        yaxis=dict(tickfont=dict(color=C["ink"], size=10)),
        margin=dict(t=60, b=80, l=200, r=30),
        height=max(360, len(pivot) * 28 + 120),
    )
    return fig


def fig_renovaciones(df: pd.DataFrame):
    """Barras de renovaciones con línea de tendencia."""
    df2 = df.dropna(subset=["ultimo_ano_renovado"]).copy()
    df2["anio"] = pd.to_numeric(df2["ultimo_ano_renovado"], errors="coerce")
    df2 = df2.dropna(subset=["anio"])
    if df2.empty:
        return None

    conteo = df2["anio"].astype(int).value_counts().sort_index().reset_index()
    conteo.columns = ["anio", "n"]

    fig = go.Figure()

    # Barras
    fig.add_trace(go.Bar(
        x=conteo["anio"], y=conteo["n"],
        name="Renovaciones",
        marker=dict(
            color=conteo["n"],
            colorscale=[[0, "#C8D8B8"], [1, C["green"]]],
            line=dict(width=0),
        ),
        hovertemplate="<b>%{x}</b><br>%{y} renovaciones<extra></extra>",
    ))

    # Línea suavizada de tendencia
    if len(conteo) > 2:
        fig.add_trace(go.Scatter(
            x=conteo["anio"], y=conteo["n"].rolling(2, min_periods=1).mean(),
            mode="lines",
            line=dict(color=C["accent"], width=2, dash="dot"),
            name="Tendencia",
            hoverinfo="skip",
        ))

    fig.update_layout(
        paper_bgcolor=C["surface"],
        plot_bgcolor=C["bg"],
        font=dict(family="Georgia, serif", color=C["ink2"]),
        title_text="Último año de renovación",
        title_font=dict(color=C["ink"], size=15, family="Playfair Display, serif"),
        xaxis=dict(
            title="Año", gridcolor=C["border"], linecolor=C["border"],
            tickfont=dict(color=C["ink2"]),
        ),
        yaxis=dict(
            title="Renovaciones", gridcolor=C["border"], linecolor=C["border"],
            tickfont=dict(color=C["ink2"]),
        ),
        legend=dict(
            bgcolor=C["surface"], bordercolor=C["border"], borderwidth=1,
            font=dict(color=C["ink2"], size=10),
        ),
        margin=dict(t=60, b=40, l=50, r=20),
        height=280,
        bargap=0.2,
    )
    return fig


def render_timeline_empresas(df):
    df2 = df.copy()
    df2["inicio"] = df2["fecha_matricula"].apply(parse_fecha)
    hoy = date.today()

    def _fin(row):
        f = parse_fecha(row.get("fecha_cancelacion"))
        return f if f else hoy

    df2["fin"] = df2.apply(_fin, axis=1)
    df2 = df2.dropna(subset=["inicio"]).sort_values("inicio")

    if df2.empty:
        st.info("No hay fechas de matricula disponibles para la linea de tiempo.")
        return

    max_dias = max((df2["fin"] - df2["inicio"]).apply(lambda x: x.days).max(), 1)
    n = len(df2)

    PD   = "Playfair Display"
    LR   = "Lora"
    BG   = C["bg"]
    SRF  = C["surface"]
    BRD  = C["border"]
    INK  = C["ink"]
    INK2 = C["ink2"]
    ACC  = C["accent"]
    GRN  = C["green"]
    RED  = C["red"]
    BLU  = C["blue"]

    def sp(k, v):
        return k + ":" + v + ";"

    html = []
    html.append("<div style='" + sp("background", SRF) + sp("border", "1px solid " + BRD)
                + sp("padding", "1.4rem 1.6rem") + sp("margin-top", "1rem") + "'>")
    html.append("<div style='" + sp("font-family", PD + ",serif") + sp("font-size", "1rem")
                + sp("font-weight", "600") + sp("color", INK) + sp("margin-bottom", "1rem")
                + sp("border-bottom", "1px solid " + BRD) + sp("padding-bottom", "0.5rem")
                + "'>Linea de tiempo empresarial</div>")

    for i, (_, row) in enumerate(df2.iterrows()):
        nombre  = nombre_empresa(row)[:50]
        estado  = str(row.get("estado_matricula") or "")
        inicio  = row["inicio"]
        fin     = row["fin"]
        d_dias  = (fin - inicio).days
        d_str   = antiguedad_str(d_dias / 365.25)
        camara  = str(row.get("camara_comercio") or "-")
        ciiu    = str(row.get("cod_ciiu_act_econ_pri") or "-")
        f_can   = str(row.get("fecha_cancelacion") or "")
        activa  = "ACTIV" in estado.upper() or "VIGENT" in estado.upper()
        cancel  = "CANCEL" in estado.upper() or "DISUEL" in estado.upper()

        dot  = GRN  if activa else (RED  if cancel else BLU)
        tc   = GRN  if activa else (RED  if cancel else INK2)
        tbg  = "#E8F5EC" if activa else ("#F5E8E8" if cancel else "#EEEAE4")
        tlbl = "Activa" if activa else estado.title()
        pct  = min(100, int(d_dias / max_dias * 100))
        last = (i == n - 1)

        line_bg = "linear-gradient(to bottom," + BRD + ",transparent)" if last else BRD

        fcan_html = ""
        fc = f_can.strip()
        if fc and fc not in ("nan", "None"):
            fcan_html = ("<div style='" + sp("margin-top","3px") + sp("font-size","0.73rem")
                        + sp("color", RED) + "'>"
                        + "Cancelada: <b>" + fc[:10] + "</b></div>")

        # outer row
        html.append("<div style='" + sp("display","flex") + sp("gap","0")
                    + sp("align-items","stretch") + "'>")

        # left: year + line
        html.append(
            "<div style='" + sp("width","62px") + sp("min-width","62px")
            + sp("display","flex") + sp("flex-direction","column")
            + sp("align-items","center") + "'>"
            + "<div style='" + sp("font-family", PD + ",serif") + sp("font-size","0.82rem")
            + sp("font-weight","700") + sp("color", ACC)
            + sp("line-height","1") + sp("margin-top","16px") + sp("white-space","nowrap")
            + "'>" + str(inicio.year) + "</div>"
            + "<div style='" + sp("width","2px") + sp("background", line_bg)
            + sp("flex","1") + sp("margin-top","5px") + sp("min-height","20px") + "'></div>"
            + "</div>"
        )

        # dot circle
        html.append(
            "<div style='" + sp("display","flex") + sp("align-items","flex-start")
            + sp("padding-top","16px") + sp("margin","0 -7px") + sp("z-index","2") + "'>"
            + "<div style='" + sp("width","13px") + sp("height","13px")
            + sp("border-radius","50%") + sp("background", dot)
            + "border:2.5px solid " + BG + ";"
            + "box-shadow:0 0 0 2px " + dot + "55;"
            + sp("flex-shrink","0") + "'></div></div>"
        )

        # content block
        html.append("<div style='" + sp("flex","1") + sp("padding","12px 0 22px 16px") + "'>")

        # name + badge
        html.append(
            "<div style='" + sp("display","flex") + sp("align-items","center")
            + sp("gap","8px") + sp("flex-wrap","wrap") + "'>"
            + "<span style='" + sp("font-family", PD + ",serif") + sp("font-size","0.96rem")
            + sp("font-weight","600") + sp("color", INK) + "'>" + nombre + "</span>"
            + "<span style='" + sp("font-size","0.64rem") + sp("font-weight","600")
            + sp("letter-spacing","0.07em") + sp("text-transform","uppercase")
            + sp("color", tc) + sp("background", tbg)
            + "border:1px solid " + tc + "40;"
            + sp("padding","2px 7px") + "'>" + tlbl + "</span>"
            + "</div>"
        )

        # meta info
        html.append(
            "<div style='" + sp("font-size","0.74rem") + sp("color", INK2)
            + sp("margin-top","4px") + sp("line-height","1.85") + "'>"
            + "Inscrita: <b>" + str(inicio) + "</b> &nbsp;·&nbsp; "
            + "Duracion: <b>" + d_str + "</b> &nbsp;·&nbsp; "
            + camara + fcan_html
            + "<span style='" + sp("font-size","0.71rem") + "'>CIIU: <b>" + ciiu + "</b></span>"
            + "</div>"
        )

        # progress bar
        html.append(
            "<div style='" + sp("margin-top","7px") + sp("height","3px")
            + sp("background", BRD) + sp("border-radius","2px") + sp("max-width","300px") + "'>"
            + "<div style='" + sp("height","3px") + "width:" + str(pct) + "%;"
            + sp("background", dot) + sp("border-radius","2px") + sp("opacity","0.65") + "'></div>"
            + "</div>"
            + "<div style='" + sp("font-size","0.64rem") + sp("color", INK2)
            + sp("margin-top","2px") + "'>"
            + "{:,}".format(d_dias) + " dias de actividad</div>"
        )

        html.append("</div>")  # content
        html.append("</div>")  # row

    html.append("</div>")  # outer card
    st.markdown("".join(html), unsafe_allow_html=True)


def fig_mapa_empresa(camara: str):
    """Mapa de Colombia completo con punto marcado para la empresa."""
    coords = geocode_camara(camara)
    if not coords:
        return None

    lat, lon = coords

    # Extraer nombre de ciudad legible
    ciudad = camara.title()
    for k, v in CIUDADES_COL.items():
        if v == coords and not k.startswith("camara"):
            ciudad = k.title()
            break

    fig = go.Figure()

    fig.add_trace(go.Scattergeo(
        lat=[lat],
        lon=[lon],
        text=[ciudad],
        mode="markers+text",
        textposition="top right",
        textfont=dict(size=11, color=C["ink"], family="Playfair Display, serif"),
        marker=dict(
            size=14,
            color=C["accent"],
            line=dict(color=C["bg"], width=2.5),
            symbol="circle",
        ),
        hovertemplate=f"<b>{camara.title()}</b><extra></extra>",
    ))

    fig.update_layout(
        paper_bgcolor=C["surface"],
        geo=dict(
            scope="south america",
            showland=True,
            landcolor="#EDE8DF",
            showocean=True,
            oceancolor="#D0E0EC",
            showcountries=True,
            countrycolor=C["border"],
            showcoastlines=True,
            coastlinecolor=C["border"],
            showrivers=True,
            rivercolor="#C8D8E8",
            showlakes=True,
            lakecolor="#C8D8E8",
            center=dict(lat=4.5, lon=-74.0),
            lonaxis=dict(range=[-82, -66]),
            lataxis=dict(range=[-5, 14]),
            projection_scale=6,
            bgcolor=C["surface"],
            framewidth=0,
        ),
        font=dict(family="Georgia, serif", color=C["ink2"]),
        title_text=f"Ubicación · {ciudad}",
        title_font=dict(color=C["ink"], size=13, family="Playfair Display, serif"),
        margin=dict(t=40, b=0, l=0, r=0),
        height=420,
    )
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# COMPONENTES UI
# ══════════════════════════════════════════════════════════════════════════════

def metric_card(value, label, sub=""):
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    return (f'<div class="metric-card">'
            f'<div class="val">{value}</div>'
            f'<div class="lbl">{label}</div>'
            f'{sub_html}</div>')


def render_metricas(df: pd.DataFrame):
    total = len(df)
    e = df["estado_matricula"].fillna("").str.upper()
    activas    = e.str.contains("ACTIV|VIGENT").sum()
    canceladas = e.str.contains("CANCEL|DISUEL").sum()
    tasa       = round(activas / total * 100, 1) if total else 0
    fechas     = df["fecha_matricula"].apply(parse_fecha).dropna()
    primera    = str(fechas.min().year) if not fechas.empty else "—"
    camaras    = df["camara_comercio"].nunique()

    cols = st.columns(5)
    datos = [
        (str(total),        "Total registros",         "en el RUES"),
        (str(activas),      "Empresas activas",         f"{tasa}% del total"),
        (str(canceladas),   "Canceladas / disueltas",   ""),
        (primera,           "Año primera empresa",      ""),
        (str(camaras),      "Cámaras de comercio",      "con registro"),
    ]
    for col, (v, l, s) in zip(cols, datos):
        with col:
            st.markdown(metric_card(v, l, s), unsafe_allow_html=True)


def render_empresa_card(row: pd.Series):
    nombre = nombre_empresa(row)
    estado = str(row.get("estado_matricula") or "")
    clase  = ("activa"    if "ACTIV"  in estado.upper() else
              "cancelada" if "CANCEL" in estado.upper() else "")

    mat    = row.get("matricula")            or "—"
    ciiu   = row.get("cod_ciiu_act_econ_pri") or "—"
    org    = row.get("organizacion_juridica") or "—"
    camara = row.get("camara_comercio")      or "—"
    f_mat  = row.get("fecha_matricula")      or "—"
    f_can  = str(row.get("fecha_cancelacion") or "")
    rep    = row.get("Representante Legal")  or "—"

    ant = ""
    fobj = parse_fecha(f_mat)
    if fobj:
        delta = (date.today() - fobj).days / 365.25
        ant = f" · {antiguedad_str(delta)}"

    can_html = ""
    if f_can.strip() not in ("", "nan", "None", "—"):
        can_html = f"<br>Cancelada: <b>{f_can}</b>"

    st.markdown(f"""
    <div class="emp-card {clase}">
        <div class="emp-nombre">{nombre} &nbsp; {badge_estado(estado)}</div>
        <div class="emp-meta">
            Matrícula: <b>{mat}</b> · {camara} · Inscrita: <b>{f_mat}</b>{ant}
            {can_html}<br>
            Organización: <b>{org}</b> · CIIU: <b>{ciiu}</b> · Rep. legal: <b>{rep}</b>
        </div>
    </div>
    """, unsafe_allow_html=True)
    # devuelve camara para que el caller pueda mostrar el mapa
    return str(camara)


# ══════════════════════════════════════════════════════════════════════════════
# ANÁLISIS — tabs
# ══════════════════════════════════════════════════════════════════════════════

def render_analisis(df: pd.DataFrame):
    tabs = st.tabs(["Empresas", "Supervivencia", "Temporal",
                    "Geografía", "Actividad", "Datos"])

    # Empresas
    with tabs[0]:
        st.markdown("#### Empresas registradas")
        estados = ["Todos"] + sorted(df["estado_matricula"].dropna().unique().tolist())
        sel = st.selectbox("Filtrar por estado", estados, key="est_fil")
        df_fil = df if sel == "Todos" else df[df["estado_matricula"] == sel]
        st.caption(f"{len(df_fil)} de {len(df)} registros")

        # Mostrar tarjetas de empresa
        camaras_vistas = []
        for _, row in df_fil.iterrows():
            cam = render_empresa_card(row)
            camaras_vistas.append(cam)

        # Layout 2 columnas: mapa izquierda | timeline derecha
        col_mapa, col_tl = st.columns([1, 1], gap="medium")

        with col_mapa:
            # Usar la primera cámara con coordenadas disponibles
            mapa_fig = None
            for cam in camaras_vistas:
                mapa_fig = fig_mapa_empresa(cam)
                if mapa_fig:
                    break
            if mapa_fig:
                st.plotly_chart(mapa_fig, use_container_width=True)

        with col_tl:
            render_timeline_empresas(df_fil)

    # Supervivencia
    with tabs[1]:
        st.markdown("#### Tasa de supervivencia")
        c1, c2 = st.columns([1, 1])
        with c1:
            st.plotly_chart(fig_supervivencia(df), use_container_width=True)
        with c2:
            st.plotly_chart(fig_tipo_organizacion(df), use_container_width=True)

        st.markdown("#### Por tipo de organización")
        resumen = df.groupby("organizacion_juridica").apply(
            lambda g: pd.Series({
                "Total":      len(g),
                "Activas":    g["estado_matricula"].str.upper()
                               .str.contains("ACTIV|VIGENT", na=False).sum(),
                "Canceladas": g["estado_matricula"].str.upper()
                               .str.contains("CANCEL|DISUEL", na=False).sum(),
            })
        ).reset_index()
        resumen["Tasa activas (%)"] = (
            resumen["Activas"] / resumen["Total"] * 100
        ).round(1)
        st.dataframe(resumen.sort_values("Total", ascending=False),
                     hide_index=True, use_container_width=True)

    # Temporal
    with tabs[2]:
        st.markdown("#### Evolución temporal")
        st.plotly_chart(fig_timeline(df), use_container_width=True)
        fr = fig_renovaciones(df)
        if fr:
            st.plotly_chart(fr, use_container_width=True)
        df3 = df.copy()
        df3["f"] = df3["fecha_matricula"].apply(parse_fecha)
        df3 = df3.dropna(subset=["f"])
        if not df3.empty:
            df3["ant"] = df3["f"].apply(lambda d: (date.today() - d).days / 365.25)
            c1, c2, c3 = st.columns(3)
            c1.metric("Antigüedad promedio", f"{df3['ant'].mean():.1f} años")
            c2.metric("Empresa más antigua",  f"{df3['ant'].max():.1f} años")
            c3.metric("Empresa más reciente", f"{df3['ant'].min():.1f} años")

    # Geografía
    with tabs[3]:
        st.markdown("#### Distribución geográfica")
        if df["camara_comercio"].notna().any():
            # Mapa principal de Colombia con todas las cámaras
            st.plotly_chart(fig_camaras_mapa(df), use_container_width=True)
            # Heatmap de cámara × estado
            st.plotly_chart(fig_heatmap(df), use_container_width=True)
        else:
            st.info("No hay datos de cámara de comercio disponibles.")

    # Actividad
    with tabs[4]:
        st.markdown("#### Actividad económica (CIIU)")
        if df["cod_ciiu_act_econ_pri"].notna().any():
            st.plotly_chart(fig_ciiu(df), use_container_width=True)
            st.markdown("#### CIIU × Estado de matrícula")
            pivot2 = pd.crosstab(
                df["cod_ciiu_act_econ_pri"].fillna("Sin código"),
                df["estado_matricula"].fillna("Sin info"),
            ).head(15)
            st.dataframe(pivot2, use_container_width=True)
        else:
            st.info("No hay datos de actividad CIIU disponibles.")

    # Datos
    with tabs[5]:
        st.markdown("#### Datos completos")
        col_def = [
            "razon_social", "primer_nombre", "primer_apellido",
            "camara_comercio", "organizacion_juridica", "estado_matricula",
            "fecha_matricula", "fecha_cancelacion",
            "cod_ciiu_act_econ_pri", "matricula",
        ]
        col_sel = st.multiselect(
            "Columnas",
            options=df.columns.tolist(),
            default=[c for c in col_def if c in df.columns],
        )
        if col_sel:
            st.dataframe(df[col_sel].reset_index(drop=True),
                         use_container_width=True, height=440)
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
        <div class="title">Análisis Empresarial</div>
        <div class="subtitle">RUES · Universidad Santo Tomás · Colombia</div>
    </div>
    """, unsafe_allow_html=True)

    if get_connection() is None:
        st.error(
            f"No se encontró `{PARQUET_FILE}`. "
            "Ejecuta primero: `python convertir_a_parquet.py`"
        )
        st.stop()

    st.markdown("""
    <div class="search-box">
        <h4>Consulta por número de identificación</h4>
        <p>Ingresa una cédula de ciudadanía o NIT para ver el perfil empresarial en el RUES.</p>
    </div>
    """, unsafe_allow_html=True)

    col_inp, col_btn = st.columns([5, 1])
    with col_inp:
        num_id = st.text_input(
            "Número",
            placeholder="Ej: 79500000  ·  900123456",
            label_visibility="collapsed",
            key="num_id",
        )
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
            st.error(
                f"No se encontraron registros para **{num_id}**. "
                "Verifica que el número sea correcto."
            )
            return

        st.success(f"{len(df)} registro(s) encontrado(s) para **{num_id}**")
        st.markdown("---")
        render_metricas(df)
        st.markdown("---")
        render_analisis(df)

    else:
        st.markdown("<br>", unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        guia = [
            ("①", "Ingresa tu número",
             "Cédula de ciudadanía o NIT en el campo de búsqueda."),
            ("②", "Consulta el RUES",
             "Se buscará en los 9 millones de registros del Registro Mercantil."),
            ("③", "Explora tu perfil",
             "Empresas, supervivencia, actividad CIIU, geografía y más."),
        ]
        for col, (num, tit, desc) in zip([c1, c2, c3], guia):
            with col:
                st.markdown(f"""
                <div class="metric-card" style="text-align:left">
                    <div style="font-family:'Playfair Display',serif;font-size:1.6rem;
                                color:{C['accent']};margin-bottom:.5rem">{num}</div>
                    <div style="font-weight:600;margin-bottom:.3rem">{tit}</div>
                    <div style="font-size:.79rem;color:{C['ink2']};font-style:italic">{desc}</div>
                </div>
                """, unsafe_allow_html=True)

        st.markdown(f"""
        <div style="text-align:center;color:{C['ink2']};font-size:.72rem;
                    letter-spacing:.08em;text-transform:uppercase;margin-top:3rem;
                    border-top:1px solid {C['border']};padding-top:1rem">
            CONFECAMARAS · RUES · datos.gov.co · Actualización: Febrero 2026
        </div>
        """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()