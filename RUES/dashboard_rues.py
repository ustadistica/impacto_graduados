import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# ─── CONFIG ────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="RUES · Diagnóstico Empresarial",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ─── ESTILOS ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'Space Grotesk', sans-serif;
}

.stApp { background: #0a0e1a; color: #e8eaf6; }

/* Header principal */
.hero-header {
    background: linear-gradient(135deg, #1a1f35 0%, #0d1526 50%, #111827 100%);
    border: 1px solid #2a3655;
    border-radius: 16px;
    padding: 2.5rem 3rem;
    margin-bottom: 2rem;
    position: relative;
    overflow: hidden;
}
.hero-header::before {
    content: '';
    position: absolute;
    top: -50%;
    right: -10%;
    width: 400px;
    height: 400px;
    background: radial-gradient(circle, rgba(99,102,241,0.12) 0%, transparent 70%);
    pointer-events: none;
}
.hero-title {
    font-size: 2.4rem;
    font-weight: 700;
    background: linear-gradient(135deg, #818cf8, #a5b4fc, #c7d2fe);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin: 0 0 0.3rem 0;
}
.hero-sub {
    color: #64748b;
    font-size: 1rem;
    font-weight: 400;
    margin: 0;
}

/* Tarjetas métricas */
.metric-card {
    background: #111827;
    border: 1px solid #1e2d45;
    border-radius: 12px;
    padding: 1.4rem 1.6rem;
    margin-bottom: 1rem;
    transition: border-color 0.2s;
}
.metric-card:hover { border-color: #4f46e5; }
.metric-label {
    color: #64748b;
    font-size: 0.75rem;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 0.4rem;
}
.metric-value {
    font-size: 1.8rem;
    font-weight: 700;
    color: #a5b4fc;
    font-family: 'DM Mono', monospace;
}
.metric-delta {
    font-size: 0.8rem;
    color: #10b981;
    margin-top: 0.2rem;
}

/* Badge de estado */
.badge-active {
    display: inline-block;
    background: rgba(16,185,129,0.15);
    color: #10b981;
    border: 1px solid rgba(16,185,129,0.3);
    border-radius: 99px;
    padding: 0.25rem 0.9rem;
    font-size: 0.8rem;
    font-weight: 600;
    letter-spacing: 0.05em;
}
.badge-cancelled {
    display: inline-block;
    background: rgba(239,68,68,0.15);
    color: #ef4444;
    border: 1px solid rgba(239,68,68,0.3);
    border-radius: 99px;
    padding: 0.25rem 0.9rem;
    font-size: 0.8rem;
    font-weight: 600;
    letter-spacing: 0.05em;
}
.badge-inactive {
    display: inline-block;
    background: rgba(245,158,11,0.15);
    color: #f59e0b;
    border: 1px solid rgba(245,158,11,0.3);
    border-radius: 99px;
    padding: 0.25rem 0.9rem;
    font-size: 0.8rem;
    font-weight: 600;
    letter-spacing: 0.05em;
}

/* Panel empresa */
.company-panel {
    background: linear-gradient(135deg, #111827, #0f172a);
    border: 1px solid #2a3655;
    border-radius: 16px;
    padding: 2rem;
    margin-bottom: 1.5rem;
}
.company-name {
    font-size: 1.6rem;
    font-weight: 700;
    color: #e8eaf6;
    margin-bottom: 0.5rem;
}
.info-row {
    display: flex;
    gap: 1rem;
    flex-wrap: wrap;
    margin-top: 1rem;
}
.info-chip {
    background: #1e2d45;
    border-radius: 8px;
    padding: 0.4rem 0.8rem;
    font-size: 0.82rem;
    color: #94a3b8;
}
.info-chip span {
    color: #c7d2fe;
    font-weight: 500;
}

/* Gauge container */
.gauge-box {
    background: #111827;
    border: 1px solid #1e2d45;
    border-radius: 12px;
    padding: 1rem;
    text-align: center;
}

/* Sección títulos */
.section-title {
    font-size: 1rem;
    font-weight: 600;
    color: #818cf8;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin: 1.5rem 0 1rem 0;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid #1e2d45;
}

/* Input styling override */
div[data-testid="stTextInput"] input {
    background: #111827 !important;
    border: 1px solid #2a3655 !important;
    border-radius: 10px !important;
    color: #e8eaf6 !important;
    font-family: 'DM Mono', monospace !important;
    font-size: 1.1rem !important;
    padding: 0.8rem 1rem !important;
}
div[data-testid="stTextInput"] input:focus {
    border-color: #4f46e5 !important;
    box-shadow: 0 0 0 2px rgba(79,70,229,0.2) !important;
}

/* Botón */
.stButton > button {
    background: linear-gradient(135deg, #4f46e5, #6366f1) !important;
    color: white !important;
    border: none !important;
    border-radius: 10px !important;
    padding: 0.7rem 2rem !important;
    font-weight: 600 !important;
    font-family: 'Space Grotesk', sans-serif !important;
    font-size: 0.95rem !important;
    transition: all 0.2s !important;
    width: 100% !important;
}
.stButton > button:hover {
    background: linear-gradient(135deg, #4338ca, #4f46e5) !important;
    transform: translateY(-1px) !important;
    box-shadow: 0 4px 20px rgba(79,70,229,0.4) !important;
}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {
    background: #111827;
    border-radius: 10px;
    padding: 4px;
    border: 1px solid #1e2d45;
}
.stTabs [data-baseweb="tab"] {
    color: #64748b !important;
    font-weight: 500;
    border-radius: 8px;
}
.stTabs [aria-selected="true"] {
    background: #1e2d45 !important;
    color: #a5b4fc !important;
}

/* Plotly charts background */
.js-plotly-plot { border-radius: 12px; overflow: hidden; }

/* Alerta no encontrado */
.not-found-box {
    background: rgba(239,68,68,0.08);
    border: 1px solid rgba(239,68,68,0.25);
    border-radius: 12px;
    padding: 2rem;
    text-align: center;
    color: #fca5a5;
}

/* Divider */
hr { border-color: #1e2d45 !important; }
</style>
""", unsafe_allow_html=True)

# ─── CARGA DE DATOS ─────────────────────────────────────────────────────────────
RUTA = "data/Personas_Naturales,_Personas_Jurídicas_y_Entidades_Sin_Animo_de_Lucro_20260510.csv"

PLOT_CONFIG = {
    "paper_bgcolor": "rgba(0,0,0,0)",
    "plot_bgcolor": "rgba(0,0,0,0)",
    "font": {"family": "Space Grotesk", "color": "#94a3b8", "size": 12},
    "margin": {"t": 40, "b": 40, "l": 40, "r": 40},
}

COLOR_SEQ = ["#818cf8", "#34d399", "#f59e0b", "#f472b6", "#60a5fa", "#a78bfa"]

@st.cache_data(show_spinner="Cargando base RUES (9M filas)…")
def cargar_datos():
    df = pd.read_csv(
        RUTA,
        encoding="latin1",
        low_memory=False,
        dtype={"numero_identificacion": str, "nit": str}
    )
    # Limpiar columnas de texto
    df.columns = df.columns.str.strip()
    str_cols = df.select_dtypes("object").columns
    df[str_cols] = df[str_cols].apply(lambda c: c.str.strip())

    # Parsear fechas
    for col in ["fecha_matricula", "fecha_cancelacion", "fecha_vigencia", "fecha_renovacion"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col].astype(str).str[:8], format="%Y%m%d", errors="coerce")

    # Limpiar numero_identificacion
    df["numero_identificacion"] = df["numero_identificacion"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    if "nit" in df.columns:
        df["nit"] = df["nit"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)

    # Edad empresa
    hoy = pd.Timestamp.today()
    df["edad_empresa"] = ((hoy - df["fecha_matricula"]).dt.days / 365.25).round(1)

    return df

# ─── HELPERS ───────────────────────────────────────────────────────────────────
def tasa_supervivencia(sector_df, edad):
    """
    Kaplan-Meier simplificado por sector:
    % de empresas del mismo sector que superan la edad dada.
    """
    total = len(sector_df)
    if total == 0:
        return None
    activas_plus = sector_df[
        (sector_df["estado_matricula"].str.upper() == "ACTIVA") |
        (sector_df["edad_empresa"] >= edad)
    ]
    return round(len(activas_plus) / total * 100, 1)

def score_salud(empresa, sector_df):
    """Score 0-100 basado en renovación, antigüedad y estado."""
    score = 0
    estado = str(empresa.get("estado_matricula", "")).upper()
    if estado == "ACTIVA":
        score += 40
    elif estado == "INACTIVA":
        score += 15

    edad = empresa.get("edad_empresa", 0) or 0
    if edad >= 10:
        score += 25
    elif edad >= 5:
        score += 15
    elif edad >= 2:
        score += 8

    ultimo_ano = empresa.get("ultimo_ano_renovado", 0)
    if pd.notna(ultimo_ano):
        anos_sin_renovar = datetime.now().year - int(ultimo_ano) if ultimo_ano else 99
        if anos_sin_renovar <= 1:
            score += 25
        elif anos_sin_renovar <= 3:
            score += 10

    ts = tasa_supervivencia(sector_df, edad)
    if ts and ts >= 60:
        score += 10

    return min(score, 100)

def get_badge(estado):
    estado_up = str(estado).upper()
    if estado_up == "ACTIVA":
        return '<span class="badge-active">● ACTIVA</span>'
    elif estado_up == "CANCELADA":
        return '<span class="badge-cancelled">✕ CANCELADA</span>'
    else:
        return '<span class="badge-inactive">⚠ INACTIVA</span>'

def gauge_chart(valor, titulo, color="#818cf8"):
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=valor,
        title={"text": titulo, "font": {"size": 13, "color": "#64748b"}},
        number={"suffix": "%", "font": {"size": 28, "color": "#e8eaf6", "family": "DM Mono"}},
        gauge={
            "axis": {"range": [0, 100], "tickcolor": "#1e2d45", "tickwidth": 1, "tickfont": {"color": "#475569", "size": 10}},
            "bar": {"color": color, "thickness": 0.35},
            "bgcolor": "#1e2d45",
            "borderwidth": 0,
            "steps": [
                {"range": [0, 33], "color": "rgba(239,68,68,0.1)"},
                {"range": [33, 66], "color": "rgba(245,158,11,0.1)"},
                {"range": [66, 100], "color": "rgba(16,185,129,0.1)"},
            ],
            "threshold": {"line": {"color": color, "width": 3}, "thickness": 0.8, "value": valor}
        }
    ))
    fig.update_layout(height=220, **PLOT_CONFIG)
    return fig

# ─── HEADER ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero-header">
    <p class="hero-title">🏢 RUES · Diagnóstico Empresarial</p>
    <p class="hero-sub">Registro Único Empresarial y Social · Colombia · Base oficial 2026</p>
</div>
""", unsafe_allow_html=True)

# ─── CARGA ─────────────────────────────────────────────────────────────────────
df = cargar_datos()

# ─── BÚSQUEDA ──────────────────────────────────────────────────────────────────
col_inp, col_btn = st.columns([4, 1])
with col_inp:
    cedula_input = st.text_input(
        "Número de cédula o NIT",
        placeholder="Ej: 24560913 o 900123456",
        label_visibility="collapsed"
    )
with col_btn:
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    buscar = st.button("Consultar →")

st.markdown("<hr>", unsafe_allow_html=True)

# ─── LÓGICA PRINCIPAL ──────────────────────────────────────────────────────────
if buscar and cedula_input.strip():
    query = cedula_input.strip().replace(".0", "")

    # Buscar en identificacion y nit
    mask = (df["numero_identificacion"] == query)
    if "nit" in df.columns:
        mask = mask | (df["nit"] == query)
    resultados = df[mask].copy()

    if resultados.empty:
        st.markdown(f"""
        <div class="not-found-box">
            <div style="font-size:2.5rem;margin-bottom:0.5rem">🔍</div>
            <div style="font-size:1.1rem;font-weight:600;margin-bottom:0.3rem">No encontrado</div>
            <div style="color:#94a3b8;font-size:0.9rem">La cédula/NIT <code style="color:#f87171">{query}</code> no está registrada en el RUES.</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        # Si hay múltiples registros, tomar el más reciente
        empresa = resultados.sort_values("fecha_matricula", ascending=False).iloc[0]
        tiene_multiples = len(resultados) > 1

        # Datos del sector para comparativas
        cod_ciiu = empresa.get("cod_ciiu_act_econ_pri", None)
        if pd.notna(cod_ciiu):
            sector_df = df[df["cod_ciiu_act_econ_pri"] == cod_ciiu].copy()
        else:
            sector_df = df.copy()

        edad = empresa.get("edad_empresa", 0) or 0
        estado = str(empresa.get("estado_matricula", "")).upper()
        score = score_salud(empresa, sector_df)
        ts = tasa_supervivencia(sector_df, edad) or 0

        # ── Panel empresa ──────────────────────────────────────────────
        razon = empresa.get("razon_social") or f"{empresa.get('primer_nombre','')} {empresa.get('primer_apellido','')}".strip()
        camara = empresa.get("camara_comercio", "N/A")
        org_jur = empresa.get("organizacion_juridica", "N/A")
        tipo_soc = empresa.get("tipo_sociedad", "N/A")
        cat_mat = empresa.get("categoria_matricula", "N/A")
        fecha_mat = empresa.get("fecha_matricula")
        fecha_mat_str = fecha_mat.strftime("%d/%m/%Y") if pd.notna(fecha_mat) else "N/A"
        ultimo_renovado = empresa.get("ultimo_ano_renovado", "N/A")

        st.markdown(f"""
        <div class="company-panel">
            <div class="company-name">{razon}</div>
            {get_badge(estado)}
            <div class="info-row">
                <div class="info-chip">📍 Cámara <span>{camara}</span></div>
                <div class="info-chip">🏛️ <span>{org_jur}</span></div>
                <div class="info-chip">📋 <span>{tipo_soc}</span></div>
                <div class="info-chip">📅 Matrícula <span>{fecha_mat_str}</span></div>
                <div class="info-chip">🔄 Último año renovado <span>{int(ultimo_renovado) if pd.notna(ultimo_renovado) and str(ultimo_renovado) not in ['0','nan'] else 'Sin registro'}</span></div>
                <div class="info-chip">⏳ Antigüedad <span>{edad:.1f} años</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        if tiene_multiples:
            st.info(f"ℹ️ Se encontraron {len(resultados)} registros para este ID. Mostrando el más reciente.")

        # ── Métricas clave + Gauges ────────────────────────────────────
        st.markdown('<div class="section-title">📊 Indicadores Principales</div>', unsafe_allow_html=True)

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Antigüedad</div>
                <div class="metric-value">{edad:.1f}</div>
                <div class="metric-delta">años en el mercado</div>
            </div>""", unsafe_allow_html=True)
        with col2:
            renovado_ok = str(ultimo_renovado) not in ["0", "nan", "None", "N/A"] and pd.notna(ultimo_renovado)
            anos_sin = datetime.now().year - int(float(ultimo_renovado)) if renovado_ok else 99
            color_ren = "#10b981" if anos_sin <= 1 else ("#f59e0b" if anos_sin <= 3 else "#ef4444")
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Años sin renovar</div>
                <div class="metric-value" style="color:{color_ren}">{anos_sin if anos_sin < 99 else '—'}</div>
                <div class="metric-delta">{'Al día ✓' if anos_sin <= 1 else 'Requiere atención'}</div>
            </div>""", unsafe_allow_html=True)
        with col3:
            empresas_sector = len(sector_df)
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Empresas en su sector</div>
                <div class="metric-value">{empresas_sector:,}</div>
                <div class="metric-delta">CIIU {int(cod_ciiu) if pd.notna(cod_ciiu) else 'N/A'}</div>
            </div>""", unsafe_allow_html=True)
        with col4:
            activas_sector = len(sector_df[sector_df["estado_matricula"].str.upper() == "ACTIVA"])
            pct_activas = round(activas_sector / len(sector_df) * 100, 1) if len(sector_df) > 0 else 0
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Activas en su sector</div>
                <div class="metric-value">{pct_activas}%</div>
                <div class="metric-delta">{activas_sector:,} empresas activas</div>
            </div>""", unsafe_allow_html=True)

        # Gauges
        g1, g2, g3 = st.columns(3)
        with g1:
            st.plotly_chart(gauge_chart(score, "Score de Salud Empresarial", "#818cf8"), use_container_width=True, config={"displayModeBar": False})
        with g2:
            st.plotly_chart(gauge_chart(ts, "Tasa de Supervivencia en su Sector", "#34d399"), use_container_width=True, config={"displayModeBar": False})
        with g3:
            renovacion_score = max(0, 100 - (anos_sin * 20)) if anos_sin < 99 else 0
            st.plotly_chart(gauge_chart(renovacion_score, "Nivel de Cumplimiento en Renovación", "#f59e0b"), use_container_width=True, config={"displayModeBar": False})

        # ── Tabs de análisis ───────────────────────────────────────────
        st.markdown('<div class="section-title">📈 Análisis Detallado</div>', unsafe_allow_html=True)

        tab1, tab2, tab3, tab4 = st.tabs(["🏭 Sector", "📅 Línea de tiempo", "🗺️ Contexto Nacional", "🩺 Diagnóstico"])

        # TAB 1: Sector
        with tab1:
            c1, c2 = st.columns(2)

            with c1:
                # Distribución estados en sector
                estados_sector = sector_df["estado_matricula"].value_counts().reset_index()
                estados_sector.columns = ["Estado", "Cantidad"]
                fig_estados = px.pie(
                    estados_sector,
                    names="Estado",
                    values="Cantidad",
                    title="Estados de matrícula en su sector",
                    color_discrete_sequence=COLOR_SEQ,
                    hole=0.55
                )
                fig_estados.update_traces(textposition="outside", textfont_size=11)
                fig_estados.update_layout(height=320, **PLOT_CONFIG,
                    legend=dict(orientation="h", yanchor="bottom", y=-0.2))
                st.plotly_chart(fig_estados, use_container_width=True, config={"displayModeBar": False})

            with c2:
                # Distribución organización jurídica
                org_sector = sector_df["organizacion_juridica"].value_counts().head(8).reset_index()
                org_sector.columns = ["Tipo", "Cantidad"]
                fig_org = px.bar(
                    org_sector,
                    x="Cantidad",
                    y="Tipo",
                    orientation="h",
                    title="Tipos de organización en el sector",
                    color_discrete_sequence=["#818cf8"]
                )
                fig_org.update_layout(height=320, yaxis_title="", xaxis_title="Empresas", **PLOT_CONFIG)
                fig_org.update_traces(marker_color="#818cf8")
                st.plotly_chart(fig_org, use_container_width=True, config={"displayModeBar": False})

            # Evolución de matrículas en el sector por año
            sector_df["año_matricula"] = sector_df["fecha_matricula"].dt.year
            evol = sector_df.groupby("año_matricula").size().reset_index(name="nuevas_empresas")
            evol = evol[evol["año_matricula"].between(1990, datetime.now().year)]

            fig_evol = px.area(
                evol,
                x="año_matricula",
                y="nuevas_empresas",
                title="Evolución histórica de nuevas matrículas en su sector",
                color_discrete_sequence=["#818cf8"]
            )
            fig_evol.update_traces(fill="tozeroy", fillcolor="rgba(129,140,248,0.15)", line_color="#818cf8")
            fig_evol.add_vline(
                x=empresa["fecha_matricula"].year if pd.notna(empresa["fecha_matricula"]) else datetime.now().year,
                line_dash="dot",
                line_color="#f59e0b",
                annotation_text="Su matrícula",
                annotation_font_color="#f59e0b"
            )
            fig_evol.update_layout(xaxis_title="Año", yaxis_title="Empresas matriculadas", **PLOT_CONFIG)
            st.plotly_chart(fig_evol, use_container_width=True, config={"displayModeBar": False})

        # TAB 2: Línea de tiempo
        with tab2:
            st.markdown("#### Ciclo de vida de su empresa")

            hitos = []
            if pd.notna(empresa.get("fecha_matricula")):
                hitos.append({"Evento": "Matrícula", "Fecha": empresa["fecha_matricula"], "Color": "#34d399"})
            if pd.notna(empresa.get("fecha_renovacion")):
                hitos.append({"Evento": "Última renovación", "Fecha": empresa["fecha_renovacion"], "Color": "#818cf8"})
            if pd.notna(empresa.get("fecha_cancelacion")) and empresa["fecha_cancelacion"].year < 9999:
                hitos.append({"Evento": "Cancelación", "Fecha": empresa["fecha_cancelacion"], "Color": "#ef4444"})

            if hitos:
                hitos_df = pd.DataFrame(hitos)
                hitos_df["y"] = 1

                fig_time = go.Figure()
                fig_time.add_trace(go.Scatter(
                    x=hitos_df["Fecha"],
                    y=hitos_df["y"],
                    mode="markers+text",
                    marker=dict(size=18, color=hitos_df["Color"], symbol="circle"),
                    text=hitos_df["Evento"],
                    textposition="top center",
                    hovertemplate="<b>%{text}</b><br>%{x|%d/%m/%Y}<extra></extra>"
                ))
                # Línea conectora
                fig_time.add_trace(go.Scatter(
                    x=hitos_df["Fecha"],
                    y=hitos_df["y"],
                    mode="lines",
                    line=dict(color="#1e2d45", width=2),
                    showlegend=False
                ))
                fig_time.update_layout(
                    height=200,
                    yaxis=dict(visible=False, range=[0, 2]),
                    xaxis=dict(title=""),
                    showlegend=False,
                    **PLOT_CONFIG
                )
                st.plotly_chart(fig_time, use_container_width=True, config={"displayModeBar": False})

            # Renovaciones históricas del sector
            sector_df["año_renovacion"] = sector_df["fecha_renovacion"].dt.year
            ren_hist = sector_df.groupby("año_renovacion").size().reset_index(name="renovaciones")
            ren_hist = ren_hist[ren_hist["año_renovacion"].between(2000, datetime.now().year)]

            fig_ren = px.bar(
                ren_hist,
                x="año_renovacion",
                y="renovaciones",
                title="Renovaciones anuales en su sector",
                color_discrete_sequence=["#34d399"]
            )
            fig_ren.update_layout(xaxis_title="Año", yaxis_title="Renovaciones", **PLOT_CONFIG)
            st.plotly_chart(fig_ren, use_container_width=True, config={"displayModeBar": False})

            # Distribución de edades en el sector
            edades_validas = sector_df[sector_df["edad_empresa"].between(0, 80)]["edad_empresa"]
            fig_edad = px.histogram(
                edades_validas,
                nbins=40,
                title="Distribución de antigüedad en su sector (años)",
                color_discrete_sequence=["#f59e0b"]
            )
            fig_edad.add_vline(
                x=edad,
                line_dash="dot",
                line_color="#818cf8",
                annotation_text=f"Su empresa ({edad:.0f} años)",
                annotation_font_color="#818cf8"
            )
            fig_edad.update_layout(xaxis_title="Años", yaxis_title="Empresas", **PLOT_CONFIG)
            st.plotly_chart(fig_edad, use_container_width=True, config={"displayModeBar": False})

        # TAB 3: Contexto Nacional
        with tab3:
            c1, c2 = st.columns(2)

            with c1:
                # Top 15 cámaras por volumen
                top_camaras = df["camara_comercio"].value_counts().head(15).reset_index()
                top_camaras.columns = ["Cámara", "Empresas"]
                color_cam = ["#f59e0b" if c == camara else "#818cf8" for c in top_camaras["Cámara"]]
                fig_cam = px.bar(
                    top_camaras,
                    x="Empresas",
                    y="Cámara",
                    orientation="h",
                    title="Top 15 Cámaras de Comercio",
                    color_discrete_sequence=["#818cf8"]
                )
                fig_cam.update_traces(marker_color=color_cam)
                fig_cam.update_layout(height=420, yaxis_title="", **PLOT_CONFIG)
                st.plotly_chart(fig_cam, use_container_width=True, config={"displayModeBar": False})

            with c2:
                # Distribución nacional por estado
                estados_nac = df["estado_matricula"].value_counts().reset_index()
                estados_nac.columns = ["Estado", "Empresas"]
                fig_enac = px.pie(
                    estados_nac,
                    names="Estado",
                    values="Empresas",
                    title="Distribución nacional por estado",
                    color_discrete_sequence=COLOR_SEQ,
                    hole=0.5
                )
                fig_enac.update_layout(height=420, **PLOT_CONFIG,
                    legend=dict(orientation="h", yanchor="bottom", y=-0.25))
                st.plotly_chart(fig_enac, use_container_width=True, config={"displayModeBar": False})

            # Top sectores CIIU
            top_ciiu = df["cod_ciiu_act_econ_pri"].value_counts().head(20).reset_index()
            top_ciiu.columns = ["CIIU", "Empresas"]
            top_ciiu["CIIU"] = top_ciiu["CIIU"].astype(str)
            ciiu_str = str(int(cod_ciiu)) if pd.notna(cod_ciiu) else ""
            color_ciiu = ["#f59e0b" if str(c) == ciiu_str else "#60a5fa" for c in top_ciiu["CIIU"]]

            fig_ciiu = px.bar(
                top_ciiu,
                x="CIIU",
                y="Empresas",
                title="Top 20 actividades económicas (CIIU) en Colombia",
            )
            fig_ciiu.update_traces(marker_color=color_ciiu)
            fig_ciiu.update_layout(xaxis_title="Código CIIU", yaxis_title="Empresas", **PLOT_CONFIG)
            st.plotly_chart(fig_ciiu, use_container_width=True, config={"displayModeBar": False})

        # TAB 4: Diagnóstico
        with tab4:
            st.markdown("#### 🩺 Diagnóstico integral de su empresa")

            # Score de salud detallado
            col_d1, col_d2 = st.columns([1, 2])

            with col_d1:
                if score >= 70:
                    nivel = "🟢 Saludable"
                    color_score = "#10b981"
                    desc_score = "Su empresa muestra indicadores sólidos de estabilidad y cumplimiento."
                elif score >= 40:
                    nivel = "🟡 En riesgo moderado"
                    color_score = "#f59e0b"
                    desc_score = "Hay aspectos que requieren atención para asegurar la continuidad."
                else:
                    nivel = "🔴 Alerta"
                    color_score = "#ef4444"
                    desc_score = "Su empresa presenta señales críticas que deben atenderse."

                st.markdown(f"""
                <div class="metric-card" style="border-color:{color_score}40">
                    <div class="metric-label">Nivel de salud</div>
                    <div style="font-size:1.2rem;font-weight:700;color:{color_score};margin:0.5rem 0">{nivel}</div>
                    <div style="color:#94a3b8;font-size:0.85rem">{desc_score}</div>
                    <hr style="border-color:#1e2d45;margin:1rem 0">
                    <div class="metric-label">Score calculado</div>
                    <div class="metric-value" style="color:{color_score}">{score}/100</div>
                </div>
                """, unsafe_allow_html=True)

            with col_d2:
                # Radar de dimensiones
                categorias = ["Antigüedad", "Renovación", "Supervivencia\nen sector", "Posición\nen sector", "Estado\nmatrícula"]

                antiguedad_score = min(100, edad * 5)
                renovacion_sc = max(0, 100 - (anos_sin * 20)) if anos_sin < 99 else 0
                supervivencia_sc = ts
                posicion_sc = round((1 - sector_df[sector_df["estado_matricula"].str.upper() == "CANCELADA"].shape[0] / max(len(sector_df), 1)) * 100, 1)
                estado_sc = 100 if estado == "ACTIVA" else (40 if estado == "INACTIVA" else 10)

                valores = [antiguedad_score, renovacion_sc, supervivencia_sc, posicion_sc, estado_sc]

                fig_radar = go.Figure(go.Scatterpolar(
                    r=valores + [valores[0]],
                    theta=categorias + [categorias[0]],
                    fill="toself",
                    fillcolor="rgba(129,140,248,0.15)",
                    line=dict(color="#818cf8", width=2),
                    marker=dict(color="#818cf8", size=6)
                ))
                fig_radar.update_layout(
                    polar=dict(
                        bgcolor="rgba(0,0,0,0)",
                        angularaxis=dict(tickfont=dict(size=10, color="#94a3b8"), linecolor="#1e2d45", gridcolor="#1e2d45"),
                        radialaxis=dict(range=[0, 100], tickfont=dict(size=8, color="#475569"), gridcolor="#1e2d45", linecolor="#1e2d45")
                    ),
                    height=300,
                    **PLOT_CONFIG
                )
                st.plotly_chart(fig_radar, use_container_width=True, config={"displayModeBar": False})

            # Recomendaciones
            st.markdown("#### 💡 Recomendaciones")
            recomendaciones = []

            if anos_sin > 1:
                recomendaciones.append(("⚠️ Renovación pendiente", f"Lleva {anos_sin} año(s) sin renovar la matrícula. La renovación es obligatoria y vence el 31 de marzo de cada año.", "#f59e0b"))
            else:
                recomendaciones.append(("✅ Renovación al día", "Su matrícula está renovada. Recuerde renovar antes del 31 de marzo del próximo año.", "#10b981"))

            if estado != "ACTIVA":
                recomendaciones.append(("❌ Matrícula no activa", f"Estado actual: {estado}. Consulte con su Cámara de Comercio para regularizar su situación.", "#ef4444"))

            if ts < 50:
                recomendaciones.append(("📉 Sector con alta mortalidad", f"Solo el {ts}% de empresas similares superan su antigüedad. Diversifique y fortalezca su modelo de negocio.", "#f59e0b"))
            elif ts >= 70:
                recomendaciones.append(("📈 Sector resiliente", f"El {ts}% de empresas de su sector superan su antigüedad. Buen entorno competitivo.", "#10b981"))

            if edad < 3:
                recomendaciones.append(("🆕 Empresa joven", "Los primeros 3 años son críticos. Acceda a programas de Cámaras de Comercio y Fondo Emprender.", "#818cf8"))
            elif edad >= 10:
                recomendaciones.append(("🏆 Empresa consolidada", f"Con {edad:.0f} años en el mercado, está en el segmento de empresas consolidadas.", "#10b981"))

            for titulo_rec, desc_rec, color_rec in recomendaciones:
                st.markdown(f"""
                <div style="background:#111827;border:1px solid {color_rec}33;border-left:3px solid {color_rec};
                            border-radius:10px;padding:1rem 1.2rem;margin-bottom:0.7rem">
                    <div style="font-weight:600;color:{color_rec};margin-bottom:0.3rem">{titulo_rec}</div>
                    <div style="color:#94a3b8;font-size:0.88rem">{desc_rec}</div>
                </div>
                """, unsafe_allow_html=True)

            # Ficha completa
            with st.expander("📋 Ver ficha completa de la empresa"):
                ficha = empresa.to_dict()
                ficha_limpia = {k: v for k, v in ficha.items() if pd.notna(v) and str(v) not in ["nan", "None", "0"]}
                ficha_df = pd.DataFrame(list(ficha_limpia.items()), columns=["Campo", "Valor"])
                st.dataframe(ficha_df, use_container_width=True, hide_index=True)

elif buscar and not cedula_input.strip():
    st.warning("Por favor ingresa un número de cédula o NIT.")

else:
    # Estado inicial — estadísticas generales
    st.markdown('<div class="section-title">📊 Estadísticas Generales del RUES</div>', unsafe_allow_html=True)

    if "df" in dir():
        col1, col2, col3, col4 = st.columns(4)
        total = len(df)
        activas = len(df[df["estado_matricula"].str.upper() == "ACTIVA"])
        canceladas = len(df[df["estado_matricula"].str.upper() == "CANCELADA"])
        camaras = df["camara_comercio"].nunique()

        with col1:
            st.markdown(f"""<div class="metric-card">
                <div class="metric-label">Total registros</div>
                <div class="metric-value">{total:,}</div>
            </div>""", unsafe_allow_html=True)
        with col2:
            st.markdown(f"""<div class="metric-card">
                <div class="metric-label">Empresas activas</div>
                <div class="metric-value" style="color:#10b981">{activas:,}</div>
            </div>""", unsafe_allow_html=True)
        with col3:
            st.markdown(f"""<div class="metric-card">
                <div class="metric-label">Matrículas canceladas</div>
                <div class="metric-value" style="color:#ef4444">{canceladas:,}</div>
            </div>""", unsafe_allow_html=True)
        with col4:
            st.markdown(f"""<div class="metric-card">
                <div class="metric-label">Cámaras de comercio</div>
                <div class="metric-value">{camaras}</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("<br>*Ingresa una cédula o NIT para ver el diagnóstico de una empresa específica.*", unsafe_allow_html=True)