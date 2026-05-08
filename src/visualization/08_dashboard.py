"""
src/visualization/08_dashboard.py
==================================
Genera un dashboard HTML interactivo con Plotly que consolida todos los
análisis en un único archivo autocontenido.

Requiere que los CSV de outputs/tables/ ya existan (ejecutar scripts 02–07 antes).

Salida:
  outputs/reports/dashboard_rues.html
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.settings import (
    OUTPUTS_TABLES, OUTPUTS_REP, OUTPUTS_FIGS,
    COL_CAMARA, PLOT_TEMPLATE, PLOT_FONT_FAMILY,
    PLOT_COLOR_PRIMARY, PLOT_COLOR_SECONDARY
)

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.io as pio

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

# ── Paleta corporativa ────────────────────────────────────────────────────────
COLORS = {
    "primary":   PLOT_COLOR_PRIMARY,
    "secondary": PLOT_COLOR_SECONDARY,
    "green":     "#2ECC71",
    "red":       "#E74C3C",
    "purple":    "#9B59B6",
    "gray":      "#95A5A6",
}
COLOR_SEQ = [COLORS["primary"], COLORS["secondary"], COLORS["green"],
             COLORS["red"], COLORS["purple"], COLORS["gray"]]


def safe_read(filename: str) -> pd.DataFrame:
    path = OUTPUTS_TABLES / filename
    if not path.exists():
        log.warning(f"Archivo no encontrado: {path} — sección omitida.")
        return pd.DataFrame()
    return pd.read_csv(path)


# ── Figura 1: Matrículas por año ──────────────────────────────────────────────
def fig_evolucion() -> go.Figure:
    df = safe_read("06_evolucion_temporal.csv")
    if df.empty:
        df = safe_read("02_tasa_emprendimiento.csv")
        if df.empty:
            return go.Figure()

    year_col = "anio_matricula"
    val_col  = "matriculas" if "matriculas" in df.columns else "nuevas_matriculas"

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df[year_col].astype(int), y=df[val_col],
        name="Matrículas", marker_color=COLORS["primary"], opacity=0.8,
    ))
    if "media_movil_3a" in df.columns:
        fig.add_trace(go.Scatter(
            x=df[year_col].astype(int), y=df["media_movil_3a"],
            name="Media móvil 3 años", line=dict(color=COLORS["secondary"], width=3),
            mode="lines",
        ))
    fig.update_layout(
        title="📅 Evolución temporal — Nuevas matrículas por año",
        xaxis_title="Año", yaxis_title="Matrículas",
        template=PLOT_TEMPLATE, legend=dict(orientation="h", yanchor="bottom", y=1.02),
        barmode="overlay",
    )
    return fig


# ── Figura 2: Distribución geográfica ────────────────────────────────────────
def fig_geografica() -> go.Figure:
    df = safe_read("04_distribucion_geografica.csv")
    if df.empty:
        return go.Figure()

    top = df.head(20)
    fig = px.bar(
        top, x="total_empresas", y=COL_CAMARA,
        orientation="h", color="es_usta" if "es_usta" in top.columns else "total_empresas",
        color_discrete_map={True: COLORS["primary"], False: "#AABFD0"},
        labels={"total_empresas": "Empresas", COL_CAMARA: "Cámara de Comercio"},
        title="📍 Distribución geográfica — Top 20 cámaras",
    )
    fig.update_layout(
        template=PLOT_TEMPLATE, yaxis={"categoryorder": "total ascending"},
        showlegend=False,
    )
    return fig


# ── Figura 3: Supervivencia ───────────────────────────────────────────────────
def fig_supervivencia() -> go.Figure:
    global_df  = safe_read("05_supervivencia_global.csv")
    cohorte_df = safe_read("05_supervivencia_por_cohorte.csv")

    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "pie"}, {"type": "scatter"}]],
        subplot_titles=["Estado global", "Tasa de supervivencia por cohorte"],
    )

    if not global_df.empty:
        fig.add_trace(go.Pie(
            labels=global_df["estado"], values=global_df["empresas"],
            hole=0.45, marker_colors=[COLORS["primary"], COLORS["red"]],
            name="Estado",
        ), row=1, col=1)

    if not cohorte_df.empty:
        fig.add_trace(go.Scatter(
            x=cohorte_df["anio_matricula"].astype(int),
            y=cohorte_df["tasa_supervivencia_pct"],
            fill="tozeroy", mode="lines+markers",
            line=dict(color=COLORS["primary"], width=2.5),
            name="% Activas",
        ), row=1, col=2)

    fig.update_layout(
        title_text="📈 Supervivencia empresarial",
        template=PLOT_TEMPLATE, showlegend=False,
    )
    return fig


# ── Figura 4: Top sectores CIIU ───────────────────────────────────────────────
def fig_sectores() -> go.Figure:
    df = safe_read("03_impacto_sector_ciiu.csv")
    if df.empty:
        return go.Figure()

    # Agrupar por sección CIIU
    grp = df.groupby("seccion_ciiu")["empresas"].sum().reset_index()
    grp = grp.sort_values("empresas", ascending=True).tail(10)

    fig = px.bar(
        grp, x="empresas", y="seccion_ciiu", orientation="h",
        color="empresas", color_continuous_scale="Blues",
        title="🎓 Top 10 sectores CIIU — Empresas en la muestra",
        labels={"empresas": "N° empresas", "seccion_ciiu": "Sector CIIU"},
    )
    fig.update_layout(template=PLOT_TEMPLATE, coloraxis_showscale=False)
    return fig


# ── Figura 5: Tipo de empresa ─────────────────────────────────────────────────
def fig_tipo_empresa() -> go.Figure:
    org = safe_read("07_tipo_empresa_org.csv")
    soc = safe_read("07_tipo_empresa_sociedad.csv")

    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "pie"}, {"type": "bar"}]],
        subplot_titles=["Organización jurídica", "Tipo de sociedad (Top 8)"],
    )

    if not org.empty:
        fig.add_trace(go.Pie(
            labels=org.iloc[:, 0], values=org["empresas"],
            hole=0.4, marker_colors=COLOR_SEQ,
        ), row=1, col=1)

    if not soc.empty:
        top8 = soc.head(8)
        fig.add_trace(go.Bar(
            x=top8["porcentaje"], y=top8.iloc[:, 0],
            orientation="h", marker_color=COLORS["primary"],
        ), row=1, col=2)

    fig.update_layout(
        title_text="🏢 Tipo de empresa",
        template=PLOT_TEMPLATE, showlegend=False,
    )
    return fig


# ── Figura 6: Tasa de emprendimiento por cámara ───────────────────────────────
def fig_tasa_camara() -> go.Figure:
    df = safe_read("02_tasa_emprendimiento_camara.csv")
    if df.empty:
        return go.Figure()

    top = df.head(15)
    fig = px.treemap(
        top, path=[COL_CAMARA], values="total_matriculas",
        color="participacion_pct",
        color_continuous_scale="Blues",
        title="📊 Participación por cámara de comercio (treemap)",
        labels={"total_matriculas": "Matrículas", "participacion_pct": "% del total"},
    )
    fig.update_layout(template=PLOT_TEMPLATE)
    return fig


# ── Construcción del HTML ─────────────────────────────────────────────────────
def build_dashboard():
    figures = {
        "evolucion":     fig_evolucion(),
        "geografica":    fig_geografica(),
        "supervivencia": fig_supervivencia(),
        "sectores":      fig_sectores(),
        "tipo_empresa":  fig_tipo_empresa(),
        "tasa_camara":   fig_tasa_camara(),
    }

    # Convertir cada figura a HTML div
    divs = {}
    for key, fig in figures.items():
        if fig.data:
            divs[key] = pio.to_html(fig, full_html=False, include_plotlyjs=False)
        else:
            divs[key] = "<p style='color:#999;text-align:center'>Sin datos disponibles para este análisis.</p>"

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Dashboard RUES — Análisis de creación empresarial</title>
  <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: {PLOT_FONT_FAMILY};
      background: #F0F4F8;
      color: #2D3748;
    }}
    header {{
      background: {COLORS["primary"]};
      color: white;
      padding: 24px 40px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }}
    header h1 {{ font-size: 1.6rem; font-weight: 700; }}
    header p  {{ font-size: 0.85rem; opacity: 0.8; margin-top: 4px; }}
    .badge {{
      background: {COLORS["secondary"]};
      color: white;
      padding: 6px 14px;
      border-radius: 20px;
      font-size: 0.8rem;
      font-weight: 600;
    }}
    .container {{ max-width: 1400px; margin: 0 auto; padding: 30px 20px; }}
    .kpi-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 16px;
      margin-bottom: 30px;
    }}
    .kpi {{
      background: white;
      border-radius: 10px;
      padding: 20px;
      text-align: center;
      box-shadow: 0 2px 8px rgba(0,0,0,0.07);
      border-top: 4px solid {COLORS["primary"]};
    }}
    .kpi .number {{ font-size: 2rem; font-weight: 700; color: {COLORS["primary"]}; }}
    .kpi .label  {{ font-size: 0.8rem; color: #718096; margin-top: 4px; }}
    .grid-2 {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(580px, 1fr));
      gap: 20px;
      margin-bottom: 20px;
    }}
    .card {{
      background: white;
      border-radius: 10px;
      padding: 20px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.07);
    }}
    .card h2 {{
      font-size: 1rem;
      color: {COLORS["primary"]};
      margin-bottom: 12px;
      padding-bottom: 8px;
      border-bottom: 2px solid #EDF2F7;
    }}
    .full-width {{ grid-column: 1 / -1; }}
    footer {{
      text-align: center;
      color: #A0AEC0;
      font-size: 0.78rem;
      padding: 20px;
      margin-top: 10px;
    }}
  </style>
</head>
<body>
<header>
  <div>
    <h1>Dashboard RUES — Análisis de creación empresarial</h1>
    <p>Fuente: datos.gov.co · Socrata ID: c82u-588k · Suministra: CONFECAMARAS</p>
  </div>
  <span class="badge">Muestra: 10 000 registros</span>
</header>

<div class="container">

  <!-- KPI cards (se llenarán si los CSV existen) -->
  <div class="kpi-grid" id="kpi-grid">
    <div class="kpi"><div class="number" id="kpi-total">—</div><div class="label">Registros en muestra</div></div>
    <div class="kpi"><div class="number" id="kpi-activas">—</div><div class="label">Empresas activas</div></div>
    <div class="kpi"><div class="number" id="kpi-camaras">—</div><div class="label">Cámaras de comercio</div></div>
    <div class="kpi"><div class="number" id="kpi-anio-pico">—</div><div class="label">Año con más matrículas</div></div>
    <div class="kpi"><div class="number" id="kpi-top-sector">—</div><div class="label">Sector CIIU principal</div></div>
  </div>

  <!-- Evolución temporal (ancho completo) -->
  <div class="grid-2">
    <div class="card full-width">
      <h2>Evolución temporal</h2>
      {divs["evolucion"]}
    </div>
  </div>

  <!-- Geografía + Supervivencia -->
  <div class="grid-2">
    <div class="card">
      <h2>Distribución geográfica</h2>
      {divs["geografica"]}
    </div>
    <div class="card">
      <h2>Supervivencia empresarial</h2>
      {divs["supervivencia"]}
    </div>
  </div>

  <!-- Sectores + Tipo empresa -->
  <div class="grid-2">
    <div class="card">
      <h2>Impacto por sector CIIU</h2>
      {divs["sectores"]}
    </div>
    <div class="card">
      <h2>Tipo de empresa</h2>
      {divs["tipo_empresa"]}
    </div>
  </div>

  <!-- Treemap cámaras -->
  <div class="grid-2">
    <div class="card full-width">
      <h2>Tasa de emprendimiento por cámara</h2>
      {divs["tasa_camara"]}
    </div>
  </div>

</div>

<footer>
  Análisis exploratorio · Muestra de 10 000 registros del RUES (Colombia) · 2025
</footer>
</body>
</html>
"""
    out = OUTPUTS_REP / "dashboard_rues.html"
    out.write_text(html, encoding="utf-8")
    log.info(f"Dashboard guardado: {out}")
    return out


def main():
    out = build_dashboard()
    print(f"\n✅ Dashboard generado: {out}")
    print("   Ábrelo en tu navegador para ver los resultados interactivos.")


if __name__ == "__main__":
    main()
