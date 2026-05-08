"""
src/analysis/06_evolucion_temporal.py
======================================
Analiza la evolución temporal del emprendimiento:
  - Matrículas nuevas por año (2000–2024).
  - Crecimiento interanual (%).
  - Tendencia con media móvil de 3 años.
  - Desagregado por tipo de organización jurídica.

Salidas:
  outputs/tables/06_evolucion_temporal.csv
  outputs/tables/06_evolucion_por_org.csv
  outputs/figures/06_evolucion_temporal.png
  outputs/figures/06_evolucion_org_juridica.png
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.settings import (
    SAMPLE_PATH, OUTPUTS_TABLES, OUTPUTS_FIGS,
    COL_ORG_JURIDICA, PLOT_COLOR_PRIMARY, PLOT_COLOR_SECONDARY,
    YEAR_MIN, YEAR_MAX
)

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)


def load() -> pd.DataFrame:
    if not SAMPLE_PATH.exists():
        log.error("Muestra no encontrada. Ejecuta primero: python src/ingestion/01_sample.py")
        sys.exit(1)
    return pd.read_parquet(SAMPLE_PATH)


def evolucion_anual(df: pd.DataFrame) -> pd.DataFrame:
    if "anio_matricula" not in df.columns:
        return pd.DataFrame()

    serie = (
        df.dropna(subset=["anio_matricula"])
          .query(f"anio_matricula >= {YEAR_MIN} and anio_matricula <= {YEAR_MAX}")
          .groupby("anio_matricula")
          .size()
          .reset_index(name="matriculas")
          .sort_values("anio_matricula")
    )
    serie["crecimiento_pct"] = serie["matriculas"].pct_change() * 100
    serie["media_movil_3a"]  = serie["matriculas"].rolling(3, center=True).mean()
    return serie


def evolucion_por_org(df: pd.DataFrame) -> pd.DataFrame:
    if "anio_matricula" not in df.columns or COL_ORG_JURIDICA not in df.columns:
        return pd.DataFrame()

    pivot = (
        df.dropna(subset=["anio_matricula", COL_ORG_JURIDICA])
          .query(f"anio_matricula >= {YEAR_MIN} and anio_matricula <= {YEAR_MAX}")
          .groupby(["anio_matricula", COL_ORG_JURIDICA])
          .size()
          .unstack(fill_value=0)
    )
    # Conservar solo los top-5 tipos más frecuentes
    top5 = pivot.sum().nlargest(5).index
    return pivot[top5]


def plot_evolucion(serie: pd.DataFrame):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 8), sharex=True,
                                    gridspec_kw={"height_ratios": [3, 1]})

    # Panel superior: barras + media móvil
    ax1.bar(serie["anio_matricula"].astype(int), serie["matriculas"],
            color=PLOT_COLOR_PRIMARY, alpha=0.75, width=0.7, label="Matrículas")
    ax1.plot(serie["anio_matricula"].astype(int), serie["media_movil_3a"],
             color=PLOT_COLOR_SECONDARY, linewidth=2.5, label="Media móvil 3 años")
    ax1.set_ylabel("Nuevas matrículas")
    ax1.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax1.legend()
    ax1.grid(axis="y", linestyle="--", alpha=0.4)
    ax1.set_title("Evolución temporal del emprendimiento — Muestra RUES (2000–2024)", fontsize=13, pad=10)

    # Panel inferior: crecimiento interanual
    crecimiento = serie.dropna(subset=["crecimiento_pct"])
    colors = [PLOT_COLOR_PRIMARY if v >= 0 else "#E74C3C" for v in crecimiento["crecimiento_pct"]]
    ax2.bar(crecimiento["anio_matricula"].astype(int), crecimiento["crecimiento_pct"],
            color=colors, alpha=0.8, width=0.7)
    ax2.axhline(0, color="black", linewidth=0.8)
    ax2.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax2.set_ylabel("Crecimiento\ninteranual")
    ax2.set_xlabel("Año")
    ax2.tick_params(axis="x", rotation=45)
    ax2.grid(axis="y", linestyle="--", alpha=0.4)

    plt.tight_layout()
    out = OUTPUTS_FIGS / "06_evolucion_temporal.png"
    plt.savefig(out, dpi=150)
    plt.close()
    log.info(f"Figura guardada: {out}")


def plot_evolucion_org(pivot: pd.DataFrame):
    if pivot.empty:
        return

    fig, ax = plt.subplots(figsize=(13, 6))
    colormap = plt.get_cmap("tab10")
    for i, col in enumerate(pivot.columns):
        ax.plot(pivot.index.astype(int), pivot[col],
                label=col, linewidth=2, marker="o", markersize=3,
                color=colormap(i))

    ax.set_title("Evolución por tipo de organización jurídica", fontsize=13, pad=10)
    ax.set_xlabel("Año")
    ax.set_ylabel("Matrículas")
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.legend(fontsize=8, loc="upper left")
    ax.grid(linestyle="--", alpha=0.4)
    ax.tick_params(axis="x", rotation=45)
    plt.tight_layout()
    out = OUTPUTS_FIGS / "06_evolucion_org_juridica.png"
    plt.savefig(out, dpi=150)
    plt.close()
    log.info(f"Figura guardada: {out}")


def main():
    df     = load()
    serie  = evolucion_anual(df)
    pivot  = evolucion_por_org(df)

    if serie.empty:
        log.warning("Sin datos temporales.")
        return

    out1 = OUTPUTS_TABLES / "06_evolucion_temporal.csv"
    serie.to_csv(out1, index=False, encoding="utf-8-sig")
    log.info(f"Tabla guardada: {out1}")

    if not pivot.empty:
        out2 = OUTPUTS_TABLES / "06_evolucion_por_org.csv"
        pivot.to_csv(out2, encoding="utf-8-sig")
        log.info(f"Tabla guardada: {out2}")

    plot_evolucion(serie)
    plot_evolucion_org(pivot)

    print("\n── Evolución temporal (últimos 10 años) ──────────────────────")
    print(serie.tail(10).to_string(index=False))


if __name__ == "__main__":
    main()
