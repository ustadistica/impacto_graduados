"""
src/analysis/04_distribucion_geografica.py
===========================================
Distribución geográfica de empresas por cámara de comercio:
  - Total de empresas por ciudad/cámara.
  - Participación porcentual.
  - Gráfico de barras horizontal + mapa de calor por año × cámara.

Salidas:
  outputs/tables/04_distribucion_geografica.csv
  outputs/tables/04_camara_x_anio.csv
  outputs/figures/04_distribucion_geografica.png
  outputs/figures/04_heatmap_camara_anio.png
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.settings import (
    SAMPLE_PATH, OUTPUTS_TABLES, OUTPUTS_FIGS,
    COL_CAMARA, PLOT_COLOR_PRIMARY, CAMARAS_USTA,
    YEAR_MIN, YEAR_MAX
)

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)


def load() -> pd.DataFrame:
    if not SAMPLE_PATH.exists():
        log.error("Muestra no encontrada. Ejecuta primero: python src/ingestion/01_sample.py")
        sys.exit(1)
    return pd.read_parquet(SAMPLE_PATH)


def distribucion_por_camara(df: pd.DataFrame) -> pd.DataFrame:
    if COL_CAMARA not in df.columns:
        log.warning(f"Columna '{COL_CAMARA}' no encontrada.")
        return pd.DataFrame()

    cam = (
        df.groupby(COL_CAMARA)
          .size()
          .reset_index(name="total_empresas")
          .sort_values("total_empresas", ascending=False)
    )
    total = cam["total_empresas"].sum()
    cam["participacion_pct"] = (cam["total_empresas"] / total * 100).round(2)
    cam["es_usta"] = cam[COL_CAMARA].isin(CAMARAS_USTA)
    return cam


def camara_por_anio(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot cámara × año para el heatmap."""
    if COL_CAMARA not in df.columns or "anio_matricula" not in df.columns:
        return pd.DataFrame()

    pivot = (
        df.dropna(subset=["anio_matricula"])
          .query(f"anio_matricula >= {YEAR_MIN} and anio_matricula <= {YEAR_MAX}")
          .groupby([COL_CAMARA, "anio_matricula"])
          .size()
          .unstack(fill_value=0)
    )
    # Ordenar por total
    pivot = pivot.loc[pivot.sum(axis=1).sort_values(ascending=False).index]
    return pivot


def plot_distribucion(cam: pd.DataFrame):
    top = cam.head(20)
    colors = [PLOT_COLOR_PRIMARY if r else "#AABFD0" for r in top["es_usta"]]

    fig, ax = plt.subplots(figsize=(10, 8))
    bars = ax.barh(top[COL_CAMARA], top["total_empresas"], color=colors, alpha=0.88)

    for bar, val in zip(bars, top["total_empresas"]):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                f"{val:,}", va="center", fontsize=8)

    ax.set_title("Distribución geográfica — Empresas por cámara de comercio\n(azul oscuro = sede USTA)",
                 fontsize=12, pad=10)
    ax.set_xlabel("Número de empresas en la muestra")
    ax.invert_yaxis()
    plt.tight_layout()
    out = OUTPUTS_FIGS / "04_distribucion_geografica.png"
    plt.savefig(out, dpi=150)
    plt.close()
    log.info(f"Figura guardada: {out}")


def plot_heatmap(pivot: pd.DataFrame):
    if pivot.empty or pivot.shape[0] < 2:
        return

    top_pivot = pivot.head(15)  # Top 15 cámaras
    data = top_pivot.values.astype(float)
    data_norm = np.log1p(data)  # Escala logarítmica para mejor visualización

    fig, ax = plt.subplots(figsize=(14, 7))
    im = ax.imshow(data_norm, aspect="auto", cmap="YlOrRd")

    ax.set_xticks(range(len(top_pivot.columns)))
    ax.set_xticklabels(top_pivot.columns.astype(int), rotation=45, ha="right", fontsize=7)
    ax.set_yticks(range(len(top_pivot.index)))
    ax.set_yticklabels(top_pivot.index, fontsize=7)

    ax.set_title("Heatmap: matrículas por cámara y año (escala log)", fontsize=12, pad=10)
    plt.colorbar(im, ax=ax, label="log(1 + empresas)")
    plt.tight_layout()
    out = OUTPUTS_FIGS / "04_heatmap_camara_anio.png"
    plt.savefig(out, dpi=150)
    plt.close()
    log.info(f"Figura guardada: {out}")


def main():
    df    = load()
    cam   = distribucion_por_camara(df)
    pivot = camara_por_anio(df)

    if cam.empty:
        log.warning("Sin datos geográficos.")
        return

    out1 = OUTPUTS_TABLES / "04_distribucion_geografica.csv"
    cam.to_csv(out1, index=False, encoding="utf-8-sig")
    log.info(f"Tabla guardada: {out1}")

    if not pivot.empty:
        out2 = OUTPUTS_TABLES / "04_camara_x_anio.csv"
        pivot.to_csv(out2, encoding="utf-8-sig")
        log.info(f"Tabla guardada: {out2}")

    plot_distribucion(cam)
    plot_heatmap(pivot)

    print("\n── Distribución geográfica — Top 15 cámaras ────────────────")
    print(cam.head(15).to_string(index=False))


if __name__ == "__main__":
    main()
