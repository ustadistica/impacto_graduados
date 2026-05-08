"""
src/analysis/02_tasa_emprendimiento.py
=======================================
Calcula la «tasa de emprendimiento» general:
  - Número de matrículas nuevas por año (proxy de creación empresarial).
  - Desagregado por cámara de comercio.
  - Tasa acumulada relativa al total de registros de la muestra.

Salidas:
  outputs/tables/02_tasa_emprendimiento.csv
  outputs/tables/02_tasa_emprendimiento_camara.csv
  outputs/figures/02_tasa_emprendimiento.png
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.settings import (
    SAMPLE_PATH, OUTPUTS_TABLES, OUTPUTS_FIGS,
    COL_CAMARA, PLOT_COLOR_PRIMARY, PLOT_TEMPLATE, PLOT_FONT_FAMILY
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


def tasa_por_anio(df: pd.DataFrame) -> pd.DataFrame:
    """Matrículas nuevas por año."""
    serie = (
        df.dropna(subset=["anio_matricula"])
          .groupby("anio_matricula")
          .size()
          .reset_index(name="nuevas_matriculas")
          .sort_values("anio_matricula")
    )
    total = serie["nuevas_matriculas"].sum()
    serie["tasa_relativa_pct"] = (serie["nuevas_matriculas"] / total * 100).round(2)
    serie["acumulado"] = serie["nuevas_matriculas"].cumsum()
    return serie


def tasa_por_camara(df: pd.DataFrame) -> pd.DataFrame:
    """Matrículas por cámara de comercio."""
    if COL_CAMARA not in df.columns:
        log.warning(f"Columna '{COL_CAMARA}' no encontrada.")
        return pd.DataFrame()

    cam = (
        df.groupby(COL_CAMARA)
          .size()
          .reset_index(name="total_matriculas")
          .sort_values("total_matriculas", ascending=False)
    )
    total = cam["total_matriculas"].sum()
    cam["participacion_pct"] = (cam["total_matriculas"] / total * 100).round(2)
    return cam


def plot_tasa_anio(serie: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(
        serie["anio_matricula"].astype(int),
        serie["nuevas_matriculas"],
        color=PLOT_COLOR_PRIMARY, alpha=0.85, width=0.7
    )
    ax.set_title("Matrículas nuevas por año (muestra RUES)", fontsize=14, pad=12)
    ax.set_xlabel("Año")
    ax.set_ylabel("Nuevas matrículas")
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.tick_params(axis="x", rotation=45)
    plt.tight_layout()
    out = OUTPUTS_FIGS / "02_tasa_emprendimiento.png"
    plt.savefig(out, dpi=150)
    plt.close()
    log.info(f"Figura guardada: {out}")


def main():
    df    = load()
    serie = tasa_por_anio(df)
    camara = tasa_por_camara(df)

    # Guardar tablas
    out1 = OUTPUTS_TABLES / "02_tasa_emprendimiento.csv"
    serie.to_csv(out1, index=False, encoding="utf-8-sig")
    log.info(f"Tabla guardada: {out1}")

    if not camara.empty:
        out2 = OUTPUTS_TABLES / "02_tasa_emprendimiento_camara.csv"
        camara.to_csv(out2, index=False, encoding="utf-8-sig")
        log.info(f"Tabla guardada: {out2}")

    # Gráfico
    plot_tasa_anio(serie)

    # Resumen en consola
    print("\n── Tasa de emprendimiento — Top 10 años ─────────────────────")
    print(serie.sort_values("nuevas_matriculas", ascending=False).head(10).to_string(index=False))


if __name__ == "__main__":
    main()
