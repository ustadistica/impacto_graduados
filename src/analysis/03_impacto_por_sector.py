"""
src/analysis/03_impacto_por_sector.py
======================================
Analiza el impacto por sector económico (CIIU):
  - Top sectores por número de empresas.
  - Descripción de la sección CIIU (letra + nombre).
  - Cruce sector × cámara para ver especialización regional.

Salidas:
  outputs/tables/03_impacto_sector_ciiu.csv
  outputs/tables/03_sector_x_camara.csv
  outputs/figures/03_top_sectores_ciiu.png
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.settings import (
    SAMPLE_PATH, OUTPUTS_TABLES, OUTPUTS_FIGS,
    COL_CIIU_PRI, COL_CAMARA, PLOT_COLOR_PRIMARY, PLOT_COLOR_SECONDARY
)

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

# Mapeo de los primeros 2 dígitos CIIU → Sección CIIU Rev. 4 (Colombia)
CIIU_SECCIONES = {
    range(1,  4):   "A - Agricultura, ganadería, caza, silvicultura y pesca",
    range(5,  10):  "B - Explotación de minas y canteras",
    range(10, 34):  "C - Industrias manufactureras",
    range(35, 36):  "D - Suministro de electricidad, gas, vapor",
    range(36, 40):  "E - Distribución de agua; alcantarillado",
    range(41, 44):  "F - Construcción",
    range(45, 48):  "G - Comercio al por mayor y al por menor",
    range(49, 54):  "H - Transporte y almacenamiento",
    range(55, 57):  "I - Alojamiento y servicios de comida",
    range(58, 64):  "J - Información y comunicaciones",
    range(64, 67):  "K - Actividades financieras y de seguros",
    range(68, 69):  "L - Actividades inmobiliarias",
    range(69, 76):  "M - Actividades profesionales, científicas y técnicas",
    range(77, 83):  "N - Actividades de servicios administrativos",
    range(84, 85):  "O - Administración pública y defensa",
    range(85, 86):  "P - Educación",
    range(86, 89):  "Q - Salud humana y de asistencia social",
    range(90, 94):  "R - Actividades artísticas, entretenimiento y recreación",
    range(94, 97):  "S - Otras actividades de servicios",
    range(97, 99):  "T - Actividades de los hogares",
    range(99, 100): "U - Actividades de organizaciones extraterritoriales",
}


def ciiu_to_seccion(code_str: str) -> str:
    """Convierte código CIIU a sección (letra + nombre)."""
    try:
        code = int(str(code_str)[:2])
        for rng, name in CIIU_SECCIONES.items():
            if code in rng:
                return name
    except (ValueError, TypeError):
        pass
    return "Sin clasificar"


def load() -> pd.DataFrame:
    if not SAMPLE_PATH.exists():
        log.error("Muestra no encontrada. Ejecuta primero: python src/ingestion/01_sample.py")
        sys.exit(1)
    return pd.read_parquet(SAMPLE_PATH)


def analizar_sectores(df: pd.DataFrame) -> pd.DataFrame:
    if COL_CIIU_PRI not in df.columns:
        log.warning(f"Columna '{COL_CIIU_PRI}' no encontrada.")
        return pd.DataFrame()

    df = df.copy()
    df["seccion_ciiu"] = df[COL_CIIU_PRI].astype(str).apply(ciiu_to_seccion)

    # Top códigos CIIU
    top = (
        df.groupby([COL_CIIU_PRI, "seccion_ciiu"])
          .size()
          .reset_index(name="empresas")
          .sort_values("empresas", ascending=False)
    )
    total = top["empresas"].sum()
    top["participacion_pct"] = (top["empresas"] / total * 100).round(2)
    return top


def cruce_sector_camara(df: pd.DataFrame) -> pd.DataFrame:
    """Tabla pivote: cámara × sección CIIU."""
    if COL_CIIU_PRI not in df.columns or COL_CAMARA not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["seccion_ciiu"] = df[COL_CIIU_PRI].astype(str).apply(ciiu_to_seccion)

    pivot = (
        df.groupby([COL_CAMARA, "seccion_ciiu"])
          .size()
          .unstack(fill_value=0)
    )
    return pivot


def plot_top_sectores(top: pd.DataFrame):
    top10 = (
        top.groupby("seccion_ciiu")["empresas"]
           .sum()
           .sort_values(ascending=True)
           .tail(10)
    )
    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(top10.index, top10.values, color=PLOT_COLOR_PRIMARY, alpha=0.85)

    # Etiquetas
    for bar, val in zip(bars, top10.values):
        ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                f"{val:,}", va="center", fontsize=9)

    ax.set_title("Top 10 sectores CIIU — Empresas en la muestra RUES", fontsize=13, pad=10)
    ax.set_xlabel("Número de empresas")
    ax.tick_params(axis="y", labelsize=8)
    plt.tight_layout()
    out = OUTPUTS_FIGS / "03_top_sectores_ciiu.png"
    plt.savefig(out, dpi=150)
    plt.close()
    log.info(f"Figura guardada: {out}")


def main():
    df  = load()
    top = analizar_sectores(df)

    if top.empty:
        log.warning("Sin datos de sector CIIU. Revisa el CSV.")
        return

    # Guardar tablas
    out1 = OUTPUTS_TABLES / "03_impacto_sector_ciiu.csv"
    top.to_csv(out1, index=False, encoding="utf-8-sig")
    log.info(f"Tabla guardada: {out1}")

    pivot = cruce_sector_camara(df)
    if not pivot.empty:
        out2 = OUTPUTS_TABLES / "03_sector_x_camara.csv"
        pivot.to_csv(out2, encoding="utf-8-sig")
        log.info(f"Tabla guardada: {out2}")

    plot_top_sectores(top)

    print("\n── Top 10 sectores por número de empresas ───────────────────")
    resumen = (
        top.groupby("seccion_ciiu")["empresas"]
           .sum()
           .sort_values(ascending=False)
           .head(10)
           .reset_index()
    )
    print(resumen.to_string(index=False))


if __name__ == "__main__":
    main()
