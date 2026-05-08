"""
src/analysis/07_tipo_empresa.py
================================
Analiza el tipo de empresa creada:
  - Distribución por organización jurídica (persona natural, SAS, ESAL, etc.).
  - Distribución por tipo de sociedad.
  - Categoría de matrícula.
  - Evolución del tipo predominante por año.

Salidas:
  outputs/tables/07_tipo_empresa_org.csv
  outputs/tables/07_tipo_empresa_sociedad.csv
  outputs/figures/07_tipo_empresa_donut.png
  outputs/figures/07_tipo_sociedad_barras.png
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.settings import (
    SAMPLE_PATH, OUTPUTS_TABLES, OUTPUTS_FIGS,
    COL_ORG_JURIDICA, COL_TIPO_SOCIEDAD, COL_CATEGORIA,
    PLOT_COLOR_PRIMARY
)

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

# Paleta para el donut
PALETTE = [
    "#1B6CA8", "#F4A223", "#2ECC71", "#E74C3C", "#9B59B6",
    "#1ABC9C", "#E67E22", "#34495E", "#D35400", "#27AE60"
]


def load() -> pd.DataFrame:
    if not SAMPLE_PATH.exists():
        log.error("Muestra no encontrada. Ejecuta primero: python src/ingestion/01_sample.py")
        sys.exit(1)
    return pd.read_parquet(SAMPLE_PATH)


def distribucion_org(df: pd.DataFrame) -> pd.DataFrame:
    if COL_ORG_JURIDICA not in df.columns:
        return pd.DataFrame()

    cnt = (
        df[COL_ORG_JURIDICA]
        .value_counts()
        .reset_index()
    )
    cnt.columns = ["organizacion_juridica", "empresas"]
    total = cnt["empresas"].sum()
    cnt["porcentaje"] = (cnt["empresas"] / total * 100).round(2)
    return cnt


def distribucion_sociedad(df: pd.DataFrame) -> pd.DataFrame:
    if COL_TIPO_SOCIEDAD not in df.columns:
        return pd.DataFrame()

    cnt = (
        df[COL_TIPO_SOCIEDAD]
        .value_counts()
        .reset_index()
    )
    cnt.columns = ["tipo_sociedad", "empresas"]
    total = cnt["empresas"].sum()
    cnt["porcentaje"] = (cnt["empresas"] / total * 100).round(2)
    return cnt


def distribucion_categoria(df: pd.DataFrame) -> pd.DataFrame:
    if COL_CATEGORIA not in df.columns:
        return pd.DataFrame()

    cnt = df[COL_CATEGORIA].value_counts().reset_index()
    cnt.columns = ["categoria_matricula", "empresas"]
    total = cnt["empresas"].sum()
    cnt["porcentaje"] = (cnt["empresas"] / total * 100).round(2)
    return cnt


def plot_donut_org(org: pd.DataFrame):
    # Agrupar tipos con menos del 2% en "Otros"
    umbral = org["empresas"].sum() * 0.02
    main   = org[org["empresas"] >= umbral].copy()
    otros  = org[org["empresas"] < umbral]["empresas"].sum()
    if otros > 0:
        main = pd.concat([
            main,
            pd.DataFrame([{"organizacion_juridica": "OTROS", "empresas": otros,
                           "porcentaje": round(otros / org["empresas"].sum() * 100, 2)}])
        ], ignore_index=True)

    fig, ax = plt.subplots(figsize=(9, 7))
    wedges, texts, autotexts = ax.pie(
        main["empresas"],
        labels=main["organizacion_juridica"],
        colors=PALETTE[:len(main)],
        autopct=lambda p: f"{p:.1f}%" if p > 3 else "",
        startangle=90,
        wedgeprops={"linewidth": 2, "edgecolor": "white"},
        pctdistance=0.80,
    )
    # Hacer donut
    centre_circle = plt.Circle((0, 0), 0.55, fc="white")
    ax.add_artist(centre_circle)

    for at in autotexts:
        at.set_fontsize(9)

    ax.set_title("Tipo de organización jurídica — Muestra RUES", fontsize=13, pad=15)
    plt.tight_layout()
    out = OUTPUTS_FIGS / "07_tipo_empresa_donut.png"
    plt.savefig(out, dpi=150)
    plt.close()
    log.info(f"Figura guardada: {out}")


def plot_barras_sociedad(soc: pd.DataFrame):
    if soc.empty:
        return

    top = soc.head(10)
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(top["tipo_sociedad"], top["porcentaje"],
                   color=PLOT_COLOR_PRIMARY, alpha=0.85)
    for bar, val in zip(bars, top["porcentaje"]):
        ax.text(bar.get_width() + 0.2, bar.get_y() + bar.get_height() / 2,
                f"{val:.1f}%", va="center", fontsize=9)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter())
    ax.set_title("Tipo de sociedad — Top 10 (% del total)", fontsize=12, pad=10)
    ax.set_xlabel("% de empresas")
    ax.invert_yaxis()
    plt.tight_layout()
    out = OUTPUTS_FIGS / "07_tipo_sociedad_barras.png"
    plt.savefig(out, dpi=150)
    plt.close()
    log.info(f"Figura guardada: {out}")


def main():
    df  = load()
    org = distribucion_org(df)
    soc = distribucion_sociedad(df)
    cat = distribucion_categoria(df)

    if not org.empty:
        out1 = OUTPUTS_TABLES / "07_tipo_empresa_org.csv"
        org.to_csv(out1, index=False, encoding="utf-8-sig")
        log.info(f"Tabla guardada: {out1}")
        plot_donut_org(org)

    if not soc.empty:
        out2 = OUTPUTS_TABLES / "07_tipo_empresa_sociedad.csv"
        soc.to_csv(out2, index=False, encoding="utf-8-sig")
        log.info(f"Tabla guardada: {out2}")
        plot_barras_sociedad(soc)

    if not cat.empty:
        out3 = OUTPUTS_TABLES / "07_categoria_matricula.csv"
        cat.to_csv(out3, index=False, encoding="utf-8-sig")
        log.info(f"Tabla guardada: {out3}")

    print("\n── Organización jurídica ─────────────────────────────────────")
    if not org.empty:
        print(org.to_string(index=False))
    print("\n── Tipo de sociedad — Top 10 ─────────────────────────────────")
    if not soc.empty:
        print(soc.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
