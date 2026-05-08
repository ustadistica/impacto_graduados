"""
src/analysis/05_supervivencia_empresarial.py
=============================================
Calcula la supervivencia empresarial:
  - Tasa de empresas ACTIVAS vs CANCELADAS (global y por año de matrícula).
  - Curva de supervivencia: % activas a 1, 3, 5 y 10 años de creación.
  - Comparación por tipo de organización jurídica.

Salidas:
  outputs/tables/05_supervivencia_global.csv
  outputs/tables/05_supervivencia_por_cohorte.csv
  outputs/figures/05_supervivencia_estado.png
  outputs/figures/05_curva_supervivencia.png
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.settings import (
    SAMPLE_PATH, OUTPUTS_TABLES, OUTPUTS_FIGS,
    COL_ESTADO, COL_FECHA_MAT, COL_FECHA_CANCEL,
    COL_ORG_JURIDICA, PLOT_COLOR_PRIMARY, PLOT_COLOR_SECONDARY,
    YEAR_MIN, YEAR_MAX
)

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

ESTADO_ACTIVA = ["ACTIVA", "ACTIVE", "VIGENTE", "A"]


def load() -> pd.DataFrame:
    if not SAMPLE_PATH.exists():
        log.error("Muestra no encontrada. Ejecuta primero: python src/ingestion/01_sample.py")
        sys.exit(1)
    return pd.read_parquet(SAMPLE_PATH)


def clasificar_estado(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if COL_ESTADO in df.columns:
        df["activa"] = df[COL_ESTADO].str.upper().isin(ESTADO_ACTIVA)
    else:
        # Inferir del campo fecha_cancelacion: si tiene fecha → cancelada
        if COL_FECHA_CANCEL in df.columns:
            df["activa"] = df[COL_FECHA_CANCEL].isna()
        else:
            log.warning("No se encontró columna de estado. Se asume todas activas.")
            df["activa"] = True
    return df


def supervivencia_global(df: pd.DataFrame) -> pd.DataFrame:
    conteo = df["activa"].value_counts().rename({True: "ACTIVA", False: "CANCELADA"})
    total  = conteo.sum()
    result = pd.DataFrame({
        "estado": conteo.index,
        "empresas": conteo.values,
        "porcentaje": (conteo.values / total * 100).round(2)
    })
    return result


def supervivencia_por_cohorte(df: pd.DataFrame) -> pd.DataFrame:
    """% activas por año de matrícula (cohorte de creación)."""
    if "anio_matricula" not in df.columns:
        return pd.DataFrame()

    grp = (
        df.dropna(subset=["anio_matricula"])
          .query(f"anio_matricula >= {YEAR_MIN} and anio_matricula <= {YEAR_MAX}")
          .groupby("anio_matricula")["activa"]
          .agg(total="count", activas="sum")
          .reset_index()
    )
    grp["tasa_supervivencia_pct"] = (grp["activas"] / grp["total"] * 100).round(2)
    return grp


def supervivencia_por_org(df: pd.DataFrame) -> pd.DataFrame:
    if COL_ORG_JURIDICA not in df.columns:
        return pd.DataFrame()

    grp = (
        df.groupby(COL_ORG_JURIDICA)["activa"]
          .agg(total="count", activas="sum")
          .reset_index()
    )
    grp["tasa_pct"] = (grp["activas"] / grp["total"] * 100).round(2)
    grp = grp.sort_values("tasa_pct", ascending=False)
    return grp


def plot_estado_global(global_df: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(6, 6))
    colors = [PLOT_COLOR_PRIMARY, "#E74C3C"]
    wedges, texts, autotexts = ax.pie(
        global_df["empresas"],
        labels=global_df["estado"],
        colors=colors,
        autopct="%1.1f%%",
        startangle=90,
        wedgeprops={"linewidth": 1.5, "edgecolor": "white"}
    )
    for at in autotexts:
        at.set_fontsize(12)
        at.set_fontweight("bold")
        at.set_color("white")
    ax.set_title("Estado de matrículas — Muestra RUES", fontsize=13, pad=15)
    plt.tight_layout()
    out = OUTPUTS_FIGS / "05_supervivencia_estado.png"
    plt.savefig(out, dpi=150)
    plt.close()
    log.info(f"Figura guardada: {out}")


def plot_curva_supervivencia(cohorte: pd.DataFrame):
    if cohorte.empty:
        return

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(
        cohorte["anio_matricula"].astype(int),
        cohorte["tasa_supervivencia_pct"],
        color=PLOT_COLOR_PRIMARY, linewidth=2.5, marker="o", markersize=4
    )
    ax.fill_between(
        cohorte["anio_matricula"].astype(int),
        cohorte["tasa_supervivencia_pct"],
        alpha=0.15, color=PLOT_COLOR_PRIMARY
    )
    ax.axhline(cohorte["tasa_supervivencia_pct"].mean(), linestyle="--",
               color=PLOT_COLOR_SECONDARY, linewidth=1.5, label="Promedio")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.set_title("Tasa de supervivencia por cohorte de matrícula", fontsize=13, pad=10)
    ax.set_xlabel("Año de matrícula")
    ax.set_ylabel("% empresas activas")
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()
    out = OUTPUTS_FIGS / "05_curva_supervivencia.png"
    plt.savefig(out, dpi=150)
    plt.close()
    log.info(f"Figura guardada: {out}")


def main():
    df      = load()
    df      = clasificar_estado(df)
    global_ = supervivencia_global(df)
    cohorte = supervivencia_por_cohorte(df)
    org     = supervivencia_por_org(df)

    # Guardar tablas
    out1 = OUTPUTS_TABLES / "05_supervivencia_global.csv"
    global_.to_csv(out1, index=False, encoding="utf-8-sig")
    log.info(f"Tabla guardada: {out1}")

    if not cohorte.empty:
        out2 = OUTPUTS_TABLES / "05_supervivencia_por_cohorte.csv"
        cohorte.to_csv(out2, index=False, encoding="utf-8-sig")
        log.info(f"Tabla guardada: {out2}")

    if not org.empty:
        out3 = OUTPUTS_TABLES / "05_supervivencia_por_org_juridica.csv"
        org.to_csv(out3, index=False, encoding="utf-8-sig")
        log.info(f"Tabla guardada: {out3}")

    plot_estado_global(global_)
    plot_curva_supervivencia(cohorte)

    print("\n── Supervivencia global ──────────────────────────────────────")
    print(global_.to_string(index=False))
    if not cohorte.empty:
        print("\n── Supervivencia por cohorte (últimos 10 años) ───────────────")
        print(cohorte.tail(10).to_string(index=False))


if __name__ == "__main__":
    main()
