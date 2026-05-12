"""
src/04_supervivencia.py
========================
Indicador 4 — Supervivencia empresarial.

Calcula:
  · Tasa de activas vs canceladas global y por cohorte de año de matrícula
  · "Curva de supervivencia": % de activas que quedan según antigüedad
  · Comparación por tipo de organización jurídica

Salidas:
  outputs/tables/04_supervivencia_cohorte.csv
  outputs/figures/04a_curva_supervivencia.png
  outputs/figures/04b_supervivencia_por_tipo.png
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

from config.settings import (
    SAMPLE_PATH, DIR_FIGURES, DIR_TABLES,
    COL_ESTADO, COL_ORG_JUR,
    PALETTE_POS, PALETTE_NEG, PALETTE_MAIN, PALETTE_ACCENT,
    YEAR_MIN, YEAR_MAX, FIG_DPI,
)

plt.rcParams.update({"font.family": "DejaVu Sans",
                     "axes.spines.top": False,
                     "axes.spines.right": False})

# ── Carga ──────────────────────────────────────────────────────────────────────
df = pd.read_csv(SAMPLE_PATH, low_memory=False)
df[COL_ESTADO] = df[COL_ESTADO].str.strip().str.upper()
df = df[df["ANIO_MATRICULA"].notna()]
df["ANIO_MATRICULA"] = df["ANIO_MATRICULA"].astype(int)
df = df[(df["ANIO_MATRICULA"] >= YEAR_MIN) & (df["ANIO_MATRICULA"] <= YEAR_MAX)]

# ── 4A · Curva de supervivencia por cohorte ───────────────────────────────────
cohorte = (
    df.groupby("ANIO_MATRICULA")
    .agg(
        total=("ESTADO_MATRICULA", "count"),
        activas=("ESTADO_MATRICULA", lambda x: (x == "ACTIVA").sum()),
    )
    .assign(tasa=lambda d: d["activas"] / d["total"] * 100)
    .reset_index()
)

fig, ax = plt.subplots(figsize=(13, 5))
ax.fill_between(cohorte["ANIO_MATRICULA"], cohorte["tasa"],
                alpha=0.15, color=PALETTE_POS)
ax.plot(cohorte["ANIO_MATRICULA"], cohorte["tasa"],
        color=PALETTE_POS, linewidth=2.5, marker="o", markersize=4)

# Línea de referencia promedio
avg = cohorte["tasa"].mean()
ax.axhline(avg, color=PALETTE_ACCENT, linestyle="--", linewidth=1.5, alpha=0.8,
           label=f"Promedio general: {avg:.1f}%")

ax.yaxis.set_major_formatter(mtick.PercentFormatter())
ax.set_xlabel("Año de matrícula", fontsize=11)
ax.set_ylabel("% empresas activas", fontsize=11)
ax.set_title("Curva de supervivencia empresarial por cohorte de matrícula\n(% de registros aún activos por año de apertura)",
             fontsize=13, fontweight="bold")
ax.legend(fontsize=10)
ax.set_xlim(YEAR_MIN, YEAR_MAX)
plt.tight_layout()
fig.savefig(DIR_FIGURES / "04a_curva_supervivencia.png", dpi=FIG_DPI)
plt.close()
print("✅  04a_curva_supervivencia.png")

# ── 4B · Supervivencia por tipo de organización jurídica ──────────────────────
if COL_ORG_JUR in df.columns:
    df[COL_ORG_JUR] = df[COL_ORG_JUR].str.strip().str.title()
    por_tipo = (
        df.groupby(COL_ORG_JUR)
        .agg(total=("ESTADO_MATRICULA", "count"),
             activas=("ESTADO_MATRICULA", lambda x: (x == "ACTIVA").sum()))
        .assign(tasa=lambda d: d["activas"] / d["total"] * 100)
        .query("total >= 20")
        .sort_values("tasa", ascending=True)
        .tail(12)
        .reset_index()
    )

    colors = [PALETTE_POS if t >= avg else PALETTE_NEG for t in por_tipo["tasa"]]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.barh(por_tipo[COL_ORG_JUR], por_tipo["tasa"], color=colors, alpha=0.85, edgecolor="white")
    ax.axvline(avg, color=PALETTE_ACCENT, linestyle="--", linewidth=1.5,
               label=f"Promedio: {avg:.1f}%")
    ax.xaxis.set_major_formatter(mtick.PercentFormatter())
    for i, row in por_tipo.iterrows():
        ax.text(row["tasa"] + 0.5, i, f"{row['total']:,} registros", va="center", fontsize=8)
    ax.set_title("Tasa de supervivencia por tipo de organización jurídica",
                 fontsize=13, fontweight="bold")
    ax.set_xlabel("% empresas activas", fontsize=11)
    ax.legend(fontsize=10)
    plt.tight_layout()
    fig.savefig(DIR_FIGURES / "04b_supervivencia_por_tipo.png", dpi=FIG_DPI)
    plt.close()
    print("✅  04b_supervivencia_por_tipo.png")
else:
    print("⚠️  Columna ORGANIZACION_JURIDICA no encontrada — saltando 04b")

# ── Tablas ────────────────────────────────────────────────────────────────────
cohorte.to_csv(DIR_TABLES / "04_supervivencia_cohorte.csv", index=False)
print("✅  tabla guardada")