"""
src/05_evolucion_temporal.py
=============================
Indicador 5 — Evolución temporal del emprendimiento.

Calcula:
  · Número de matrículas nuevas por año (2000–2024)
  · Crecimiento acumulado
  · Top-5 ciudades: evolución temporal comparada
  · Heatmap ciudad × año

Salidas:
  outputs/tables/05_evolucion_anual.csv
  outputs/figures/05a_matriculas_por_anio.png
  outputs/figures/05b_top5_ciudades_temporal.png
  outputs/figures/05c_heatmap_ciudad_anio.png
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns

from config.settings import (
    SAMPLE_PATH, DIR_FIGURES, DIR_TABLES,
    COL_CAMARA, PALETTE_MAIN, PALETTE_SEC, PALETTE_ACCENT,
    YEAR_MIN, YEAR_MAX, FIG_DPI,
)

plt.rcParams.update({"font.family": "DejaVu Sans",
                     "axes.spines.top": False,
                     "axes.spines.right": False})

# ── Carga ──────────────────────────────────────────────────────────────────────
df = pd.read_csv(SAMPLE_PATH, low_memory=False)
df = df[df["ANIO_MATRICULA"].notna()].copy()
df["ANIO_MATRICULA"] = df["ANIO_MATRICULA"].astype(int)
df = df[(df["ANIO_MATRICULA"] >= YEAR_MIN) & (df["ANIO_MATRICULA"] <= YEAR_MAX)]

anual = (
    df.groupby("ANIO_MATRICULA")
    .size()
    .reset_index(name="nuevas_matriculas")
    .sort_values("ANIO_MATRICULA")
)
anual["acumulado"] = anual["nuevas_matriculas"].cumsum()
anual["crecimiento_pct"] = anual["nuevas_matriculas"].pct_change() * 100

# ── 5A · Barras + línea acumulado ─────────────────────────────────────────────
fig, ax1 = plt.subplots(figsize=(14, 5))
ax2 = ax1.twinx()

ax1.bar(anual["ANIO_MATRICULA"], anual["nuevas_matriculas"],
        color=PALETTE_MAIN, alpha=0.75, label="Nuevas matrículas")
ax2.plot(anual["ANIO_MATRICULA"], anual["acumulado"],
         color=PALETTE_ACCENT, linewidth=2.5, linestyle="--", label="Acumulado")

ax1.set_xlabel("Año", fontsize=11)
ax1.set_ylabel("Nuevas matrículas", fontsize=11, color=PALETTE_MAIN)
ax2.set_ylabel("Acumulado", fontsize=11, color=PALETTE_ACCENT)
ax1.set_title("Evolución anual de matrículas — RUES (2000–2024)", fontsize=14, fontweight="bold")

lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=10)
plt.tight_layout()
fig.savefig(DIR_FIGURES / "05a_matriculas_por_anio.png", dpi=FIG_DPI)
plt.close()
print("✅  05a_matriculas_por_anio.png")

# ── 5B · Top-5 ciudades — evolución comparada ─────────────────────────────────
top5_ciudades = (
    df.groupby(COL_CAMARA)
    .size()
    .sort_values(ascending=False)
    .head(5)
    .index.tolist()
)

pivot5 = (
    df[df[COL_CAMARA].isin(top5_ciudades)]
    .groupby(["ANIO_MATRICULA", COL_CAMARA])
    .size()
    .reset_index(name="n")
    .pivot(index="ANIO_MATRICULA", columns=COL_CAMARA, values="n")
    .fillna(0)
)

fig, ax = plt.subplots(figsize=(14, 5))
for col in pivot5.columns:
    ax.plot(pivot5.index, pivot5[col], linewidth=2, marker="o", markersize=3, label=col)

ax.set_xlabel("Año", fontsize=11)
ax.set_ylabel("Matrículas", fontsize=11)
ax.set_title("Evolución temporal — Top 5 ciudades", fontsize=14, fontweight="bold")
ax.legend(fontsize=9, loc="upper left")
plt.tight_layout()
fig.savefig(DIR_FIGURES / "05b_top5_ciudades_temporal.png", dpi=FIG_DPI)
plt.close()
print("✅  05b_top5_ciudades_temporal.png")

# ── 5C · Heatmap ciudad × año ─────────────────────────────────────────────────
top10_ciudades = (
    df.groupby(COL_CAMARA).size().sort_values(ascending=False).head(10).index.tolist()
)
heatmap_data = (
    df[df[COL_CAMARA].isin(top10_ciudades)]
    .groupby(["ANIO_MATRICULA", COL_CAMARA])
    .size()
    .reset_index(name="n")
    .pivot(index=COL_CAMARA, columns="ANIO_MATRICULA", values="n")
    .fillna(0)
)

fig, ax = plt.subplots(figsize=(16, 6))
sns.heatmap(
    heatmap_data,
    cmap="YlOrRd",
    linewidths=0.3,
    linecolor="white",
    ax=ax,
    cbar_kws={"label": "Nº de matrículas"},
    fmt=".0f",
)
ax.set_title("Heatmap: Matrículas por ciudad y año (Top 10 ciudades)", fontsize=14, fontweight="bold")
ax.set_xlabel("Año de matrícula", fontsize=11)
ax.set_ylabel("Ciudad / Cámara", fontsize=11)
plt.tight_layout()
fig.savefig(DIR_FIGURES / "05c_heatmap_ciudad_anio.png", dpi=FIG_DPI)
plt.close()
print("✅  05c_heatmap_ciudad_anio.png")

# ── Tabla ─────────────────────────────────────────────────────────────────────
anual.to_csv(DIR_TABLES / "05_evolucion_anual.csv", index=False)
print("✅  tabla guardada")