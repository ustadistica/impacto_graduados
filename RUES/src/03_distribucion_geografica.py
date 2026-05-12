"""
src/03_distribucion_geografica.py
==================================
Indicador 3 — Distribución geográfica.

Calcula:
  · Volumen de matrículas por cámara de comercio (ciudad/región)
  · % de empresas activas por ciudad
  · Mapa de calor de actividad por región (barras + colores)
  · Top-20 ciudades con mayor volumen total y mayor tasa de activas

Salidas:
  outputs/tables/03_distribucion_geografica.csv
  outputs/figures/03a_mapa_calor_ciudades.png
  outputs/figures/03b_tasa_activas_ciudad.png
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np

from config.settings import (
    SAMPLE_PATH, DIR_FIGURES, DIR_TABLES,
    COL_CAMARA, COL_ESTADO,
    PALETTE_MAIN, PALETTE_POS, PALETTE_NEG, FIG_DPI,
)

plt.rcParams.update({"font.family": "DejaVu Sans",
                     "axes.spines.top": False,
                     "axes.spines.right": False})

# ── Carga ──────────────────────────────────────────────────────────────────────
df = pd.read_csv(SAMPLE_PATH, low_memory=False)
df[COL_CAMARA] = df[COL_CAMARA].str.strip().str.title()
df[COL_ESTADO] = df[COL_ESTADO].str.strip().str.upper()

geo = (
    df.groupby(COL_CAMARA)
    .agg(
        total=("ESTADO_MATRICULA", "count"),
        activas=("ESTADO_MATRICULA", lambda x: (x == "ACTIVA").sum()),
    )
    .assign(tasa_activa=lambda d: d["activas"] / d["total"] * 100)
    .sort_values("total", ascending=False)
    .reset_index()
)

# ── 3A · Mapa de calor (barras coloreadas por tasa) ───────────────────────────
top20 = geo.head(20).copy()
norm = mcolors.Normalize(vmin=top20["tasa_activa"].min(),
                         vmax=top20["tasa_activa"].max())
cmap = plt.cm.RdYlGn

fig, ax = plt.subplots(figsize=(12, 7))
bars = ax.barh(
    top20[COL_CAMARA][::-1],
    top20["total"][::-1],
    color=[cmap(norm(v)) for v in top20["tasa_activa"][::-1]],
    edgecolor="white", linewidth=0.5,
)
for bar, row in zip(bars, top20.iloc[::-1].itertuples()):
    ax.text(bar.get_width() + 3,
            bar.get_y() + bar.get_height() / 2,
            f"{row.total:,} | {row.tasa_activa:.0f}% activas",
            va="center", fontsize=8.5, color="#333")

sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
sm.set_array([])
cbar = fig.colorbar(sm, ax=ax, shrink=0.6, pad=0.01)
cbar.set_label("% empresas activas", fontsize=10)

ax.set_xlabel("Total de matrículas", fontsize=11)
ax.set_title("Distribución geográfica — Top 20 cámaras de comercio\n(color = tasa de actividad)",
             fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(DIR_FIGURES / "03a_mapa_calor_ciudades.png", dpi=FIG_DPI)
plt.close()
print("✅  03a_mapa_calor_ciudades.png")

# ── 3B · Tasa de activas por ciudad (burbuja) ─────────────────────────────────
top15 = geo.head(15).copy()

fig, ax = plt.subplots(figsize=(10, 6))
scatter = ax.scatter(
    top15["total"],
    top15["tasa_activa"],
    s=top15["activas"] / top15["activas"].max() * 2000,
    c=top15["tasa_activa"],
    cmap="RdYlGn",
    alpha=0.8, edgecolors="white", linewidths=1.5,
)
for _, row in top15.iterrows():
    ax.annotate(
        row[COL_CAMARA],
        (row["total"], row["tasa_activa"]),
        textcoords="offset points", xytext=(6, 3),
        fontsize=8, color="#222",
    )
ax.set_xlabel("Total de matrículas en la muestra", fontsize=11)
ax.set_ylabel("% empresas activas", fontsize=11)
ax.set_title("Tasa de actividad vs volumen por ciudad\n(tamaño burbuja = nº de activas)",
             fontsize=13, fontweight="bold")
fig.colorbar(scatter, ax=ax, label="% activas")
plt.tight_layout()
fig.savefig(DIR_FIGURES / "03b_tasa_activas_ciudad.png", dpi=FIG_DPI)
plt.close()
print("✅  03b_tasa_activas_ciudad.png")

# ── Tabla ─────────────────────────────────────────────────────────────────────
geo.to_csv(DIR_TABLES / "03_distribucion_geografica.csv", index=False)
print("✅  tabla guardada")