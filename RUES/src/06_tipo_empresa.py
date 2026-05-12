"""
src/06_tipo_empresa.py
=======================
Indicador 6 — Tipo de empresa / organización jurídica.

Calcula:
  · Distribución por organización jurídica (SAS, Persona Natural, ESAL, etc.)
  · Evolución temporal del tipo de empresa más frecuente
  · Cruce tipo × estado (activa/cancelada)

Salidas:
  outputs/tables/06_tipos_empresa.csv
  outputs/figures/06a_tipos_empresa_barras.png
  outputs/figures/06b_tipo_evolucion.png
  outputs/figures/06c_tipo_x_estado.png
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
    COL_ORG_JUR, COL_ESTADO,
    PALETTE_MAIN, PALETTE_SEC, PALETTE_ACCENT, PALETTE_POS, PALETTE_NEG,
    YEAR_MIN, YEAR_MAX, FIG_DPI,
)

plt.rcParams.update({"font.family": "DejaVu Sans",
                     "axes.spines.top": False,
                     "axes.spines.right": False})

# ── Carga ──────────────────────────────────────────────────────────────────────
df = pd.read_csv(SAMPLE_PATH, low_memory=False)
df[COL_ESTADO] = df[COL_ESTADO].str.strip().str.upper()

if COL_ORG_JUR not in df.columns:
    print(f"⚠️  Columna '{COL_ORG_JUR}' no encontrada. Columnas disponibles: {list(df.columns)}")
    sys.exit(0)

df[COL_ORG_JUR] = df[COL_ORG_JUR].str.strip().str.title().fillna("Sin información")

# ── 6A · Distribución total por tipo ──────────────────────────────────────────
tipos = df[COL_ORG_JUR].value_counts().head(12).reset_index()
tipos.columns = ["tipo", "conteo"]
tipos["pct"] = tipos["conteo"] / tipos["conteo"].sum() * 100

cmap_colors = plt.cm.tab10.colors
fig, ax = plt.subplots(figsize=(12, 6))
bars = ax.barh(tipos["tipo"][::-1], tipos["conteo"][::-1],
               color=[cmap_colors[i % 10] for i in range(len(tipos))][::-1],
               alpha=0.85, edgecolor="white")
for bar, row in zip(bars, tipos.iloc[::-1].itertuples()):
    ax.text(bar.get_width() + 3,
            bar.get_y() + bar.get_height() / 2,
            f"{row.conteo:,}  ({row.pct:.1f}%)",
            va="center", fontsize=9)
ax.set_xlabel("Número de matrículas", fontsize=11)
ax.set_title("Distribución por tipo de organización jurídica", fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(DIR_FIGURES / "06a_tipos_empresa_barras.png", dpi=FIG_DPI)
plt.close()
print("✅  06a_tipos_empresa_barras.png")

# ── 6B · Evolución del top-4 tipos por año ───────────────────────────────────
df_yr = df[df["ANIO_MATRICULA"].notna()].copy()
df_yr["ANIO_MATRICULA"] = df_yr["ANIO_MATRICULA"].astype(int)
df_yr = df_yr[(df_yr["ANIO_MATRICULA"] >= YEAR_MIN) & (df_yr["ANIO_MATRICULA"] <= YEAR_MAX)]

top4 = tipos["tipo"].head(4).tolist()
evo = (
    df_yr[df_yr[COL_ORG_JUR].isin(top4)]
    .groupby(["ANIO_MATRICULA", COL_ORG_JUR])
    .size()
    .reset_index(name="n")
    .pivot(index="ANIO_MATRICULA", columns=COL_ORG_JUR, values="n")
    .fillna(0)
)

fig, ax = plt.subplots(figsize=(14, 5))
for i, col in enumerate(evo.columns):
    ax.plot(evo.index, evo[col], linewidth=2.2, marker="o", markersize=3,
            color=cmap_colors[i], label=col)
ax.set_xlabel("Año", fontsize=11)
ax.set_ylabel("Matrículas", fontsize=11)
ax.set_title("Evolución temporal por tipo de organización jurídica (Top 4)", fontsize=13, fontweight="bold")
ax.legend(fontsize=9)
plt.tight_layout()
fig.savefig(DIR_FIGURES / "06b_tipo_evolucion.png", dpi=FIG_DPI)
plt.close()
print("✅  06b_tipo_evolucion.png")

# ── 6C · Cruce tipo × estado (stacked bar) ────────────────────────────────────
cruce = (
    df.groupby([COL_ORG_JUR, COL_ESTADO])
    .size()
    .reset_index(name="n")
)
top6 = tipos["tipo"].head(6).tolist()
cruce = cruce[cruce[COL_ORG_JUR].isin(top6)]
pivot_cruce = cruce.pivot(index=COL_ORG_JUR, columns=COL_ESTADO, values="n").fillna(0)

# Normalizar a %
pivot_pct = pivot_cruce.div(pivot_cruce.sum(axis=1), axis=0) * 100
pivot_pct = pivot_pct.loc[top6]

fig, ax = plt.subplots(figsize=(12, 5))
bottom = np.zeros(len(pivot_pct))
color_map = {"ACTIVA": PALETTE_POS, "CANCELADA": PALETTE_NEG}
for col in pivot_pct.columns:
    color = color_map.get(col, PALETTE_ACCENT)
    bars = ax.bar(pivot_pct.index, pivot_pct[col], bottom=bottom,
                  label=col, color=color, alpha=0.85)
    bottom += pivot_pct[col].values

ax.yaxis.set_major_formatter(mtick.PercentFormatter())
ax.set_ylabel("% del total", fontsize=11)
ax.set_title("Estado de matrícula por tipo de organización jurídica", fontsize=13, fontweight="bold")
ax.legend(fontsize=10)
plt.xticks(rotation=20, ha="right")
plt.tight_layout()
fig.savefig(DIR_FIGURES / "06c_tipo_x_estado.png", dpi=FIG_DPI)
plt.close()
print("✅  06c_tipo_x_estado.png")

# ── Tabla ─────────────────────────────────────────────────────────────────────
tipos.to_csv(DIR_TABLES / "06_tipos_empresa.csv", index=False)
print("✅  tabla guardada")