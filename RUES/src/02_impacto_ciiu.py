"""
src/02_impacto_ciiu.py
======================
Indicador 2 — Impacto por sector económico (CIIU).

Calcula:
  · Top-20 códigos CIIU más frecuentes
  · Agrupación por sección CIIU (primera letra del código)
  · Descripción textual de las secciones más relevantes
  · Treemap de distribución sectorial

Salidas:
  outputs/tables/02_top_ciiu.csv
  outputs/tables/02_secciones_ciiu.csv
  outputs/figures/02a_top_ciiu_barras.png
  outputs/figures/02b_secciones_ciiu_pie.png
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np

from config.settings import (
    SAMPLE_PATH, DIR_FIGURES, DIR_TABLES,
    COL_CIIU, COL_ESTADO,
    PALETTE_MAIN, FIG_DPI,
)

plt.rcParams.update({"font.family": "DejaVu Sans",
                     "axes.spines.top": False,
                     "axes.spines.right": False})

# Diccionario secciones CIIU Rev. 4 (Colombia)
SECCIONES_CIIU = {
    "A": "Agricultura, ganadería, caza, silvicultura y pesca",
    "B": "Explotación de minas y canteras",
    "C": "Industrias manufactureras",
    "D": "Suministro de electricidad, gas, vapor",
    "E": "Distribución de agua; alcantarillado",
    "F": "Construcción",
    "G": "Comercio al por mayor y por menor",
    "H": "Transporte y almacenamiento",
    "I": "Alojamiento y servicios de comida",
    "J": "Información y comunicaciones",
    "K": "Actividades financieras y de seguros",
    "L": "Actividades inmobiliarias",
    "M": "Actividades profesionales, científicas y técnicas",
    "N": "Actividades de servicios administrativos y de apoyo",
    "O": "Administración pública y defensa",
    "P": "Educación",
    "Q": "Actividades de atención de la salud humana",
    "R": "Actividades artísticas, entretenimiento y recreación",
    "S": "Otras actividades de servicios",
    "T": "Actividades de los hogares",
    "U": "Actividades de organizaciones extraterritoriales",
}

# ── Carga ──────────────────────────────────────────────────────────────────────
df = pd.read_csv(SAMPLE_PATH, low_memory=False)
df[COL_CIIU] = df[COL_CIIU].astype(str).str.strip().str.upper()
df = df[df[COL_CIIU].notna() & (df[COL_CIIU] != "NAN") & (df[COL_CIIU] != "")]

# Sección (primera letra o primeros 4 dígitos)
df["SECCION_CIIU"] = df[COL_CIIU].str[0]

# ── 2A · Top-20 CIIU ──────────────────────────────────────────────────────────
top_ciiu = (
    df[COL_CIIU].value_counts()
    .head(20)
    .reset_index()
    .rename(columns={"index": "ciiu", COL_CIIU: "conteo"})
)
# compatibilidad pandas >= 2.0
if "index" not in top_ciiu.columns and top_ciiu.columns[0] == COL_CIIU:
    top_ciiu.columns = ["ciiu", "conteo"]
else:
    top_ciiu = top_ciiu.set_axis(["ciiu", "conteo"], axis=1)

top_ciiu["seccion"] = top_ciiu["ciiu"].str[0].map(SECCIONES_CIIU).fillna("Otra")

cmap = cm.get_cmap("Blues", len(top_ciiu) + 4)
colors = [cmap(i + 2) for i in range(len(top_ciiu))][::-1]

fig, ax = plt.subplots(figsize=(12, 8))
bars = ax.barh(top_ciiu["ciiu"][::-1], top_ciiu["conteo"][::-1],
               color=colors, edgecolor="white")
for bar, row in zip(bars, top_ciiu.iloc[::-1].itertuples()):
    ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
            f"{row.conteo:,}", va="center", fontsize=9)

ax.set_xlabel("Número de matrículas", fontsize=11)
ax.set_title("Top-20 actividades económicas (CIIU) en la muestra RUES", fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(DIR_FIGURES / "02a_top_ciiu_barras.png", dpi=FIG_DPI)
plt.close()
print("✅  02a_top_ciiu_barras.png")

# ── 2B · Secciones CIIU — pie ─────────────────────────────────────────────────
secciones = (
    df["SECCION_CIIU"]
    .map(SECCIONES_CIIU)
    .fillna("Otra")
    .value_counts()
    .head(10)
)
pct_otros = 100 - secciones.sum() / len(df) * 100

fig, ax = plt.subplots(figsize=(10, 7))
wedges, texts, autotexts = ax.pie(
    secciones,
    labels=[s[:35] for s in secciones.index],
    autopct=lambda p: f"{p:.1f}%" if p > 3 else "",
    startangle=140,
    colors=plt.cm.Set3.colors[:len(secciones)],
    wedgeprops=dict(edgecolor="white", linewidth=1.5),
)
ax.set_title("Distribución por sección económica CIIU\n(Top 10 secciones)", fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(DIR_FIGURES / "02b_secciones_ciiu_pie.png", dpi=FIG_DPI)
plt.close()
print("✅  02b_secciones_ciiu_pie.png")

# ── Tablas ────────────────────────────────────────────────────────────────────
top_ciiu.to_csv(DIR_TABLES / "02_top_ciiu.csv", index=False)
secciones.reset_index().rename(columns={"index": "seccion", "SECCION_CIIU": "conteo"}).to_csv(
    DIR_TABLES / "02_secciones_ciiu.csv", index=False
)
print("✅  tablas guardadas")