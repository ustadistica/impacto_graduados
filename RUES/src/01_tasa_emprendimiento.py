"""
src/01_tasa_emprendimiento.py  (versión actualizada)
=====================================================
Métricas por cámara de comercio:
  · % de matrículas de esa cámara sobre el total nacional (muestra)
  · % de activas de esa cámara sobre el total de activas nacionales
  · Tasa interna: activas / propias (%)

Salidas:
  outputs/tables/01_tasa_emprendimiento.csv
  outputs/tables/01_top_camaras.csv
  outputs/figures/01a_estados_donut.png
  outputs/figures/01b_top_camaras_pct.png
  outputs/reports/01_tasa_emprendimiento.json
"""

import sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from config.settings import (
    SAMPLE_PATH, DIR_FIGURES, DIR_TABLES, DIR_REPORTS,
    COL_ESTADO, COL_CAMARA,
    PALETTE_POS, PALETTE_NEG, PALETTE_MAIN, PALETTE_SEC, PALETTE_ACCENT,
    FIG_DPI,
)

plt.rcParams.update({"font.family": "DejaVu Sans",
                     "axes.spines.top": False, "axes.spines.right": False})

df = pd.read_csv(SAMPLE_PATH, low_memory=False)
df[COL_ESTADO] = df[COL_ESTADO].str.strip().str.upper()
df[COL_CAMARA] = df[COL_CAMARA].str.strip().str.title()

n_total   = len(df)
n_activas = (df[COL_ESTADO] == "ACTIVA").sum()
n_canc    = (df[COL_ESTADO] == "CANCELADA").sum()
tasa_activa = n_activas / n_total * 100

# ── 1A · Donut estados ────────────────────────────────────────────────────────
n_otro = n_total - n_activas - n_canc
sizes  = [n_activas, n_canc, n_otro]
colors = [PALETTE_POS, PALETTE_NEG, "#BDBDBD"]
labels = [f"Activas\n{n_activas:,}", f"Canceladas\n{n_canc:,}", f"Otro\n{n_otro:,}"]

fig, ax = plt.subplots(figsize=(7, 7))
wedges, texts, autotexts = ax.pie(
    sizes, labels=labels, colors=colors,
    autopct="%1.1f%%", startangle=90,
    wedgeprops=dict(width=0.55), textprops=dict(fontsize=12),
)
for at in autotexts:
    at.set_fontsize(13); at.set_fontweight("bold")
ax.set_title(f"Distribución de matrículas por estado\n(muestra n={n_total:,})",
             fontsize=15, fontweight="bold", pad=20)
ax.text(0, -1.3, f"Tasa de actividad: {tasa_activa:.1f}%",
        ha="center", fontsize=13, color=PALETTE_POS, fontweight="bold")
plt.tight_layout()
fig.savefig(DIR_FIGURES / "01a_estados_donut.png", dpi=FIG_DPI)
plt.close()
print("✅  01a_estados_donut.png")

# ── Cálculo de métricas por cámara ───────────────────────────────────────────
geo = (
    df.groupby(COL_CAMARA)
    .agg(
        total=(COL_ESTADO, "count"),
        activas=(COL_ESTADO, lambda x: (x == "ACTIVA").sum()),
    )
    .reset_index()
)
geo["pct_sobre_total"]    = (geo["total"]   / n_total   * 100).round(2)  # % matrículas sobre nacional
geo["pct_activas_nac"]    = (geo["activas"] / n_activas * 100).round(2)  # % activas sobre total activas nac.
geo["tasa_interna"]       = (geo["activas"] / geo["total"] * 100).round(2)  # activas / propias

geo = geo.sort_values("total", ascending=False).reset_index(drop=True)
top10 = geo.head(10).copy()

# ── 1B · Gráfica de barras agrupadas con los 3 porcentajes ───────────────────
x = np.arange(len(top10))
w = 0.26

fig, ax = plt.subplots(figsize=(14, 6))
b1 = ax.bar(x - w, top10["pct_sobre_total"],  w, label="% matrículas sobre total nacional", color=PALETTE_MAIN,   alpha=0.85, edgecolor="white")
b2 = ax.bar(x,     top10["pct_activas_nac"],  w, label="% activas sobre total activas nac.", color=PALETTE_SEC,    alpha=0.85, edgecolor="white")
b3 = ax.bar(x + w, top10["tasa_interna"],     w, label="Tasa activas / propias",             color=PALETTE_ACCENT, alpha=0.85, edgecolor="white")

for bars in [b1, b2, b3]:
    for bar in bars:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, h + 0.3,
                f"{h:.1f}%", ha="center", va="bottom", fontsize=7, color="#333")

ax.set_xticks(x)
ax.set_xticklabels(top10[COL_CAMARA], rotation=22, ha="right", fontsize=10)
ax.set_ylabel("Porcentaje (%)", fontsize=11)
ax.set_title("Top 10 cámaras de comercio — Análisis porcentual", fontsize=14, fontweight="bold")
ax.legend(fontsize=9, loc="upper right")
ax.set_ylim(0, max(top10[["pct_sobre_total","pct_activas_nac","tasa_interna"]].max()) * 1.2)
plt.tight_layout()
fig.savefig(DIR_FIGURES / "01b_top_camaras_pct.png", dpi=FIG_DPI)
plt.close()
print("✅  01b_top_camaras_pct.png")

# ── Tablas y reporte ──────────────────────────────────────────────────────────
top10.to_csv(DIR_TABLES / "01_top_camaras.csv", index=False)
pd.DataFrame({
    "indicador": ["Total muestra", "Activas", "Canceladas", "Tasa actividad (%)"],
    "valor": [n_total, int(n_activas), int(n_canc), round(tasa_activa, 2)],
}).to_csv(DIR_TABLES / "01_tasa_emprendimiento.csv", index=False)

with open(DIR_REPORTS / "01_tasa_emprendimiento.json", "w", encoding="utf-8") as f:
    json.dump({"tasa_actividad_pct": round(tasa_activa, 2),
               "activas": int(n_activas), "canceladas": int(n_canc), "total": n_total},
              f, ensure_ascii=False, indent=2)
print("✅  tablas y reporte guardados")