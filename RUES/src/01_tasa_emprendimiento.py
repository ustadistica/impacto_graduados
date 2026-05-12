"""
src/01_tasa_emprendimiento.py
==============================
Indicador 1 — Tasa de emprendimiento (general, no por institución).

Calcula:
  · % de registros activos vs total en la muestra
  · Distribución por tipo de identificación (persona natural vs jurídica)
  · Top-10 cámaras de comercio con mayor actividad empresarial
  · Tasa de emprendimiento aproximada: empresas por cada 1 000 registros
    activos en cada cámara

Salidas:
  outputs/tables/01_tasa_emprendimiento.csv
  outputs/figures/01a_estados_donut.png
  outputs/figures/01b_top_camaras.png
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import json

from config.settings import (
    SAMPLE_PATH, DIR_FIGURES, DIR_TABLES, DIR_REPORTS,
    COL_ESTADO, COL_CAMARA, COL_TIPO_ID,
    PALETTE_POS, PALETTE_NEG, PALETTE_MAIN, PALETTE_SEC, PALETTE_ACCENT,
    FIG_DPI,
)

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "axes.spines.top": False,
    "axes.spines.right": False,
})

# ── Carga ──────────────────────────────────────────────────────────────────────
df = pd.read_csv(SAMPLE_PATH, low_memory=False)
n_total = len(df)

# ── 1A · Distribución de estados ──────────────────────────────────────────────
estados = df[COL_ESTADO].value_counts()
activas = estados.get("ACTIVA", estados.get("ACTIVE", 0))
canceladas = estados.get("CANCELADA", estados.get("CANCELLED", 0))
otras = n_total - activas - canceladas

tasa_activa = activas / n_total * 100

fig, ax = plt.subplots(figsize=(7, 7))
sizes  = [activas, canceladas, otras] if otras > 0 else [activas, canceladas]
colors = [PALETTE_POS, PALETTE_NEG, "#BDBDBD"] if otras > 0 else [PALETTE_POS, PALETTE_NEG]
labels = [f"Activas\n{activas:,}", f"Canceladas\n{canceladas:,}"]
if otras > 0:
    labels.append(f"Otro\n{otras:,}")

wedges, texts, autotexts = ax.pie(
    sizes, labels=labels, colors=colors,
    autopct="%1.1f%%", startangle=90,
    wedgeprops=dict(width=0.55),
    textprops=dict(fontsize=12),
)
for at in autotexts:
    at.set_fontsize(13)
    at.set_fontweight("bold")

ax.set_title(
    f"Distribución de matrículas por estado\n(muestra n={n_total:,})",
    fontsize=15, fontweight="bold", pad=20,
)
ax.text(0, -1.3, f"Tasa de actividad: {tasa_activa:.1f}%",
        ha="center", fontsize=13, color=PALETTE_POS, fontweight="bold")

plt.tight_layout()
fig.savefig(DIR_FIGURES / "01a_estados_donut.png", dpi=FIG_DPI)
plt.close()
print("✅  01a_estados_donut.png")

# ── 1B · Top-10 cámaras ───────────────────────────────────────────────────────
top_camaras = (
    df.groupby(COL_CAMARA)
    .agg(total=("ESTADO_MATRICULA", "count"),
         activas=("ESTADO_MATRICULA", lambda x: (x.str.upper() == "ACTIVA").sum()))
    .assign(tasa=lambda d: d["activas"] / d["total"] * 100)
    .sort_values("total", ascending=False)
    .head(10)
    .reset_index()
)

fig, ax = plt.subplots(figsize=(12, 5))
bars = ax.barh(
    top_camaras[COL_CAMARA][::-1],
    top_camaras["total"][::-1],
    color=PALETTE_MAIN, alpha=0.85, edgecolor="white",
)
ax.barh(
    top_camaras[COL_CAMARA][::-1],
    top_camaras["activas"][::-1],
    color=PALETTE_SEC, alpha=0.9, label="Activas",
)
for bar, row in zip(bars, top_camaras.iloc[::-1].itertuples()):
    ax.text(bar.get_width() + 5, bar.get_y() + bar.get_height() / 2,
            f"{row.tasa:.0f}% activas", va="center", fontsize=9, color="#333")

ax.set_xlabel("Número de matrículas", fontsize=11)
ax.set_title("Top-10 cámaras de comercio por volumen de matrículas", fontsize=14, fontweight="bold")
legend_patches = [
    mpatches.Patch(color=PALETTE_MAIN, label="Total"),
    mpatches.Patch(color=PALETTE_SEC, label="Activas"),
]
ax.legend(handles=legend_patches, loc="lower right")
plt.tight_layout()
fig.savefig(DIR_FIGURES / "01b_top_camaras.png", dpi=FIG_DPI)
plt.close()
print("✅  01b_top_camaras.png")

# ── Tabla resumen ─────────────────────────────────────────────────────────────
resumen = pd.DataFrame({
    "indicador": ["Total registros muestra", "Activas", "Canceladas", "Tasa actividad (%)"],
    "valor": [n_total, int(activas), int(canceladas), round(tasa_activa, 2)],
})
resumen.to_csv(DIR_TABLES / "01_tasa_emprendimiento.csv", index=False)
top_camaras.to_csv(DIR_TABLES / "01_top_camaras.csv", index=False)
print("✅  tablas guardadas")

# Reporte JSON
with open(DIR_REPORTS / "01_tasa_emprendimiento.json", "w", encoding="utf-8") as f:
    json.dump({
        "tasa_actividad_pct": round(tasa_activa, 2),
        "activas": int(activas),
        "canceladas": int(canceladas),
        "total": n_total,
    }, f, ensure_ascii=False, indent=2)
print("✅  01_tasa_emprendimiento.json")