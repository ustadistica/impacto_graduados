"""
src/02_impacto_ciiu.py  (versión actualizada)
==============================================
Indicador 2 — Sectores CIIU + tasa de emprendimiento por sector.

Calcula:
  · Top-15 CIIU por volumen
  · Tasa de activas por CIIU (activas / total de ese CIIU)
  · Comparativo: sectores con mayor volumen vs sectores con mayor tasa

Salidas:
  outputs/tables/02_top_ciiu.csv
  outputs/tables/02_tasa_por_ciiu.csv
  outputs/figures/02a_top_ciiu_barras.png
  outputs/figures/02b_tasa_emprendimiento_ciiu.png
  outputs/figures/02c_volumen_vs_tasa.png
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm

from config.settings import (
    SAMPLE_PATH, DIR_FIGURES, DIR_TABLES,
    COL_CIIU, COL_ESTADO,
    PALETTE_MAIN, PALETTE_ACCENT, PALETTE_POS, PALETTE_NEG, FIG_DPI,
)

plt.rcParams.update({"font.family": "DejaVu Sans",
                     "axes.spines.top": False, "axes.spines.right": False})

CIIU_DESC = {
    "9999": "Sin clasificar", "4711": "Supermercados", "5611": "Restaurantes",
    "5630": "Bares/cantinas", "9499": "Otras asociaciones", "9602": "Peluquerías",
    "4771": "Tiendas ropa", "4719": "Misceláneas", "4773": "Farmacias",
    "1410": "Confección", "8299": "Servicios admin.", "4752": "Ferreterías",
    "4520": "Talleres vehículos", "5619": "Otros restaurantes", "4723": "Carnicerías",
    "5613": "Cafeterías", "4761": "Librerías", "4759": "Artículos hogar",
    "1081": "Panadería", "6810": "Inmobiliario",
}

# ── Carga ──────────────────────────────────────────────────────────────────────
df = pd.read_csv(SAMPLE_PATH, low_memory=False)
df[COL_ESTADO] = df[COL_ESTADO].str.strip().str.upper()
df[COL_CIIU]   = df[COL_CIIU].astype(str).str.strip().str.upper().str.replace(r"[^A-Z0-9]","",regex=True)
df = df[df[COL_CIIU].notna() & (df[COL_CIIU] != "NAN") & (df[COL_CIIU] != "") & (df[COL_CIIU].str.len() >= 2)]

n_total = len(df)

ciiu_stats = (
    df.groupby(COL_CIIU)
    .agg(
        total=(COL_ESTADO, "count"),
        activas=(COL_ESTADO, lambda x: (x == "ACTIVA").sum()),
    )
    .reset_index()
)
ciiu_stats["tasa_activas"]  = (ciiu_stats["activas"] / ciiu_stats["total"] * 100).round(1)
ciiu_stats["pct_del_total"] = (ciiu_stats["total"] / n_total * 100).round(2)
ciiu_stats["descripcion"]   = ciiu_stats[COL_CIIU].map(CIIU_DESC).fillna("Otro")

top15_vol  = ciiu_stats.sort_values("total",       ascending=False).head(15).copy()
top15_tasa = ciiu_stats[ciiu_stats["total"] >= 30].sort_values("tasa_activas", ascending=False).head(15).copy()

# ── 2A · Top-15 por volumen ───────────────────────────────────────────────────
labels_a = [f"{r[COL_CIIU]} · {r['descripcion']}" for _, r in top15_vol.iterrows()]
cmap = cm.get_cmap("Blues", len(top15_vol) + 4)
colors_a = [cmap(i + 3) for i in range(len(top15_vol))][::-1]

fig, ax = plt.subplots(figsize=(13, 8))
bars = ax.barh(labels_a[::-1], top15_vol["total"][::-1], color=colors_a, edgecolor="white")
for bar, (_, row) in zip(bars, top15_vol.iloc[::-1].iterrows()):
    ax.text(bar.get_width() + 5, bar.get_y() + bar.get_height() / 2,
            f"{row['total']:,}  ({row['pct_del_total']}%)", va="center", fontsize=8.5)
ax.set_xlabel("Número de matrículas", fontsize=11)
ax.set_title("Top 15 sectores CIIU por volumen de matrículas", fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(DIR_FIGURES / "02a_top_ciiu_barras.png", dpi=FIG_DPI)
plt.close()
print("✅  02a_top_ciiu_barras.png")

# ── 2B · Tasa de emprendimiento (activas) por CIIU ───────────────────────────
labels_b = [f"{r[COL_CIIU]} · {r['descripcion']}" for _, r in top15_tasa.iterrows()]
tasa_vals = top15_tasa["tasa_activas"].values
colors_b  = [PALETTE_POS if t >= 50 else (PALETTE_ACCENT if t >= 30 else PALETTE_MAIN) for t in tasa_vals]

fig, ax = plt.subplots(figsize=(13, 8))
bars = ax.barh(labels_b[::-1], tasa_vals[::-1],
               color=colors_b[::-1], edgecolor="white", alpha=0.88)
for bar, t in zip(bars, tasa_vals[::-1]):
    ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
            f"{t:.1f}%", va="center", fontsize=9, fontweight="bold")
ax.axvline(ciiu_stats["tasa_activas"].mean(), color="#999", linestyle="--",
           linewidth=1.4, label=f"Promedio: {ciiu_stats['tasa_activas'].mean():.1f}%")
ax.set_xlabel("Tasa de actividad (%)", fontsize=11)
ax.set_title("Tasa de emprendimiento activo por sector CIIU\n(sectores con ≥30 registros)", fontsize=14, fontweight="bold")
ax.legend(fontsize=10)
plt.tight_layout()
fig.savefig(DIR_FIGURES / "02b_tasa_emprendimiento_ciiu.png", dpi=FIG_DPI)
plt.close()
print("✅  02b_tasa_emprendimiento_ciiu.png")

# ── 2C · Scatter volumen vs tasa ─────────────────────────────────────────────
plot_df = ciiu_stats[ciiu_stats["total"] >= 20].copy()
fig, ax = plt.subplots(figsize=(12, 7))
sc = ax.scatter(
    plot_df["total"], plot_df["tasa_activas"],
    s=plot_df["activas"] / plot_df["activas"].max() * 800 + 30,
    c=plot_df["tasa_activas"], cmap="RdYlGn", vmin=0, vmax=100,
    alpha=0.82, edgecolors="white", linewidths=1,
)
# Etiquetar los más relevantes
top_label = plot_df.nlargest(10, "total")
for _, row in top_label.iterrows():
    ax.annotate(
        f"{row[COL_CIIU]}\n{row['descripcion'][:18]}",
        (row["total"], row["tasa_activas"]),
        textcoords="offset points", xytext=(7, 3),
        fontsize=7.5, color="#333",
    )
ax.set_xlabel("Número total de matrículas", fontsize=11)
ax.set_ylabel("Tasa de empresas activas (%)", fontsize=11)
ax.set_title("Volumen de matrículas vs Tasa de actividad por CIIU\n(tamaño burbuja = nº de activas)", fontsize=13, fontweight="bold")
fig.colorbar(sc, ax=ax, label="% activas", shrink=0.7)
plt.tight_layout()
fig.savefig(DIR_FIGURES / "02c_volumen_vs_tasa.png", dpi=FIG_DPI)
plt.close()
print("✅  02c_volumen_vs_tasa.png")

# ── Tablas ────────────────────────────────────────────────────────────────────
top15_vol.to_csv(DIR_TABLES / "02_top_ciiu.csv", index=False)
top15_tasa.sort_values("tasa_activas", ascending=False).to_csv(DIR_TABLES / "02_tasa_por_ciiu.csv", index=False)
print("✅  tablas guardadas")