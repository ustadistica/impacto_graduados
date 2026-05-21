"""
src/06_tipo_empresa.py  (versión actualizada)
=============================================
Indicador 6 — Tipo de empresa con análisis separado Persona Natural vs Resto.

Persona Natural:
  · CIIU más frecuentes entre activas
  · Distribución por ciudad/cámara
  · Tasa de actividad propia
  · Evolución temporal

Otras formas jurídicas (SAS, Ltda., ESAL, etc.):
  · Ranking por volumen con tasa de activas
  · Comparativo de tasas vs Persona Natural
  · Evolución temporal del top-4

Salidas:
  outputs/tables/06_tipos_empresa.csv
  outputs/tables/06_persona_natural.csv
  outputs/tables/06_otros_tipos.csv
  outputs/figures/06a_persona_natural_ciiu.png
  outputs/figures/06b_otros_tipos_tasa.png
  outputs/figures/06c_evolucion_tipos.png
  outputs/figures/06d_comparativo_pn_vs_resto.png
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.patches as mpatches

from config.settings import (
    SAMPLE_PATH, DIR_FIGURES, DIR_TABLES,
    COL_ORG_JUR, COL_ESTADO, COL_CIIU, COL_CAMARA,
    PALETTE_MAIN, PALETTE_SEC, PALETTE_ACCENT, PALETTE_POS, PALETTE_NEG,
    YEAR_MIN, YEAR_MAX, FIG_DPI,
)

plt.rcParams.update({"font.family": "DejaVu Sans",
                     "axes.spines.top": False, "axes.spines.right": False})

CIIU_DESC = {
    "9999": "Sin clasificar", "4711": "Supermercados", "5611": "Restaurantes",
    "5630": "Bares/cantinas", "9499": "Otras asociaciones", "9602": "Peluquerías",
    "4771": "Tiendas ropa", "4719": "Misceláneas", "4773": "Farmacias",
    "1410": "Confección", "8299": "Servicios admin.", "4752": "Ferreterías",
    "4741": "Ferreterías/maquinaria",
}

# ── Carga ──────────────────────────────────────────────────────────────────────
df = pd.read_csv(SAMPLE_PATH, low_memory=False)
df[COL_ESTADO]  = df[COL_ESTADO].str.strip().str.upper()
df[COL_CIIU]    = df[COL_CIIU].astype(str).str.strip().str.upper().str.replace(r"[^A-Z0-9]","",regex=True)
df[COL_ORG_JUR] = df[COL_ORG_JUR].astype(str).str.strip().str.title()
df[COL_CAMARA]  = df[COL_CAMARA].astype(str).str.strip().str.title()

# Split: Persona Natural vs Resto
mask_pn = df[COL_ORG_JUR].str.contains("Persona Natural", case=False, na=False)
df_pn   = df[mask_pn].copy()
df_otro = df[~mask_pn].copy()

n_pn    = len(df_pn)
n_otro  = len(df_otro)
act_pn  = (df_pn[COL_ESTADO]  == "ACTIVA").sum()
act_otro= (df_otro[COL_ESTADO] == "ACTIVA").sum()
tasa_pn  = act_pn  / n_pn   * 100
tasa_otro= act_otro/ n_otro  * 100

print(f"Persona Natural : {n_pn:,} registros | {act_pn:,} activas | tasa {tasa_pn:.1f}%")
print(f"Otros tipos     : {n_otro:,} registros | {act_otro:,} activas | tasa {tasa_otro:.1f}%")

# ── 6A · Persona Natural — CIIU de activas ────────────────────────────────────
pn_activas = df_pn[df_pn[COL_ESTADO]=="ACTIVA"]
pn_ciiu = (
    pn_activas.groupby(COL_CIIU).size()
    .reset_index(name="n")
    .sort_values("n", ascending=False)
    .head(10)
)
pn_ciiu["descripcion"] = pn_ciiu[COL_CIIU].map(CIIU_DESC).fillna("Otro")
pn_ciiu["pct"] = (pn_ciiu["n"] / act_pn * 100).round(1)

labels_pn = [f"{r[COL_CIIU]} · {r['descripcion']}" for _, r in pn_ciiu.iterrows()]
cmap = plt.cm.get_cmap("Blues", len(pn_ciiu)+4)
colors_pn = [cmap(i+3) for i in range(len(pn_ciiu))][::-1]

fig, axes = plt.subplots(1, 2, figsize=(15, 6))

ax = axes[0]
bars = ax.barh(labels_pn[::-1], pn_ciiu["n"][::-1], color=colors_pn, edgecolor="white")
for bar, (_, row) in zip(bars, pn_ciiu.iloc[::-1].iterrows()):
    ax.text(bar.get_width()+0.5, bar.get_y()+bar.get_height()/2,
            f"{row['n']}  ({row['pct']}%)", va="center", fontsize=9)
ax.set_xlabel("Nº de personas naturales activas", fontsize=10)
ax.set_title("Top 10 sectores de\nPersonas Naturales activas", fontsize=12, fontweight="bold")

# Ciudades donde están
pn_cam = (
    pn_activas.groupby(COL_CAMARA).size()
    .sort_values(ascending=False).head(8)
    .reset_index(name="n")
)
ax2 = axes[1]
bars2 = ax2.bar(pn_cam[COL_CAMARA], pn_cam["n"],
                color=PALETTE_MAIN, alpha=0.82, edgecolor="white")
for bar, v in zip(bars2, pn_cam["n"]):
    ax2.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.3,
             str(v), ha="center", fontsize=9)
ax2.set_xticklabels(pn_cam[COL_CAMARA], rotation=22, ha="right", fontsize=9)
ax2.set_ylabel("Nº de activas", fontsize=10)
ax2.set_title("Distribución geográfica de\nPersonas Naturales activas", fontsize=12, fontweight="bold")

fig.suptitle(f"Persona Natural — {n_pn:,} registros | {act_pn:,} activas | Tasa: {tasa_pn:.1f}%",
             fontsize=13, fontweight="bold", color="#0D2B5E")
plt.tight_layout()
fig.savefig(DIR_FIGURES / "06a_persona_natural_ciiu.png", dpi=FIG_DPI)
plt.close()
print("✅  06a_persona_natural_ciiu.png")

# ── 6B · Otros tipos — ranking con tasa de activas ───────────────────────────
NOMBRE_CORTO = {
    "Sociedades Por Acciones Simplificadas Sas": "SAS",
    "Sociedad Limitada": "Ltda.",
    "Empresas Unipersonales": "Emp. Unipersonal",
    "Las Demás Organizaciones Civiles,Corporaciones,Fundaciones": "Org. Civiles",
    "Corporaciones, Asociaciones Y Fundaciones Creadas Para Adelantar Actividades En Comunidades Indígenas.": "Corp. Indígenas",
    "S.A.": "S.A.",
    "Sociedad En Comandita Simple": "Cdte. Simple",
    "Fundaciones": "Fundaciones",
    "Corporaciones": "Corporaciones",
}

otros_stats = (
    df_otro.groupby(COL_ORG_JUR)
    .agg(total=(COL_ESTADO,"count"),
         activas=(COL_ESTADO, lambda x: (x=="ACTIVA").sum()))
    .reset_index()
)
otros_stats["tasa"] = (otros_stats["activas"]/otros_stats["total"]*100).round(1)
otros_stats["nombre_corto"] = otros_stats[COL_ORG_JUR].map(NOMBRE_CORTO).fillna(
    otros_stats[COL_ORG_JUR].str[:30]
)
otros_stats = otros_stats.sort_values("total", ascending=False).head(8).reset_index(drop=True)

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Volumen
ax = axes[0]
colors_v = [PALETTE_MAIN]*len(otros_stats)
bars = ax.barh(otros_stats["nombre_corto"][::-1], otros_stats["total"][::-1],
               color=colors_v, alpha=0.85, edgecolor="white")
for bar, (_, row) in zip(bars, otros_stats.iloc[::-1].iterrows()):
    ax.text(bar.get_width()+1, bar.get_y()+bar.get_height()/2,
            f"{row['total']:,}", va="center", fontsize=9)
ax.set_xlabel("Total de matrículas", fontsize=10)
ax.set_title("Volumen por forma jurídica\n(excl. Persona Natural)", fontsize=12, fontweight="bold")

# Tasa de activas
ax2 = axes[1]
tasa_vals = otros_stats["tasa"].values
colors_t  = [PALETTE_POS if t>=75 else (PALETTE_ACCENT if t>=50 else PALETTE_MAIN) for t in tasa_vals]
bars2 = ax2.barh(otros_stats["nombre_corto"][::-1], tasa_vals[::-1],
                 color=colors_t[::-1], alpha=0.85, edgecolor="white")
ax2.axvline(tasa_pn, color=PALETTE_NEG, linestyle="--", linewidth=1.5,
            label=f"Tasa P. Natural: {tasa_pn:.1f}%")
for bar, t in zip(bars2, tasa_vals[::-1]):
    ax2.text(bar.get_width()+0.5, bar.get_y()+bar.get_height()/2,
             f"{t:.1f}%", va="center", fontsize=9, fontweight="bold")
ax2.xaxis.set_major_formatter(mtick.PercentFormatter())
ax2.set_xlabel("Tasa de empresas activas (%)", fontsize=10)
ax2.set_title("Tasa de actividad por forma jurídica\nvs Persona Natural", fontsize=12, fontweight="bold")
ax2.legend(fontsize=9)

fig.suptitle(f"Formas jurídicas distintas a Persona Natural — {n_otro:,} registros | Tasa promedio: {tasa_otro:.1f}%",
             fontsize=13, fontweight="bold", color="#0D2B5E")
plt.tight_layout()
fig.savefig(DIR_FIGURES / "06b_otros_tipos_tasa.png", dpi=FIG_DPI)
plt.close()
print("✅  06b_otros_tipos_tasa.png")

# ── 6C · Evolución temporal top-4 tipos ─────────────────────────────────────
df_yr = df[df["ANIO_MATRICULA"].notna()].copy()
df_yr["ANIO_MATRICULA"] = df_yr["ANIO_MATRICULA"].astype(int)
df_yr = df_yr[(df_yr["ANIO_MATRICULA"]>=YEAR_MIN)&(df_yr["ANIO_MATRICULA"]<=YEAR_MAX)]
df_yr["tipo_simple"] = np.where(mask_pn.reindex(df_yr.index, fill_value=False),
                                "Persona Natural", df_yr[COL_ORG_JUR].map(NOMBRE_CORTO).fillna("Otro"))

top4 = ["Persona Natural","SAS","Ltda.","Emp. Unipersonal"]
evo = (
    df_yr[df_yr["tipo_simple"].isin(top4)]
    .groupby(["ANIO_MATRICULA","tipo_simple"]).size()
    .reset_index(name="n")
    .pivot(index="ANIO_MATRICULA", columns="tipo_simple", values="n").fillna(0)
)

fig, ax = plt.subplots(figsize=(14,5))
colores = [PALETTE_MAIN, PALETTE_ACCENT, PALETTE_POS, "#9B59B6"]
for col, color in zip(evo.columns, colores):
    ax.plot(evo.index, evo[col], linewidth=2.2, marker="o", markersize=3,
            label=col, color=color)
ax.set_xlabel("Año de matrícula", fontsize=11)
ax.set_ylabel("Nº de matrículas", fontsize=11)
ax.set_title("Evolución temporal por tipo de empresa (Top 4)", fontsize=13, fontweight="bold")
ax.legend(fontsize=9)
plt.tight_layout()
fig.savefig(DIR_FIGURES / "06c_evolucion_tipos.png", dpi=FIG_DPI)
plt.close()
print("✅  06c_evolucion_tipos.png")

# ── 6D · Comparativo PN vs Resto ─────────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(13, 5))
grupos = ["Persona Natural", "Otras formas jurídicas"]
totales = [n_pn, n_otro]
activas_g= [act_pn, act_otro]
tasas_g  = [tasa_pn, tasa_otro]

ax = axes[0]
x = np.arange(2)
b1 = ax.bar(x-0.2, totales,  0.35, label="Total",   color=[PALETTE_MAIN, PALETTE_SEC], alpha=0.7)
b2 = ax.bar(x+0.2, activas_g,0.35, label="Activas", color=[PALETTE_POS,  PALETTE_ACCENT], alpha=0.9)
ax.set_xticks(x); ax.set_xticklabels(grupos, fontsize=10)
ax.set_ylabel("Nº de empresas", fontsize=10)
ax.set_title("Volumen total vs Activas", fontsize=12, fontweight="bold")
ax.legend(fontsize=9)
for bars in [b1, b2]:
    for bar in bars:
        ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+20,
                f"{int(bar.get_height()):,}", ha="center", fontsize=9)

ax2 = axes[1]
bars_t = ax2.bar(grupos, tasas_g, color=[PALETTE_MAIN, PALETTE_ACCENT], alpha=0.85, edgecolor="white")
for bar, t in zip(bars_t, tasas_g):
    ax2.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.5,
             f"{t:.1f}%", ha="center", fontsize=12, fontweight="bold")
ax2.yaxis.set_major_formatter(mtick.PercentFormatter())
ax2.set_ylabel("Tasa de actividad (%)", fontsize=10)
ax2.set_title("Tasa de actividad comparada", fontsize=12, fontweight="bold")
ax2.set_ylim(0, max(tasas_g)*1.2)

fig.suptitle("Persona Natural vs Otras formas jurídicas", fontsize=14, fontweight="bold", color="#0D2B5E")
plt.tight_layout()
fig.savefig(DIR_FIGURES / "06d_comparativo_pn_vs_resto.png", dpi=FIG_DPI)
plt.close()
print("✅  06d_comparativo_pn_vs_resto.png")

# ── Tablas ────────────────────────────────────────────────────────────────────
df.groupby(COL_ORG_JUR).agg(
    conteo=(COL_ESTADO,"count"),
    activas=(COL_ESTADO, lambda x:(x=="ACTIVA").sum())
).assign(pct=lambda d: d["conteo"]/len(df)*100, tasa=lambda d: d["activas"]/d["conteo"]*100
).sort_values("conteo",ascending=False).reset_index().to_csv(DIR_TABLES/"06_tipos_empresa.csv",index=False)

pn_ciiu.to_csv(DIR_TABLES/"06_persona_natural.csv",index=False)
otros_stats.to_csv(DIR_TABLES/"06_otros_tipos.csv",index=False)
print("✅  tablas guardadas")