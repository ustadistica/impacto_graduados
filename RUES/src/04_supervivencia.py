"""
src/04_supervivencia.py  (versión actualizada)
===============================================
Indicador 4 — Supervivencia empresarial con tabla por cohorte + sector CIIU.

Calcula:
  · Empresas activas creadas en el último año (2024)
  · Empresas activas creadas hace 3 años (2021) que siguen activas
  · Empresas creadas hace más de 10 años (≤2014) que siguen activas
  · Para cada cohorte: top sectores CIIU y estadísticas

Salidas:
  outputs/tables/04_supervivencia_cohorte.csv
  outputs/tables/04_cohorte_2024.csv
  outputs/tables/04_cohorte_3anios.csv
  outputs/tables/04_cohorte_10anios.csv
  outputs/figures/04a_curva_supervivencia.png
  outputs/figures/04b_tabla_cohortes.png
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec

from config.settings import (
    SAMPLE_PATH, DIR_FIGURES, DIR_TABLES,
    COL_ESTADO, COL_CIIU, COL_CAMARA,
    PALETTE_POS, PALETTE_NEG, PALETTE_MAIN, PALETTE_ACCENT,
    YEAR_MIN, YEAR_MAX, FIG_DPI,
)

plt.rcParams.update({"font.family": "DejaVu Sans",
                     "axes.spines.top": False, "axes.spines.right": False})

CIIU_DESC = {
    "9999": "Sin clasificar", "4711": "Supermercados", "5611": "Restaurantes",
    "5630": "Bares/cantinas", "9499": "Otras asociaciones", "9602": "Peluquerías",
    "4771": "Tiendas ropa", "4719": "Misceláneas", "4773": "Farmacias",
    "1410": "Confección", "8299": "Servicios admin.", "4752": "Ferreterías",
    "4520": "Talleres vehículos", "6810": "Inmobiliario", "4290": "Construcción",
    "7110": "Ingeniería/arquitectura", "5619": "Otros restaurantes",
}

# ── Carga ──────────────────────────────────────────────────────────────────────
df = pd.read_csv(SAMPLE_PATH, low_memory=False)
df[COL_ESTADO] = df[COL_ESTADO].str.strip().str.upper()
df[COL_CIIU]   = df[COL_CIIU].astype(str).str.strip().str.upper().str.replace(r"[^A-Z0-9]","",regex=True)
df = df[df["ANIO_MATRICULA"].notna()].copy()
df["ANIO_MATRICULA"] = df["ANIO_MATRICULA"].astype(int)
df = df[(df["ANIO_MATRICULA"] >= YEAR_MIN) & (df["ANIO_MATRICULA"] <= YEAR_MAX)]

# ── Curva de supervivencia por cohorte ────────────────────────────────────────
cohorte = (
    df.groupby("ANIO_MATRICULA")
    .agg(total=(COL_ESTADO,"count"),
         activas=(COL_ESTADO, lambda x: (x=="ACTIVA").sum()))
    .assign(tasa=lambda d: d["activas"]/d["total"]*100)
    .reset_index()
)

# ── 4A · Curva ────────────────────────────────────────────────────────────────
avg = cohorte["tasa"].mean()
fig, ax = plt.subplots(figsize=(13, 5))
ax.fill_between(cohorte["ANIO_MATRICULA"], cohorte["tasa"], alpha=0.12, color=PALETTE_POS)
ax.plot(cohorte["ANIO_MATRICULA"], cohorte["tasa"],
        color=PALETTE_POS, linewidth=2.5, marker="o", markersize=4)
ax.axhline(avg, color=PALETTE_ACCENT, linestyle="--", linewidth=1.5,
           label=f"Promedio: {avg:.1f}%")
ax.yaxis.set_major_formatter(mtick.PercentFormatter())
ax.set_xlabel("Año de matrícula", fontsize=11)
ax.set_ylabel("% empresas activas", fontsize=11)
ax.set_title("Curva de supervivencia por cohorte de matrícula", fontsize=14, fontweight="bold")
ax.legend(fontsize=10)
ax.set_xlim(YEAR_MIN, YEAR_MAX)
plt.tight_layout()
fig.savefig(DIR_FIGURES / "04a_curva_supervivencia.png", dpi=FIG_DPI)
plt.close()
print("✅  04a_curva_supervivencia.png")

# ── Función para tabla de cohorte + CIIU ─────────────────────────────────────
def tabla_cohorte(df_cohorte: pd.DataFrame, nombre: str) -> pd.DataFrame:
    activas = df_cohorte[df_cohorte[COL_ESTADO] == "ACTIVA"]
    total_cohorte   = len(df_cohorte)
    total_activas_c = len(activas)
    tasa_c          = total_activas_c / total_cohorte * 100 if total_cohorte else 0

    # Top sectores de las activas
    ciiu_c = (
        activas.groupby(COL_CIIU)
        .size()
        .reset_index(name="n_activas")
        .sort_values("n_activas", ascending=False)
        .head(8)
    )
    ciiu_c["descripcion"] = ciiu_c[COL_CIIU].map(CIIU_DESC).fillna("Otro")
    ciiu_c["pct_en_cohorte"] = (ciiu_c["n_activas"] / total_activas_c * 100).round(1)

    print(f"\n── {nombre} ──")
    print(f"   Total registros   : {total_cohorte:,}")
    print(f"   Empresas activas  : {total_activas_c:,}")
    print(f"   Tasa supervivencia: {tasa_c:.1f}%")
    print(f"   Top sectores activos:")
    for _, r in ciiu_c.iterrows():
        print(f"     {r[COL_CIIU]} · {r['descripcion']:<25} {r['n_activas']:>4} ({r['pct_en_cohorte']:.1f}%)")

    ciiu_c["cohorte"]          = nombre
    ciiu_c["total_cohorte"]    = total_cohorte
    ciiu_c["activas_cohorte"]  = total_activas_c
    ciiu_c["tasa_supervivencia"] = round(tasa_c, 1)
    return ciiu_c

# ── Las tres cohortes ─────────────────────────────────────────────────────────
anio_actual = 2024
c_ult   = df[df["ANIO_MATRICULA"] == anio_actual]
c_3anio = df[df["ANIO_MATRICULA"] == anio_actual - 3]
c_10mas = df[df["ANIO_MATRICULA"] <= anio_actual - 10]

t_ult   = tabla_cohorte(c_ult,   f"Último año ({anio_actual})")
t_3anio = tabla_cohorte(c_3anio, f"Hace 3 años ({anio_actual-3})")
t_10mas = tabla_cohorte(c_10mas, f"Más de 10 años (≤{anio_actual-10})")

# ── 4B · Visualización de las tres tablas ─────────────────────────────────────
def meta(df_c, df_full):
    act = df_full[df_full[COL_ESTADO]=="ACTIVA"]
    tot = len(df_full)
    return len(act), tot, len(act)/tot*100 if tot else 0

m_ult   = meta(c_ult, c_ult)
m_3anio = meta(c_3anio, c_3anio)
m_10mas = meta(c_10mas, c_10mas)

cohortes_meta = [
    (f"Último año\n({anio_actual})", m_ult,   t_ult,   PALETTE_ACCENT),
    (f"Hace 3 años\n({anio_actual-3})", m_3anio, t_3anio, PALETTE_MAIN),
    (f"Más de 10 años\n(≤{anio_actual-10})", m_10mas, t_10mas, PALETTE_POS),
]

fig = plt.figure(figsize=(18, 12))
gs = GridSpec(2, 3, figure=fig, hspace=0.45, wspace=0.35)

# Fila 1: KPI cards
for col, (titulo, (n_act, n_tot, tasa), _, color) in enumerate(cohortes_meta):
    ax = fig.add_subplot(gs[0, col])
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    ax.axis("off")
    ax.add_patch(plt.Rectangle((0,0),1,1, fill=True, color="#F0F5FD",
                                transform=ax.transAxes, zorder=0, linewidth=0))
    ax.text(0.5, 0.82, titulo, ha="center", fontsize=13, fontweight="bold", color="#0D2B5E")
    ax.text(0.5, 0.55, f"{n_act:,}", ha="center", fontsize=30, fontweight="bold", color=color)
    ax.text(0.5, 0.38, "empresas activas", ha="center", fontsize=11, color="#8896B0")
    ax.text(0.5, 0.22, f"de {n_tot:,} registros", ha="center", fontsize=10, color="#8896B0")
    ax.text(0.5, 0.08, f"Supervivencia: {tasa:.1f}%", ha="center", fontsize=12,
            fontweight="bold", color=color,
            bbox=dict(boxstyle="round,pad=0.3", facecolor=color, alpha=0.12))

# Fila 2: Top sectores de activas
for col, (titulo, (n_act,_,_), df_top, color) in enumerate(cohortes_meta):
    ax = fig.add_subplot(gs[1, col])
    n = min(6, len(df_top))
    top = df_top.head(n).reset_index(drop=True)
    labels = [f"{r[COL_CIIU]} · {r['descripcion'][:20]}" for _, r in top.iterrows()]
    vals   = top["n_activas"].values
    bar_colors = [color] * n
    # gradiente de opacidad
    bar_colors_alpha = [(*plt.matplotlib.colors.to_rgb(color), 0.5 + 0.5*(1-i/n)) for i in range(n)]

    bars = ax.barh(labels[::-1], vals[::-1], color=PALETTE_MAIN, alpha=0.75, edgecolor="white")
    for bar, v in zip(bars, vals[::-1]):
        ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2,
                str(v), va="center", fontsize=8.5)
    ax.set_xlabel("Nº de activas", fontsize=9)
    ax.set_title(f"Top sectores — {titulo.replace(chr(10),' ')}", fontsize=10, fontweight="bold", color="#0D2B5E")
    ax.tick_params(axis="y", labelsize=8)

fig.suptitle("Supervivencia empresarial por cohorte y sector económico",
             fontsize=16, fontweight="bold", y=0.98, color="#0D2B5E")
fig.savefig(DIR_FIGURES / "04b_tabla_cohortes.png", dpi=FIG_DPI, bbox_inches="tight")
plt.close()
print("✅  04b_tabla_cohortes.png")

# ── Tablas CSV ────────────────────────────────────────────────────────────────
cohorte.to_csv(DIR_TABLES / "04_supervivencia_cohorte.csv", index=False)
t_ult.to_csv(DIR_TABLES   / "04_cohorte_ultimo_anio.csv",   index=False)
t_3anio.to_csv(DIR_TABLES / "04_cohorte_3_anios.csv",       index=False)
t_10mas.to_csv(DIR_TABLES / "04_cohorte_10_mas_anios.csv",  index=False)
print("✅  tablas guardadas")