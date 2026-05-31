"""
ANÁLISIS DE GRADUADOS USTA — SNIES 2018-2024
Genera todos los datos necesarios para construir el HTML de la presentación.

USO:
    python analisis_graduados_usta.py

ENTRADA:
    SNIES_contexto.xlsx   (en la misma carpeta, o ajusta RUTA_EXCEL)

SALIDAS:
    outputs_usta/  → 11 gráficas PNG + CSVs con todos los datos

DEPENDENCIAS:
    pip install pandas openpyxl matplotlib numpy
"""

import os
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from scipy import stats as scipy_stats

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────
RUTA_EXCEL = "data/processed/SNIES_contexto.xlsx"   # <-- ajusta si el archivo está en otra carpeta
OUTPUT_DIR = "artifacts/outputs_usta"
os.makedirs(OUTPUT_DIR, exist_ok=True)

USTA_ID = 1704.0

# IDs de universidades multicampus para comparación
MULTICAMPUS = {
    1101.0: "U. Nacional",
    1806.0: "U. Libre",
    1818.0: "U. Cooperativa",
    1710.0: "U. P. Bolivariana",
    1701.0: "P. U. Javeriana",
    1704.0: "U. Santo Tomás",
    1707.0: "U. J. Tadeo Lozano",
    1801.0: "U. La Gran Colombia",
    2829.0: "UNIMINUTO",
    1719.0: "U. Católica",
}

COLOR_USTA   = "#001a3d"
COLOR_AZ     = "#1A4E8A"
COLOR_AZC    = "#4A90D9"
COLOR_AZL    = "#7BB3F0"
COLOR_DORADO = "#C49A22"
COLOR_ROJO   = "#dc2626"
COLOR_VERDE  = "#16a34a"

fmt_miles = mticker.FuncFormatter(lambda x, _: f"{x:,.0f}")
SEP = "=" * 70


# ─────────────────────────────────────────────────────────────
# PASO 1 · CARGA Y NORMALIZACIÓN
# ─────────────────────────────────────────────────────────────
print("Cargando datos SNIES...")
df = pd.read_excel(RUTA_EXCEL)

# Eliminar filas sin año o sin graduados
df = df.dropna(subset=["anio", "graduados"])
df["anio"] = df["anio"].astype(int)
df["graduados"] = pd.to_numeric(df["graduados"], errors="coerce").fillna(0)

# Normalizar campos con mayúsculas/minúsculas inconsistentes
def norm(col, valor):
    return df[col].str.upper().str.strip() == str(valor).upper().strip()

def norm_field(col):
    return df[col].str.upper().str.strip()

df["_sector"]     = norm_field("sector")
df["_nivel_ac"]   = norm_field("nivel_academico")
df["_nivel_form"] = norm_field("nivel_formacion")
df["_metodologia"]= norm_field("metodologia")
df["_sexo"]       = norm_field("sexo")

# Normalizar departamento IES para USTA (múltiples grafías)
def norm_dpto(s):
    s = str(s).upper().strip()
    if "BOGOT" in s:  return "Bogotá D.C."
    if "SANTANDER" in s and "NORTE" not in s: return "Santander"
    if "BOYAC" in s:  return "Boyacá"
    if "META" in s:   return "Meta"
    return s.title()

df["_dpto_ies"] = df["dpto_ies"].apply(norm_dpto)

AÑOS = sorted(df["anio"].unique())
print(f"✓ {len(df):,} registros cargados | Años: {AÑOS[0]}–{AÑOS[-1]}")

# Subconjuntos principales
USTA = df[df["ies_padre"] == USTA_ID].copy()
USTA["sede"] = USTA["_dpto_ies"].map({
    "Bogotá D.C.": "Bogotá",
    "Santander":   "Bucaramanga",
    "Boyacá":      "Tunja",
    "Meta":        "Villavicencio",
})

MC = df[df["ies_padre"].isin(MULTICAMPUS)].copy()
MC["nombre"] = MC["ies_padre"].map(MULTICAMPUS)

print(f"✓ USTA: {USTA['graduados'].sum():,.0f} graduados | Sedes: {USTA['sede'].dropna().unique().tolist()}")
print()


# ─────────────────────────────────────────────────────────────
# PASO 2 · KPIs GENERALES
# ─────────────────────────────────────────────────────────────
print(SEP)
print("KPIs GENERALES  (SNIES 2018-2024)")
print(SEP)

total_nac   = df["graduados"].sum()
total_usta  = USTA["graduados"].sum()
share_usta  = total_usta / total_nac * 100

usta_anual = USTA.groupby("anio")["graduados"].sum()
crec = (usta_anual[AÑOS[-1]] - usta_anual[AÑOS[0]]) / usta_anual[AÑOS[0]] * 100

# Ranking USTA entre TODAS las IES
ranking_all = df.groupby("ies_padre")["graduados"].sum().sort_values(ascending=False)
rank_usta   = list(ranking_all.index).index(USTA_ID) + 1

# Ranking USTA entre PRIVADAS
privadas = df[df["_sector"].isin(["PRIVADA", "PRIVADO"])]
ranking_priv = privadas.groupby("ies_padre")["graduados"].sum().sort_values(ascending=False)
rank_priv = list(ranking_priv.index).index(USTA_ID) + 1

print(f"  Total nacional (2018-2024):    {total_nac:,.0f}")
print(f"  Total USTA (2018-2024):        {total_usta:,.0f}")
print(f"  Participación USTA:            {share_usta:.2f}%")
print(f"  Crecimiento 2018→2024:         {crec:+.1f}%")
print(f"  Máximo histórico:              {usta_anual.max():,.0f} ({usta_anual.idxmax()})")
print(f"  Mínimo (COVID):                {usta_anual.min():,.0f} ({usta_anual.idxmin()})")
print(f"  Ranking USTA (todas las IES):  #{rank_usta}")
print(f"  Ranking USTA (privadas):       #{rank_priv}")

# Guardar KPIs
kpis = pd.DataFrame([{
    "total_usta": int(total_usta),
    "total_nacional": int(total_nac),
    "share_pct": round(share_usta, 2),
    "crecimiento_pct": round(crec, 1),
    "maximo": int(usta_anual.max()),
    "anio_maximo": int(usta_anual.idxmax()),
    "minimo": int(usta_anual.min()),
    "anio_minimo": int(usta_anual.idxmin()),
    "ranking_todas": rank_usta,
    "ranking_privadas": rank_priv,
}])
kpis.to_csv(f"{OUTPUT_DIR}/kpis_generales.csv", index=False)


# ─────────────────────────────────────────────────────────────
# PASO 3 · S1 · EVOLUCIÓN ANUAL
# ─────────────────────────────────────────────────────────────
print(f"\n{SEP}\nS1 · EVOLUCIÓN ANUAL\n{SEP}")

# USTA vs comparativas
COMP_IDS = {
    1701.0: "P. U. Javeriana",
    2829.0: "UNIMINUTO",
    1818.0: "U. Cooperativa",
}

ev_df = pd.DataFrame({"anio": AÑOS}).set_index("anio")
ev_df["U. Santo Tomás"] = USTA.groupby("anio")["graduados"].sum()
for uid, nombre in COMP_IDS.items():
    ev_df[nombre] = df[df["ies_padre"] == uid].groupby("anio")["graduados"].sum()

print(ev_df.to_string())
ev_df.to_csv(f"{OUTPUT_DIR}/s1_evolucion_anual.csv")

# Gráfica
fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(ev_df.index, ev_df["U. Santo Tomás"], color=COLOR_USTA,
        lw=3, marker="o", ms=7, label="U. Santo Tomás", zorder=5)
for i, col in enumerate(ev_df.columns[1:]):
    ax.plot(ev_df.index, ev_df[col], lw=1.8, linestyle="--", alpha=0.75,
            color=[COLOR_AZC, COLOR_DORADO, "#9e9e9e"][i], marker=".", ms=5, label=col)
ax.set_title("Evolución de Graduados — USTA vs Comparativas\n(SNIES 2018–2024)", fontsize=13)
ax.set_xlabel("Año"); ax.set_ylabel("Graduados")
ax.yaxis.set_major_formatter(fmt_miles)
ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/g1_evolucion_anual.png", dpi=150); plt.close()


# ─────────────────────────────────────────────────────────────
# PASO 4 · S2 · TOP 10 PROGRAMAS
# ─────────────────────────────────────────────────────────────
print(f"\n{SEP}\nS2 · TOP 10 PROGRAMAS\n{SEP}")

top_prog = (USTA.groupby("programa")["graduados"].sum()
               .sort_values(ascending=False).head(10).reset_index())
top_prog.columns = ["programa", "graduados"]

print(top_prog.to_string(index=False))
top_prog.to_csv(f"{OUTPUT_DIR}/s2_top_programas.csv", index=False)

# Gráfica
fig, ax = plt.subplots(figsize=(11, 6))
nombres = [p[:38] + "…" if len(p) > 38 else p for p in top_prog["programa"]]
colores = [COLOR_USTA if i == 0 else (COLOR_AZ if i < 4 else COLOR_AZC)
           for i in range(len(top_prog))]
bars = ax.barh(nombres[::-1], top_prog["graduados"].values[::-1],
               color=colores[::-1], height=0.65)
ax.bar_label(bars, fmt=lambda x: f"{x:,.0f}", padding=5, fontsize=9)
ax.set_title("USTA — Top 10 Programas por Graduados (2018–2024)", fontsize=12)
ax.set_xlabel("Graduados"); ax.xaxis.set_major_formatter(fmt_miles)
ax.grid(axis="x", alpha=0.3)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/g2_top_programas.png", dpi=150); plt.close()


# ─────────────────────────────────────────────────────────────
# PASO 5 · S3 · GÉNERO
# ─────────────────────────────────────────────────────────────
print(f"\n{SEP}\nS3 · GÉNERO\n{SEP}")

# Promedio total
gen_total = USTA.groupby("_sexo")["graduados"].sum()
total_g   = gen_total.sum()
mujeres   = gen_total.get("FEMENINO", gen_total.get("MUJER", 0))
pct_f     = mujeres / total_g * 100
pct_m     = 100 - pct_f
print(f"  Promedio 2018-2024:  {pct_f:.1f}% Mujeres · {pct_m:.1f}% Hombres")

# Evolución anual % mujeres
def pct_mujeres_anio(subdf):
    g = subdf.groupby(["anio", "_sexo"])["graduados"].sum().unstack(fill_value=0)
    g.columns = [c.upper() for c in g.columns]
    f_col = [c for c in g.columns if "FEM" in c or "MUJER" in c]
    total = g.sum(axis=1)
    return (g[f_col].sum(axis=1) / total * 100).round(1)

brecha_usta = pct_mujeres_anio(USTA)
brecha_df   = pd.DataFrame({"anio": AÑOS, "USTA": brecha_usta.reindex(AÑOS).values})
for uid, nombre in COMP_IDS.items():
    brecha_df[nombre] = pct_mujeres_anio(df[df["ies_padre"] == uid]).reindex(AÑOS).values
brecha_df = brecha_df.set_index("anio")

print("\n  Evolución % mujeres por año:")
print(brecha_df.to_string())
brecha_df.to_csv(f"{OUTPUT_DIR}/s3_brecha_genero.csv")

# Gráfica donut + brecha
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
axes[0].pie([pct_f, pct_m], labels=["Mujeres", "Hombres"],
            colors=[COLOR_AZC, COLOR_USTA], autopct="%1.1f%%", startangle=90,
            wedgeprops=dict(width=0.55))
axes[0].set_title("Distribución por Género\n(promedio 2018–2024)")
ax3b = axes[1]
ax3b.plot(brecha_df.index, brecha_df["USTA"], color=COLOR_USTA,
          lw=2.5, marker="o", ms=6, label="U. Santo Tomás")
for i, col in enumerate(brecha_df.columns[1:]):
    ax3b.plot(brecha_df.index, brecha_df[col], lw=1.5, linestyle="--", alpha=0.75,
              color=[COLOR_AZC, COLOR_DORADO, "#9e9e9e"][i], label=col)
ax3b.axhline(50, color=COLOR_ROJO, linestyle=":", lw=1.5, alpha=0.5, label="Paridad 50%")
ax3b.set_title("Evolución % Mujeres Graduadas")
ax3b.set_xlabel("Año"); ax3b.set_ylabel("% Mujeres")
ax3b.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
ax3b.legend(fontsize=9); ax3b.grid(axis="y", alpha=0.3)
plt.suptitle("USTA — Análisis de Género (SNIES 2018–2024)", fontsize=12)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/g3_genero.png", dpi=150); plt.close()


# ─────────────────────────────────────────────────────────────
# PASO 6 · S4 · NIVEL DE FORMACIÓN Y MODALIDAD
# ─────────────────────────────────────────────────────────────
print(f"\n{SEP}\nS4 · NIVEL DE FORMACIÓN Y MODALIDAD\n{SEP}")

# Nivel de formación normalizado
niv_map = {
    "UNIVERSITARIO":  "Pregrado",
    "UNIVERSITARIA":  "Pregrado",
    "TECNOLÓGICO":    "Pregrado",
    "TECNOLÓGICA":    "Pregrado",
    "TECNOLOGICO":    "Pregrado",
    "TECNOLOGICA":    "Pregrado",
    "FORMACIÓN TÉCNICA PROFESIONAL": "Pregrado",
    "FORMACION TECNICA PROFESIONAL": "Pregrado",
    "ESPECIALIZACIÓN UNIVERSITARIA": "Especialización",
    "ESPECIALIZACION UNIVERSITARIA": "Especialización",
    "ESPECIALIZACIÓN MÉDICO QUIRÚRGICA": "Especialización",
    "ESPECIALIZACION MEDICO QUIRURGICA": "Especialización",
    "ESPECIALIZACIÓN TECNOLÓGICA": "Especialización",
    "ESPECIALIZACIÓN TÉCNICO PROFESIONAL": "Especialización",
    "MAESTRÍA": "Maestría",
    "MAESTRIA":  "Maestría",
    "DOCTORADO": "Doctorado",
}
USTA["_niv_agrupado"] = USTA["_nivel_form"].map(niv_map).fillna("Otro")

niv_total = USTA.groupby("_niv_agrupado")["graduados"].sum()
niv_pct   = (niv_total / niv_total.sum() * 100).round(1)
niv_orden = ["Pregrado", "Especialización", "Maestría", "Doctorado"]
niv_pct   = niv_pct.reindex([n for n in niv_orden if n in niv_pct.index])

print("  Nivel de formación:")
for k, v in niv_pct.items():
    print(f"    {k:<25}: {v}%  ({niv_total[k]:,.0f} graduados)")

# Modalidad normalizada
mod_map = {
    "PRESENCIAL":              "Presencial",
    "PRESENCIAL-VIRTUAL":      "Presencial",
    "PRESENCIAL-DUAL":         "Presencial",
    "PRESENCIAL-A DISTANCIA":  "Presencial",
    "HÍBRIDA (PRESENCIAL-VIRTUAL)": "Presencial",
    "DISTANCIA (TRADICIONAL)": "Distancia",
    "A DISTANCIA":             "Distancia",
    "VIRTUAL-A DISTANCIA":     "Distancia",
    "DISTANCIA (VIRTUAL)":     "Virtual",
    "VIRTUAL":                 "Virtual",
    "VIRTUAL-DUAL":            "Virtual",
}
USTA["_mod_agrupada"] = USTA["_metodologia"].map(mod_map).fillna("Otro")

mod_total = USTA.groupby("_mod_agrupada")["graduados"].sum()
mod_pct   = (mod_total / mod_total.sum() * 100).round(1)
mod_orden = ["Presencial", "Distancia", "Virtual"]
mod_pct   = mod_pct.reindex([m for m in mod_orden if m in mod_pct.index])

print("\n  Modalidad:")
for k, v in mod_pct.items():
    print(f"    {k:<25}: {v}%  ({mod_total[k]:,.0f} graduados)")

# Guardar
pd.DataFrame({"nivel": niv_pct.index, "pct": niv_pct.values,
              "graduados": niv_total.reindex(niv_pct.index).values}).to_csv(
    f"{OUTPUT_DIR}/s4_nivel_formacion.csv", index=False)
pd.DataFrame({"modalidad": mod_pct.index, "pct": mod_pct.values,
              "graduados": mod_total.reindex(mod_pct.index).values}).to_csv(
    f"{OUTPUT_DIR}/s4_modalidad.csv", index=False)

# Gráfica
fig, axes = plt.subplots(1, 2, figsize=(12, 5))
axes[0].pie(niv_pct.values, labels=niv_pct.index,
            colors=[COLOR_USTA, COLOR_AZ, COLOR_AZC, COLOR_AZL],
            autopct="%1.1f%%", startangle=90, wedgeprops=dict(width=0.55))
axes[0].set_title("Nivel de Formación (2018–2024)")
axes[1].pie(mod_pct.values, labels=mod_pct.index,
            colors=[COLOR_AZ, COLOR_AZC, COLOR_AZL],
            autopct="%1.1f%%", startangle=90, wedgeprops=dict(width=0.55))
axes[1].set_title("Modalidad (2018–2024)")
plt.suptitle("USTA — Distribución por Nivel y Modalidad", fontsize=12)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/g4_nivel_modalidad.png", dpi=150); plt.close()


# ─────────────────────────────────────────────────────────────
# PASO 7 · S5 · COBERTURA GEOGRÁFICA
# ─────────────────────────────────────────────────────────────
print(f"\n{SEP}\nS5 · COBERTURA GEOGRÁFICA\n{SEP}")

geo_rows = []
for uid, nombre in MULTICAMPUS.items():
    sub = df[df["ies_padre"] == uid]
    n_dptos = sub["dpto_programa"].apply(norm_dpto).nunique()
    geo_rows.append({"universidad": nombre, "dptos_cubiertos": n_dptos})
geo_df = pd.DataFrame(geo_rows).sort_values("dptos_cubiertos", ascending=False).reset_index(drop=True)

print(geo_df.to_string(index=False))
geo_df.to_csv(f"{OUTPUT_DIR}/s5_cobertura_geo.csv", index=False)

# Gráfica
fig, ax = plt.subplots(figsize=(10, 5))
colors_geo = [COLOR_USTA if "Santo" in u else COLOR_AZ for u in geo_df["universidad"]]
bars = ax.bar(geo_df["universidad"], geo_df["dptos_cubiertos"],
              color=colors_geo, width=0.6)
ax.bar_label(bars, padding=3, fontsize=10)
ax.set_title("Departamentos Cubiertos por Universidad (2018–2024)", fontsize=12)
ax.set_ylabel("N° Departamentos"); ax.set_ylim(0, 38)
ax.grid(axis="y", alpha=0.3)
plt.xticks(rotation=20, ha="right", fontsize=9)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/g5_cobertura_geo.png", dpi=150); plt.close()


# ─────────────────────────────────────────────────────────────
# PASO 8 · S6 · % POSGRADO COMPARATIVO
# ─────────────────────────────────────────────────────────────
print(f"\n{SEP}\nS6 · % POSGRADO COMPARATIVO\n{SEP}")

posg_rows = []
for uid, nombre in MULTICAMPUS.items():
    sub   = df[df["ies_padre"] == uid]
    total = sub["graduados"].sum()
    posg  = sub[sub["_nivel_ac"] == "POSGRADO"]["graduados"].sum()
    pct   = posg / total * 100 if total > 0 else 0
    posg_rows.append({"universidad": nombre, "posg": int(posg),
                      "total": int(total), "pct_posgrado": round(pct, 1)})
posg_df = pd.DataFrame(posg_rows).sort_values("pct_posgrado", ascending=False).reset_index(drop=True)

print(posg_df.to_string(index=False))
posg_df.to_csv(f"{OUTPUT_DIR}/s6_posgrado_comparativo.csv", index=False)

# Gráfica
fig, ax = plt.subplots(figsize=(10, 5))
posg_s    = posg_df.sort_values("pct_posgrado")
colors_pg = [COLOR_USTA if "Santo" in u else "#AAAAAA" for u in posg_s["universidad"]]
bars = ax.barh(posg_s["universidad"], posg_s["pct_posgrado"],
               color=colors_pg, height=0.6)
ax.bar_label(bars, fmt=lambda x: f"{x:.1f}%", padding=5, fontsize=9)
ax.set_title("% Posgrado sobre Total de Graduados (2018–2024)", fontsize=12)
ax.set_xlabel("% Posgrado"); ax.grid(axis="x", alpha=0.3)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/g6_posgrado_comparativo.png", dpi=150); plt.close()


# ─────────────────────────────────────────────────────────────
# PASO 9 · S7 · GRADUADOS POR SEDE USTA
# ─────────────────────────────────────────────────────────────
print(f"\n{SEP}\nS7 · GRADUADOS POR SEDE USTA\n{SEP}")

sedes_df = (USTA.groupby(["anio", "sede"])["graduados"].sum()
               .unstack(fill_value=0))
sedes_df.index = sedes_df.index.astype(int)

print(sedes_df.to_string())
print("\n  Crecimiento 2018→2024 por sede:")
for col in sedes_df.columns:
    b, e = sedes_df[col].iloc[0], sedes_df[col].iloc[-1]
    print(f"    {col:<20}: {b:,.0f} → {e:,.0f}  ({(e-b)/b*100:+.1f}%)")

sedes_df.to_csv(f"{OUTPUT_DIR}/s7_sedes_usta.csv")

# Gráfica
colors_sedes = [COLOR_USTA, COLOR_AZ, COLOR_AZC, COLOR_AZL]
fig, axes = plt.subplots(1, 2, figsize=(15, 6))
for i, col in enumerate(sedes_df.columns):
    axes[0].plot(sedes_df.index, sedes_df[col], color=colors_sedes[i],
                 lw=2.5, marker="o", ms=6, label=col)
axes[0].set_title("Evolución por Sede"); axes[0].set_xlabel("Año"); axes[0].set_ylabel("Graduados")
axes[0].yaxis.set_major_formatter(fmt_miles); axes[0].legend(); axes[0].grid(axis="y", alpha=0.3)
bottom = np.zeros(len(sedes_df))
for i, col in enumerate(sedes_df.columns):
    axes[1].bar(sedes_df.index, sedes_df[col], bottom=bottom,
                label=col, color=colors_sedes[i], alpha=0.88)
    bottom += sedes_df[col].fillna(0).values
axes[1].set_title("Composición Apilada por Sede"); axes[1].set_xlabel("Año"); axes[1].set_ylabel("Graduados")
axes[1].yaxis.set_major_formatter(fmt_miles); axes[1].legend(); axes[1].grid(axis="y", alpha=0.3)
plt.suptitle("USTA — Graduados por Sede (SNIES 2018–2024)", fontsize=13)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/g7_sedes_usta.png", dpi=150); plt.close()


# ─────────────────────────────────────────────────────────────
# PASO 10 · S8 · POSGRADO POR SEDE USTA
# ─────────────────────────────────────────────────────────────
print(f"\n{SEP}\nS8 · POSGRADO POR SEDE USTA\n{SEP}")

USTA_posg = USTA[USTA["_nivel_ac"] == "POSGRADO"]

posg_sede = (USTA_posg.groupby(["anio", "sede"])["graduados"].sum()
                       .unstack(fill_value=0))
posg_sede.index = posg_sede.index.astype(int)

print("  Posgrados por sede y año:")
print(posg_sede.to_string())

posg_tipo = (USTA_posg.groupby("_nivel_form")["graduados"].sum()
                       .sort_values(ascending=False))
print("\n  Posgrados por tipo:")
for k, v in posg_tipo.items():
    print(f"    {k:<45}: {v:,.0f}  ({v/posg_tipo.sum()*100:.1f}%)")

posg_sede.to_csv(f"{OUTPUT_DIR}/s8_posgrado_sede.csv")
pd.DataFrame({"tipo": posg_tipo.index, "graduados": posg_tipo.values}).to_csv(
    f"{OUTPUT_DIR}/s8_posgrado_tipo.csv", index=False)


# ─────────────────────────────────────────────────────────────
# PASO 11 · S9 · DINÁMICA DE PROGRAMAS
# ─────────────────────────────────────────────────────────────
print(f"\n{SEP}\nS9 · DINÁMICA DE PROGRAMAS (2018→2024)\n{SEP}")

prog_anio = USTA.groupby(["programa", "anio"])["graduados"].sum().unstack(fill_value=0)
prog_base = prog_anio[AÑOS[0]]
prog_end  = prog_anio[AÑOS[-1]]
validos   = (prog_base > 20) & (prog_end > 0)   # filtro: al menos 20 en 2018
dyn = ((prog_end[validos] - prog_base[validos]) / prog_base[validos] * 100).round(1)
dyn_df = dyn.sort_values(ascending=False).reset_index()
dyn_df.columns = ["programa", "variacion_pct"]

print(dyn_df.head(15).to_string(index=False))
dyn_df.to_csv(f"{OUTPUT_DIR}/s9_dinamica_programas.csv", index=False)

# Gráfica (top 12 por variación absoluta)
top_dyn = pd.concat([dyn_df.head(6), dyn_df.tail(6)]).sort_values("variacion_pct")
fig, ax = plt.subplots(figsize=(11, 6))
colors_dyn = [COLOR_ROJO if v < 0 else COLOR_AZ for v in top_dyn["variacion_pct"]]
names_dyn  = [p[:35] + "…" if len(p) > 35 else p for p in top_dyn["programa"]]
bars = ax.barh(names_dyn, top_dyn["variacion_pct"], color=colors_dyn, height=0.65)
ax.bar_label(bars, fmt=lambda x: f"{x:+.0f}%", padding=5, fontsize=9)
ax.axvline(0, color="black", lw=0.8)
ax.set_title("Dinámica de Programas — Variación % (2018→2024)\nTop crecimientos y decrecimientos", fontsize=12)
ax.set_xlabel("Variación %"); ax.grid(axis="x", alpha=0.3)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/g8_dinamica_programas.png", dpi=150); plt.close()


# ─────────────────────────────────────────────────────────────
# PASO 12 · S10 · PROYECCIÓN 2025-2027
# ─────────────────────────────────────────────────────────────
print(f"\n{SEP}\nS10 · PROYECCIÓN 2025-2027\n{SEP}")

x = np.array(AÑOS, dtype=float)
y = usta_anual.reindex(AÑOS).values.astype(float)

slope, intercept, r, p, se = scipy_stats.linregress(x, y)
t_crit = scipy_stats.t.ppf(0.975, df=len(x) - 2)

proy_años = [2025, 2026, 2027]
proy_vals, ic_inf, ic_sup = [], [], []
for yr in proy_años:
    pred = intercept + slope * yr
    # Error estándar de predicción
    se_pred = se * np.sqrt(1 + 1/len(x) + (yr - x.mean())**2 / ((x - x.mean())**2).sum())
    proy_vals.append(round(pred))
    ic_inf.append(round(pred - t_crit * se_pred))
    ic_sup.append(round(pred + t_crit * se_pred))

print(f"  Regresión lineal: y = {slope:.0f}·año + ({intercept:.0f})  |  R²= {r**2:.4f}")
print(f"\n  Proyección:")
for yr, v, lo, hi in zip(proy_años, proy_vals, ic_inf, ic_sup):
    print(f"    {yr}: {v:,.0f}  (IC 95%: {lo:,.0f} – {hi:,.0f})")

proy_df = pd.DataFrame({"anio": proy_años, "proyectado": proy_vals,
                         "ic_inf": ic_inf, "ic_sup": ic_sup})
proy_df.to_csv(f"{OUTPUT_DIR}/s10_proyeccion.csv", index=False)

# Gráfica
fig, ax = plt.subplots(figsize=(12, 5))
ax.fill_between(proy_años, ic_inf, ic_sup, color=COLOR_AZC, alpha=0.15, label="IC 95%")
ax.plot(AÑOS, y, color=COLOR_USTA, lw=3, marker="o", ms=6, label="Real (SNIES)")
ax.plot([AÑOS[-1]] + proy_años, [y[-1]] + proy_vals,
        color=COLOR_AZC, lw=2.5, linestyle="--", marker="o", ms=5, label="Proyectado")
ax.set_title("USTA — Proyección de Graduados 2025–2027\n(Regresión lineal | SNIES 2018–2024)", fontsize=12)
ax.set_xlabel("Año"); ax.set_ylabel("Graduados")
ax.yaxis.set_major_formatter(fmt_miles); ax.legend(); ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/g9_proyeccion.png", dpi=150); plt.close()


# ─────────────────────────────────────────────────────────────
# PASO 13 · S11 · RANKING PRIVADAS (contexto nacional)
# ─────────────────────────────────────────────────────────────
print(f"\n{SEP}\nS11 · RANKING PRIVADAS (contexto nacional)\n{SEP}")

priv_df = df[df["_sector"].isin(["PRIVADA", "PRIVADO"])]
rank_priv_full = (priv_df.groupby(["ies_padre", "institucion"])["graduados"]
                          .sum().reset_index()
                          .sort_values("graduados", ascending=False).head(15))
rank_priv_full["ranking"] = range(1, len(rank_priv_full) + 1)

# Posgrado %
for idx, row in rank_priv_full.iterrows():
    sub  = priv_df[priv_df["ies_padre"] == row["ies_padre"]]
    posg = sub[sub["_nivel_ac"] == "POSGRADO"]["graduados"].sum()
    rank_priv_full.loc[idx, "pct_posgrado"] = round(posg / row["graduados"] * 100, 1)

print(rank_priv_full[["ranking", "institucion", "graduados", "pct_posgrado"]].to_string(index=False))
rank_priv_full.to_csv(f"{OUTPUT_DIR}/s11_ranking_privadas.csv", index=False)

# Versión reducida multicampus para gráfica
nac_mc = []
for uid, nombre in MULTICAMPUS.items():
    sub   = df[df["ies_padre"] == uid]
    total = sub["graduados"].sum()
    posg  = sub[sub["_nivel_ac"] == "POSGRADO"]["graduados"].sum()
    nac_mc.append({"universidad": nombre, "graduados": int(total),
                   "pct_posgrado": round(posg / total * 100, 1) if total > 0 else 0})
nac_df = pd.DataFrame(nac_mc).sort_values("graduados", ascending=False).reset_index(drop=True)
nac_df.to_csv(f"{OUTPUT_DIR}/s11_contexto_mc.csv", index=False)


# ─────────────────────────────────────────────────────────────
# PASO 14 · S12 · ÁREAS DE CONOCIMIENTO
# ─────────────────────────────────────────────────────────────
print(f"\n{SEP}\nS12 · ÁREAS DE CONOCIMIENTO\n{SEP}")

areas_2018 = (USTA[USTA["anio"] == AÑOS[0]]
              .groupby("area_conocimiento")["graduados"].sum())
areas_2024 = (USTA[USTA["anio"] == AÑOS[-1]]
              .groupby("area_conocimiento")["graduados"].sum())

areas_df = pd.DataFrame({"grad_2018": areas_2018, "grad_2024": areas_2024}).fillna(0)
areas_df["variacion_pct"] = ((areas_df["grad_2024"] - areas_df["grad_2018"])
                              / areas_df["grad_2018"].replace(0, np.nan) * 100).round(1)
areas_df = areas_df.sort_values("grad_2024", ascending=False).reset_index()
areas_df.columns = ["area", "grad_2018", "grad_2024", "variacion_pct"]

print(areas_df.to_string(index=False))
areas_df.to_csv(f"{OUTPUT_DIR}/s12_areas_conocimiento.csv", index=False)

# Gráfica
fig, ax = plt.subplots(figsize=(12, 5))
x = np.arange(len(areas_df)); w = 0.38
ax.bar(x - w/2, areas_df["grad_2018"], width=w, label="2018",
       color=COLOR_USTA, alpha=0.75, zorder=3)
ax.bar(x + w/2, areas_df["grad_2024"], width=w, label="2024",
       color=COLOR_AZC, alpha=0.9, zorder=3)
labels_areas = [a[:22] + "…" if len(a) > 22 else a for a in areas_df["area"]]
ax.set_xticks(x); ax.set_xticklabels(labels_areas, rotation=20, ha="right", fontsize=8)
ax.yaxis.set_major_formatter(fmt_miles)
ax.set_title("USTA — Graduados por Área de Conocimiento: 2018 vs 2024", fontsize=12)
ax.set_ylabel("Graduados"); ax.legend(); ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/g10_areas_conocimiento.png", dpi=150); plt.close()


# ─────────────────────────────────────────────────────────────
# PASO 15 · S12b · DISTRIBUCIÓN SEMESTRAL
# ─────────────────────────────────────────────────────────────
print(f"\n{SEP}\nS12b · DISTRIBUCIÓN SEMESTRAL\n{SEP}")

sem_df = (USTA.groupby(["anio", "semestre"])["graduados"].sum()
               .unstack(fill_value=0))
sem_df.columns = [f"S{int(c)}" for c in sem_df.columns]
sem_df.index   = sem_df.index.astype(int)
sem_df["total"]  = sem_df.sum(axis=1)
sem_df["pct_S2"] = (sem_df["S2"] / sem_df["total"] * 100).round(1)

print(sem_df.to_string())
sem_df.to_csv(f"{OUTPUT_DIR}/s12b_semestral.csv")

# Gráfica
fig, ax = plt.subplots(figsize=(10, 5))
ax.bar(sem_df.index, sem_df["S1"], label="Semestre 1", color=COLOR_AZC, alpha=0.9)
ax.bar(sem_df.index, sem_df["S2"], bottom=sem_df["S1"],
       label="Semestre 2", color=COLOR_USTA, alpha=0.88)
for yr in sem_df.index:
    ax.text(yr, sem_df.loc[yr, "total"] + 80,
            f'{sem_df.loc[yr, "pct_S2"]}% S2', ha="center", fontsize=8, color=COLOR_USTA)
ax.set_title("USTA — Distribución Semestral de Graduados (2018–2024)", fontsize=12)
ax.set_xlabel("Año"); ax.set_ylabel("Graduados")
ax.yaxis.set_major_formatter(fmt_miles); ax.legend(); ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/g11_semestral.png", dpi=150); plt.close()


# ─────────────────────────────────────────────────────────────
# TABLA MAESTRA RESUMEN
# ─────────────────────────────────────────────────────────────
print(f"\n{SEP}\nTABLA MAESTRA MULTICAMPUS\n{SEP}")

resumen = []
for uid, nombre in MULTICAMPUS.items():
    sub    = df[df["ies_padre"] == uid]
    total  = sub["graduados"].sum()
    g2018  = sub[sub["anio"] == AÑOS[0]]["graduados"].sum()
    g2024  = sub[sub["anio"] == AÑOS[-1]]["graduados"].sum()
    crec_u = (g2024 - g2018) / g2018 * 100 if g2018 > 0 else 0
    posg   = sub[sub["_nivel_ac"] == "POSGRADO"]["graduados"].sum()
    pct_pg = posg / total * 100 if total > 0 else 0
    f_vals = sub[sub["_sexo"].isin(["FEMENINO", "MUJER"])]["graduados"].sum()
    pct_f  = f_vals / total * 100 if total > 0 else 0
    dptos  = sub["dpto_programa"].apply(norm_dpto).nunique()
    resumen.append({
        "Universidad":     nombre,
        "Total 2018-2024": int(total),
        "Grad. 2018":      int(g2018),
        "Grad. 2024":      int(g2024),
        "Crec. %":         round(crec_u, 1),
        "% Posgrado":      round(pct_pg, 1),
        "% Mujeres":       round(pct_f, 1),
        "N° Dptos.":       dptos,
    })

resumen_df = pd.DataFrame(resumen).sort_values("Total 2018-2024", ascending=False)
print(resumen_df.to_string(index=False))
resumen_df.to_csv(f"{OUTPUT_DIR}/tabla_maestra.csv", index=False)


# ─────────────────────────────────────────────────────────────
# RESUMEN FINAL
# ─────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print(f"✓ ANÁLISIS COMPLETO — Archivos en: {OUTPUT_DIR}/")
print(SEP)
print(f"""
ARCHIVOS GENERADOS:
  {OUTPUT_DIR}/
  ├── g1_evolucion_anual.png        ← USTA vs comparativas
  ├── g2_top_programas.png          ← Top 10 programas
  ├── g3_genero.png                 ← Donut género + brecha temporal
  ├── g4_nivel_modalidad.png        ← Nivel y modalidad
  ├── g5_cobertura_geo.png          ← Cobertura geográfica
  ├── g6_posgrado_comparativo.png   ← % Posgrado comparativo
  ├── g7_sedes_usta.png             ← Sedes líneas + apilada
  ├── g8_dinamica_programas.png     ← Variación % por programa
  ├── g9_proyeccion.png             ← Proyección 2025-2027
  ├── g10_areas_conocimiento.png    ← Áreas 2018 vs 2024
  ├── g11_semestral.png             ← Distribución semestral
  ├── kpis_generales.csv
  ├── s1_evolucion_anual.csv
  ├── s2_top_programas.csv
  ├── s3_brecha_genero.csv
  ├── s4_nivel_formacion.csv  /  s4_modalidad.csv
  ├── s5_cobertura_geo.csv
  ├── s6_posgrado_comparativo.csv
  ├── s7_sedes_usta.csv
  ├── s8_posgrado_sede.csv  /  s8_posgrado_tipo.csv
  ├── s9_dinamica_programas.csv
  ├── s10_proyeccion.csv
  ├── s11_ranking_privadas.csv  /  s11_contexto_mc.csv
  ├── s12_areas_conocimiento.csv
  ├── s12b_semestral.csv
  └── tabla_maestra.csv
""")