"""
01_eda_snies.py
Análisis Exploratorio de Datos — Graduados SNIES
Ejecutar desde la raíz del proyecto: python notebooks/01_eda_snies.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'src'))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

from ingest import load_data, get_usta_data

# ── Configuración visual ──────────────────────────────────────────────────────
sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)
plt.rcParams["figure.figsize"] = (13, 5)
plt.rcParams["axes.spines.top"] = False
plt.rcParams["axes.spines.right"] = False
fmt_miles = mticker.FuncFormatter(lambda x, _: f"{int(x):,}")


def seccion(titulo):
    print(f"\n{'='*55}")
    print(f"  {titulo}")
    print(f"{'='*55}")


# ── 1. CARGA ──────────────────────────────────────────────────────────────────
df = load_data()
usta = get_usta_data(df)


# ── 2. RESUMEN GENERAL ────────────────────────────────────────────────────────
seccion("1. RESUMEN GENERAL")
print(f"  Filas         : {df.shape[0]:,}")
print(f"  Columnas      : {df.shape[1]}")
print(f"  Años          : {sorted(df['anio'].dropna().unique().tolist())}")
print(f"  Semestres     : {sorted(df['semestre'].dropna().unique().tolist())}")
print(f"  Instituciones : {df['institucion'].nunique():,}")
print(f"  Programas     : {df['programa'].nunique():,}")
print(f"  Departamentos : {df['dpto_programa'].nunique()}")
print()
nulos = df.isnull().sum()
print("  Nulos por columna:")
print(nulos[nulos > 0] if nulos.sum() > 0 else "  Sin valores nulos ✓")
print()
print("  Estadísticas del target (graduados):")
print(df["graduados"].describe().round(2).to_string())


# ── 3. DISTRIBUCIÓN DEL TARGET ───────────────────────────────────────────────
seccion("2. DISTRIBUCIÓN DEL TARGET")

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

axes[0].hist(df["graduados"], bins=70, color="#1976D2", edgecolor="white", alpha=0.85)
axes[0].axvline(df["graduados"].mean(),   color="#FF5722", linestyle="--", lw=1.8,
                label=f"Media: {df['graduados'].mean():.1f}")
axes[0].axvline(df["graduados"].median(), color="#FFC107", linestyle="--", lw=1.8,
                label=f"Mediana: {df['graduados'].median():.1f}")
axes[0].set_title("Distribución de Graduados", fontweight="bold")
axes[0].set_xlabel("Número de Graduados")
axes[0].set_ylabel("Frecuencia")
axes[0].legend()

axes[1].hist(np.log1p(df["graduados"]), bins=70, color="#388E3C", edgecolor="white", alpha=0.85)
axes[1].set_title("Distribución de Graduados — Escala log1p", fontweight="bold")
axes[1].set_xlabel("log(Graduados + 1)")
axes[1].set_ylabel("Frecuencia")

plt.suptitle("Variable Target: Graduados", fontsize=14, fontweight="bold")
plt.tight_layout()
plt.savefig("artifacts/eda_01_distribucion_target.png", dpi=150)
plt.show()

print(f"  Asimetría (skewness) : {df['graduados'].skew():.3f}")
print(f"  Curtosis             : {df['graduados'].kurt():.3f}")


# ── 4. EVOLUCIÓN TEMPORAL ─────────────────────────────────────────────────────
seccion("3. EVOLUCIÓN TEMPORAL")

pivot = df.groupby(["anio", "semestre"])["graduados"].sum().unstack()
pivot.columns = [f"Semestre {int(c)}" for c in pivot.columns]

ax = pivot.plot(kind="bar", figsize=(13, 5), color=["#1565C0", "#F57C00"],
                edgecolor="white", width=0.7)
ax.set_title("Total Graduados por Año y Semestre", fontsize=14, fontweight="bold")
ax.set_xlabel("Año")
ax.set_ylabel("Total Graduados")
ax.yaxis.set_major_formatter(fmt_miles)
ax.legend(title="Semestre")
plt.xticks(rotation=45)
for container in ax.containers:
    ax.bar_label(container, fmt=lambda x: f"{int(x):,}", padding=3, fontsize=8)

plt.tight_layout()
plt.savefig("artifacts/eda_02_evolucion_temporal.png", dpi=150)
plt.show()

total_anio = df.groupby("anio")["graduados"].sum()
print("\n  Total graduados por año:")
for anio, total in total_anio.items():
    print(f"    {anio}: {total:,}")


# ── 5. BRECHA DE GÉNERO ───────────────────────────────────────────────────────
seccion("4. BRECHA DE GÉNERO")

fig, axes = plt.subplots(1, 2, figsize=(13, 5))
colores_sexo = {"Femenino": "#E91E63", "Masculino": "#1565C0"}

por_sexo = (df[df["sexo"].isin(["Femenino", "Masculino"])]
            .groupby("sexo")["graduados"].sum())
por_sexo.plot(
    kind="pie", ax=axes[0],
    autopct="%1.1f%%", colors=list(colores_sexo.values()),
    startangle=90, wedgeprops={"edgecolor": "white", "linewidth": 2.5},
    textprops={"fontsize": 12}
)
axes[0].set_title("Proporción de Graduados por Sexo", fontweight="bold")
axes[0].set_ylabel("")

evol = (df[df["sexo"].isin(["Femenino", "Masculino"])]
        .groupby(["anio", "sexo"])["graduados"].sum().unstack())
for sexo, color in colores_sexo.items():
    if sexo in evol.columns:
        axes[1].plot(evol.index.astype(int), evol[sexo], marker="o", label=sexo,
                     color=color, linewidth=2.5, markersize=7)

axes[1].set_title("Evolución de Graduados por Sexo y Año", fontweight="bold")
axes[1].set_xlabel("Año")
axes[1].set_ylabel("Total Graduados")
axes[1].yaxis.set_major_formatter(fmt_miles)
axes[1].legend(title="Sexo")

plt.tight_layout()
plt.savefig("artifacts/eda_03_genero.png", dpi=150)
plt.show()

brecha = (df[df["sexo"].isin(["Femenino", "Masculino"])]
          .groupby("sexo")["graduados"].sum())
ratio = brecha.get("Femenino", 0) / brecha.get("Masculino", 1)
print(f"  Ratio Femenino/Masculino: {ratio:.3f}  ({ratio*100 - 100:+.1f}% diferencia)")


# ── 6. ÁREA DE CONOCIMIENTO ───────────────────────────────────────────────────
seccion("5. ÁREA DE CONOCIMIENTO")

area_total = (
    df[df["area_conocimiento"].notna() &
       ~df["area_conocimiento"].isin(["Sin clasificar"])]
    .groupby("area_conocimiento")["graduados"].sum()
    .sort_values()
)
palette_a = sns.color_palette("Blues_r", len(area_total))

fig, ax = plt.subplots(figsize=(13, 7))
bars = ax.barh(area_total.index, area_total.values, color=palette_a, edgecolor="white")
for bar, val in zip(bars, area_total.values):
    ax.text(bar.get_width() + area_total.max() * 0.005,
            bar.get_y() + bar.get_height() / 2,
            f"{int(val):,}", va="center", fontsize=9)

ax.set_title("Total Graduados por Área de Conocimiento", fontsize=14, fontweight="bold")
ax.set_xlabel("Total Graduados")
ax.xaxis.set_major_formatter(fmt_miles)
plt.tight_layout()
plt.savefig("artifacts/eda_04_area_conocimiento.png", dpi=150)
plt.show()


# ── 7. NIVEL ACADÉMICO Y METODOLOGÍA ─────────────────────────────────────────
seccion("6. NIVEL ACADÉMICO Y METODOLOGÍA")

fig, axes = plt.subplots(1, 2, figsize=(13, 5))

nivel = df.groupby("nivel_academico")["graduados"].sum().sort_values(ascending=False)
colors_n = ["#26A69A", "#AB47BC"][:len(nivel)]
axes[0].bar(nivel.index, nivel.values, color=colors_n, edgecolor="white")
axes[0].set_title("Graduados por Nivel Académico", fontweight="bold")
axes[0].set_ylabel("Total Graduados")
axes[0].yaxis.set_major_formatter(fmt_miles)
for i, (_, val) in enumerate(nivel.items()):
    axes[0].text(i, val * 1.01, f"{int(val):,}", ha="center", fontsize=10)

metodo = df.groupby("metodologia")["graduados"].sum().sort_values(ascending=False)
colors_m = ["#FF7043", "#FFA726", "#66BB6A", "#42A5F5", "#AB47BC"][:len(metodo)]
axes[1].bar(metodo.index, metodo.values, color=colors_m, edgecolor="white")
axes[1].set_title("Graduados por Metodología", fontweight="bold")
axes[1].set_ylabel("Total Graduados")
axes[1].yaxis.set_major_formatter(fmt_miles)
for i, (_, val) in enumerate(metodo.items()):
    axes[1].text(i, val * 1.01, f"{int(val):,}", ha="center", fontsize=10)
plt.setp(axes[1].get_xticklabels(), rotation=15, ha="right")

plt.tight_layout()
plt.savefig("artifacts/eda_05_nivel_metodologia.png", dpi=150)
plt.show()


# ── 8. TOP 15 DEPARTAMENTOS ───────────────────────────────────────────────────
seccion("7. TOP 15 DEPARTAMENTOS")

top_dptos = (df.groupby("dpto_programa")["graduados"].sum()
               .sort_values(ascending=False).head(15))
palette_d = sns.color_palette("viridis", len(top_dptos))

fig, ax = plt.subplots(figsize=(13, 6))
bars = ax.bar(range(len(top_dptos)), top_dptos.values, color=palette_d, edgecolor="white")
ax.set_xticks(range(len(top_dptos)))
ax.set_xticklabels(top_dptos.index, rotation=35, ha="right", fontsize=9)
for bar, val in zip(bars, top_dptos.values):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.01,
            f"{int(val):,}", ha="center", fontsize=8)

ax.set_title("Top 15 Departamentos por Graduados", fontsize=14, fontweight="bold")
ax.set_ylabel("Total Graduados")
ax.yaxis.set_major_formatter(fmt_miles)
plt.tight_layout()
plt.savefig("artifacts/eda_06_top_departamentos.png", dpi=150)
plt.show()


# ── 9. HEATMAP ÁREA × METODOLOGÍA ────────────────────────────────────────────
seccion("8. HEATMAP ÁREA × METODOLOGÍA")

heatmap_data = (
    df[df["area_conocimiento"].notna() &
       ~df["area_conocimiento"].isin(["Sin clasificar"])]
    .groupby(["area_conocimiento", "metodologia"])["graduados"]
    .sum()
    .unstack(fill_value=0)
)

plt.figure(figsize=(13, 8))
sns.heatmap(
    heatmap_data,
    annot=True,
    fmt=",",
    cmap="YlOrRd",
    linewidths=0.5,
    cbar_kws={"label": "Total Graduados"}
)
plt.title("Graduados por Área del Conocimiento × Metodología",
          fontsize=13, fontweight="bold")
plt.ylabel("Área del Conocimiento")
plt.xlabel("Metodología")
plt.tight_layout()
plt.savefig("artifacts/eda_07_heatmap_area_metodologia.png", dpi=150)
plt.show()


# ── 10. ANÁLISIS USTA ─────────────────────────────────────────────────────────
seccion("9. ANÁLISIS USTA")

def sede_usta(dpto):
    d = str(dpto).upper()
    if "BOGOT"     in d:                          return "Bogotá"
    if "SANTANDER" in d and "NORTE" not in d:     return "Bucaramanga"
    if "BOYAC"     in d:                          return "Tunja"
    return "Otra"

usta["sede"] = usta["dpto_ies"].apply(sede_usta)

print(f"  Registros USTA : {len(usta):,}")
print(f"  Sedes          : {usta['sede'].unique().tolist()}")

# Graduados por área
usta_area = usta.groupby("area_conocimiento")["graduados"].sum().sort_values()

fig, ax = plt.subplots(figsize=(13, 6))
bars = ax.barh(usta_area.index, usta_area.values, color="#880E4F", edgecolor="white")
for bar, val in zip(bars, usta_area.values):
    ax.text(bar.get_width() + usta_area.max() * 0.005,
            bar.get_y() + bar.get_height() / 2,
            f"{int(val):,}", va="center", fontsize=9)

ax.set_title("USTA — Graduados por Área de Conocimiento", fontsize=14, fontweight="bold")
ax.set_xlabel("Total Graduados")
ax.xaxis.set_major_formatter(fmt_miles)
plt.tight_layout()
plt.savefig("artifacts/eda_08_usta_area.png", dpi=150)
plt.show()

# Evolución por año y semestre
usta_evol = (usta.groupby(["anio", "semestre"])["graduados"]
                  .sum().unstack(fill_value=0))
usta_evol.columns = [f"Semestre {int(c)}" for c in usta_evol.columns]

ax2 = usta_evol.plot(kind="bar", figsize=(13, 5),
                     color=["#AD1457", "#F06292"], edgecolor="white", width=0.7)
ax2.set_title("USTA — Evolución de Graduados por Año y Semestre",
              fontsize=14, fontweight="bold")
ax2.set_xlabel("Año")
ax2.set_ylabel("Total Graduados")
ax2.yaxis.set_major_formatter(fmt_miles)
ax2.legend(title="Semestre")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("artifacts/eda_09_usta_evolucion.png", dpi=150)
plt.show()

# Graduados por sede
usta_sede = usta.groupby("sede")["graduados"].sum().sort_values(ascending=False)
fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.bar(usta_sede.index, usta_sede.values,
              color=["#1A3E6B", "#C8A951", "#228B22", "#8B0000"][:len(usta_sede)],
              edgecolor="white")
for bar, val in zip(bars, usta_sede.values):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.01,
            f"{int(val):,}", ha="center", fontsize=11, fontweight="bold")
ax.set_title("USTA — Graduados por Sede (2018–2024)", fontsize=14, fontweight="bold")
ax.set_ylabel("Total Graduados")
ax.yaxis.set_major_formatter(fmt_miles)
plt.tight_layout()
plt.savefig("artifacts/eda_10_usta_sedes.png", dpi=150)
plt.show()


# ── 11. CORRELACIONES ─────────────────────────────────────────────────────────
seccion("10. CORRELACIONES")

num_cols = [
    "id_sector", "id_caracter", "id_nivel_academico", "id_nivel_formacion",
    "id_metodologia", "id_area", "id_nbc", "id_sexo",
    "cod_dpto_programa", "anio", "semestre", "graduados"
]

# Convertir todo a float para evitar errores con Int64 y strings
df_corr = df[num_cols].apply(pd.to_numeric, errors="coerce").astype(float)
corr = df_corr.corr()

plt.figure(figsize=(12, 9))
mask = np.triu(np.ones_like(corr, dtype=bool))
sns.heatmap(
    corr, mask=mask, annot=True, fmt=".2f",
    cmap="coolwarm", center=0, linewidths=0.5,
    cbar_kws={"shrink": 0.8}
)
plt.title("Matriz de Correlación — Variables Numéricas", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.savefig("artifacts/eda_11_correlaciones.png", dpi=150)
plt.show()

top_corr = (
    corr["graduados"]
    .drop("graduados")
    .abs()
    .sort_values(ascending=False)
)
print("\n  Correlación con 'graduados' (valor absoluto):")
print(top_corr.round(4).to_string())

print("\n✅  EDA completado. Gráficas guardadas en artifacts/")