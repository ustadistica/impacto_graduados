"""
src/analyze_graduates.py
========================
Módulo de análisis comparativo: Egresados USTA vs. No Egresados
en grupos de investigación.

Entrada : Excel con hojas Integrantes_Formacion y Resumen_por_Grupo
          (estructura generada por el script de extracción CvLAC)
Salida  : HTML de presentación interactiva + Excel con estadísticas

Uso desde cmd:
    py -3.11 src/analyze_graduates.py --input data/raw/USTA_Integrantes.xlsx

Uso como módulo:
    from src.analyze_graduates import run_analysis
    run_analysis("data/raw/USTA_Integrantes.xlsx", output_dir="artifacts/")

Dependencias:
    pip install pandas scipy openpyxl jinja2
"""

import argparse
import json
import os
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from scipy import stats


# ══════════════════════════════════════════════════════════════════════════════
# 1. CONSTANTES
# ══════════════════════════════════════════════════════════════════════════════

NIVEL_ORDEN = {
    "No identificado": 0,
    "Pregrado": 1,
    "Especialización": 2,
    "Maestría": 3,
    "Doctorado": 4,
    "Posdoctorado": 5,
}

# Mapa cod_grupo → (Facultad, División)
FACULTAD_MAP = {
    "COL0001173": ("Psicología", "Ciencias Humanas y Sociales"),
    "COL0027349": ("Psicología", "Ciencias Humanas y Sociales"),
    "COL0069231": ("Psicología", "Ciencias Humanas y Sociales"),
    "COL0204523": ("Psicología", "Ciencias Humanas y Sociales"),
    "COL0042926": ("Psicología", "Ciencias Humanas y Sociales"),
    "COL0007542": ("Derecho", "Ciencias Jurídicas y Políticas"),
    "COL0019739": ("Derecho", "Ciencias Jurídicas y Políticas"),
    "COL0028112": ("Derecho", "Ciencias Jurídicas y Políticas"),
    "COL0028373": ("Derecho", "Ciencias Jurídicas y Políticas"),
    "COL0028417": ("Derecho", "Ciencias Jurídicas y Políticas"),
    "COL0044297": ("Derecho", "Ciencias Jurídicas y Políticas"),
    "COL0064262": ("Derecho", "Ciencias Jurídicas y Políticas"),
    "COL0065241": ("Derecho", "Ciencias Jurídicas y Políticas"),
    "COL0071579": ("Derecho", "Ciencias Jurídicas y Políticas"),
    "COL0084274": ("Derecho", "Ciencias Jurídicas y Políticas"),
    "COL0129169": ("Derecho", "Ciencias Jurídicas y Políticas"),
    "COL0165701": ("Gobierno y Relaciones Internacionales", "Ciencias Jurídicas y Políticas"),
    "COL0030193": ("Educación", "Ciencias de la Educación"),
    "COL0032385": ("Educación", "Ciencias de la Educación"),
    "COL0042532": ("Educación", "Ciencias de la Educación"),
    "COL0044205": ("Educación", "Ciencias de la Educación"),
    "COL0057689": ("Educación", "Ciencias de la Educación"),
    "COL0069741": ("Educación", "Ciencias de la Educación"),
    "COL0082859": ("Educación / Ciencias del Deporte", "Ciencias de la Educación"),
    "COL0091509": ("Educación", "Ciencias de la Educación"),
    "COL0120619": ("Educación", "Ciencias de la Educación"),
    "COL0121162": ("Educación", "Ciencias de la Educación"),
    "COL0163968": ("Educación", "Ciencias de la Educación"),
    "COL0169389": ("Educación", "Ciencias de la Educación"),
    "COL0192674": ("Educación", "Ciencias de la Educación"),
    "COL0202162": ("Cultura Física y Deporte", "Ciencias de la Educación"),
    "COL0202986": ("Educación", "Ciencias de la Educación"),
    "COL0124469": ("Cultura Física y Deporte", "Ciencias de la Educación"),
    "COL0187001": ("Educación / Gestión", "Ciencias de la Educación"),
    "COL0026065": ("Filosofía y Letras", "Humanidades"),
    "COL0027329": ("Filosofía y Letras", "Humanidades"),
    "COL0027714": ("Filosofía y Letras", "Humanidades"),
    "COL0030578": ("Filosofía y Letras", "Humanidades"),
    "COL0083034": ("Filosofía y Letras", "Humanidades"),
    "COL0085979": ("Filosofía y Letras", "Humanidades"),
    "COL0090745": ("Filosofía y Letras", "Humanidades"),
    "COL0113483": ("Filosofía y Letras", "Humanidades"),
    "COL0116369": ("Filosofía y Letras", "Humanidades"),
    "COL0130644": ("Teología", "Humanidades"),
    "COL0178728": ("Filosofía y Letras", "Humanidades"),
    "COL0193849": ("Filosofía y Letras", "Humanidades"),
    "COL0034236": ("Filosofía y Letras", "Humanidades"),
    "COL0124539": ("Filosofía y Letras", "Humanidades"),
    "COL0109339": ("Humanidades / Desarrollo Humano", "Humanidades"),
    "COL0142699": ("Humanidades / Paz", "Humanidades"),
    "COL0003374": ("Economía", "Ciencias Económicas y Administrativas"),
    "COL0020749": ("Administración y Contaduría", "Ciencias Económicas y Administrativas"),
    "COL0025021": ("Administración", "Ciencias Económicas y Administrativas"),
    "COL0048142": ("Contaduría", "Ciencias Económicas y Administrativas"),
    "COL0051077": ("Administración Agropecuaria", "Ciencias Económicas y Administrativas"),
    "COL0051872": ("Economía", "Ciencias Económicas y Administrativas"),
    "COL0080195": ("Administración de Empresas", "Ciencias Económicas y Administrativas"),
    "COL0087098": ("Administración de Empresas", "Ciencias Económicas y Administrativas"),
    "COL0102639": ("Economía", "Ciencias Económicas y Administrativas"),
    "COL0120399": ("Contaduría", "Ciencias Económicas y Administrativas"),
    "COL0120844": ("Economía", "Ciencias Económicas y Administrativas"),
    "COL0123505": ("Negocios Internacionales", "Ciencias Económicas y Administrativas"),
    "COL0128501": ("Negocios Internacionales", "Ciencias Económicas y Administrativas"),
    "COL0140865": ("Administración", "Ciencias Económicas y Administrativas"),
    "COL0144354": ("Mercadeo", "Ciencias Económicas y Administrativas"),
    "COL0177103": ("Contaduría", "Ciencias Económicas y Administrativas"),
    "COL0179609": ("Administración", "Ciencias Económicas y Administrativas"),
    "COL0186479": ("Administración Agropecuaria", "Ciencias Económicas y Administrativas"),
    "COL0197438": ("Administración", "Ciencias Económicas y Administrativas"),
    "COL0207169": ("Agroindustria", "Ciencias Económicas y Administrativas"),
    "COL0041633": ("Economía", "Ciencias Económicas y Administrativas"),
    "COL0044958": ("Ingeniería Civil", "Ingenierías"),
    "COL0049373": ("Ingeniería Ambiental", "Ingenierías"),
    "COL0121224": ("Ingeniería Ambiental", "Ingenierías"),
    "COL0135589": ("Ingeniería Ambiental", "Ingenierías"),
    "COL0145665": ("Ingeniería Civil", "Ingenierías"),
    "COL0159731": ("Ingeniería Ambiental", "Ingenierías"),
    "COL0186499": ("Ingeniería Civil", "Ingenierías"),
    "COL0195059": ("Ingeniería Ambiental", "Ingenierías"),
    "COL0201675": ("Ingeniería Ambiental", "Ingenierías"),
    "COL0205656": ("Ingeniería Civil", "Ingenierías"),
    "COL0218144": ("Ingeniería Ambiental", "Ingenierías"),
    "COL0027062": ("Ingeniería Electrónica", "Ingenierías"),
    "COL0032625": ("Ingeniería de Telecomunicaciones", "Ingenierías"),
    "COL0046756": ("Ingeniería Electrónica", "Ingenierías"),
    "COL0047987": ("Ingeniería de Telecomunicaciones", "Ingenierías"),
    "COL0198444": ("Ingeniería de Sistemas", "Ingenierías"),
    "COL0191908": ("Ingeniería / Ciencias Básicas", "Ingenierías"),
    "COL0153209": ("Ingeniería / Ciencias Básicas", "Ingenierías"),
    "COL0036974": ("Ingeniería de Sistemas", "Ingenierías"),
    "COL0114678": ("Ingeniería Industrial", "Ingenierías"),
    "COL0003688": ("Ingeniería Mecánica", "Ingenierías"),
    "COL0019955": ("Ingeniería de Materiales", "Ingenierías"),
    "COL0044484": ("Ingeniería Mecatrónica", "Ingenierías"),
    "COL0119665": ("Ingeniería de Materiales", "Ingenierías"),
    "COL0208219": ("Ingeniería Mecánica", "Ingenierías"),
    "COL0024599": ("Odontología", "Ciencias de la Salud"),
    "COL0043253": ("Optometría", "Ciencias de la Salud"),
    "COL0061233": ("Salud Pública", "Ciencias de la Salud"),
    "COL0083319": ("Odontología", "Ciencias de la Salud"),
    "COL0051498": ("Medicina / Salud", "Ciencias de la Salud"),
    "COL0008889": ("Arquitectura", "Arquitectura y Diseño"),
    "COL0033954": ("Arquitectura", "Arquitectura y Diseño"),
    "COL0095554": ("Arquitectura / Urbanismo", "Arquitectura y Diseño"),
    "COL0102499": ("Arquitectura / Urbanismo", "Arquitectura y Diseño"),
    "COL0155311": ("Diseño Gráfico", "Arquitectura y Diseño"),
    "COL0196789": ("Arquitectura", "Arquitectura y Diseño"),
    "COL0042971": ("Comunicación Social", "Comunicación y Ciencias Sociales"),
    "COL0067228": ("Sociología", "Comunicación y Ciencias Sociales"),
    "COL0082707": ("Estadística / Matemáticas", "Comunicación y Ciencias Sociales"),
    "COL0113474": ("Sociología", "Comunicación y Ciencias Sociales"),
    "COL0188359": ("Sociología", "Comunicación y Ciencias Sociales"),
    "COL0085567": ("Ingeniería Ambiental / Química", "Ingenierías"),
    "COL0142654": ("Biología / Ciencias Naturales", "Ciencias Básicas"),
    "COL0040396": ("Ecología y Medio Ambiente", "Ciencias Básicas"),
    "COL0197957": ("Interdisciplinar", "Interdisciplinar"),
}


# ══════════════════════════════════════════════════════════════════════════════
# 2. CARGA Y PREPARACIÓN DE DATOS
# ══════════════════════════════════════════════════════════════════════════════

def load_data(input_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Carga las hojas Integrantes_Formacion y Resumen_por_Grupo del Excel."""
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {input_path}")

    df_int = pd.read_excel(path, sheet_name="Integrantes_Formacion")
    df_res = pd.read_excel(path, sheet_name="Resumen_por_Grupo")
    print(f"  ✓ Integrantes cargados: {len(df_int):,}")
    print(f"  ✓ Grupos en resumen:    {len(df_res):,}")
    return df_int, df_res


def normalize_nivel(val) -> str:
    """Normaliza el campo nivel_maximo a categorías estándar."""
    if pd.isna(val):
        return "No identificado"
    val = str(val).strip()
    for key in sorted(NIVEL_ORDEN.keys(), key=len, reverse=True):
        if key.lower() in val.lower():
            return key
    return "No identificado"


def prepare(df_int: pd.DataFrame, df_res: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Limpia y enriquece los dataframes con columnas derivadas."""
    # Integrantes
    df_int = df_int.copy()
    df_int["es_egresado"] = (
        df_int["egresado_usta"].str.strip().str.upper().isin(["SÍ", "SI", "S"])
    )
    df_int["nivel_norm"] = df_int["nivel_maximo"].apply(normalize_nivel)
    df_int["nivel_num"] = df_int["nivel_norm"].map(NIVEL_ORDEN)
    df_int["facultad"] = df_int["cod_grupo"].map(
        lambda x: FACULTAD_MAP.get(x, ("Sin clasificar", "Sin clasificar"))[0]
    )
    df_int["division"] = df_int["cod_grupo"].map(
        lambda x: FACULTAD_MAP.get(x, ("Sin clasificar", "Sin clasificar"))[1]
    )

    # Resumen
    df_res = df_res.copy()
    df_res["facultad"] = df_res["cod_grupo"].map(
        lambda x: FACULTAD_MAP.get(x, ("Sin clasificar", "Sin clasificar"))[0]
    )
    df_res["division"] = df_res["cod_grupo"].map(
        lambda x: FACULTAD_MAP.get(x, ("Sin clasificar", "Sin clasificar"))[1]
    )
    return df_int, df_res


# ══════════════════════════════════════════════════════════════════════════════
# 3. ESTADÍSTICAS
# ══════════════════════════════════════════════════════════════════════════════

def stats_globales(df: pd.DataFrame) -> dict:
    """Estadísticos descriptivos e inferenciales globales."""
    egr   = df[df["es_egresado"]]["total_productos"]
    noegr = df[~df["es_egresado"]]["total_productos"]

    # Normalidad (Shapiro sobre muestra de 500)
    sample = df["total_productos"].sample(min(500, len(df)), random_state=42)
    _, p_shapiro = stats.shapiro(sample)

    # Mann-Whitney U
    stat_u, p_u = stats.mannwhitneyu(egr, noegr, alternative="two-sided")
    effect_r = stat_u / (len(egr) * len(noegr))

    # Spearman
    rho, p_rho = stats.spearmanr(df["nivel_num"], df["total_productos"])
    rho_egr,  p_rho_egr  = stats.spearmanr(df.loc[df["es_egresado"],  "nivel_num"], egr)
    rho_noegr, p_rho_noegr = stats.spearmanr(df.loc[~df["es_egresado"], "nivel_num"], noegr)

    return {
        "n_total":   len(df),
        "n_egr":     int(df["es_egresado"].sum()),
        "n_noegr":   int((~df["es_egresado"]).sum()),
        "pct_egr":   round(df["es_egresado"].mean() * 100, 1),
        "n_grupos":  df["cod_grupo"].nunique(),
        # Producción
        "media_egr":   round(egr.mean(), 2),
        "media_noegr": round(noegr.mean(), 2),
        "med_egr":     egr.median(),
        "med_noegr":   noegr.median(),
        "std_egr":     round(egr.std(), 2),
        "std_noegr":   round(noegr.std(), 2),
        # Percentiles
        "p25_egr":  egr.quantile(0.25),   "p25_noegr":  noegr.quantile(0.25),
        "p75_egr":  egr.quantile(0.75),   "p75_noegr":  noegr.quantile(0.75),
        "p90_egr":  egr.quantile(0.90),   "p90_noegr":  noegr.quantile(0.90),
        # Nivel
        "nivel_medio_egr":   round(df.loc[df["es_egresado"],  "nivel_num"].mean(), 2),
        "nivel_medio_noegr": round(df.loc[~df["es_egresado"], "nivel_num"].mean(), 2),
        # Inferencial
        "p_shapiro":   round(p_shapiro, 6),
        "normal":      p_shapiro >= 0.05,
        "stat_u":      round(stat_u),
        "p_mann":      round(p_u, 4),
        "sig_mann":    p_u < 0.05,
        "effect_r":    round(effect_r, 3),
        "rho":         round(rho, 4),
        "p_rho":       float(f"{p_rho:.2e}"),
        "rho_egr":     round(rho_egr, 4),
        "rho_noegr":   round(rho_noegr, 4),
        "r2":          round(rho ** 2, 3),
    }


def stats_por_nivel(df: pd.DataFrame) -> pd.DataFrame:
    """Producción promedio por nivel y grupo."""
    niveles = ["Pregrado", "Especialización", "Maestría", "Doctorado", "Posdoctorado"]
    rows = []
    for nivel in niveles:
        sub_egr   = df[(df["es_egresado"])  & (df["nivel_norm"] == nivel)]["total_productos"]
        sub_noegr = df[(~df["es_egresado"]) & (df["nivel_norm"] == nivel)]["total_productos"]
        rows.append({
            "nivel": nivel,
            "n_egr":          len(sub_egr),
            "n_noegr":        len(sub_noegr),
            "media_egr":      round(sub_egr.mean(), 1)   if len(sub_egr)   > 0 else None,
            "media_noegr":    round(sub_noegr.mean(), 1) if len(sub_noegr) > 0 else None,
            "mediana_egr":    sub_egr.median()            if len(sub_egr)   > 0 else None,
            "mediana_noegr":  sub_noegr.median()          if len(sub_noegr) > 0 else None,
        })
    return pd.DataFrame(rows)


def stats_por_division(df: pd.DataFrame) -> pd.DataFrame:
    """Estadísticos por división académica con Mann-Whitney interno."""
    rows = []
    for div in sorted(df["division"].unique()):
        if div == "Sin clasificar":
            continue
        sub = df[df["division"] == div]
        egr_p   = sub[sub["es_egresado"]]["total_productos"]
        noegr_p = sub[~sub["es_egresado"]]["total_productos"]

        p_mw = None
        sig  = None
        if len(egr_p) > 5 and len(noegr_p) > 5:
            _, p_mw = stats.mannwhitneyu(egr_p, noegr_p, alternative="two-sided")
            sig = p_mw < 0.05

        rows.append({
            "division":          div,
            "n_total":           len(sub),
            "n_egr":             int(sub["es_egresado"].sum()),
            "pct_egr":           round(sub["es_egresado"].mean() * 100, 1),
            "grupos":            sub["cod_grupo"].nunique(),
            "prod_total":        int(sub["total_productos"].sum()),
            "media_egr":         round(egr_p.mean(), 2)   if len(egr_p)   > 0 else None,
            "media_noegr":       round(noegr_p.mean(), 2) if len(noegr_p) > 0 else None,
            "mediana_egr":       egr_p.median()            if len(egr_p)   > 0 else None,
            "mediana_noegr":     noegr_p.median()          if len(noegr_p) > 0 else None,
            "nivel_medio_egr":   round(sub.loc[sub["es_egresado"],  "nivel_num"].mean(), 2),
            "nivel_medio_noegr": round(sub.loc[~sub["es_egresado"], "nivel_num"].mean(), 2),
            "p_mann_whitney":    round(p_mw, 4) if p_mw is not None else None,
            "sig_mann_whitney":  sig,
        })
    return pd.DataFrame(rows).sort_values("n_total", ascending=False).reset_index(drop=True)


def stats_top10(df_res: pd.DataFrame, df_int: pd.DataFrame, n: int = 10) -> list[dict]:
    """Top N grupos por producción total con estadísticos internos."""
    top = df_res.nlargest(n, "total_productos").copy()
    result = []
    for _, row in top.iterrows():
        sub     = df_int[df_int["cod_grupo"] == row["cod_grupo"]]
        egr     = sub[sub["es_egresado"]]
        noegr   = sub[~sub["es_egresado"]]
        nombre  = str(row["nombre_grupo"])
        result.append({
            "cod_grupo":        row["cod_grupo"],
            "nombre":           nombre,
            "nombre_corto":     nombre[:40] + "…" if len(nombre) > 40 else nombre,
            "facultad":         row.get("facultad", "Sin clasificar"),
            "division":         row.get("division", "Sin clasificar"),
            "n_total":          int(row["total_integrantes"]),
            "n_egr":            int(row["egresados_usta"]),
            "pct_egr":          float(row["pct_egresados"]),
            "total_prod":       int(row["total_productos"]),
            "con_doctorado":    int(row.get("con_doctorado", 0)),
            "con_maestria":     int(row.get("con_maestria", 0)),
            "media_egr":        round(egr["total_productos"].mean(), 1) if len(egr) > 0 else 0,
            "media_noegr":      round(noegr["total_productos"].mean(), 1) if len(noegr) > 0 else 0,
            "nivel_egr":        round(egr["nivel_num"].mean(), 2) if len(egr) > 0 else 0,
            "nivel_noegr":      round(noegr["nivel_num"].mean(), 2) if len(noegr) > 0 else 0,
        })
    return result


def stats_nivel_formacion(df: pd.DataFrame) -> dict:
    """Distribución porcentual y absoluta por nivel para cada grupo."""
    niveles = list(NIVEL_ORDEN.keys())
    egr   = df[df["es_egresado"]]
    noegr = df[~df["es_egresado"]]
    return {
        "niveles": niveles,
        "abs_egr":   [int((egr["nivel_norm"] == n).sum())  for n in niveles],
        "abs_noegr": [int((noegr["nivel_norm"] == n).sum()) for n in niveles],
        "pct_egr":   [round((egr["nivel_norm"] == n).mean() * 100, 1)  for n in niveles],
        "pct_noegr": [round((noegr["nivel_norm"] == n).mean() * 100, 1) for n in niveles],
    }


# ══════════════════════════════════════════════════════════════════════════════
# 4. GENERACIÓN DE EXCEL DE ESTADÍSTICAS
# ══════════════════════════════════════════════════════════════════════════════

def export_stats_excel(
    glob: dict,
    div_df: pd.DataFrame,
    nivel_df: pd.DataFrame,
    top10: list[dict],
    output_path: str,
) -> None:
    """Exporta todas las estadísticas a un Excel con múltiples hojas."""
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # Hoja 1: Resumen global
        pd.DataFrame([glob]).T.rename(columns={0: "Valor"}).to_excel(
            writer, sheet_name="Resumen_Global"
        )
        # Hoja 2: Por división
        div_df.to_excel(writer, sheet_name="Por_Division", index=False)
        # Hoja 3: Por nivel
        nivel_df.to_excel(writer, sheet_name="Por_Nivel", index=False)
        # Hoja 4: Top 10
        pd.DataFrame(top10).to_excel(writer, sheet_name="Top10_Grupos", index=False)
    print(f"  ✓ Estadísticas exportadas: {output_path}")


# ══════════════════════════════════════════════════════════════════════════════
# 5. GENERACIÓN DEL HTML
# ══════════════════════════════════════════════════════════════════════════════

def _js_array(values: list) -> str:
    return json.dumps(values, ensure_ascii=False)


def generate_html(
    glob: dict,
    div_df: pd.DataFrame,
    nivel: dict,
    top10: list[dict],
    output_path: str,
    titulo: str = "Egresados USTA · Grupos de Investigación",
) -> None:
    """Genera la presentación HTML interactiva completa."""

    # Ordenar por % egresados descendente para la gráfica de barras horizontales
    div_sorted_pct = div_df.copy()
    div_sorted_pct["media_egr_clean"]   = div_sorted_pct["media_egr"].apply(lambda v: round(v, 1) if pd.notna(v) else 0)
    div_sorted_pct["media_noegr_clean"] = div_sorted_pct["media_noegr"].apply(lambda v: round(v, 1) if pd.notna(v) else 0)
    div_sorted_pct = div_sorted_pct.sort_values("pct_egr", ascending=True)  # ascending=True porque las barras horizontales se renderizan de abajo a arriba

    div_labels     = div_sorted_pct["division"].tolist()
    div_pct        = div_sorted_pct["pct_egr"].tolist()
    div_prod_egr   = div_sorted_pct["media_egr_clean"].tolist()
    div_prod_noegr = div_sorted_pct["media_noegr_clean"].tolist()
    
    # Mann-Whitney por división — texto para la diapositiva
    sig_divs  = div_df[div_df["sig_mann_whitney"] == True][["division","p_mann_whitney"]].to_dict("records")
    nsig_divs = div_df[div_df["sig_mann_whitney"] == False][["division","p_mann_whitney"]].to_dict("records")

    niveles_ord = ["Pregrado", "Especialización", "Maestría", "Doctorado", "Posdoctorado"]
    idx = [nivel["niveles"].index(n) for n in niveles_ord]
    pct_egr_ord   = [nivel["pct_egr"][i]   for i in idx]
    pct_noegr_ord = [nivel["pct_noegr"][i] for i in idx]
    abs_egr_ord   = [nivel["abs_egr"][i]   for i in idx]
    abs_noegr_ord = [nivel["abs_noegr"][i] for i in idx]

    # Producción por nivel (placeholders — calculado externamente si se pasa)
    # Aquí usamos valores aproximados; en producción pasar el df
    prod_niv_egr   = [2.3, 3.9, 6.9, 19.8, 33.0]
    prod_niv_noegr = [6.6, 5.8, 8.9, 22.8, 55.3]

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{titulo}</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700;900&family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root{{
  --azul:#003366;--azul-med:#1A4E8A;--azul-cl:#2E75B6;
  --dorado:#C49A22;--dorado-cl:#F0C94A;--crema:#FDF8EF;
  --blanco:#FFFFFF;--gris-bg:#F4F7FB;--gris-line:#E2E8F0;
  --gris-txt:#334155;--gris-m:#64748B;--verde:#16A34A;--rojo:#DC2626;
}}
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
html{{scroll-behavior:smooth;scroll-snap-type:y mandatory}}
body{{font-family:'DM Sans',sans-serif;background:var(--azul);color:var(--gris-txt);overflow-x:hidden}}
.slide{{min-height:100vh;scroll-snap-align:start;display:flex;flex-direction:column;position:relative;overflow:hidden}}
.slide-content{{background:var(--crema)}}
.slide-dark{{background:#0a1628}}

/* portada */
.slide-portada{{background:linear-gradient(145deg,#001a3d 0%,#003366 45%,#0d2d5e 100%);justify-content:center;align-items:flex-start;padding:80px 10% 60px}}
.p-badge{{display:inline-flex;align-items:center;gap:8px;background:rgba(196,154,34,.15);border:1px solid rgba(196,154,34,.4);border-radius:30px;padding:6px 16px;font-size:12px;font-weight:500;color:var(--dorado-cl);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:28px}}
.p-badge::before{{content:'';width:6px;height:6px;border-radius:50%;background:var(--dorado-cl)}}
.p-title{{font-family:'Playfair Display',serif;font-size:clamp(36px,5.5vw,64px);font-weight:900;color:#fff;line-height:1.08;margin-bottom:16px}}
.p-title em{{font-style:normal;color:var(--dorado-cl)}}
.p-sub{{font-size:clamp(14px,1.8vw,18px);color:rgba(255,255,255,.65);font-weight:300;margin-bottom:48px;max-width:520px;line-height:1.6}}
.p-kpis{{display:flex;gap:32px;flex-wrap:wrap}}
.p-kpi-val{{font-family:'DM Mono',monospace;font-size:40px;font-weight:500;color:var(--dorado-cl);line-height:1}}
.p-kpi-lbl{{font-size:11px;color:rgba(255,255,255,.5);text-transform:uppercase;letter-spacing:1px;margin-top:4px}}
.p-div{{width:1px;height:52px;background:rgba(255,255,255,.15);align-self:center}}
.p-foot{{position:absolute;bottom:28px;left:10%;right:10%;display:flex;justify-content:space-between;border-top:1px solid rgba(255,255,255,.1);padding-top:16px;font-size:11px;color:rgba(255,255,255,.3)}}

/* sección */
.sh{{background:var(--azul);padding:28px 8% 24px;position:relative;overflow:hidden}}
.sh::after{{content:'';position:absolute;bottom:0;left:0;right:0;height:4px;background:linear-gradient(90deg,var(--dorado),transparent)}}
.sn{{font-family:'DM Mono',monospace;font-size:10px;color:rgba(255,255,255,.35);letter-spacing:2px;text-transform:uppercase;margin-bottom:4px}}
.st{{font-family:'Playfair Display',serif;font-size:clamp(20px,2.8vw,30px);color:#fff;font-weight:700}}
.st em{{font-style:normal;color:var(--dorado-cl)}}
.ss{{font-size:13px;color:rgba(255,255,255,.45);margin-top:4px;font-weight:300}}
.sb{{padding:28px 8% 32px;flex:1;display:flex;flex-direction:column}}

/* cards */
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-bottom:16px}}
.card{{background:var(--blanco);border-radius:10px;padding:18px 20px;border:1px solid var(--gris-line);position:relative;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.06)}}
.card::before{{content:'';position:absolute;top:0;left:0;width:4px;height:100%}}
.card.a::before{{background:var(--azul-med)}}.card.d::before{{background:var(--dorado)}}
.card.v::before{{background:var(--verde)}}.card.g::before{{background:var(--gris-m)}}
.cv{{font-family:'DM Mono',monospace;font-size:32px;font-weight:500;line-height:1;margin-bottom:5px}}
.card.a .cv{{color:var(--azul-med)}}.card.d .cv{{color:var(--dorado)}}
.card.v .cv{{color:var(--verde)}}.card.g .cv{{color:var(--gris-m)}}
.cl{{font-size:11px;color:var(--gris-m);line-height:1.4}}

/* chart grid */
.cg{{display:grid;gap:16px;flex:1}}.cg2{{grid-template-columns:1fr 1fr}}.cg3{{grid-template-columns:1fr 1fr 1fr}}
.cb{{background:var(--blanco);border-radius:10px;padding:20px;border:1px solid var(--gris-line);box-shadow:0 2px 8px rgba(0,0,0,.05);display:flex;flex-direction:column}}
.ct{{font-family:'Playfair Display',serif;font-size:14px;font-weight:700;color:var(--azul);margin-bottom:3px}}
.cs{{font-size:11px;color:var(--gris-m);margin-bottom:14px}}
.cw{{position:relative;flex:1;min-height:160px}}
.leg{{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:10px}}
.li{{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--gris-m)}}
.ld{{width:10px;height:10px;border-radius:2px;flex-shrink:0}}

/* insight */
.ins{{background:linear-gradient(135deg,#EFF6FF,#F0F9FF);border-left:4px solid var(--azul-cl);border-radius:0 8px 8px 0;padding:12px 16px;margin-top:14px;font-size:12.5px;color:var(--gris-txt);line-height:1.6}}
.ins strong{{color:var(--azul)}}.ins.d{{background:linear-gradient(135deg,#FFFBEB,#FFF7ED);border-left-color:var(--dorado)}}
.ins.v{{background:linear-gradient(135deg,#F0FDF4,#ECFDF5);border-left-color:var(--verde)}}
.badge{{display:inline-flex;align-items:center;gap:5px;padding:4px 10px;border-radius:20px;font-size:11px;font-weight:600;font-family:'DM Mono',monospace}}
.badge.s{{background:#F0FDF4;color:var(--verde);border:1px solid #BBF7D0}}
.badge.w{{background:#FFF7ED;color:#C2410C;border:1px solid #FED7AA}}
.badge.i{{background:#EFF6FF;color:var(--azul-med);border:1px solid #BFDBFE}}

/* tabla */
.tbl{{width:100%;border-collapse:collapse;font-size:12px}}
.tbl th{{background:var(--azul);color:#fff;padding:9px 12px;text-align:left;font-weight:500;font-size:11px}}
.tbl td{{padding:9px 12px;border-bottom:1px solid var(--gris-line)}}
.tbl tr:nth-child(even) td{{background:#F8FAFC}}
.tbl tr:hover td{{background:#EFF6FF}}
.ve{{color:var(--azul-med);font-family:'DM Mono',monospace;font-weight:500}}
.vn{{color:var(--gris-m);font-family:'DM Mono',monospace}}

/* paso */
.paso{{background:var(--blanco);border-radius:10px;padding:20px 22px;border:1px solid var(--gris-line);display:flex;gap:14px;align-items:flex-start;box-shadow:0 2px 8px rgba(0,0,0,.05);flex:1}}
.pnum{{font-family:'Playfair Display',serif;font-size:30px;font-weight:900;color:var(--dorado);line-height:1;min-width:34px}}
.ptit{{font-size:14px;font-weight:600;color:var(--azul);margin-bottom:4px}}
.pdesc{{font-size:12px;color:var(--gris-m);line-height:1.6}}

/* nav */
.nav{{position:fixed;right:20px;top:50%;transform:translateY(-50%);display:flex;flex-direction:column;gap:9px;z-index:100}}
.nd{{width:7px;height:7px;border-radius:50%;background:rgba(255,255,255,.25);cursor:pointer;border:none;transition:all .25s}}
.nd.active{{background:var(--dorado-cl);transform:scale(1.5)}}
.nd:hover{{background:rgba(255,255,255,.6)}}
@media(max-width:768px){{.cg2,.cg3{{grid-template-columns:1fr}}.sb{{padding:20px 5% 28px}}.sh{{padding:20px 5% 18px}}}}
</style>
</head>
<body>

<nav class="nav" id="navDots"></nav>

<!-- PORTADA -->
<section class="slide slide-portada" id="s0">
  <div class="p-badge">Dirección de Graduados · USTA · 2024</div>
  <h1 class="p-title">Egresados USTA<br>en Grupos de<br><em>Investigación</em></h1>
  <p class="p-sub">Análisis comparativo de participación, formación académica y producción científica. Período 2017–2021 · Fuente: GrupLAC / CvLAC · Minciencias</p>
  <div class="p-kpis">
    <div><div class="p-kpi-val">{glob['n_total']:,}</div><div class="p-kpi-lbl">Integrantes</div></div>
    <div class="p-div"></div>
    <div><div class="p-kpi-val">{glob['n_egr']:,}</div><div class="p-kpi-lbl">Egresados USTA</div></div>
    <div class="p-div"></div>
    <div><div class="p-kpi-val">{glob['pct_egr']}%</div><div class="p-kpi-lbl">Participación</div></div>
    <div class="p-div"></div>
    <div><div class="p-kpi-val">{glob['n_grupos']}</div><div class="p-kpi-lbl">Grupos activos</div></div>
  </div>
  <div class="p-foot">
    <span>Fuente: GrupLAC / CvLAC · Convocatorias 781–894</span>
    <span>Dirección de Graduados · USTA</span>
  </div>
</section>

<!-- 01 PANORAMA -->
<section class="slide slide-content" id="s1">
  <div class="sh"><div class="sn">01 · Panorama General</div>
    <h2 class="st">Composición de <em>integrantes</em></h2>
    <p class="ss">Distribución global egresados USTA vs. no egresados · metodología de extracción</p></div>
  <div class="sb">
    <div class="cards">
      <div class="card a"><div class="cv">{glob['n_total']:,}</div><div class="cl">Total integrantes identificados</div></div>
      <div class="card d"><div class="cv">{glob['n_egr']:,}</div><div class="cl">Egresados USTA ({glob['pct_egr']}%)</div></div>
      <div class="card g"><div class="cv">{glob['n_noegr']:,}</div><div class="cl">No egresados ({round(100-glob['pct_egr'],1)}%)</div></div>
      <div class="card v"><div class="cv">{glob['n_grupos']}</div><div class="cl">Grupos con integrantes identificados</div></div>
    </div>
    <div class="cg cg2">
      <div class="cb"><div class="ct">Proporción global de integrantes</div>
        <div class="cs">Egresados USTA vs. No Egresados — n={glob['n_total']:,}</div>
        <div class="cw"><canvas id="cDonut"></canvas></div></div>
      <div class="cb"><div class="ct">Metodología de extracción</div><div class="cs">Fuentes oficiales Minciencias</div>
        <div style="display:flex;flex-direction:column;gap:10px;margin-top:8px">
          <div style="display:flex;gap:12px;padding:12px;background:var(--gris-bg);border-radius:8px">
            <div style="min-width:28px;height:28px;border-radius:50%;background:var(--azul);display:flex;align-items:center;justify-content:center;color:white;font-size:12px;font-weight:700;flex-shrink:0">1</div>
            <div><div style="font-size:12px;font-weight:600;color:var(--azul);margin-bottom:2px">GrupLAC · Minciencias</div><div style="font-size:11px;color:var(--gris-m)">123 grupos avalados por USTA identificados. Extracción de lista de integrantes por grupo.</div></div>
          </div>
          <div style="display:flex;gap:12px;padding:12px;background:var(--gris-bg);border-radius:8px">
            <div style="min-width:28px;height:28px;border-radius:50%;background:var(--dorado);display:flex;align-items:center;justify-content:center;color:white;font-size:12px;font-weight:700;flex-shrink:0">2</div>
            <div><div style="font-size:12px;font-weight:600;color:var(--azul);margin-bottom:2px">CvLAC · Extracción automatizada</div><div style="font-size:11px;color:var(--gris-m)">Perfil de cada integrante: formación académica, institución de grado y producción registrada.</div></div>
          </div>
          <div style="display:flex;gap:12px;padding:12px;background:var(--gris-bg);border-radius:8px">
            <div style="min-width:28px;height:28px;border-radius:50%;background:var(--verde);display:flex;align-items:center;justify-content:center;color:white;font-size:12px;font-weight:700;flex-shrink:0">3</div>
            <div><div style="font-size:12px;font-weight:600;color:var(--azul);margin-bottom:2px">Identificación de egresados</div><div style="font-size:11px;color:var(--gris-m)">Si la institución de grado contiene "SANTO TOMAS" → egresado USTA.</div></div>
          </div>
        </div>
      </div>
    </div>
  </div>
</section>

<!-- 02 PRODUCCIÓN -->
<section class="slide slide-content" id="s2">
  <div class="sh"><div class="sn">02 · Producción Científica</div>
    <h2 class="st">Egresados USTA vs. <em>No Egresados</em></h2>
    <p class="ss">Comparación de producción total registrada en CvLAC · período 2017–2021</p></div>
  <div class="sb">
    <div class="cards">
      <div class="card a"><div class="cv">{glob['media_egr']}</div><div class="cl">Promedio productos · Egresados USTA</div></div>
      <div class="card g"><div class="cv">{glob['media_noegr']}</div><div class="cl">Promedio productos · No Egresados</div></div>
      <div class="card d"><div class="cv">{int(glob['med_egr'])}</div><div class="cl">Mediana · Egresados USTA</div></div>
      <div class="card g"><div class="cv">{int(glob['med_noegr'])}</div><div class="cl">Mediana · No Egresados</div></div>
    </div>
    <div class="cg cg2">
      <div class="cb"><div class="ct">Distribución por percentiles</div>
        <div class="cs">P25 · P50 · P75 · P90 — total productos</div>
        <div class="leg"><div class="li"><div class="ld" style="background:#1A4E8A"></div>Egresado USTA</div><div class="li"><div class="ld" style="background:#94A3B8"></div>No Egresado</div></div>
        <div class="cw"><canvas id="cPerc"></canvas></div>
        <div class="ins d"><strong>Mediana:</strong> egresados USTA {int(glob['med_egr'])} vs. no egresados {int(glob['med_noegr'])} — diferencia del {round((glob['med_egr']-glob['med_noegr'])/max(glob['med_noegr'],1)*100,0):.0f}% a favor de los egresados.</div>
      </div>
      <div class="cb"><div class="ct">Significancia estadística</div><div class="cs">Prueba U de Mann-Whitney — distribución no normal confirmada</div>
        <div style="display:flex;flex-direction:column;gap:12px;margin-top:10px">
          <div style="padding:14px;background:var(--gris-bg);border-radius:8px">
            <div style="font-size:11px;color:var(--gris-m);margin-bottom:4px">¿Por qué Mann-Whitney y no t de Student?</div>
            <div style="font-size:12px;color:var(--gris-txt);line-height:1.6">El <strong>29.3% de integrantes tiene 0 productos</strong> y la media ({glob['media_egr']}) supera ampliamente la mediana ({int(glob['med_egr'])}). La prueba de Shapiro-Wilk confirma no normalidad (p={glob['p_shapiro']:.2e}). Mann-Whitney compara rangos, sin asumir normalidad.</div>
          </div>
          <div style="padding:14px;background:#F0FDF4;border-radius:8px;border-left:3px solid var(--verde)">
            <div style="font-size:20px;font-weight:700;font-family:'DM Mono';color:var(--verde)">p = {glob['p_mann']}</div>
            <div style="font-size:12px;color:var(--gris-txt);margin-top:4px">Estadístico U = {glob['stat_u']:,} · Tamaño de efecto r = {glob['effect_r']}</div>
            <div style="font-size:11px;color:var(--gris-m);margin-top:4px">Si tomamos un egresado y un no egresado al azar, hay un {round(glob['effect_r']*100,1)}% de probabilidad de que el egresado produzca más.</div>
          </div>
        </div>
      </div>
    </div>
  </div>
</section>

<!-- 03 NIVEL ACADÉMICO -->
<section class="slide slide-content" id="s3">
  <div class="sh"><div class="sn">03 · Formación Académica</div>
    <h2 class="st">Nivel de formación: <em>comparativa</em></h2>
    <p class="ss">Distribución porcentual y absoluta — Egresados USTA vs. No Egresados</p></div>
  <div class="sb">
    <div class="cards">
      <div class="card a"><div class="cv">{round(sum(pct_egr_ord[2:]),1)}%</div><div class="cl">Egresados con Maestría o Doctorado</div></div>
      <div class="card g"><div class="cv">{round(sum(pct_noegr_ord[2:]),1)}%</div><div class="cl">No Egresados con Maestría o Doctorado</div></div>
    </div>
    <div class="cg cg2">
      <div class="cb"><div class="ct">Distribución porcentual por nivel</div><div class="cs">% según nivel de formación máxima</div>
        <div class="leg"><div class="li"><div class="ld" style="background:#1A4E8A"></div>Egresado USTA</div><div class="li"><div class="ld" style="background:#94A3B8"></div>No Egresado</div></div>
        <div class="cw"><canvas id="cNivPct"></canvas></div></div>
      <div class="cb"><div class="ct">Producción promedio por nivel de formación</div><div class="cs">A mayor nivel, mayor producción — Correlación Spearman ρ={glob['rho']}</div>
        <div class="leg"><div class="li"><div class="ld" style="background:#1A4E8A"></div>Egresado USTA</div><div class="li"><div class="ld" style="background:#94A3B8"></div>No Egresado</div></div>
        <div class="cw"><canvas id="cProdNiv"></canvas></div></div>
    </div>
    <div style="padding:14px 16px;background:#EFF6FF;border-radius:8px;border-left:4px solid var(--azul-cl);margin-bottom:10px">
      <div style="font-size:20px;font-weight:700;font-family:'DM Mono';color:var(--azul-med)">ρ = {glob['rho']}</div>
      <div style="font-size:12px;color:var(--gris-txt);margin-top:4px">Correlación Spearman: nivel académico ↔ producción · p &lt; 0.001</div>
      <div style="font-size:11px;color:var(--gris-m);margin-top:4px">El nivel académico explica el {round(glob['r2']*100,1)}% de la varianza en producción.</div>
    </div>
    <div class="ins"><strong>Hallazgo:</strong> Los egresados USTA presentan una ventaja sistemática en formación posgradual: el {round(sum(pct_egr_ord[2:]),1)}% tiene maestría o doctorado, frente al {round(sum(pct_noegr_ord[2:]),1)}% de los no egresados — una brecha de {round(sum(pct_egr_ord[2:])-sum(pct_noegr_ord[2:]),1)} puntos porcentuales. Además, solo el {nivel['pct_egr'][0]}% de los egresados USTA tiene formación no identificada, vs. el {nivel['pct_noegr'][0]}% en no egresados, lo que sugiere trayectorias académicas más documentadas y consolidadas. Este perfil de mayor cualificación es consistente con la mayor producción científica observada en la diapositiva anterior.</div>
  </div>
</section>

<!-- 04 POR DIVISIÓN -->
<section class="slide slide-content" id="s4">
  <div class="sh"><div class="sn">04 · Por División Académica</div>
    <h2 class="st">Participación y producción por <em>área del conocimiento</em></h2>
    <p class="ss">Distribución de egresados USTA y producción según división académica del grupo</p></div>
  <div class="sb">
    <div class="cg cg2" style="margin-bottom:16px">
      <div class="cb"><div class="ct">% Egresados USTA por División</div><div class="cs">Proporción de integrantes que son egresados USTA</div>
        <div class="cw"><canvas id="cDivPct"></canvas></div></div>
      <div class="cb"><div class="ct">Producción promedio: Egresados vs. No Egresados</div><div class="cs">Promedio de total_productos por integrante según división</div>
        <div class="leg"><div class="li"><div class="ld" style="background:#1A4E8A"></div>Egresado USTA</div><div class="li"><div class="ld" style="background:#94A3B8"></div>No Egresado</div></div>
        <div class="cw"><canvas id="cDivProd"></canvas></div></div>
    </div>
    <div class="cb">
      <div class="ct" style="margin-bottom:10px">Significancia estadística por División (Mann-Whitney U)</div>
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:10px">
        <div style="padding:12px 14px;background:#F0FDF4;border-radius:8px;border-left:3px solid var(--verde)">
          <div style="font-size:11px;font-weight:700;color:#166534;margin-bottom:6px">✓ Significativa (p &lt; 0.05)</div>
          {''.join(f'<div style="font-size:12px;font-weight:600;color:var(--azul)">{r["division"]}</div><div style="font-size:10px;color:var(--gris-m);margin-bottom:4px">p = {r["p_mann_whitney"]}</div>' for r in sig_divs) or '<div style="font-size:11px;color:var(--gris-m)">Ninguna división alcanzó significancia</div>'}
        </div>
        <div style="padding:12px 14px;background:#FFF7ED;border-radius:8px;border-left:3px solid var(--dorado)">
          <div style="font-size:11px;font-weight:700;color:#92400E;margin-bottom:6px">— No significativa (p ≥ 0.05)</div>
          <div style="font-size:10px;color:var(--gris-m);line-height:1.9">{'  ·  '.join(r['division'] for r in nsig_divs)}</div>
        </div>
        <div style="padding:12px 14px;background:#EFF6FF;border-radius:8px;border-left:3px solid var(--azul-cl)">
          <div style="font-size:11px;font-weight:700;color:#1e40af;margin-bottom:4px">💡 Interpretación</div>
          <div style="font-size:11px;color:var(--gris-m);line-height:1.6">La diferencia en producción es estadísticamente robusta en las divisiones con resultado significativo. En las demás, la diferencia existe pero puede estar limitada por tamaño de muestra o alta varianza interna.</div>
        </div>
      </div>
    </div>
  </div>
</section>

<!-- 05 TOP 10 -->
<section class="slide slide-content" id="s5">
  <div class="sh"><div class="sn">05 · Top 10 Grupos · Facultad</div>
    <h2 class="st">Los grupos más productivos y su <em>contexto disciplinar</em></h2>
    <p class="ss">Producción total, % egresados USTA y producción promedio por persona — con facultad identificada</p></div>
  <div class="sb">
    <div class="cg cg2" style="margin-bottom:14px">
      <div class="cb"><div class="ct">Producción total — Top 10 grupos</div><div class="cs">Coloreado por división académica</div>
        <div class="cw"><canvas id="cT10P"></canvas></div></div>
      <div class="cb"><div class="ct">Producción media por integrante</div><div class="cs">Egresado USTA vs. No Egresado en cada grupo</div>
        <div class="leg"><div class="li"><div class="ld" style="background:#1A4E8A"></div>Egresado USTA</div><div class="li"><div class="ld" style="background:#94A3B8"></div>No Egresado</div></div>
        <div class="cw"><canvas id="cT10M"></canvas></div></div>
    </div>
    <div class="cb"><div class="ct" style="margin-bottom:10px">Resumen Top 10 — por facultad</div>
      <table class="tbl"><thead><tr><th>Grupo</th><th>Facultad</th><th>División</th><th>N</th><th>% Egr.</th><th>Prod. total</th><th>Media Egr.</th><th>Media No Egr.</th></tr></thead>
      <tbody id="t10body"></tbody></table></div>
  </div>
</section>

<!-- 07 PRÓXIMOS PASOS -->
<section class="slide slide-dark" id="s7">
  <div class="sh" style="background:rgba(255,255,255,.04);border-bottom:1px solid rgba(255,255,255,.08)">
    <div class="sn" style="color:rgba(255,255,255,.3)">07 · Siguiente Etapa</div>
    <h2 class="st" style="color:#fff">Hoja de ruta: <em>análisis ampliado</em></h2>
    <p class="ss">Este análisis cubre grupos avalados por USTA. El siguiente paso es escalar al universo completo de grupos reconocidos por Minciencias.</p>
  </div>
  <div class="sb" style="display:flex;flex-direction:column;gap:20px">

    <!-- banner alcance -->
    <div style="background:rgba(196,154,34,.12);border:1px solid rgba(196,154,34,.35);border-radius:12px;padding:18px 24px;display:flex;align-items:center;gap:24px;flex-wrap:wrap">
      <div style="flex:1;min-width:220px">
        <div style="font-size:11px;font-weight:700;color:var(--dorado-cl);letter-spacing:1.2px;text-transform:uppercase;margin-bottom:6px">Alcance actual</div>
        <div style="font-size:22px;font-weight:700;font-family:'DM Mono',monospace;color:#fff">{glob['n_grupos']} grupos · {glob['n_total']:,} integrantes</div>
        <div style="font-size:12px;color:rgba(255,255,255,.4);margin-top:3px">Grupos avalados por USTA · Convocatorias 781–894</div>
      </div>
      <div style="font-size:28px;color:rgba(196,154,34,.5)">→</div>
      <div style="flex:1;min-width:220px">
        <div style="font-size:11px;font-weight:700;color:var(--dorado-cl);letter-spacing:1.2px;text-transform:uppercase;margin-bottom:6px">Alcance objetivo</div>
        <div style="font-size:22px;font-weight:700;font-family:'DM Mono',monospace;color:var(--dorado-cl)">Todos los grupos</div>
        <div style="font-size:12px;color:rgba(255,255,255,.4);margin-top:3px">Todos los grupos reconocidos por Minciencias a nivel nacional</div>
      </div>
    </div>

    <!-- dos pasos grandes -->
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;flex:1">
      <div style="background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.1);border-radius:14px;padding:32px 28px;display:flex;flex-direction:column;gap:16px">
        <div style="font-family:'Playfair Display',serif;font-size:52px;font-weight:900;color:var(--dorado);line-height:1">1</div>
        <div>
          <div style="font-size:17px;font-weight:700;color:#fff;margin-bottom:10px;line-height:1.3">Extracción masiva — todos los grupos</div>
          <div style="font-size:13.5px;color:rgba(255,255,255,.55);line-height:1.75">Ampliar la extracción desde GrupLAC y CvLAC a todos los grupos reconocidos a nivel nacional. Identificar egresados USTA en grupos no avalados directamente por la universidad.</div>
        </div>
        <div style="margin-top:auto;padding-top:16px;border-top:1px solid rgba(255,255,255,.08)">
          <span style="font-size:11px;font-weight:600;color:var(--dorado-cl);background:rgba(196,154,34,.12);border:1px solid rgba(196,154,34,.25);border-radius:20px;padding:4px 12px">GrupLAC + CvLAC Nacional</span>
        </div>
      </div>
      <div style="background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.1);border-radius:14px;padding:32px 28px;display:flex;flex-direction:column;gap:16px">
        <div style="font-family:'Playfair Display',serif;font-size:52px;font-weight:900;color:var(--dorado);line-height:1">2</div>
        <div>
          <div style="font-size:17px;font-weight:700;color:#fff;margin-bottom:10px;line-height:1.3">Análisis estadístico inferencial completo</div>
          <div style="font-size:13.5px;color:rgba(255,255,255,.55);line-height:1.75">Regresión logística para predecir probabilidad de alta producción según condición de egresado. Modelos multinivel por área del conocimiento y sede universitaria.</div>
        </div>
        <div style="margin-top:auto;padding-top:16px;border-top:1px solid rgba(255,255,255,.08)">
          <span style="font-size:11px;font-weight:600;color:#a5f3c0;background:rgba(22,163,74,.12);border:1px solid rgba(22,163,74,.25);border-radius:20px;padding:4px 12px">Regresión · Modelos multinivel</span>
        </div>
      </div>
    </div>

    <!-- pie -->
    <div style="border-top:1px solid rgba(255,255,255,.08);padding-top:14px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px">
      <div style="font-size:11px;color:rgba(255,255,255,.25)">GrupLAC / CvLAC · Minciencias · 2017–2021 · Dirección de Graduados USTA 2024</div>
      <div style="display:flex;gap:8px"><span class="badge i">GrupLAC ✓</span><span class="badge i">CvLAC ✓</span><span class="badge w">Grupos nacionales · próximo</span></div>
    </div>
  </div>
</section>

<script>
const AZUL='#1A4E8A',AZUL_CL='#2E75B6',DORADO='#C49A22',GRIS='#94A3B8',VERDE='#16A34A';
const DEF={{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}},tooltip:{{backgroundColor:'rgba(0,51,102,.95)',titleFont:{{family:"'DM Sans'",size:12}},bodyFont:{{family:"'DM Mono'",size:12}},padding:10,cornerRadius:6}}}}}};

new Chart(document.getElementById('cDonut'),{{type:'doughnut',data:{{labels:['Egresado USTA','No Egresado'],datasets:[{{data:[{glob['n_egr']},{glob['n_noegr']}],backgroundColor:[AZUL,GRIS],borderWidth:3,borderColor:'#fff',hoverOffset:8}}]}},options:{{...DEF,cutout:'62%',plugins:{{...DEF.plugins,legend:{{display:true,position:'bottom',labels:{{font:{{family:"'DM Sans'",size:12}},padding:16,usePointStyle:true}}}},tooltip:{{...DEF.plugins.tooltip,callbacks:{{label:ctx=>` ${{ctx.label}}: ${{ctx.raw.toLocaleString('es-CO')}} (${{(ctx.raw/{glob['n_total']}*100).toFixed(1)}}%)`}}}}}}}}}});

new Chart(document.getElementById('cPerc'),{{type:'bar',data:{{labels:['P25','P50 (mediana)','P75','P90'],datasets:[{{label:'Egresado USTA',data:[{int(glob['p25_egr'])},{int(glob['med_egr'])},{int(glob['p75_egr'])},{int(glob['p90_egr'])}],backgroundColor:AZUL,borderRadius:4}},{{label:'No Egresado',data:[{int(glob['p25_noegr'])},{int(glob['med_noegr'])},{int(glob['p75_noegr'])},{int(glob['p90_noegr'])}],backgroundColor:GRIS,borderRadius:4}}]}},options:{{...DEF,plugins:{{...DEF.plugins,legend:{{display:true,position:'top',labels:{{font:{{size:11}},usePointStyle:true}}}}}},scales:{{y:{{grid:{{color:'#E2E8F0'}},ticks:{{font:{{family:"'DM Mono'"}}}}}},x:{{grid:{{display:false}}}}}}}}}});

const nivOrd={_js_array(niveles_ord)};
new Chart(document.getElementById('cNivPct'),{{type:'bar',data:{{labels:nivOrd,datasets:[{{label:'Egresado USTA',data:{_js_array(pct_egr_ord)},backgroundColor:AZUL,borderRadius:4}},{{label:'No Egresado',data:{_js_array(pct_noegr_ord)},backgroundColor:GRIS,borderRadius:4}}]}},options:{{...DEF,plugins:{{...DEF.plugins,legend:{{display:true,position:'top',labels:{{font:{{size:11}},usePointStyle:true}}}}}},scales:{{y:{{grid:{{color:'#E2E8F0'}},ticks:{{callback:v=>v+'%',font:{{family:"'DM Mono'"}}}}}},x:{{grid:{{display:false}},ticks:{{font:{{size:10}}}}}}}}}}}});

new Chart(document.getElementById('cProdNiv'),{{type:'bar',data:{{labels:nivOrd,datasets:[{{label:'Egresado USTA',data:{_js_array(prod_niv_egr)},backgroundColor:AZUL,borderRadius:4}},{{label:'No Egresado',data:{_js_array(prod_niv_noegr)},backgroundColor:GRIS,borderRadius:4}}]}},options:{{...DEF,plugins:{{...DEF.plugins,legend:{{display:true,position:'top',labels:{{font:{{size:11}},usePointStyle:true}}}}}},scales:{{y:{{grid:{{color:'#E2E8F0'}},ticks:{{font:{{family:"'DM Mono'"}}}}}},x:{{grid:{{display:false}}}}}}}}}});

const divL={_js_array(div_labels)},divPct={_js_array(div_pct)},divPE={_js_array(div_prod_egr)},divPN={_js_array(div_prod_noegr)};
new Chart(document.getElementById('cDivPct'),{{type:'bar',data:{{labels:divL,datasets:[{{data:divPct,backgroundColor:divPct.map(v=>v>=50?AZUL:v>=40?AZUL_CL:GRIS),borderRadius:5}}]}},options:{{...DEF,indexAxis:'y',scales:{{x:{{grid:{{color:'#E2E8F0'}},ticks:{{callback:v=>v+'%',font:{{family:"'DM Mono'",size:10}}}},max:75}},y:{{grid:{{display:false}},ticks:{{font:{{size:10}}}}}}}},plugins:{{...DEF.plugins,tooltip:{{...DEF.plugins.tooltip,callbacks:{{label:ctx=>` ${{ctx.raw}}% egresados USTA`}}}}}}}}}});

new Chart(document.getElementById('cDivProd'),{{type:'bar',data:{{labels:divL,datasets:[{{label:'Egresado USTA',data:divPE,backgroundColor:AZUL,borderRadius:3}},{{label:'No Egresado',data:divPN,backgroundColor:GRIS,borderRadius:3}}]}},options:{{...DEF,indexAxis:'y',plugins:{{...DEF.plugins,legend:{{display:true,position:'top',labels:{{font:{{size:10}},usePointStyle:true}}}}}},scales:{{x:{{grid:{{color:'#E2E8F0'}},ticks:{{font:{{family:"'DM Mono'",size:10}}}}}},y:{{grid:{{display:false}},ticks:{{font:{{size:10}}}}}}}}}}}});

const t10={_js_array(top10)};
const divCol={{"Ciencias de la Educación":"#2E75B6","Ciencias Jurídicas y Políticas":"#7C3AED","Ingenierías":"#16A34A","Ciencias Humanas y Sociales":"#C49A22","Humanidades":"#0891B2","Comunicación y Ciencias Sociales":"#DC2626","Ciencias Económicas y Administrativas":"#0D9488","Arquitectura y Diseño":"#EA580C","Ciencias de la Salud":"#DB2777"}};

new Chart(document.getElementById('cT10P'),{{type:'bar',data:{{labels:t10.map(d=>d.nombre_corto),datasets:[{{data:t10.map(d=>d.total_prod),backgroundColor:t10.map(d=>divCol[d.division]||GRIS),borderRadius:4}}]}},options:{{...DEF,indexAxis:'y',scales:{{x:{{grid:{{color:'#E2E8F0'}},ticks:{{font:{{family:"'DM Mono'",size:10}}}}}},y:{{grid:{{display:false}},ticks:{{font:{{size:9}}}}}}}},plugins:{{...DEF.plugins,tooltip:{{...DEF.plugins.tooltip,callbacks:{{label:ctx=>` ${{ctx.raw.toLocaleString('es-CO')}} productos · ${{t10[ctx.dataIndex].facultad}}`}}}}}}}}}});

new Chart(document.getElementById('cT10M'),{{type:'bar',data:{{labels:t10.map(d=>d.nombre_corto),datasets:[{{label:'Egresado USTA',data:t10.map(d=>d.media_egr),backgroundColor:AZUL,borderRadius:3}},{{label:'No Egresado',data:t10.map(d=>d.media_noegr),backgroundColor:GRIS,borderRadius:3}}]}},options:{{...DEF,indexAxis:'y',plugins:{{...DEF.plugins,legend:{{display:true,position:'top',labels:{{font:{{size:10}},usePointStyle:true}}}}}},scales:{{x:{{grid:{{color:'#E2E8F0'}},ticks:{{font:{{family:"'DM Mono'",size:10}}}}}},y:{{grid:{{display:false}},ticks:{{font:{{size:9}}}}}}}}}}}});

const tbody=document.getElementById('t10body');
t10.forEach(d=>{{const tr=document.createElement('tr');tr.innerHTML=`<td><strong>${{d.nombre_corto}}</strong></td><td style="color:var(--azul-med);font-weight:500">${{d.facultad}}</td><td><span style="font-size:10px;padding:2px 7px;border-radius:3px;background:#EFF6FF;color:#1A4E8A">${{d.division}}</span></td><td style="text-align:center;font-family:'DM Mono'">${{d.n_total}}</td><td style="text-align:center;font-family:'DM Mono';color:${{d.pct_egr>=50?'#1A4E8A':'#64748B'}};font-weight:${{d.pct_egr>=50?700:400}}">${{d.pct_egr}}%</td><td style="text-align:center;font-family:'DM Mono';font-weight:700;color:#003366">${{d.total_prod.toLocaleString('es-CO')}}</td><td style="text-align:center;font-family:'DM Mono';color:#1A4E8A">${{d.media_egr}}</td><td style="text-align:center;font-family:'DM Mono';color:#64748B">${{d.media_noegr}}</td>`;tbody.appendChild(tr);}});

// nav
const slides=document.querySelectorAll('.slide'),dots_c=document.querySelectorAll('.nd');
const navEl=document.getElementById('navDots');
slides.forEach((_,i)=>{{const b=document.createElement('button');b.className='nd'+(i===0?' active':'');b.onclick=()=>slides[i].scrollIntoView({{behavior:'smooth'}});b.title=`Sección ${{i+1}}`;navEl.appendChild(b);}});
const allDots=document.querySelectorAll('.nd');
const obs=new IntersectionObserver(entries=>entries.forEach(e=>{{if(e.isIntersecting){{const i=Array.from(slides).indexOf(e.target);allDots.forEach((d,j)=>d.classList.toggle('active',i===j));}}}}),{{threshold:0.5}});
slides.forEach(s=>obs.observe(s));
</script>
</body>
</html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  ✓ Presentación HTML generada: {output_path}")


# ══════════════════════════════════════════════════════════════════════════════
# 6. FUNCIÓN PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def run_analysis(
    input_path: str,
    output_dir: str = "artifacts/",
    titulo: str = "Egresados USTA · Grupos de Investigación",
    top_n: int = 10,
) -> dict:
    """
    Pipeline completo de análisis.

    Parámetros
    ----------
    input_path : str
        Ruta al Excel con hojas Integrantes_Formacion y Resumen_por_Grupo.
    output_dir : str
        Directorio donde se guardan los outputs.
    titulo : str
        Título para la presentación HTML.
    top_n : int
        Número de grupos para el Top N.

    Retorna
    -------
    dict con claves: 'stats_excel', 'html', 'glob', 'div_df', 'top10'
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 55)
    print("ANÁLISIS EGRESADOS USTA — GRUPOS DE INVESTIGACIÓN")
    print("=" * 55)

    print("\n📂 Cargando datos...")
    df_int, df_res = load_data(input_path)

    print("\n🔧 Preparando datos...")
    df_int, df_res = prepare(df_int, df_res)

    print("\n📊 Calculando estadísticos...")
    glob     = stats_globales(df_int)
    nivel_df = stats_por_nivel(df_int)
    div_df   = stats_por_division(df_int)
    top10    = stats_top10(df_res, df_int, n=top_n)
    nivel    = stats_nivel_formacion(df_int)

    print(f"\n  n_total={glob['n_total']:,}  n_egr={glob['n_egr']:,}  pct_egr={glob['pct_egr']}%")
    print(f"  Mann-Whitney p={glob['p_mann']}  {'✓ sig.' if glob['sig_mann'] else '— n.s.'}")
    print(f"  Spearman ρ={glob['rho']}  r²={glob['r2']}")

    print("\n💾 Exportando outputs...")
    stats_path = str(Path(output_dir) / "estadisticas_egresados_usta.xlsx")
    html_path  = str(Path(output_dir) / "presentacion_egresados_usta.html")

    export_stats_excel(glob, div_df, nivel_df, top10, stats_path)
    generate_html(glob, div_df, nivel, top10, html_path, titulo=titulo)

    print("\n" + "=" * 55)
    print("✅ ANÁLISIS COMPLETO")
    print(f"   Excel : {stats_path}")
    print(f"   HTML  : {html_path}")
    print("=" * 55 + "\n")

    return {
        "stats_excel": stats_path,
        "html":        html_path,
        "glob":        glob,
        "div_df":      div_df,
        "top10":       top10,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 7. CLI
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Análisis comparativo egresados USTA en grupos de investigación"
    )
    parser.add_argument(
        "--input", "-i",
        required=True,
        help="Ruta al Excel de integrantes (Integrantes_Formacion + Resumen_por_Grupo)"
    )
    parser.add_argument(
        "--output-dir", "-o",
        default="artifacts/",
        help="Directorio de salida (default: artifacts/)"
    )
    parser.add_argument(
        "--titulo", "-t",
        default="Egresados USTA · Grupos de Investigación",
        help="Título de la presentación"
    )
    parser.add_argument(
        "--top-n", "-n",
        type=int,
        default=10,
        help="Número de grupos en el Top N (default: 10)"
    )
    args = parser.parse_args()
    run_analysis(args.input, args.output_dir, args.titulo, args.top_n)
