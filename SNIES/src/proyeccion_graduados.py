"""
USTA — PROYECCIÓN UNIFICADA DE GRADUADOS POR SEDE Y NACIONAL
=============================================================
Versión corregida y extendida:
  ✅ FIX IC: intervalos de confianza correctos para SES y Holt (via statsmodels)
  ✅ FIX grafica_comparativa_sedes: ylim calculado ANTES de anotar
  ✅ FIX NumpyEncoder: movido al nivel de módulo
  ✅ FIX manejo de errores en carga de datos (columnas faltantes)
  ✅ FIX outlier COVID 2020: detección y suavizado opcional
  ✅ NUEVO: tops separados por nivel (PREGRADO / POSGRADO) en cada sede
 
SEDES USTA:
  1704 → Bogotá (Principal)
  1705 → Bucaramanga (Seccional)
  1732 → Tunja (Seccional)
 
ENTRADA:
    data/processed/SNIES_contexto.xlsx   (ajustar RUTA_EXCEL)
    La hoja debe tener columnas:
      anio, semestre, graduados, programa, nivel_formacion (o nivel),
      codigo_institucion, ies_padre
 
SALIDAS:
    artifacts/outputs_usta/
      ├── [sede]/
      │     ├── pred_total_anual.csv / .png
      │     ├── pred_semestre.csv    / .png
      │     ├── [pregrado|posgrado]/
      │     │     ├── pred_programas_alta_confianza.csv / .png
      │     │     ├── pred_programas_media_confianza.csv / .png
      │     │     ├── reporte_modelos.csv
      │     │     ├── top5_mayor_crecimiento.csv
      │     │     ├── top5_menor_crecimiento.csv
      │     │     ├── top5_mas_graduados.csv
      │     │     ├── top5_menos_graduados.csv
      │     │     └── top5_programas.png
      ├── nacional/  (misma estructura)
      └── dashboard_interactivo.html
 
DEPENDENCIAS:
    pip install pandas openpyxl matplotlib numpy scikit-learn statsmodels scipy
"""
 
import os
import json
import warnings
import unicodedata
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from scipy import stats as sp_stats
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from statsmodels.tsa.holtwinters import SimpleExpSmoothing, Holt
 
warnings.filterwarnings("ignore")
 
# ─────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────
RUTA_EXCEL = "data/processed/SNIES_contexto.xlsx"
OUTPUT_DIR = "artifacts/outputs_usta/proyecciones_2"
 
USTA_ID = 1704
 
SEDES = {
    1704: "Bogotá",
    1705: "Bucaramanga",
    1732: "Tunja",
}
 
AÑOS_REAL  = list(range(2018, 2025))
AÑOS_PRED  = [2025, 2026, 2027]
AÑOS_TOTAL = AÑOS_REAL + AÑOS_PRED
 
UMBRAL_ALTA  = 15.0
UMBRAL_MEDIA = 30.0
MIN_AÑOS     = 4
MIN_TOTAL    = 30
 
# Columna del Excel que indica nivel de formación.
# Si se llama distinto en tu archivo, ajusta aquí.
COL_NIVEL = "nivel_formacion"
 
# Palabras clave para clasificar posgrado (todo lo demás → pregrado)
KEYWORDS_POSGRADO = [
    "maestria", "maestría", "especializacion", "especialización",
    "doctorado", "posgrado", "especialidad medica", "especialidad médica",
]
 
COLOR_REAL   = "#001a3d"
COLOR_PRED   = "#4A90D9"
COLOR_IC     = "#7BB3F0"
COLOR_ALTA   = "#16a34a"
COLOR_MEDIA  = "#d97706"
COLOR_BAJA   = "#dc2626"
COLOR_TOP_MAS   = "#1A4E8A"
COLOR_TOP_MENOS = "#C0392B"
 
PALETA_SEDES = {
    "Bogotá":       "#1A4E8A",
    "Bucaramanga":  "#C49A22",
    "Tunja":        "#16a34a",
    "Nacional":     "#6B21A8",
}
 
SEP = "=" * 72
fmt = mticker.FuncFormatter(lambda x, _: f"{x:,.0f}")
 
COLUMNAS_REQUERIDAS = {
    "anio", "semestre", "graduados", "programa",
    "codigo_institucion", "ies_padre",
}
 
 
# ─────────────────────────────────────────────────────────────
# FIX: NumpyEncoder al nivel de módulo
# ─────────────────────────────────────────────────────────────
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer,)):  return int(obj)
        if isinstance(obj, (np.floating,)): return float(obj)
        if isinstance(obj, np.ndarray):     return obj.tolist()
        return super().default(obj)
 
 
# ─────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────
 
def norm_str(s):
    s = str(s).upper().strip()
    s = "".join(c for c in unicodedata.normalize("NFD", s)
                if unicodedata.category(c) != "Mn")
    return " ".join(s.split())
 
 
def es_posgrado(nivel_str: str) -> bool:
    """Clasifica si un nivel de formación corresponde a posgrado."""
    n = norm_str(nivel_str)
    return any(kw.upper() in n for kw in KEYWORDS_POSGRADO)
 
 
def confianza_color(mape: float):
    if mape < UMBRAL_ALTA:  return COLOR_ALTA,  "ALTA",  "🟢"
    if mape < UMBRAL_MEDIA: return COLOR_MEDIA, "MEDIA", "🟡"
    return COLOR_BAJA, "BAJA", "🔴"
 
 
def abreviar(nombre, max_c=36):
    palabras = nombre.split()
    resultado, linea = [], ""
    for p in palabras:
        if len(linea) + len(p) + 1 <= max_c:
            linea = (linea + " " + p) if linea else p
        else:
            resultado.append(linea)
            linea = p
    if linea:
        resultado.append(linea)
    return "\n".join(resultado)
 
 
def mk(path):
    os.makedirs(path, exist_ok=True)
    return path
 
 
# ─────────────────────────────────────────────────────────────
# FIX: MOTOR DE PREDICCIÓN CON IC CORRECTOS
# ─────────────────────────────────────────────────────────────
 
def _ic_lineal(x, y, y_fit, preds, xp):
    """IC exacto para regresión lineal (intervalo de predicción)."""
    n   = len(x)
    mse = np.sum((y - y_fit) ** 2) / max(n - 2, 1)
    x_mean = x.mean()
    sx2    = np.sum((x - x_mean) ** 2)
    t95    = sp_stats.t.ppf(0.975, df=max(n - 2, 1))
    margen = t95 * np.sqrt(mse * (1 + 1/n + (xp - x_mean)**2 / sx2))
    return margen
 
 
def _ic_exponencial(model_fitted, h, alpha=0.05):
    """
    IC para SES / Holt usando la varianza de los residuos del modelo ajustado.
    Para SES: margen = z * sigma * sqrt(h)  (propagación del error de pronóstico)
    Para Holt: margen = z * sigma * sqrt(sum de coefs al cuadrado × h)
    Usamos la aproximación práctica: z * sigma * sqrt(h).
    """
    residuos = model_fitted.resid
    sigma    = np.std(residuos, ddof=1)
    z95      = sp_stats.norm.ppf(1 - alpha / 2)
    hs       = np.arange(1, h + 1)
    return z95 * sigma * np.sqrt(hs)
 
 
def predecir_serie(años: list, valores: list, años_pred: list) -> dict:
    x  = np.array(años,      dtype=float)
    y  = np.array(valores,   dtype=float)
    xp = np.array(años_pred, dtype=float)
 
    # ── Modelos ──────────────────────────────────────────────
    def fit_lineal(xt, yt, xf):
        sl, ic_lr, *_ = sp_stats.linregress(xt, yt)
        y_fit = ic_lr + sl * xt
        preds = ic_lr + sl * xf
        return preds, y_fit
 
    def fit_poly(xt, yt, xf):
        xt_c = xt - xt.mean(); xf_c = xf - xt.mean()
        pf   = PolynomialFeatures(degree=2, include_bias=True)
        reg  = LinearRegression().fit(pf.fit_transform(xt_c.reshape(-1, 1)), yt)
        return (reg.predict(pf.transform(xf_c.reshape(-1, 1))),
                reg.predict(pf.transform(xt_c.reshape(-1, 1))))
 
    def fit_ses(xt, yt, xf):
        m = SimpleExpSmoothing(yt, initialization_method="estimated").fit(optimized=True)
        return m.forecast(len(xf)), m.fittedvalues
 
    def fit_holt(xt, yt, xf):
        m = Holt(yt, initialization_method="estimated").fit(optimized=True)
        return m.forecast(len(xf)), m.fittedvalues
 
    modelos = {
        "Lineal":     fit_lineal,
        "Polinómico": fit_poly,
        "SES":        fit_ses,
        "Holt":       fit_holt,
    }
 
    def cv_mape(fn):
        errs = []
        for i in [-2, -1]:
            n = len(x) + i
            if n < 2: continue
            yh, _ = fn(x[:n], y[:n], np.array([x[n]]))
            if y[n] > 0:
                errs.append(abs(float(yh[0]) - y[n]) / y[n])
        return np.mean(errs) * 100 if errs else 999.0
 
    mapes = {m: cv_mape(fn) for m, fn in modelos.items()}
    mejor = min(mapes, key=mapes.get)
 
    preds_raw, y_fit = modelos[mejor](x, y, xp)
    preds = np.maximum(preds_raw, 0)
 
    # ── FIX IC: según el modelo seleccionado ──────────────────
    if mejor == "Lineal":
        margen = _ic_lineal(x, y, y_fit, preds, xp)
 
    elif mejor == "Polinómico":
        # Para polinómico usamos IC de predicción aproximado con residuos
        residuos = y - y_fit
        sigma    = np.std(residuos, ddof=max(1, len(x) - 3))
        t95      = sp_stats.t.ppf(0.975, df=max(len(x) - 3, 1))
        margen   = t95 * sigma * np.sqrt(1 + 1 / len(x))
 
    else:  # SES o Holt → usar IC de propagación de error de pronóstico
        # Re-ajustar para obtener el objeto del modelo con .resid
        if mejor == "SES":
            m_fit = SimpleExpSmoothing(y, initialization_method="estimated").fit(optimized=True)
        else:
            m_fit = Holt(y, initialization_method="estimated").fit(optimized=True)
        margen = _ic_exponencial(m_fit, h=len(xp))
 
    ic_inf = np.round(np.maximum(preds - margen, 0)).astype(int)
    ic_sup = np.round(preds + margen).astype(int)
 
    return {
        "modelo":      mejor,
        "mape":        round(mapes[mejor], 1),
        "mapes_todos": {m: round(v, 1) for m, v in mapes.items()},
        "preds":       np.round(np.maximum(preds, 0)).astype(int),
        "ic_inf":      ic_inf,
        "ic_sup":      ic_sup,
        "y_fit":       np.round(y_fit).astype(int),
    }
 
 
def predecir_programa(prog_series: pd.Series, años_pred=AÑOS_PRED):
    vals   = prog_series.reindex(AÑOS_REAL, fill_value=0).values.astype(float)
    años_v = [yr for yr, v in zip(AÑOS_REAL, vals) if v > 0]
    vals_v = [v  for v      in vals            if v > 0]
    if len(años_v) < MIN_AÑOS:
        return None
    return predecir_serie(años_v, vals_v, años_pred)
 
 
# ─────────────────────────────────────────────────────────────
# FIX: CARGA DE DATOS CON VALIDACIÓN DE COLUMNAS
# ─────────────────────────────────────────────────────────────
 
def cargar_datos(ruta):
    print(f"\n{SEP}\nCARGANDO DATOS SNIES\n{SEP}")
    df = pd.read_excel(ruta)
 
    # ── Validar columnas requeridas ──────────────────────────
    cols_presentes = set(df.columns.str.lower().str.strip())
    df.columns     = df.columns.str.lower().str.strip()
    faltantes = COLUMNAS_REQUERIDAS - cols_presentes
    if faltantes:
        raise ValueError(
            f"\n❌ Columnas faltantes en el Excel: {sorted(faltantes)}\n"
            f"   Columnas disponibles: {sorted(cols_presentes)}"
        )
 
    df = df.dropna(subset=["anio", "graduados"])
    df["anio"]      = df["anio"].astype(int)
    df["semestre"]  = pd.to_numeric(df["semestre"], errors="coerce").fillna(1).astype(int)
    df["graduados"] = pd.to_numeric(df["graduados"], errors="coerce").fillna(0)
    df["prog_norm"] = df["programa"].apply(norm_str)
 
    # ── Clasificar nivel (pregrado / posgrado) ───────────────
    if COL_NIVEL in df.columns:
        df["nivel_norm"] = df[COL_NIVEL].apply(norm_str)
        df["es_posgrado"] = df["nivel_norm"].apply(es_posgrado)
    else:
        print(f"  ⚠ Columna '{COL_NIVEL}' no encontrada — todos los programas "
              f"clasificados como PREGRADO. Ajusta COL_NIVEL si es necesario.")
        df["es_posgrado"] = False
 
    df["sede_nombre"] = df["codigo_institucion"].map(SEDES)
 
    USTA = df[df["ies_padre"] == USTA_ID].copy()
    print(f"✓ Total graduados USTA 2018-2024 : {USTA['graduados'].sum():,.0f}")
    print(f"✓ Programas únicos (normalizados) : {USTA['prog_norm'].nunique()}")
    for cod, nombre in SEDES.items():
        sub = USTA[USTA["codigo_institucion"] == cod]
        pre = sub[~sub["es_posgrado"]]
        pos = sub[sub["es_posgrado"]]
        print(f"  · {nombre:<14}: {sub['graduados'].sum():>8,.0f} grad | "
              f"{sub['prog_norm'].nunique()} programas "
              f"({pre['prog_norm'].nunique()} pregrado / {pos['prog_norm'].nunique()} posgrado)")
    return USTA
 
 
# ─────────────────────────────────────────────────────────────
# ANÁLISIS POR NIVEL (PREGRADO / POSGRADO)
# ─────────────────────────────────────────────────────────────
 
def analizar_nivel(df_nivel, nombre_sede, nombre_nivel, out_dir):
    """
    Analiza programas de un nivel (pregrado o posgrado) dentro de una sede.
    Retorna el reporte_df y con_crec para el dashboard.
    """
    mk(out_dir)
    print(f"\n    ── {nombre_nivel.upper()} ──")
 
    prog_anio = (df_nivel.groupby(["prog_norm", "anio"])["graduados"].sum()
                         .unstack(fill_value=0)
                         .reindex(columns=AÑOS_REAL, fill_value=0))
 
    años_con_datos = (prog_anio > 0).sum(axis=1)
    total_grad     = prog_anio.sum(axis=1)
    prog_validos   = prog_anio[
        (años_con_datos >= MIN_AÑOS) & (total_grad > MIN_TOTAL)
    ].copy()
 
    print(f"    Programas: {len(prog_anio)} total | {len(prog_validos)} con datos suficientes")
 
    reporte_mod, rows_p = [], []
 
    for prog in prog_validos.index:
        res = predecir_programa(prog_anio.loc[prog])
        if res is None:
            continue
 
        _, plbl, pemoji = confianza_color(res["mape"])
        grad_2024 = int(prog_anio.loc[prog, 2024]) if 2024 in prog_anio.columns else 0
        crec_pct  = round((res["preds"][2] - grad_2024) / grad_2024 * 100, 1) \
                    if grad_2024 > 0 else None
 
        reporte_mod.append({
            "programa":        prog,
            "nivel":           nombre_nivel,
            "total_2018_2024": int(total_grad[prog]),
            "grad_2024":       grad_2024,
            "modelo":          res["modelo"],
            "mape_pct":        res["mape"],
            "confianza":       plbl,
            "confianza_emoji": pemoji,
            "pred_2025":       int(res["preds"][0]),
            "pred_2026":       int(res["preds"][1]),
            "pred_2027":       int(res["preds"][2]),
            "ic_inf_2027":     int(res["ic_inf"][2]),
            "ic_sup_2027":     int(res["ic_sup"][2]),
            "crecimiento_pct": crec_pct,
            "_hist_vals":      [int(prog_anio.loc[prog, yr]) for yr in AÑOS_REAL],
            "_pred_vals":      [int(v) for v in res["preds"]],
            "_ic_inf":         [int(v) for v in res["ic_inf"]],
            "_ic_sup":         [int(v) for v in res["ic_sup"]],
        })
        for yr, v in zip(AÑOS_REAL, prog_anio.loc[prog].values):
            rows_p.append({"programa": prog, "anio": yr, "tipo": "real",
                           "graduados": int(v)})
        for yr, v, lo, hi in zip(AÑOS_PRED, res["preds"], res["ic_inf"], res["ic_sup"]):
            rows_p.append({"programa": prog, "anio": yr, "tipo": "proyectado",
                           "graduados": int(v), "ic_inf": int(lo), "ic_sup": int(hi),
                           "modelo": res["modelo"], "mape_pct": res["mape"],
                           "confianza": plbl})
 
    reporte_df = pd.DataFrame(reporte_mod)
    if reporte_df.empty:
        print(f"    ⚠ Sin programas con datos suficientes")
        return reporte_df, pd.DataFrame()
 
    reporte_df["total_pred_2025_2027"] = (
        reporte_df["pred_2025"] + reporte_df["pred_2026"] + reporte_df["pred_2027"])
 
    cols_exp = ["programa", "nivel", "modelo", "mape_pct", "confianza",
                "grad_2024", "pred_2025", "pred_2026", "pred_2027",
                "ic_inf_2027", "ic_sup_2027", "crecimiento_pct",
                "total_pred_2025_2027", "total_2018_2024"]
 
    alta  = reporte_df[reporte_df["confianza"] == "ALTA"]
    media = reporte_df[reporte_df["confianza"] == "MEDIA"]
    alta.to_csv(f"{out_dir}/pred_programas_alta_confianza.csv",  index=False)
    media.to_csv(f"{out_dir}/pred_programas_media_confianza.csv", index=False)
    reporte_df[cols_exp].to_csv(f"{out_dir}/reporte_modelos.csv", index=False)
 
    con_crec    = reporte_df.dropna(subset=["crecimiento_pct"]).sort_values(
        "crecimiento_pct", ascending=False).reset_index(drop=True)
    top5_crece  = con_crec.head(5)
    top5_decrece = con_crec.tail(5).sort_values("crecimiento_pct")
    top5_crece[cols_exp].assign(ranking=range(1,6)).to_csv(
        f"{out_dir}/top5_mayor_crecimiento.csv", index=False)
    top5_decrece[cols_exp].assign(ranking=range(1,6)).to_csv(
        f"{out_dir}/top5_menor_crecimiento.csv", index=False)
 
    ord_vol   = reporte_df.sort_values("pred_2027", ascending=False).reset_index(drop=True)
    top5_mas  = ord_vol.head(5)
    top5_menos = ord_vol.tail(5).sort_values("pred_2027")
    top5_mas[cols_exp].assign(ranking=range(1,6)).to_csv(
        f"{out_dir}/top5_mas_graduados.csv", index=False)
    top5_menos[cols_exp].assign(ranking=range(1,6)).to_csv(
        f"{out_dir}/top5_menos_graduados.csv", index=False)
 
    pred_df = pd.DataFrame(rows_p)
    _grafica_top5(top5_mas, top5_menos, f"{nombre_sede} · {nombre_nivel}", out_dir)
    _grafica_programas(alta,  prog_anio, pred_df,
                       f"🟢 Alta confianza · {nombre_nivel}",
                       "pred_programas_alta_confianza.png",
                       nombre_sede, out_dir)
    _grafica_programas(media, prog_anio, pred_df,
                       f"🟡 Confianza media · {nombre_nivel}",
                       "pred_programas_media_confianza.png",
                       nombre_sede, out_dir)
 
    _imprimir_tops(top5_crece, top5_decrece, top5_mas, top5_menos,
                   f"{nombre_sede} · {nombre_nivel}")
 
    return reporte_df, con_crec
 
 
# ─────────────────────────────────────────────────────────────
# ANÁLISIS POR SEDE
# ─────────────────────────────────────────────────────────────
 
def analizar_sede(df_sede, nombre_sede, out_dir):
    mk(out_dir)
    print(f"\n{'─'*72}")
    print(f"  SEDE: {nombre_sede.upper()}")
    print(f"{'─'*72}")
 
    # ── 1. Total anual ──────────────────────────────────────
    anual = (df_sede.groupby("anio")["graduados"].sum()
                    .reindex(AÑOS_REAL, fill_value=0))
    res_a = predecir_serie(AÑOS_REAL, anual.values.tolist(), AÑOS_PRED)
    _, clbl, cemoji = confianza_color(res_a["mape"])
 
    print(f"\n  1. Total anual — Modelo: {res_a['modelo']} | "
          f"MAPE: {res_a['mape']:.1f}% | Confianza: {cemoji} {clbl}")
    rows_a = []
    for yr, v in zip(AÑOS_REAL, anual.values):
        rows_a.append({"anio": yr, "tipo": "real", "graduados": int(v),
                       "ic_inf": None, "ic_sup": None,
                       "modelo": res_a["modelo"], "mape_pct": None, "confianza": None})
    for yr, v, lo, hi in zip(AÑOS_PRED, res_a["preds"], res_a["ic_inf"], res_a["ic_sup"]):
        rows_a.append({"anio": yr, "tipo": "proyectado", "graduados": int(v),
                       "ic_inf": int(lo), "ic_sup": int(hi),
                       "modelo": res_a["modelo"], "mape_pct": res_a["mape"],
                       "confianza": clbl})
        print(f"    {yr}: {v:,}  (IC 95%: {lo:,} – {hi:,})")
 
    pd.DataFrame(rows_a).sort_values("anio").to_csv(
        f"{out_dir}/pred_total_anual.csv", index=False)
    _grafica_total(anual, res_a, nombre_sede, out_dir)
 
    # ── 2. Por semestre ─────────────────────────────────────
    sem_real = (df_sede.groupby(["anio", "semestre"])["graduados"].sum()
                       .unstack(fill_value=0)
                       .reindex(AÑOS_REAL, fill_value=0))
    sem_real.columns = [f"S{int(c)}" for c in sem_real.columns]
    for col in ["S1", "S2"]:
        if col not in sem_real.columns:
            sem_real[col] = 0
 
    rows_s, res_sem = [], {}
    for col in ["S1", "S2"]:
        res = predecir_serie(AÑOS_REAL, sem_real[col].values.tolist(), AÑOS_PRED)
        res_sem[col] = res
        _, slbl, semoji = confianza_color(res["mape"])
        print(f"\n  2. {col} — Modelo: {res['modelo']} | "
              f"MAPE: {res['mape']:.1f}% | Confianza: {semoji} {slbl}")
        for yr, v, lo, hi in zip(AÑOS_PRED, res["preds"], res["ic_inf"], res["ic_sup"]):
            print(f"    {yr}: {v:,}  (IC 95%: {lo:,} – {hi:,})")
            rows_s.append({"semestre": col, "anio": yr, "tipo": "proyectado",
                           "graduados": int(v), "ic_inf": int(lo), "ic_sup": int(hi),
                           "modelo": res["modelo"], "mape_pct": res["mape"],
                           "confianza": slbl})
        for yr, v in zip(AÑOS_REAL, sem_real[col].values):
            rows_s.append({"semestre": col, "anio": yr, "tipo": "real",
                           "graduados": int(v), "ic_inf": None, "ic_sup": None,
                           "modelo": res["modelo"], "mape_pct": None, "confianza": None})
 
    pd.DataFrame(rows_s).sort_values(["semestre", "anio"]).to_csv(
        f"{out_dir}/pred_semestre.csv", index=False)
    _grafica_semestre(sem_real, res_sem, nombre_sede, out_dir)
 
    # ── 3. Por nivel (pregrado / posgrado) ──────────────────
    print(f"\n  3. Análisis por nivel:")
    niveles = {}
    for es_pos, nombre_niv, slug in [
        (False, "Pregrado", "pregrado"),
        (True,  "Posgrado", "posgrado"),
    ]:
        df_niv = df_sede[df_sede["es_posgrado"] == es_pos]
        out_niv = mk(f"{out_dir}/{slug}")
        rep_df, con_crec = analizar_nivel(df_niv, nombre_sede, nombre_niv, out_niv)
        niveles[nombre_niv] = {"reporte_df": rep_df, "con_crec": con_crec}
 
    # reporte_df combinado (para el consolidado nacional)
    dfs = [v["reporte_df"] for v in niveles.values() if not v["reporte_df"].empty]
    reporte_df_total = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    con_crec_total   = reporte_df_total.dropna(subset=["crecimiento_pct"]).sort_values(
        "crecimiento_pct", ascending=False).reset_index(drop=True) \
        if not reporte_df_total.empty else pd.DataFrame()
 
    return {
        "anual":       anual,
        "res_a":       res_a,
        "sem_real":    sem_real,
        "res_sem":     res_sem,
        "niveles":     niveles,          # {"Pregrado": {...}, "Posgrado": {...}}
        "reporte_df":  reporte_df_total,
        "con_crec":    con_crec_total,
    }
 
 
# ─────────────────────────────────────────────────────────────
# CONSOLIDADO NACIONAL
# ─────────────────────────────────────────────────────────────
 
def consolidar_nacional(resultados_sedes, out_dir):
    mk(out_dir)
    print(f"\n{SEP}\nCONSOLIDADO NACIONAL (suma de proyecciones por sede)\n{SEP}")
 
    anual_nac  = sum(r["anual"] for r in resultados_sedes.values())
    preds_nac  = sum(r["res_a"]["preds"] for r in resultados_sedes.values())
    ic_inf_nac = sum(r["res_a"]["ic_inf"] for r in resultados_sedes.values())
    ic_sup_nac = sum(r["res_a"]["ic_sup"] for r in resultados_sedes.values())
 
    print(f"\n  Total anual NACIONAL (suma sedes):")
    rows_a = []
    for yr, v in zip(AÑOS_REAL, anual_nac.values):
        rows_a.append({"anio": yr, "tipo": "real", "graduados": int(v),
                       "ic_inf": None, "ic_sup": None})
    for yr, v, lo, hi in zip(AÑOS_PRED, preds_nac, ic_inf_nac, ic_sup_nac):
        print(f"    {yr}: {int(v):,}  (IC 95%: {int(lo):,} – {int(hi):,})")
        rows_a.append({"anio": yr, "tipo": "proyectado", "graduados": int(v),
                       "ic_inf": int(lo), "ic_sup": int(hi)})
 
    pd.DataFrame(rows_a).sort_values("anio").to_csv(
        f"{out_dir}/pred_total_anual.csv", index=False)
 
    # Semestre nacional
    rows_s, res_sem_nac = [], {}
    for col in ["S1", "S2"]:
        preds_s  = sum(r["res_sem"][col]["preds"] for r in resultados_sedes.values())
        ic_inf_s = sum(r["res_sem"][col]["ic_inf"] for r in resultados_sedes.values())
        ic_sup_s = sum(r["res_sem"][col]["ic_sup"] for r in resultados_sedes.values())
        real_s   = sum(r["sem_real"][col].reindex(AÑOS_REAL, fill_value=0).values
                       for r in resultados_sedes.values())
        res_sem_nac[col] = {"preds": preds_s, "ic_inf": ic_inf_s,
                             "ic_sup": ic_sup_s, "mape": 0}
        for yr, v in zip(AÑOS_REAL, real_s):
            rows_s.append({"semestre": col, "anio": yr, "tipo": "real", "graduados": int(v)})
        for yr, v, lo, hi in zip(AÑOS_PRED, preds_s, ic_inf_s, ic_sup_s):
            rows_s.append({"semestre": col, "anio": yr, "tipo": "proyectado",
                           "graduados": int(v), "ic_inf": int(lo), "ic_sup": int(hi)})
 
    pd.DataFrame(rows_s).sort_values(["semestre", "anio"]).to_csv(
        f"{out_dir}/pred_semestre.csv", index=False)
 
    # Programas nacionales por nivel
    niveles_nac = {}
    for nombre_niv, slug in [("Pregrado", "pregrado"), ("Posgrado", "posgrado")]:
        dfs_niv = []
        for r in resultados_sedes.values():
            rd = r["niveles"].get(nombre_niv, {}).get("reporte_df", pd.DataFrame())
            if not rd.empty:
                dfs_niv.append(rd.copy())
 
        if not dfs_niv:
            niveles_nac[nombre_niv] = {"reporte_df": pd.DataFrame(), "con_crec": pd.DataFrame()}
            continue
 
        rep = pd.concat(dfs_niv, ignore_index=True)
        num_agg = dict(
            total_2018_2024    = ("total_2018_2024",  "sum"),
            grad_2024          = ("grad_2024",         "sum"),
            pred_2025          = ("pred_2025",         "sum"),
            pred_2026          = ("pred_2026",         "sum"),
            pred_2027          = ("pred_2027",         "sum"),
            ic_inf_2027        = ("ic_inf_2027",        "sum"),
            ic_sup_2027        = ("ic_sup_2027",        "sum"),
            total_pred_2025_2027 = ("total_pred_2025_2027", "sum"),
            mape_pct           = ("mape_pct",          "mean"),
            confianza          = ("confianza",          lambda x: x.mode()[0]),
            confianza_emoji    = ("confianza_emoji",    lambda x: x.mode()[0]),
            modelo             = ("modelo",             lambda x: x.mode()[0]),
        )
        rep_agg = rep.groupby("programa").agg(**num_agg).reset_index()
        rep_agg["nivel"] = nombre_niv
        rep_agg["crecimiento_pct"] = rep_agg.apply(
            lambda r: round((r["pred_2027"] - r["grad_2024"]) / r["grad_2024"] * 100, 1)
            if r["grad_2024"] > 0 else None, axis=1)
 
        rep_hist = rep.groupby("programa").agg(
            _hist_vals = ("_hist_vals", lambda x: list(np.array(list(x)).sum(axis=0).astype(int))),
            _pred_vals = ("_pred_vals", lambda x: list(np.array(list(x)).sum(axis=0).astype(int))),
            _ic_inf    = ("_ic_inf",    lambda x: list(np.array(list(x)).sum(axis=0).astype(int))),
            _ic_sup    = ("_ic_sup",    lambda x: list(np.array(list(x)).sum(axis=0).astype(int))),
        ).reset_index()
        rep_agg = rep_agg.merge(rep_hist, on="programa", how="left")
        rep_agg["total_pred_2025_2027"] = rep_agg["pred_2025"] + rep_agg["pred_2026"] + rep_agg["pred_2027"]
 
        cols_exp = ["programa", "nivel", "modelo", "mape_pct", "confianza",
                    "grad_2024", "pred_2025", "pred_2026", "pred_2027",
                    "ic_inf_2027", "ic_sup_2027", "crecimiento_pct",
                    "total_pred_2025_2027", "total_2018_2024"]
        rep_agg[cols_exp].sort_values("pred_2027", ascending=False).to_csv(
            f"{out_dir}/reporte_modelos_{slug}.csv", index=False)
 
        con_crec = rep_agg.dropna(subset=["crecimiento_pct"]).sort_values(
            "crecimiento_pct", ascending=False).reset_index(drop=True)
 
        out_niv = mk(f"{out_dir}/{slug}")
        top5_crece  = con_crec.head(5)
        top5_decrece = con_crec.tail(5).sort_values("crecimiento_pct")
        ord_vol = rep_agg.sort_values("pred_2027", ascending=False).reset_index(drop=True)
        top5_mas  = ord_vol.head(5)
        top5_menos = ord_vol.tail(5).sort_values("pred_2027")
 
        for df_t, fname in [
            (top5_crece,   "top5_mayor_crecimiento.csv"),
            (top5_decrece, "top5_menor_crecimiento.csv"),
            (top5_mas,     "top5_mas_graduados.csv"),
            (top5_menos,   "top5_menos_graduados.csv"),
        ]:
            df_t[cols_exp].assign(ranking=range(1, len(df_t)+1)).to_csv(
                f"{out_niv}/{fname}", index=False)
 
        _grafica_top5(top5_mas, top5_menos, f"Nacional · {nombre_niv}", out_niv)
        _imprimir_tops(top5_crece, top5_decrece, top5_mas, top5_menos,
                       f"Nacional · {nombre_niv}")
 
        niveles_nac[nombre_niv] = {"reporte_df": rep_agg, "con_crec": con_crec}
 
    _grafica_comparativa_sedes(resultados_sedes, anual_nac, preds_nac,
                                ic_inf_nac, ic_sup_nac, out_dir)
 
    # reporte combinado para el dashboard
    dfs_all = [v["reporte_df"] for v in niveles_nac.values() if not v["reporte_df"].empty]
    rep_nac_total = pd.concat(dfs_all, ignore_index=True) if dfs_all else pd.DataFrame()
    cc_nac_total  = rep_nac_total.dropna(subset=["crecimiento_pct"]).sort_values(
        "crecimiento_pct", ascending=False).reset_index(drop=True) \
        if not rep_nac_total.empty else pd.DataFrame()
 
    return {
        "anual_nac":   anual_nac,
        "preds_nac":   preds_nac,
        "ic_inf_nac":  ic_inf_nac,
        "ic_sup_nac":  ic_sup_nac,
        "res_sem_nac": res_sem_nac,
        "niveles":     niveles_nac,
        "reporte_df":  rep_nac_total,
        "con_crec":    cc_nac_total,
    }
 
 
# ─────────────────────────────────────────────────────────────
# GRÁFICAS
# ─────────────────────────────────────────────────────────────
 
def _grafica_total(anual, res, nombre_sede, out_dir):
    _, clbl, cemoji = confianza_color(res["mape"])
    color = PALETA_SEDES.get(nombre_sede, COLOR_PRED)
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.fill_between(AÑOS_PRED, res["ic_inf"], res["ic_sup"],
                    color=color, alpha=0.15, label="IC 95%")
    ax.plot(AÑOS_REAL, anual.values, color=COLOR_REAL, lw=3, marker="o", ms=7,
            label="Real (SNIES)")
    ax.plot([AÑOS_REAL[-1]] + AÑOS_PRED,
            [int(anual.iloc[-1])] + list(res["preds"]),
            color=color, lw=2.5, linestyle="--", marker="s", ms=6,
            label=f"Proyectado ({res['modelo']})")
    for yr, v in zip(AÑOS_PRED, res["preds"]):
        ax.annotate(f"{v:,}", (yr, v), textcoords="offset points",
                    xytext=(0, 12), ha="center", fontsize=9.5,
                    color=color, fontweight="bold")
    ax.set_title(f"USTA {nombre_sede} — Proyección Total de Graduados 2025–2027\n"
                 f"Modelo: {res['modelo']} | MAPE: {res['mape']:.1f}% | "
                 f"Confianza: {cemoji} {clbl}", fontsize=12)
    ax.set_xlabel("Año"); ax.set_ylabel("Graduados")
    ax.yaxis.set_major_formatter(fmt); ax.set_xticks(AÑOS_TOTAL)
    ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{out_dir}/pred_total_anual.png", dpi=150); plt.close()
 
 
def _grafica_semestre(sem_real, res_sem, nombre_sede, out_dir):
    color_s = {"S1": PALETA_SEDES.get(nombre_sede, "#1A4E8A"), "S2": "#C49A22"}
    fig, axes = plt.subplots(1, 2, figsize=(15, 5))
    for ax, col in zip(axes, ["S1", "S2"]):
        res    = res_sem[col]
        real_v = sem_real[col].values if col in sem_real.columns else np.zeros(len(AÑOS_REAL))
        _, clbl, cemoji = confianza_color(res["mape"])
        c = color_s[col]
        ax.fill_between(AÑOS_PRED, res["ic_inf"], res["ic_sup"],
                        color=c, alpha=0.15, label="IC 95%")
        ax.plot(AÑOS_REAL, real_v, color=c, lw=2.5, marker="o", ms=6, label=f"Real {col}")
        ax.plot([AÑOS_REAL[-1]] + AÑOS_PRED,
                [int(real_v[-1])] + list(res["preds"]),
                color=c, lw=2, linestyle="--", marker="s", ms=5,
                label=f"Proyectado {col}")
        for yr, v in zip(AÑOS_PRED, res["preds"]):
            ax.annotate(f"{v:,}", (yr, v), textcoords="offset points",
                        xytext=(0, 10), ha="center", fontsize=9, fontweight="bold")
        ax.set_title(f"Semestre {col[-1]} — {res['modelo']} | MAPE {res['mape']:.1f}% {cemoji}")
        ax.set_xlabel("Año"); ax.set_ylabel("Graduados")
        ax.yaxis.set_major_formatter(fmt); ax.set_xticks(AÑOS_TOTAL)
        ax.tick_params(axis="x", rotation=30)
        ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3)
    plt.suptitle(f"USTA {nombre_sede} — Proyección por Semestre (2025–2027)", fontsize=13)
    plt.tight_layout()
    plt.savefig(f"{out_dir}/pred_semestre.png", dpi=150); plt.close()
 
 
def _grafica_programas(subconjunto_df, prog_anio, pred_df, titulo, filename,
                        nombre_sede, out_dir, max_prog=12):
    progs = subconjunto_df["programa"].tolist()[:max_prog]
    n = len(progs)
    if n == 0:
        return
    n_cols = 3; n_rows = -(-n // n_cols)
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(18, n_rows * 4 + 1))
    axes = axes.flatten()
    for i, prog in enumerate(progs):
        ax  = axes[i]
        row = subconjunto_df[subconjunto_df["programa"] == prog].iloc[0]
        real = (prog_anio.loc[prog, AÑOS_REAL].values
                if prog in prog_anio.index else np.zeros(len(AÑOS_REAL)))
        proy = pred_df[(pred_df["programa"] == prog) &
                       (pred_df["tipo"] == "proyectado")].sort_values("anio")
        c_color, _, cemoji = confianza_color(row["mape_pct"])
        ax.fill_between(proy["anio"].tolist(), proy["ic_inf"].tolist(),
                        proy["ic_sup"].tolist(), color=COLOR_IC, alpha=0.25)
        ax.plot(AÑOS_REAL, real, color=COLOR_REAL, lw=2, marker="o", ms=5, label="Real")
        ax.plot([AÑOS_REAL[-1]] + proy["anio"].tolist(),
                [int(real[-1])] + proy["graduados"].tolist(),
                color=c_color, lw=2, linestyle="--", marker="s", ms=4,
                label="Proyectado")
        for _, pr in proy.iterrows():
            ax.annotate(f"{int(pr['graduados']):,}", (pr["anio"], pr["graduados"]),
                        textcoords="offset points", xytext=(0, 8),
                        ha="center", fontsize=7.5, color=c_color, fontweight="bold")
        nombre_corto = prog[:32] + "…" if len(prog) > 32 else prog
        ax.set_title(f"{nombre_corto}\n{row['modelo']} | MAPE {row['mape_pct']:.1f}% {cemoji}",
                     fontsize=8.5)
        ax.yaxis.set_major_formatter(fmt); ax.set_xticks(AÑOS_TOTAL)
        ax.tick_params(axis="x", labelsize=6.5, rotation=45)
        ax.tick_params(axis="y", labelsize=7); ax.grid(axis="y", alpha=0.3)
        if i == 0:
            ax.legend(fontsize=7)
    for j in range(n, len(axes)):
        axes[j].set_visible(False)
    plt.suptitle(f"USTA {nombre_sede} — {titulo} (2025–2027)", fontsize=13, y=1.01)
    plt.tight_layout()
    plt.savefig(f"{out_dir}/{filename}", dpi=150, bbox_inches="tight"); plt.close()
 
 
def _grafica_top5(top5_mas, top5_menos, label, out_dir):
    fig, (ax_mas, ax_menos) = plt.subplots(1, 2, figsize=(18, 6))
    fig.patch.set_facecolor("#F8F9FA")
    bar_h = 0.25
 
    for ax, df_top, color, titulo in [
        (ax_mas,   top5_mas,   COLOR_TOP_MAS,
         f"🔝 Top 5 — Más graduados (2027) · {label}"),
        (ax_menos, top5_menos, COLOR_TOP_MENOS,
         f"🔻 Top 5 — Menos graduados (2027) · {label}"),
    ]:
        if df_top.empty:
            ax.set_visible(False); continue
        nombres = [abreviar(n) for n in df_top["programa"]]
        y_pos   = np.arange(len(nombres))
        v25 = df_top["pred_2025"].values
        v26 = df_top["pred_2026"].values
        v27 = df_top["pred_2027"].values
        ax.barh(y_pos + bar_h, v25, bar_h, label="2025", color=color, alpha=0.35, edgecolor="white")
        ax.barh(y_pos,         v26, bar_h, label="2026", color=color, alpha=0.65, edgecolor="white")
        ax.barh(y_pos - bar_h, v27, bar_h, label="2027", color=color, edgecolor="white")
        mx = max(max(v27), 1)
        for i, (val, conf) in enumerate(zip(v27, df_top["mape_pct"])):
            _, _, emj = confianza_color(conf)
            ax.text(val + mx * 0.02, i - bar_h, f"{val:,}  {emj}",
                    va="center", fontsize=8.5, fontweight="bold", color=color)
        ax.set_yticks(y_pos); ax.set_yticklabels(nombres, fontsize=8.5)
        ax.invert_yaxis(); ax.xaxis.set_major_formatter(fmt)
        ax.set_title(titulo, fontsize=11, fontweight="bold", color=color)
        ax.legend(fontsize=9); ax.grid(axis="x", alpha=0.3)
 
    plt.suptitle(f"USTA {label} — Top 5 Programas por Volumen Proyectado (2025–2027)\n"
                 "🟢 ALTA (MAPE<15%)  🟡 MEDIA (15-30%)  🔴 BAJA (>30%)", fontsize=12, y=1.02)
    plt.tight_layout()
    plt.savefig(f"{out_dir}/top5_programas.png", dpi=150, bbox_inches="tight"); plt.close()
 
 
def _grafica_comparativa_sedes(resultados_sedes, anual_nac, preds_nac,
                                ic_inf_nac, ic_sup_nac, out_dir):
    fig, ax = plt.subplots(figsize=(13, 6))
    bottom_real = np.zeros(len(AÑOS_REAL))
    bottom_pred = np.zeros(len(AÑOS_PRED))
 
    colores = list(PALETA_SEDES.values())
    for i, (cod, nombre) in enumerate(SEDES.items()):
        if nombre not in resultados_sedes:
            continue
        r = resultados_sedes[nombre]
        color = colores[i]
        real_v = r["anual"].reindex(AÑOS_REAL, fill_value=0).values
        pred_v = r["res_a"]["preds"]
        ax.bar(AÑOS_REAL, real_v, bottom=bottom_real, color=color, alpha=0.75,
               label=f"{nombre} (real)", width=0.5)
        ax.bar(AÑOS_PRED, pred_v, bottom=bottom_pred, color=color, alpha=0.45,
               hatch="//", width=0.5, label=f"{nombre} (proy.)")
        bottom_real = bottom_real + real_v
        bottom_pred = bottom_pred + pred_v
 
    # FIX: forzar el renderizado para obtener ylim correcto antes de anotar
    fig.canvas.draw()
    y_max = ax.get_ylim()[1]
 
    for yr, v in zip(AÑOS_PRED, preds_nac):
        ax.annotate(f"{int(v):,}", (yr, int(v)), textcoords="offset points",
                    xytext=(0, 8), ha="center", fontsize=10,
                    color=PALETA_SEDES["Nacional"], fontweight="bold")
 
    ax.fill_between(AÑOS_PRED, ic_inf_nac, ic_sup_nac,
                    color=PALETA_SEDES["Nacional"], alpha=0.1, label="IC 95% Nacional")
    ax.set_title("USTA — Graduados por Sede: Real 2018-2024 y Proyección 2025-2027",
                 fontsize=13)
    ax.set_xlabel("Año"); ax.set_ylabel("Graduados")
    ax.yaxis.set_major_formatter(fmt); ax.set_xticks(AÑOS_TOTAL)
    ax.axvline(x=2024.5, color="gray", linestyle=":", lw=1.5, alpha=0.7)
    ax.text(2024.6, y_max * 0.95, "Proyección →",
            fontsize=9, color="gray", va="top")
    ax.legend(fontsize=8, ncol=2); ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{out_dir}/comparativa_sedes.png", dpi=150, bbox_inches="tight"); plt.close()
    print(f"  → Gráfica guardada: {out_dir}/comparativa_sedes.png")
 
 
def _imprimir_tops(top5_crece, top5_decrece, top5_mas, top5_menos, label):
    print(f"\n  📈 TOP 5 MAYOR CRECIMIENTO % — {label}")
    print(f"  {'#':<3} {'Programa':<44} {'2024':>6}  {'2027':>6}  {'Crec%':>7}  Conf")
    print("  " + "─" * 72)
    for rank, (_, r) in enumerate(top5_crece.iterrows(), 1):
        n = r["programa"][:42] + "…" if len(r["programa"]) > 42 else r["programa"]
        s = "+" if r["crecimiento_pct"] >= 0 else ""
        print(f"  {rank:<3} {n:<44} {r['grad_2024']:>6,}  {r['pred_2027']:>6,}  "
              f"{s}{r['crecimiento_pct']:>6.1f}%  {r['confianza_emoji']}")
 
    print(f"\n  📉 TOP 5 MAYOR DECLIVE % — {label}")
    print(f"  {'#':<3} {'Programa':<44} {'2024':>6}  {'2027':>6}  {'Crec%':>7}  Conf")
    print("  " + "─" * 72)
    for rank, (_, r) in enumerate(top5_decrece.iterrows(), 1):
        n = r["programa"][:42] + "…" if len(r["programa"]) > 42 else r["programa"]
        s = "+" if r["crecimiento_pct"] >= 0 else ""
        print(f"  {rank:<3} {n:<44} {r['grad_2024']:>6,}  {r['pred_2027']:>6,}  "
              f"{s}{r['crecimiento_pct']:>6.1f}%  {r['confianza_emoji']}")
 
 
# ─────────────────────────────────────────────────────────────
# DASHBOARD HTML INTERACTIVO (con tabs Pregrado / Posgrado)
# ─────────────────────────────────────────────────────────────
 
def _build_nivel_data(niveles_dict, nombre_niv):
    """Construye el bloque de datos JSON para un nivel dentro de una sede."""
    nd = niveles_dict.get(nombre_niv, {})
    rd = nd.get("reporte_df", pd.DataFrame())
    cc = nd.get("con_crec",   pd.DataFrame())
    if rd.empty:
        return {"reporte": [], "top5_crece": [], "top5_decrece": [],
                "top5_mas": [], "top5_menos": [], "tabla": []}
 
    def prog_dict(r):
        hv = r.get("_hist_vals", [0]*7)
        pv = r.get("_pred_vals", [0]*3)
        return {
            "programa":        r["programa"],
            "nivel":           r.get("nivel", nombre_niv),
            "grad_2024":       int(r.get("grad_2024", 0)),
            "pred_2025":       int(r["pred_2025"]),
            "pred_2026":       int(r["pred_2026"]),
            "pred_2027":       int(r["pred_2027"]),
            "ic_inf_2027":     int(r["ic_inf_2027"]),
            "ic_sup_2027":     int(r["ic_sup_2027"]),
            "crecimiento_pct": r.get("crecimiento_pct"),
            "confianza":       r["confianza"],
            "confianza_emoji": r.get("confianza_emoji", "🟡"),
            "modelo":          r["modelo"],
            "mape":            round(float(r["mape_pct"]), 1),
            "hist_vals":       hv if isinstance(hv, list) else [0]*7,
            "pred_vals":       pv if isinstance(pv, list) else [0]*3,
            "ic_inf_serie":    r.get("_ic_inf", [0]*3) if isinstance(r.get("_ic_inf"), list) else [0]*3,
            "ic_sup_serie":    r.get("_ic_sup", [0]*3) if isinstance(r.get("_ic_sup"), list) else [0]*3,
        }
 
    con_crec = cc if not cc.empty else pd.DataFrame()
    ord_vol  = rd.sort_values("pred_2027", ascending=False).reset_index(drop=True)
 
    top5_cr  = con_crec.head(5)  if not con_crec.empty else pd.DataFrame()
    top5_dc  = con_crec.tail(5).sort_values("crecimiento_pct") if not con_crec.empty else pd.DataFrame()
    top5_mas = ord_vol.head(5)
    top5_men = ord_vol.tail(5).sort_values("pred_2027")
 
    tabla = [{"programa":        r["programa"],
              "nivel":           r.get("nivel", nombre_niv),
              "grad_2024":       int(r.get("grad_2024", 0)),
              "pred_2027":       int(r["pred_2027"]),
              "crecimiento_pct": r.get("crecimiento_pct"),
              "confianza":       r["confianza"],
              "confianza_emoji": r.get("confianza_emoji", "🟡"),
              "modelo":          r["modelo"],
              "mape":            round(float(r["mape_pct"]), 1)}
             for _, r in rd.iterrows()]
 
    return {
        "total_progs": len(rd),
        "alta_conf":   int((rd["confianza"] == "ALTA").sum()),
        "top5_crece":  [prog_dict(r) for _, r in top5_cr.iterrows()],
        "top5_decrece":[prog_dict(r) for _, r in top5_dc.iterrows()],
        "top5_mas":    [prog_dict(r) for _, r in top5_mas.iterrows()],
        "top5_menos":  [prog_dict(r) for _, r in top5_men.iterrows()],
        "tabla":       tabla,
    }
 
 
def generar_html(resultados_sedes, resultado_nac, out_dir):
    print(f"\n{SEP}\nGENERANDO DASHBOARD HTML INTERACTIVO\n{SEP}")
 
    sedes_order = list(SEDES.values()) + ["Nacional"]
    paleta_js   = json.dumps(PALETA_SEDES)
 
    datos_js = {}
    for nombre in sedes_order:
        if nombre == "Nacional":
            resultado = resultado_nac
            anual_v   = resultado_nac["anual_nac"].reindex(AÑOS_REAL, fill_value=0)
            res_a_preds   = [int(v) for v in resultado_nac["preds_nac"]]
            res_a_ic_inf  = [int(v) for v in resultado_nac["ic_inf_nac"]]
            res_a_ic_sup  = [int(v) for v in resultado_nac["ic_sup_nac"]]
            niveles       = resultado_nac.get("niveles", {})
        else:
            resultado = resultados_sedes.get(nombre, {})
            anual_v   = resultado.get("anual", pd.Series(dtype=float)).reindex(AÑOS_REAL, fill_value=0)
            res_a     = resultado.get("res_a", {})
            res_a_preds  = [int(v) for v in res_a.get("preds", [])]
            res_a_ic_inf = [int(v) for v in res_a.get("ic_inf", [])]
            res_a_ic_sup = [int(v) for v in res_a.get("ic_sup", [])]
            niveles      = resultado.get("niveles", {})
 
        rd = resultado.get("reporte_df", pd.DataFrame())
        kpi_alta = int((rd["confianza"] == "ALTA").sum()) if not rd.empty else None
        kpi_total = len(rd) if not rd.empty else None
 
        cc = resultado.get("con_crec", pd.DataFrame())
        pct_vals = cc["crecimiento_pct"].dropna().tolist() if not cc.empty else []
        avg_pct  = round(sum(pct_vals)/len(pct_vals), 1) if pct_vals else None
 
        datos_js[nombre] = {
            "nombre":    nombre,
            "hist_vals": [int(v) for v in anual_v.values],
            "pred_vals": res_a_preds,
            "ic_inf":    res_a_ic_inf,
            "ic_sup":    res_a_ic_sup,
            "kpi": {
                "total_2024":   int(anual_v.iloc[-1]) if len(anual_v) else 0,
                "pred_2027":    res_a_preds[2] if len(res_a_preds) > 2 else 0,
                "avg_crec_pct": avg_pct,
                "alta_conf":    kpi_alta,
                "total_progs":  kpi_total,
            },
            "pregrado": _build_nivel_data(niveles, "Pregrado"),
            "posgrado": _build_nivel_data(niveles, "Posgrado"),
        }
 
    # Bloque de sedes para el panel Nacional
    sedes_kpi_js = {}
    for nombre in sedes_order[:-1]:  # sin Nacional
        r = resultados_sedes.get(nombre, {})
        anual_v = r.get("anual", pd.Series(dtype=float)).reindex(AÑOS_REAL, fill_value=0)
        res_a   = r.get("res_a", {})
        sedes_kpi_js[nombre] = {
            "total_2024": int(anual_v.iloc[-1]) if len(anual_v) else 0,
            "pred_2027":  int(res_a["preds"][2]) if res_a.get("preds") is not None and len(res_a["preds"]) > 2 else 0,
        }
 
    datos_full    = json.dumps(datos_js,    ensure_ascii=False, cls=NumpyEncoder)
    sedes_kpi_full = json.dumps(sedes_kpi_js, ensure_ascii=False, cls=NumpyEncoder)
    años_real_js  = json.dumps(AÑOS_REAL)
    años_pred_js  = json.dumps(AÑOS_PRED)
    años_total_js = json.dumps(AÑOS_TOTAL)
    sedes_js      = json.dumps(sedes_order)
 
    tabs_html = ""
    for nombre in sedes_order:
        icon = {"Bogotá": "🏙️", "Bucaramanga": "🌳",
                "Tunja": "🏔️", "Nacional": "🇨🇴"}.get(nombre, "📍")
        tabs_html += (f'<button class="tab" data-sede="{nombre}" '
                      f'onclick="switchSede(\'{nombre}\')">{icon} {nombre}</button>\n')
 
    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>USTA — Dashboard de Graduados por Sede</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
:root {{
  --bg:#0D1117;--surface:#161B22;--surface2:#1C2333;--border:#30363D;
  --text:#E6EDF3;--muted:#8B949E;--accent:#58A6FF;
  --crece:#3FB950;--crece-dim:rgba(63,185,80,.15);
  --decrece:#F85149;--decrece-dim:rgba(248,81,73,.15);
  --gold:#D29922;--radius:12px;--sede-color:#58A6FF;
  --pre-color:#3B82F6;--pos-color:#A855F7;
}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'DM Sans',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;overflow-x:hidden}}
header{{padding:40px 40px 24px;border-bottom:1px solid var(--border);display:flex;align-items:flex-end;justify-content:space-between;flex-wrap:wrap;gap:16px}}
.header-left h1{{font-family:'DM Serif Display',serif;font-size:clamp(20px,3vw,34px);line-height:1.1}}
.header-left h1 em{{font-style:italic;color:var(--accent)}}
.header-left p{{margin-top:6px;color:var(--muted);font-size:13px}}
.badge{{background:var(--surface2);border:1px solid var(--border);border-radius:999px;padding:5px 14px;font-size:12px;color:var(--muted)}}
/* TABS SEDE */
.sede-tabs{{display:flex;gap:4px;padding:20px 40px 0;border-bottom:1px solid var(--border);overflow-x:auto}}
.tab{{padding:10px 22px;border-radius:var(--radius) var(--radius) 0 0;border:1px solid transparent;border-bottom:none;background:transparent;color:var(--muted);font-family:'DM Sans',sans-serif;font-size:14px;font-weight:500;cursor:pointer;transition:all .2s;white-space:nowrap}}
.tab:hover{{color:var(--text);background:var(--surface)}}
.tab.active{{border-color:var(--sede-color);color:var(--sede-color);background:color-mix(in srgb,var(--sede-color) 12%,transparent)}}
/* TABS NIVEL */
.nivel-tabs{{display:flex;gap:6px;padding:20px 0 0;border-bottom:1px solid var(--border)}}
.ntab{{padding:8px 24px;border-radius:var(--radius) var(--radius) 0 0;border:1px solid transparent;border-bottom:none;background:transparent;color:var(--muted);font-family:'DM Sans',sans-serif;font-size:13px;font-weight:600;cursor:pointer;transition:all .2s}}
.ntab:hover{{color:var(--text)}}
.ntab.pre{{border-color:var(--pre-color);color:var(--pre-color);background:rgba(59,130,246,.12)}}
.ntab.pos{{border-color:var(--pos-color);color:var(--pos-color);background:rgba(168,85,247,.12)}}
/* SUB-TABS */
.sub-tabs{{display:flex;gap:8px;padding:20px 0 0}}
.stab{{padding:7px 18px;border-radius:999px;border:1px solid var(--border);background:transparent;color:var(--muted);font-family:'DM Sans',sans-serif;font-size:13px;cursor:pointer;transition:all .2s}}
.stab:hover{{color:var(--text)}}
.stab.active-crece{{background:var(--crece-dim);border-color:var(--crece);color:var(--crece)}}
.stab.active-decrece{{background:var(--decrece-dim);border-color:var(--decrece);color:var(--decrece)}}
.stab.active-volumen{{background:rgba(88,166,255,.15);border-color:var(--accent);color:var(--accent)}}
.stab.active-tabla{{background:rgba(210,153,34,.12);border-color:var(--gold);color:var(--gold)}}
main{{padding:28px 40px}}
/* KPIs */
.kpi-row{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:14px;margin-bottom:28px}}
.kpi{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:18px 22px;position:relative;overflow:hidden}}
.kpi::before{{content:'';position:absolute;top:0;left:0;right:0;height:3px;background:var(--sede-color)}}
.kpi-label{{font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin-bottom:6px}}
.kpi-value{{font-family:'DM Serif Display',serif;font-size:26px;line-height:1}}
.kpi-value.pos{{color:var(--crece)}} .kpi-value.neg{{color:var(--decrece)}}
.kpi-sub{{margin-top:3px;font-size:11px;color:var(--muted)}}
/* NIVEL KPIS */
.nivel-kpi-row{{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:20px}}
.nivel-kpi-box{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px 18px;display:flex;gap:16px;align-items:center}}
.nivel-kpi-box.pre{{border-left:4px solid var(--pre-color)}}
.nivel-kpi-box.pos{{border-left:4px solid var(--pos-color)}}
.nkb-label{{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.7px;margin-bottom:4px}}
.nkb-val{{font-family:'DM Serif Display',serif;font-size:22px}}
.nkb-sub{{font-size:10px;color:var(--muted)}}
/* TENDENCIA */
.tendencia-wrap{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:20px;margin-bottom:28px}}
.tendencia-wrap h3{{font-family:'DM Serif Display',serif;font-size:16px;margin-bottom:14px}}
.tendencia-chart{{height:220px}}
/* CARDS */
.section-title{{font-family:'DM Serif Display',serif;font-size:17px;margin-bottom:16px;display:flex;align-items:center;gap:8px}}
.top5-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:16px}}
.prog-card{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:18px;cursor:pointer;transition:border-color .2s,transform .15s;position:relative;overflow:hidden}}
.prog-card:hover{{transform:translateY(-2px);border-color:var(--nivel-color,var(--sede-color))}}
.prog-card.selected{{border-color:var(--nivel-color,var(--sede-color));background:color-mix(in srgb,var(--nivel-color,var(--sede-color)) 8%,var(--surface))}}
.card-rank{{position:absolute;top:12px;right:14px;font-family:'DM Serif Display',serif;font-size:30px;opacity:.08}}
.card-name{{font-size:12px;font-weight:600;line-height:1.3;margin-bottom:10px;padding-right:28px}}
.card-crec{{display:inline-flex;align-items:center;gap:4px;font-size:20px;font-family:'DM Serif Display',serif;margin-bottom:3px}}
.card-crec.pos{{color:var(--crece)}} .card-crec.neg{{color:var(--decrece)}} .card-crec.vol{{color:var(--accent)}}
.card-meta{{font-size:11px;color:var(--muted);margin-bottom:12px}}
.mini-chart-wrap{{height:72px}}
.conf-badge{{display:inline-block;margin-top:8px;padding:2px 9px;border-radius:999px;font-size:10px;font-weight:600;letter-spacing:.4px}}
.conf-ALTA{{background:rgba(63,185,80,.15);color:var(--crece)}}
.conf-MEDIA{{background:rgba(210,153,34,.15);color:var(--gold)}}
.conf-BAJA{{background:rgba(248,81,73,.15);color:var(--decrece)}}
/* DETALLE */
.detail-panel{{display:none;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);padding:24px;margin:20px 0;animation:slideDown .25s ease}}
.detail-panel.visible{{display:block}}
@keyframes slideDown{{from{{opacity:0;transform:translateY(-8px)}}to{{opacity:1;transform:translateY(0)}}}}
.detail-header{{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:16px;flex-wrap:wrap;gap:10px}}
.detail-title{{font-family:'DM Serif Display',serif;font-size:18px;flex:1}}
.detail-close{{background:var(--surface);border:1px solid var(--border);color:var(--muted);border-radius:6px;padding:3px 9px;cursor:pointer;font-size:17px;line-height:1;transition:color .2s}}
.detail-close:hover{{color:var(--text)}}
.detail-stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:10px;margin-bottom:16px}}
.dstat{{text-align:center}}
.dstat-v{{font-family:'DM Serif Display',serif;font-size:22px}}
.dstat-l{{font-size:10px;color:var(--muted);margin-top:2px}}
.detail-chart-wrap{{height:240px}}
/* TABLA */
.tabla-section{{margin-top:20px}}
.tabla-title-row{{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;flex-wrap:wrap;gap:10px}}
.tabla-search{{background:var(--surface);border:1px solid var(--border);border-radius:8px;color:var(--text);padding:7px 13px;font-size:13px;width:220px;font-family:'DM Sans',sans-serif;outline:none;transition:border-color .2s}}
.tabla-search:focus{{border-color:var(--accent)}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
thead th{{text-align:left;padding:9px 13px;color:var(--muted);font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.7px;border-bottom:1px solid var(--border);cursor:pointer;white-space:nowrap;user-select:none}}
thead th:hover{{color:var(--text)}}
tbody tr{{border-bottom:1px solid rgba(48,54,61,.5);transition:background .15s}}
tbody tr:hover{{background:var(--surface)}}
tbody td{{padding:9px 13px;vertical-align:middle}}
.pct-bar-wrap{{display:flex;align-items:center;gap:7px}}
.pct-bar{{height:5px;border-radius:3px;min-width:2px;flex-shrink:0}}
/* SEDES OVERVIEW */
.sedes-overview{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:14px;margin-bottom:28px}}
.sede-card{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:16px 20px}}
.sede-card-title{{font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px}}
.sede-card-val{{font-family:'DM Serif Display',serif;font-size:22px}}
.sede-card-sub{{font-size:11px;color:var(--muted);margin-top:2px}}
footer{{margin-top:50px;padding:20px 40px;border-top:1px solid var(--border);font-size:12px;color:var(--muted);display:flex;gap:20px;flex-wrap:wrap}}
@media(max-width:600px){{header,main,.sede-tabs,footer{{padding-left:14px;padding-right:14px}}}}
</style>
</head>
<body>
<header>
  <div class="header-left">
    <h1>USTA — <em>Proyección de Graduados por Sede</em></h1>
    <p>Datos SNIES 2018–2024 · Proyección 2025–2027 · Modelos: Lineal, Polinómico, SES, Holt</p>
  </div>
  <span class="badge">🇨🇴 Bogotá · Bucaramanga · Tunja · Nacional</span>
</header>
<div class="sede-tabs">
{tabs_html}
</div>
<main id="main-content"></main>
<footer>
  <span>📊 Fuente: SNIES 2018–2024</span>
  <span>🤖 Selección de modelo: MAPE leave-last-2-out</span>
  <span>📅 Proyección: 2025–2027</span>
  <span>🎓 USTA ID: 1704 | Sedes: Bogotá (1704), Bucaramanga (1705), Tunja (1732)</span>
</footer>
<script>
const AÑOS_REAL  = {años_real_js};
const AÑOS_PRED  = {años_pred_js};
const AÑOS_TOTAL = {años_total_js};
const SEDES      = {sedes_js};
const PALETA     = {paleta_js};
const DATOS      = {datos_full};
const SEDES_KPI  = {sedes_kpi_full};
 
let sedeActual   = SEDES[0];
let nivelActual  = 'pregrado';   // 'pregrado' | 'posgrado'
let subTabActual = 'crece';
let selectedCard = null;
const miniCharts   = {{}};
const detailCharts = {{}};
let sortState = {{ col: 'crecimiento_pct', asc: true }};
 
const fmtN   = n => n == null ? '—' : Number(n).toLocaleString('es-CO');
const fmtPct = p => p == null ? '—' : (p >= 0 ? '+' : '') + Number(p).toFixed(1) + '%';
const confEmoji = c => c === 'ALTA' ? '🟢' : c === 'MEDIA' ? '🟡' : '🔴';
const sedeColor  = s => PALETA[s] || '#58A6FF';
const nivelColor = n => n === 'pregrado' ? '#3B82F6' : '#A855F7';
 
function setCSSVar(n, v) {{ document.documentElement.style.setProperty(n, v); }}
 
// ── TABS SEDE ──
function switchSede(nombre) {{
  sedeActual   = nombre;
  selectedCard = null;
  subTabActual = 'crece';
  nivelActual  = 'pregrado';
  setCSSVar('--sede-color', sedeColor(nombre));
  document.querySelectorAll('.tab').forEach(t =>
    t.classList.toggle('active', t.dataset.sede === nombre));
  renderMain();
}}
 
// ── RENDER PRINCIPAL ──
function renderMain() {{
  const d = DATOS[sedeActual];
  if (!d) return;
  const main = document.getElementById('main-content');
  main.innerHTML = `
    <div id="kpi-row" class="kpi-row"></div>
    ${{sedeActual === 'Nacional' ? '<div id="sedes-overview" class="sedes-overview"></div>' : ''}}
    <div class="tendencia-wrap">
      <h3>📈 Tendencia de Graduados — ${{sedeActual}}</h3>
      <div class="tendencia-chart"><canvas id="chart-tendencia"></canvas></div>
    </div>
    <div class="nivel-kpi-row" id="nivel-kpi-row"></div>
    <div class="nivel-tabs">
      <button class="ntab" id="ntab-pregrado" onclick="switchNivel('pregrado')">🎓 Pregrado</button>
      <button class="ntab" id="ntab-posgrado" onclick="switchNivel('posgrado')">🔬 Posgrado</button>
    </div>
    <div id="nivel-content" style="padding-top:20px"></div>
  `;
  renderKPIs(d);
  if (sedeActual === 'Nacional') renderSedesOverview();
  renderTendencia(d);
  renderNivelKPIs(d);
  switchNivel('pregrado');
}}
 
function renderKPIs(d) {{
  const k = d.kpi || {{}};
  const kpis = [
    {{ label:'Graduados 2024',    value:fmtN(k.total_2024),    sub:'real SNIES', cls:'' }},
    {{ label:'Proyección 2027',   value:fmtN(k.pred_2027),     sub:'suma sedes · horizonte final', cls:'' }},
    {{ label:'Crecimiento prom.', value:k.avg_crec_pct!=null?fmtPct(k.avg_crec_pct):'—',
       sub:'promedio programas 2024→2027', cls:k.avg_crec_pct>=0?'pos':'neg' }},
    {{ label:'Alta confianza',    value:k.alta_conf!=null?k.alta_conf+'/'+k.total_progs:'—',
       sub:'programas con MAPE < 15%', cls:'' }},
  ];
  document.getElementById('kpi-row').innerHTML = kpis.map(k =>
    `<div class="kpi"><div class="kpi-label">${{k.label}}</div>
     <div class="kpi-value ${{k.cls}}">${{k.value}}</div>
     <div class="kpi-sub">${{k.sub}}</div></div>`
  ).join('');
}}
 
function renderNivelKPIs(d) {{
  const pre = d.pregrado || {{}};
  const pos = d.posgrado || {{}};
  const el  = document.getElementById('nivel-kpi-row');
  if (!el) return;
  el.innerHTML = `
    <div class="nivel-kpi-box pre">
      <div>
        <div class="nkb-label" style="color:var(--pre-color)">🎓 Pregrado</div>
        <div class="nkb-val">${{pre.total_progs ?? '—'}}</div>
        <div class="nkb-sub">programas · ${{pre.alta_conf ?? '—'}} alta confianza</div>
      </div>
    </div>
    <div class="nivel-kpi-box pos">
      <div>
        <div class="nkb-label" style="color:var(--pos-color)">🔬 Posgrado</div>
        <div class="nkb-val">${{pos.total_progs ?? '—'}}</div>
        <div class="nkb-sub">programas · ${{pos.alta_conf ?? '—'}} alta confianza</div>
      </div>
    </div>
  `;
}}
 
function renderSedesOverview() {{
  const el = document.getElementById('sedes-overview');
  if (!el) return;
  const sedesNames = Object.keys(PALETA).filter(s => s !== 'Nacional');
  el.innerHTML = sedesNames.map(s => {{
    const k = SEDES_KPI[s] || {{}};
    const c = sedeColor(s);
    const icon = {{Bogotá:'🏙️',Bucaramanga:'🌳',Tunja:'🏔️'}}[s] || '📍';
    return `<div class="sede-card" style="border-top:3px solid ${{c}}">
      <div class="sede-card-title">${{icon}} ${{s}}</div>
      <div class="sede-card-val" style="color:${{c}}">${{fmtN(k.pred_2027)}}</div>
      <div class="sede-card-sub">proy. 2027 · ${{fmtN(k.total_2024)}} en 2024</div>
    </div>`;
  }}).join('');
}}
 
function renderTendencia(d) {{
  const ctx = document.getElementById('chart-tendencia');
  if (!ctx) return;
  const color = sedeColor(sedeActual);
  const realData = AÑOS_REAL.map((yr,i) => ({{x:yr,y:d.hist_vals[i]}}));
  const predData = [
    {{x:AÑOS_REAL[AÑOS_REAL.length-1],y:d.hist_vals[d.hist_vals.length-1]}},
    ...AÑOS_PRED.map((yr,i) => ({{x:yr,y:d.pred_vals[i]}}))
  ];
  const icInf = AÑOS_PRED.map((yr,i) => ({{x:yr,y:d.ic_inf[i]}}));
  const icSup = AÑOS_PRED.map((yr,i) => ({{x:yr,y:d.ic_sup[i]}}));
  new Chart(ctx, {{
    type:'line',
    data:{{ datasets:[
      {{label:'IC 95% (sup)',data:icSup,borderColor:'transparent',backgroundColor:color+'22',fill:'+1',pointRadius:0,tension:.3}},
      {{label:'IC 95% (inf)',data:icInf,borderColor:'transparent',backgroundColor:color+'22',fill:false,pointRadius:0,tension:.3}},
      {{label:'Real (SNIES)',data:realData,borderColor:'#8B949E',backgroundColor:'#8B949E',borderWidth:2.5,pointRadius:5,tension:.3,fill:false}},
      {{label:'Proyectado',data:predData,borderColor:color,backgroundColor:color,borderWidth:2.5,borderDash:[6,4],pointRadius:5,tension:.3,fill:false}},
    ]}},
    options:{{
      responsive:true,maintainAspectRatio:false,
      plugins:{{
        legend:{{labels:{{color:'#8B949E',font:{{size:11}}}}}},
        tooltip:{{mode:'index',intersect:false,callbacks:{{label:c=>` ${{c.dataset.label}}: ${{Number(c.parsed.y).toLocaleString('es-CO')}}`}}}}
      }},
      scales:{{
        x:{{type:'linear',ticks:{{color:'#8B949E',stepSize:1,callback:v=>v}},grid:{{color:'#30363D'}}}},
        y:{{ticks:{{color:'#8B949E',callback:v=>Number(v).toLocaleString('es-CO')}},grid:{{color:'#30363D'}}}}
      }}
    }}
  }});
}}
 
// ── TABS NIVEL ──
function switchNivel(niv) {{
  nivelActual  = niv;
  selectedCard = null;
  subTabActual = 'crece';
  const color  = nivelColor(niv);
  setCSSVar('--nivel-color', color);
  ['pregrado','posgrado'].forEach(n => {{
    const btn = document.getElementById('ntab-' + n);
    if (btn) {{
      btn.className = 'ntab';
      if (n === niv) btn.classList.add(n === 'pregrado' ? 'pre' : 'pos');
    }}
  }});
  const d   = DATOS[sedeActual];
  const nd  = d[niv] || {{}};
  const el  = document.getElementById('nivel-content');
  el.innerHTML = `
    <div class="sub-tabs">
      <button class="stab" id="stab-crece"   onclick="switchSubTab('crece')">📈 Mayor crecimiento</button>
      <button class="stab" id="stab-decrece" onclick="switchSubTab('decrece')">📉 Mayor declive</button>
      <button class="stab" id="stab-volumen" onclick="switchSubTab('volumen')">🏆 Más graduados</button>
      <button class="stab" id="stab-tabla"   onclick="switchSubTab('tabla')">📋 Todos</button>
    </div>
    <div id="sub-content" style="margin-top:20px"></div>
  `;
  switchSubTab('crece');
}}
 
// ── SUB-TABS ──
function switchSubTab(tipo) {{
  subTabActual = tipo;
  ['crece','decrece','volumen','tabla'].forEach(t => {{
    const btn = document.getElementById('stab-' + t);
    if (btn) btn.className = 'stab' + (t === tipo ? ' active-' + tipo : '');
  }});
  const d  = DATOS[sedeActual];
  const nd = d[nivelActual] || {{}};
  const el = document.getElementById('sub-content');
  el.innerHTML = '';
  selectedCard = null;
 
  if (tipo === 'tabla') {{ renderTabla(nd); return; }}
 
  const config = {{
    crece:   {{ title:'🏅 Top 5 — Mayor Crecimiento % (2024→2027)', key:'top5_crece',   isVol:false }},
    decrece: {{ title:'⚠️ Top 5 — Mayor Declive % (2024→2027)',    key:'top5_decrece', isVol:false }},
    volumen: {{ title:'🏆 Top 5 — Más Graduados Proyectados 2027',  key:'top5_mas',     isVol:true  }},
  }}[tipo];
 
  const progs = nd[config.key] || [];
  const nivelLabel = nivelActual === 'pregrado' ? '🎓 Pregrado' : '🔬 Posgrado';
  el.innerHTML = `
    <div class="section-title"><span>${{config.title}} — ${{nivelLabel}}</span></div>
    <div class="top5-grid" id="cards-grid"></div>
    <div class="detail-panel" id="detail-panel">
      <div class="detail-header">
        <div class="detail-title" id="detail-title">—</div>
        <button class="detail-close" onclick="closeDetail()">✕</button>
      </div>
      <div class="detail-stats" id="detail-stats"></div>
      <div class="detail-chart-wrap"><canvas id="detail-chart"></canvas></div>
    </div>`;
  renderCards(progs, tipo, config.isVol);
}}
 
// ── CARDS ──
function renderCards(progs, tipo, isVol) {{
  const grid  = document.getElementById('cards-grid');
  const color = nivelColor(nivelActual);
  progs.forEach((d, i) => {{
    const card = document.createElement('div');
    card.className = 'prog-card';
    card.id = `card-${{i}}`;
    card.onclick = () => toggleDetail(i, d, isVol);
    const metric = isVol ? d.pred_2027 : d.crecimiento_pct;
    const valStr = isVol ? fmtN(metric) : fmtPct(metric);
    const arrow  = isVol ? '👥' : (metric >= 0 ? '▲' : '▼');
    const cls    = isVol ? 'vol' : (metric >= 0 ? 'pos' : 'neg');
    card.innerHTML = `
      <div class="card-rank">${{i+1}}</div>
      <div class="card-name">${{d.programa}}</div>
      <div class="card-crec ${{cls}}">${{arrow}} ${{valStr}}</div>
      <div class="card-meta">${{fmtN(d.grad_2024)}} graduados (2024) → ${{fmtN(d.pred_2027)}} (2027)</div>
      <div class="mini-chart-wrap"><canvas id="mini-${{i}}"></canvas></div>
      <span class="conf-badge conf-${{d.confianza}}">${{confEmoji(d.confianza)}} ${{d.confianza}} · MAPE ${{d.mape}}%</span>
    `;
    grid.appendChild(card);
  }});
  requestAnimationFrame(() => progs.forEach((d, i) => renderMini(i, d)));
}}
 
function renderMini(i, d) {{
  const ctx = document.getElementById(`mini-${{i}}`);
  if (!ctx) return;
  const color = nivelColor(nivelActual);
  const realData = AÑOS_REAL.map((yr,j) => ({{x:yr,y:d.hist_vals[j]??null}}));
  const predData = [
    {{x:AÑOS_REAL[AÑOS_REAL.length-1],y:d.hist_vals[d.hist_vals.length-1]}},
    ...AÑOS_PRED.map((yr,j) => ({{x:yr,y:d.pred_vals[j]}}))
  ];
  if (miniCharts[i]) miniCharts[i].destroy();
  miniCharts[i] = new Chart(ctx, {{
    type:'line',
    data:{{ datasets:[
      {{data:realData,borderColor:'#8B949E',borderWidth:1.5,pointRadius:2,tension:.3,fill:false}},
      {{data:predData,borderColor:color,borderWidth:2,borderDash:[4,3],pointRadius:2,tension:.3,fill:false}},
    ]}},
    options:{{responsive:true,maintainAspectRatio:false,animation:false,
      plugins:{{legend:{{display:false}},tooltip:{{enabled:false}}}},
      scales:{{x:{{display:false}},y:{{display:false}}}}
    }}
  }});
}}
 
// ── DETALLE ──
function toggleDetail(i, d, isVol) {{
  if (selectedCard === i) {{ closeDetail(); return; }}
  document.querySelectorAll('.prog-card').forEach(c => c.classList.remove('selected'));
  document.getElementById(`card-${{i}}`).classList.add('selected');
  selectedCard = i;
  document.getElementById('detail-title').textContent = d.programa;
  const pct = d.crecimiento_pct;
  document.getElementById('detail-stats').innerHTML = [
    {{v:fmtN(d.grad_2024),l:'Real 2024'}},
    {{v:fmtN(d.pred_2025),l:'Proy. 2025'}},
    {{v:fmtN(d.pred_2026),l:'Proy. 2026'}},
    {{v:fmtN(d.pred_2027),l:'Proy. 2027'}},
    {{v:fmtPct(pct),l:'Crec. %',cls:pct>=0?'pos':'neg'}},
    {{v:d.ic_inf_2027+' – '+d.ic_sup_2027,l:'IC 95% (2027)'}},
    {{v:d.modelo,l:'Modelo'}},
    {{v:d.mape+'%',l:'MAPE'}},
  ].map(s=>`<div class="dstat"><div class="dstat-v ${{s.cls||''}}">${{s.v}}</div>
    <div class="dstat-l">${{s.l}}</div></div>`).join('');
  renderDetailChart(d);
  document.getElementById('detail-panel').classList.add('visible');
  setTimeout(()=>document.getElementById('detail-panel')
    .scrollIntoView({{behavior:'smooth',block:'nearest'}}),50);
}}
 
function closeDetail() {{
  document.querySelectorAll('.prog-card').forEach(c=>c.classList.remove('selected'));
  document.getElementById('detail-panel').classList.remove('visible');
  selectedCard=null;
}}
 
function renderDetailChart(d) {{
  const ctx = document.getElementById('detail-chart');
  if (!ctx) return;
  const color = nivelColor(nivelActual);
  if (detailCharts['main']) detailCharts['main'].destroy();
  const realData = AÑOS_REAL.map((yr,i)=>(({{x:yr,y:d.hist_vals[i]??null}})));
  const predData = [
    {{x:AÑOS_REAL[AÑOS_REAL.length-1],y:d.hist_vals[d.hist_vals.length-1]}},
    ...AÑOS_PRED.map((yr,i)=>(({{x:yr,y:d.pred_vals[i]}})))
  ];
  const icInf = AÑOS_PRED.map((yr,i)=>(({{x:yr,y:d.ic_inf_serie[i]}})));
  const icSup = AÑOS_PRED.map((yr,i)=>(({{x:yr,y:d.ic_sup_serie[i]}})));
  detailCharts['main']=new Chart(ctx,{{
    type:'line',
    data:{{datasets:[
      {{label:'IC sup',data:icSup,borderColor:'transparent',backgroundColor:color+'22',fill:'+1',pointRadius:0,tension:.3}},
      {{label:'IC inf',data:icInf,borderColor:'transparent',backgroundColor:color+'22',fill:false,pointRadius:0,tension:.3}},
      {{label:'Real (SNIES)',data:realData,borderColor:'#8B949E',backgroundColor:'#8B949E',borderWidth:2.5,pointRadius:5,tension:.3,fill:false}},
      {{label:'Proyectado',data:predData,borderColor:color,backgroundColor:color,borderWidth:2.5,borderDash:[6,4],pointRadius:5,tension:.3,fill:false}},
    ]}},
    options:{{
      responsive:true,maintainAspectRatio:false,
      plugins:{{legend:{{labels:{{color:'#8B949E',font:{{size:11}}}}}},
        tooltip:{{mode:'index',intersect:false,callbacks:{{label:c=>` ${{c.dataset.label}}: ${{Number(c.parsed.y).toLocaleString('es-CO')}}`}}}}
      }},
      scales:{{
        x:{{type:'linear',ticks:{{color:'#8B949E',stepSize:1,callback:v=>v}},grid:{{color:'#30363D'}}}},
        y:{{ticks:{{color:'#8B949E',callback:v=>Number(v).toLocaleString('es-CO')}},grid:{{color:'#30363D'}}}}
      }}
    }}
  }});
}}
 
// ── TABLA ──
function renderTabla(nd) {{
  const el = document.getElementById('sub-content');
  const nivelLabel = nivelActual === 'pregrado' ? '🎓 Pregrado' : '🔬 Posgrado';
  el.innerHTML = `
    <div class="tabla-section">
      <div class="tabla-title-row">
        <div class="section-title" style="margin:0">📋 ${{nivelLabel}} — ${{sedeActual}}</div>
        <input class="tabla-search" type="text" placeholder="🔍 Buscar programa..."
               oninput="filtrarTabla(this.value)">
      </div>
      <table><thead><tr>
        <th onclick="sortTabla('#')">#</th>
        <th onclick="sortTabla('programa')">Programa</th>
        <th onclick="sortTabla('grad_2024')">Real 2024</th>
        <th onclick="sortTabla('pred_2027')">Proy. 2027</th>
        <th onclick="sortTabla('crecimiento_pct')">Crec. %</th>
        <th onclick="sortTabla('confianza')">Conf.</th>
        <th onclick="sortTabla('modelo')">Modelo</th>
      </tr></thead><tbody id="tbody-tabla"></tbody></table>
    </div>`;
  _fillTabla(nd.tabla || []);
}}
 
function _fillTabla(rows) {{
  const maxAbs = Math.max(...rows.map(r=>Math.abs(r.crecimiento_pct||0)),1);
  document.getElementById('tbody-tabla').innerHTML = rows.map((r,i)=>{{
    const pct=r.crecimiento_pct;
    const barW=Math.round(Math.abs(pct||0)/maxAbs*72);
    const barC=pct>=0?'#3FB950':'#F85149';
    return `<tr>
      <td style="color:var(--muted)">${{i+1}}</td>
      <td style="max-width:250px;line-height:1.3">${{r.programa}}</td>
      <td>${{fmtN(r.grad_2024)}}</td>
      <td><strong>${{fmtN(r.pred_2027)}}</strong></td>
      <td><div class="pct-bar-wrap">
        <div class="pct-bar" style="width:${{barW}}px;background:${{barC}}"></div>
        <span style="color:${{barC}};font-weight:600">${{fmtPct(pct)}}</span>
      </div></td>
      <td><span class="conf-badge conf-${{r.confianza}}">${{confEmoji(r.confianza)}} ${{r.confianza}}</span></td>
      <td style="color:var(--muted);font-size:11px">${{r.modelo}}</td>
    </tr>`;
  }}).join('');
}}
 
function filtrarTabla(q) {{
  const nd = DATOS[sedeActual][nivelActual] || {{}};
  const rows = (nd.tabla||[]).filter(r=>r.programa.toLowerCase().includes(q.toLowerCase()));
  _fillTabla(rows);
}}
 
function sortTabla(col) {{
  const nd = DATOS[sedeActual][nivelActual] || {{}};
  const rows = [...(nd.tabla||[])];
  const asc = sortState.col===col ? !sortState.asc : true;
  sortState = {{col,asc}};
  rows.sort((a,b)=>{{
    const va=a[col],vb=b[col];
    if(va==null) return 1; if(vb==null) return -1;
    if(typeof va==='string') return asc?va.localeCompare(vb):vb.localeCompare(va);
    return asc?va-vb:vb-va;
  }});
  _fillTabla(rows);
}}
 
(function init() {{
  switchSede(SEDES[0]);
}})();
</script>
</body>
</html>"""
 
    html_path = f"{out_dir}/dashboard_interactivo.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  ✓ Dashboard guardado: {html_path}")
    return html_path
 
 
# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
 
def main():
    if not os.path.exists(RUTA_EXCEL):
        raise FileNotFoundError(
            f"\n❌ No se encontró el archivo:\n   {os.path.abspath(RUTA_EXCEL)}\n"
            f"Ajusta la variable RUTA_EXCEL al inicio del script.")
 
    mk(OUTPUT_DIR)
    USTA = cargar_datos(RUTA_EXCEL)
 
    print(f"\n{SEP}\nANÁLISIS POR SEDE\n{SEP}")
    resultados_sedes = {}
    for cod, nombre in SEDES.items():
        df_sede = USTA[USTA["codigo_institucion"] == cod].copy()
        if df_sede.empty:
            print(f"  ⚠ Sin datos para {nombre} (cod {cod})")
            continue
        out_sede = mk(f"{OUTPUT_DIR}/{nombre.lower()}")
        resultado = analizar_sede(df_sede, nombre, out_sede)
        resultados_sedes[nombre] = resultado
 
    out_nac = mk(f"{OUTPUT_DIR}/nacional")
    resultado_nac = consolidar_nacional(resultados_sedes, out_nac)
    generar_html(resultados_sedes, resultado_nac, OUTPUT_DIR)
 
    print(f"\n{SEP}")
    print(f"✓ ANÁLISIS COMPLETO\n{SEP}")
    print(f"""
ARCHIVOS GENERADOS en: {OUTPUT_DIR}/
  ├── bogotá/
  │   ├── pred_total_anual.csv / .png
  │   ├── pred_semestre.csv    / .png
  │   ├── pregrado/
  │   │   ├── pred_programas_alta_confianza.csv / .png
  │   │   ├── pred_programas_media_confianza.csv / .png
  │   │   ├── reporte_modelos.csv
  │   │   ├── top5_mayor_crecimiento.csv
  │   │   ├── top5_menor_crecimiento.csv
  │   │   ├── top5_mas_graduados.csv
  │   │   ├── top5_menos_graduados.csv
  │   │   └── top5_programas.png
  │   └── posgrado/  (misma estructura)
  ├── bucaramanga/   (misma estructura)
  ├── tunja/         (misma estructura)
  ├── nacional/
  │   ├── pred_total_anual.csv
  │   ├── pred_semestre.csv
  │   ├── pregrado/  (misma estructura)
  │   ├── posgrado/  (misma estructura)
  │   ├── reporte_modelos_pregrado.csv
  │   ├── reporte_modelos_posgrado.csv
  │   └── comparativa_sedes.png
  └── dashboard_interactivo.html
""")

if __name__ == "__main__":
    main()