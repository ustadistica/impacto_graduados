"""
src/00b_ingesta.py
==================
PASO 2 — Limpia y valida la muestra de 10 000 registros.
Opera sobre outputs/tables/muestra_rues_10000.csv (ya en RAM sin problema).

Salidas:
  outputs/tables/muestra_limpia.csv
  outputs/reports/ingesta_calidad.json
  outputs/reports/ingesta_log.txt
"""

import sys, re, json, logging
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import numpy as np

from config.settings import (
    SAMPLE_PATH, DIR_TABLES, DIR_REPORTS,
    COL_ID, COL_CAMARA, COL_CIIU, COL_FECHA_MAT,
    COL_ORG_JUR, COL_ESTADO, COL_LAST_YEAR, COL_RAZON, COL_TIPO_ID,
)

# ── Logger ─────────────────────────────────────────────────────────────────────
DIR_REPORTS.mkdir(parents=True, exist_ok=True)
DIR_TABLES.mkdir(parents=True, exist_ok=True)

LOG_PATH = DIR_REPORTS / "ingesta_log.txt"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8", mode="w"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("ingesta")

QA = {
    "timestamp": datetime.now().isoformat(),
    "archivo_origen": str(SAMPLE_PATH),
    "correcciones": {
        "estado_otro": 0, "ciiu_invalido": 0,
        "fecha_nula": 0, "fecha_futura": 0,
        "renovacion_incoherente": 0,
        "dup_exactos": 0, "dup_por_id": 0,
    },
}

# =============================================================================
# MAPEO DE COLUMNAS
# =============================================================================

VARIANTES = {
    COL_ID:        ["NUMERO_IDENTIFICACION","NUM_IDENTIFICACION","NIT","CEDULA",
                    "NUMERO_DE_IDENTIFICACION","ID","IDENTIFICACION",
                    "NUMERO_MATRICULA","MATRICULA","NUM_MATRICULA"],
    COL_CAMARA:    ["CAMARA_COMERCIO","CAMARA","CAMARA_DE_COMERCIO",
                    "SEDE_CAMARA","CIUDAD_CAMARA","NOMBRE_CAMARA",
                    "CAMARA_DE_COMERCIO_Y_DESARROLLO_REGIONAL"],
    COL_CIIU:      ["CIIU_PRINCIPAL","CIIU","CODIGO_CIIU","ACTIVIDAD_ECONOMICA",
                    "CIIU_PRIMARIO","ACT_ECONOMICA","CODIGO_ACTIVIDAD_ECONOMICA",
                    "ACTIVIDAD_PRINCIPAL","COD_CIIU","COD_CIIU_ACT_ECON_PRI",
                    "COD_CIIU_ACT_ECON_SEC"],
    COL_FECHA_MAT: ["FECHA_MATRICULA","FECHA_DE_MATRICULA","FECHA_REGISTRO",
                    "FEC_MATRICULA","FECHA_INSCRIPCION","FECHA_APERTURA"],
    COL_ORG_JUR:   ["ORGANIZACION_JURIDICA","ORG_JURIDICA","TIPO_SOCIEDAD",
                    "CLASIFICACION_JURIDICA","TIPO_ORGANIZACION",
                    "TIPO_DE_ORGANIZACION_JURIDICA","TIPO_EMPRESA"],
    COL_ESTADO:    ["ESTADO_MATRICULA","ESTADO","STATUS","ESTADO_REGISTRO",
                    "ESTADO_DE_LA_MATRICULA"],
    COL_LAST_YEAR: ["ULTIMO_ANO_RENOVADO","ANO_RENOVACION","AÑO_ULTIMO_RENOVADO",
                    "ULTIMO_AO_RENOVADO","ULTIMO_ANO_RENOVACION"],
    COL_RAZON:     ["RAZON_SOCIAL","NOMBRE","RAZON","NOMBRE_EMPRESA",
                    "NOMBRE_RAZON_SOCIAL"],
    COL_TIPO_ID:   ["TIPO_IDENTIFICACION","TIPO_ID","TIPO_DE_IDENTIFICACION",
                    "TIPO_DOCUMENTO","CLASE_IDENTIFICACION","CLASE_ID"],
}

def norm_header(c):
    return re.sub(r"[^\w]", "_",
        str(c).strip().upper()
               .encode("ascii", errors="ignore").decode()
               .replace(" ", "_")
    ).strip("_")

def mapear_columnas(df):
    df.columns = [norm_header(c) for c in df.columns]
    log.info(f"Columnas raw normalizadas: {list(df.columns)}")

    # Si ya existe COL_ORG_JUR en el CSV Y también TIPO_SOCIEDAD,
    # eliminar TIPO_SOCIEDAD para evitar columnas duplicadas al renombrar
    if COL_ORG_JUR in df.columns:
        for alias in ["TIPO_SOCIEDAD", "ORG_JURIDICA", "CLASIFICACION_JURIDICA"]:
            if alias in df.columns and alias != COL_ORG_JUR:
                df = df.drop(columns=[alias])
                log.info(f"Columna duplicada eliminada: '{alias}' (ya existe '{COL_ORG_JUR}')")

    rename = {}
    no_enc = []
    for canon, variantes in VARIANTES.items():
        if canon in df.columns:
            continue  # ya existe con el nombre canónico
        found = next((c for c in df.columns if c in variantes), None)
        if found:
            rename[found] = canon
        else:
            no_enc.append(canon)

    if rename:
        df = df.rename(columns=rename)
        log.info(f"Renombres aplicados: {rename}")
    if no_enc:
        log.warning(f"Columnas NO encontradas (se omiten): {no_enc}")
    return df

# =============================================================================
# TABLAS DE MAPEO
# =============================================================================

ESTADO_MAP = {
    "ACTIVA":"ACTIVA","ACTIVE":"ACTIVA","VIGENTE":"ACTIVA","INSCRITA":"ACTIVA",
    "EN_VIGENCIA":"ACTIVA",
    "CANCELADA":"CANCELADA","CANCELLED":"CANCELADA","CANCELADO":"CANCELADA",
    "INACTIVA":"CANCELADA","BAJA":"CANCELADA","REVOCADA":"CANCELADA","LIQUIDADA":"CANCELADA",
}

CAMARA_NORM = {
    "BOGOTA":"Bogotá","BOGOTÁ":"Bogotá","BOGOTA D.C":"Bogotá","BOGOTÁ D.C.":"Bogotá",
    "BOGOTA D.C.":"Bogotá","CCB":"Bogotá","CAMARA DE COMERCIO DE BOGOTA":"Bogotá",
    "MEDELLIN":"Medellín","MEDELLÍN":"Medellín",
    "CALI":"Cali","BUCARAMANGA":"Bucaramanga","BARRANQUILLA":"Barranquilla",
    "CARTAGENA":"Cartagena","MANIZALES":"Manizales","PEREIRA":"Pereira",
    "CUCUTA":"Cúcuta","CÚCUTA":"Cúcuta","IBAGUE":"Ibagué","IBAGUÉ":"Ibagué",
    "TUNJA":"Tunja","PASTO":"Pasto","VILLAVICENCIO":"Villavicencio",
    "ARMENIA":"Armenia","NEIVA":"Neiva","SANTA MARTA":"Santa Marta",
}

ORG_JUR_MAP = {
    "PERSONA NATURAL":"Persona Natural","PERSONA NATURAL COMERCIANTE":"Persona Natural",
    "P.NATURAL":"Persona Natural","NATURAL":"Persona Natural",
    "SOCIEDAD POR ACCIONES SIMPLIFICADA":"SAS","S.A.S":"SAS","S.A.S.":"SAS","SAS":"SAS",
    "SOCIEDAD ANONIMA":"S.A.","S.A":"S.A.","SA":"S.A.",
    "SOCIEDAD DE RESPONSABILIDAD LIMITADA":"Ltda.","RESPONSABILIDAD LIMITADA":"Ltda.",
    "LTDA":"Ltda.","LTDA.":"Ltda.",
    "ENTIDAD SIN ANIMO DE LUCRO":"ESAL","ESAL":"ESAL","SIN ANIMO DE LUCRO":"ESAL",
    "FUNDACION":"ESAL","ASOCIACION":"ESAL","CORPORACION":"ESAL","COOPERATIVA":"ESAL",
    "EMPRESA UNIPERSONAL":"Empresa Unipersonal","EU":"Empresa Unipersonal","E.U":"Empresa Unipersonal",
    "SOCIEDAD EN NOMBRE COLECTIVO":"SNC",
    "SUCURSAL DE SOCIEDAD EXTRANJERA":"Sucursal Extranjera",
}

FORMATOS_FECHA = ["%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%Y/%m/%d","%d.%m.%Y","%Y%m%d"]

# =============================================================================
# LIMPIEZA
# =============================================================================

def limpiar_estado(df):
    if COL_ESTADO not in df.columns: return df
    raw = df[COL_ESTADO].astype(str).str.strip().str.upper().str.replace(r"\s+","_",regex=True)
    df[COL_ESTADO] = raw.map(ESTADO_MAP).fillna("OTRO")
    n = int((df[COL_ESTADO]=="OTRO").sum())
    QA["correcciones"]["estado_otro"] += n
    if n: log.warning(f"Estado 'OTRO': {n} registros")
    log.info(f"Estados: {df[COL_ESTADO].value_counts().to_dict()}")
    return df

def limpiar_camara(df):
    if COL_CAMARA not in df.columns: return df
    def norm(val):
        if pd.isna(val): return "Sin información"
        v = str(val).strip().upper()
        if v in CAMARA_NORM: return CAMARA_NORM[v]
        for key, n in CAMARA_NORM.items():
            if key in v: return n
        return str(val).strip().title()
    df[COL_CAMARA] = df[COL_CAMARA].apply(norm)
    log.info(f"Cámaras únicas: {df[COL_CAMARA].nunique()}")
    return df

def limpiar_ciiu(df):
    if COL_CIIU not in df.columns:
        log.warning(f"'{COL_CIIU}' no encontrado. Columnas disponibles: {list(df.columns)}")
        return df
    df[COL_CIIU] = df[COL_CIIU].astype(str).str.strip().str.upper().str.replace(r"[^A-Z0-9]","",regex=True)
    mask = df[COL_CIIU].isna()|(df[COL_CIIU]=="NAN")|(df[COL_CIIU]=="")|(df[COL_CIIU].str.len()<2)
    QA["correcciones"]["ciiu_invalido"] += int(mask.sum())
    df.loc[mask, COL_CIIU] = np.nan
    log.info(f"CIIU inválidos: {mask.sum()} | únicos válidos: {df[COL_CIIU].nunique()}")
    return df

def limpiar_fechas(df):
    if COL_FECHA_MAT not in df.columns:
        log.warning(f"'{COL_FECHA_MAT}' no encontrado.")
        return df

    serie = df[COL_FECHA_MAT].astype(str).str.strip()

    # Intentar formatos explícitos primero para evitar warnings y OutOfBounds
    parsed = pd.Series(pd.NaT, index=df.index, dtype="datetime64[us]")
    for fmt in FORMATOS_FECHA:
        mask = parsed.isna() & (serie != "nan") & (serie != "") & serie.notna()
        if not mask.any(): break
        tmp = pd.to_datetime(serie[mask], format=fmt, errors="coerce")
        parsed[mask] = tmp.values

    # Último intento genérico para los que quedan
    mask = parsed.isna() & (serie != "nan") & (serie != "") & serie.notna()
    if mask.any():
        tmp = pd.to_datetime(serie[mask], errors="coerce", dayfirst=False)
        parsed[mask] = tmp.values

    df[COL_FECHA_MAT] = parsed
    df["ANIO_MATRICULA"] = df[COL_FECHA_MAT].dt.year

    anio_actual = datetime.now().year
    mask_inv = df["ANIO_MATRICULA"].notna() & (
        (df["ANIO_MATRICULA"] < 1900) | (df["ANIO_MATRICULA"] > anio_actual)
    )
    QA["correcciones"]["fecha_futura"] += int(mask_inv.sum())
    df.loc[mask_inv, ["ANIO_MATRICULA", COL_FECHA_MAT]] = np.nan

    QA["correcciones"]["fecha_nula"] += int(df["ANIO_MATRICULA"].isna().sum())
    log.info(f"Años: {df['ANIO_MATRICULA'].min()} – {df['ANIO_MATRICULA'].max()} | nulos: {df['ANIO_MATRICULA'].isna().sum()}")
    return df

def limpiar_org_juridica(df):
    if COL_ORG_JUR not in df.columns: return df
    # Asegurar que es Serie (no DataFrame por columnas duplicadas)
    col = df[COL_ORG_JUR]
    if isinstance(col, pd.DataFrame):
        log.warning("COL_ORG_JUR duplicada en el DataFrame — usando primera columna")
        col = col.iloc[:, 0]
        df[COL_ORG_JUR] = col
    raw = col.astype(str).str.strip().str.upper().str.encode("ascii", errors="ignore").str.decode("ascii")
    df[COL_ORG_JUR] = raw.map(ORG_JUR_MAP).fillna(col.astype(str).str.strip().str.title())
    log.info(f"Org. jurídica top5: {df[COL_ORG_JUR].value_counts().head(5).to_dict()}")
    return df

def validar_coherencia(df):
    if COL_LAST_YEAR in df.columns and "ANIO_MATRICULA" in df.columns:
        df[COL_LAST_YEAR] = pd.to_numeric(df[COL_LAST_YEAR], errors="coerce")
        mask = (df[COL_LAST_YEAR].notna() & df["ANIO_MATRICULA"].notna() &
                (df[COL_LAST_YEAR] < df["ANIO_MATRICULA"]))
        QA["correcciones"]["renovacion_incoherente"] += int(mask.sum())
        df.loc[mask, COL_LAST_YEAR] = np.nan
        if mask.sum(): log.warning(f"Renovación < matrícula: {mask.sum()} corregidos")
    return df

def deduplicar(df):
    n = len(df)
    df = df.drop_duplicates()
    QA["correcciones"]["dup_exactos"] = n - len(df)
    if COL_ID in df.columns:
        df[COL_ID] = df[COL_ID].astype(str).str.strip()
        n2 = len(df)
        df = df.drop_duplicates(subset=[COL_ID], keep="first")
        QA["correcciones"]["dup_por_id"] = n2 - len(df)
    log.info(f"Deduplicación: -{QA['correcciones']['dup_exactos']} exactos, "
             f"-{QA['correcciones']['dup_por_id']} por ID → {len(df):,} registros")
    return df

# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    log.info("=" * 55)
    log.info("  RUES — Limpieza de muestra")
    log.info("=" * 55)

    if not SAMPLE_PATH.exists():
        log.error(f"Muestra no encontrada: {SAMPLE_PATH}")
        log.error("Ejecuta primero: python src/00_load_sample.py")
        sys.exit(1)

    df = pd.read_csv(SAMPLE_PATH, low_memory=False, dtype=str)
    log.info(f"Muestra cargada: {len(df):,} filas | {df.shape[1]} columnas")

    df = mapear_columnas(df)
    df = limpiar_estado(df)
    df = limpiar_camara(df)
    df = limpiar_ciiu(df)
    df = limpiar_fechas(df)
    df = limpiar_org_juridica(df)
    df = validar_coherencia(df)
    df = deduplicar(df)

    # Reporte de calidad
    nulos = df.isnull().sum()
    pct   = (nulos / len(df) * 100).round(1)
    log.info("")
    log.info("  Nulos por columna clave:")
    for col in [COL_ID, COL_CAMARA, COL_CIIU, COL_FECHA_MAT, COL_ESTADO, COL_ORG_JUR]:
        if col in nulos.index:
            log.info(f"    {col:<35} {int(nulos[col]):>6,}  ({pct[col]}%)")

    QA["filas_finales"] = len(df)
    QA["nulos_pct"] = {c: float(pct[c]) for c in pct.index}

    OUT = DIR_TABLES / "muestra_limpia.csv"
    df.to_csv(OUT, index=False, encoding="utf-8")
    log.info(f"\n✅  Muestra limpia: {OUT}  ({len(df):,} registros)")

    with open(DIR_REPORTS / "ingesta_calidad.json", "w", encoding="utf-8") as f:
        json.dump(QA, f, ensure_ascii=False, indent=2, default=str)
    log.info(f"✅  Reporte: {DIR_REPORTS / 'ingesta_calidad.json'}")
    log.info(f"✅  Log    : {LOG_PATH}")
    log.info("")
    log.info("Siguiente paso → actualiza SAMPLE_PATH en config/settings.py:")
    log.info("  SAMPLE_PATH = DIR_TABLES / 'muestra_limpia.csv'")
    log.info("Luego corre los módulos de análisis (01_ al 06_)")