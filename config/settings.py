"""
config/settings.py
==================
Configuración global del proyecto RUES.
Todas las rutas, constantes y parámetros se definen aquí para que
el resto de los scripts los importen, sin repetir valores.
"""

from pathlib import Path

# ── Raíz del proyecto ────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent

# ── Directorios ──────────────────────────────────────────────────────────────
DATA_RAW       = ROOT / "data" / "raw"
DATA_SAMPLES   = ROOT / "data" / "samples"
DATA_PROCESSED = ROOT / "data" / "processed"
OUTPUTS_TABLES = ROOT / "outputs" / "tables"
OUTPUTS_FIGS   = ROOT / "outputs" / "figures"
OUTPUTS_REP    = ROOT / "outputs" / "reports"

for _d in [DATA_RAW, DATA_SAMPLES, DATA_PROCESSED,
           OUTPUTS_TABLES, OUTPUTS_FIGS, OUTPUTS_REP]:
    _d.mkdir(parents=True, exist_ok=True)

# ── Archivo fuente ────────────────────────────────────────────────────────────
CSV_FILENAME = (
    "Personas_Naturales,_Personas_Jurídicas_y_Entidades_Sin_Animo_de_Lucro_20260325.csv"
)
CSV_PATH  = DATA_RAW / CSV_FILENAME
SAMPLE_PATH = DATA_SAMPLES / "muestra_10000.parquet"

# ── Parámetros de muestreo ───────────────────────────────────────────────────
SAMPLE_SIZE   = 10_000
RANDOM_SEED   = 42

# ── Columnas clave (nombres tal como vienen en el CSV) ───────────────────────
COL_CAMARA        = "camara_comercio"
COL_MATRICULA     = "matricula"
COL_RAZON_SOCIAL  = "razon_social"
COL_IDENTIFICACION= "clase_identificacion"
COL_NIT           = "nit"
COL_CIIU_PRI      = "cod_ciiu_act_econ_pri"
COL_CIIU_SEC      = "cod_ciiu_act_econ_sec"
COL_FECHA_MAT     = "fecha_matricula"
COL_FECHA_CANCEL  = "fecha_cancelacion"
COL_ESTADO        = "estado_matricula"
COL_ORG_JURIDICA  = "organizacion_juridica"
COL_TIPO_SOCIEDAD = "tipo_sociedad"
COL_CATEGORIA     = "categoria_matricula"

# ── Sedes USTA (para filtros opcionales) ─────────────────────────────────────
CAMARAS_USTA = [
    "BOGOTA", "BUCARAMANGA", "TUNJA", "MEDELLIN PARA ANTIOQUIA",
    "VILLAVICENCIO", "MANIZALES", "CUCUTA", "IBAGUE", "HUILA",
    "PASTO", "PEREIRA", "BARRANQUILLA", "CARTAGENA", "ARMENIA",
    "SOGAMOSO", "CALI", "SANTA MARTA PARA EL MAGDALENA",
]

# ── Rango de años de análisis ────────────────────────────────────────────────
YEAR_MIN = 2000
YEAR_MAX = 2024

# ── Estilo visual ────────────────────────────────────────────────────────────
PLOT_COLOR_PRIMARY   = "#1B6CA8"
PLOT_COLOR_SECONDARY = "#F4A223"
PLOT_COLOR_SCALE     = "Blues"
PLOT_TEMPLATE        = "plotly_white"
PLOT_FONT_FAMILY     = "Inter, Arial, sans-serif"
