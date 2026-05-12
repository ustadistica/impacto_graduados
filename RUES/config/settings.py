"""
config/settings.py
==================
Parámetros globales del proyecto. Edita RUTA_CSV antes de ejecutar.
"""

from pathlib import Path

# ── Raíz del proyecto ──────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent

# ── Ruta al CSV del RUES ───────────────────────────────────────────────────────
# Cambia esta ruta al lugar donde tengas el archivo en tu máquina:
RUTA_CSV = ROOT / "data" / "Personas_Naturales,_Personas_Jurídicas_y_Entidades_Sin_Animo_de_Lucro_20260510.csv"

# ── Muestra ────────────────────────────────────────────────────────────────────
SAMPLE_N   = 10_000
RANDOM_SEED = 42

# ── Salidas ────────────────────────────────────────────────────────────────────
DIR_FIGURES = ROOT / "outputs" / "figures"
DIR_TABLES  = ROOT / "outputs" / "tables"
DIR_REPORTS = ROOT / "outputs" / "reports"
SAMPLE_PATH = ROOT / "outputs" / "tables" / "muestra_limpia.csv"

# ── Columnas del CSV ───────────────────────────────────────────────────────────
# Ajusta los nombres si difieren en tu versión del archivo.
COL_ID          = "NUMERO_IDENTIFICACION"
COL_CAMARA      = "CAMARA_COMERCIO"
COL_CIIU        = "CIIU_PRINCIPAL"
COL_FECHA_MAT   = "FECHA_MATRICULA"
COL_ORG_JUR     = "ORGANIZACION_JURIDICA"
COL_ESTADO      = "ESTADO_MATRICULA"
COL_LAST_YEAR   = "ULTIMO_ANO_RENOVADO"
COL_RAZON       = "RAZON_SOCIAL"
COL_TIPO_ID     = "TIPO_IDENTIFICACION"

# ── Estilo de gráficas ─────────────────────────────────────────────────────────
PALETTE_MAIN   = "#028090"   # teal
PALETTE_SEC    = "#02C39A"   # mint
PALETTE_ACCENT = "#F18F01"   # naranja
PALETTE_NEG    = "#C62828"   # rojo canceladas
PALETTE_POS    = "#2E7D32"   # verde activas
FIG_DPI        = 150
FIG_SIZE_WIDE  = (12, 5)
FIG_SIZE_SQ    = (8, 6)

# ── Año de corte para análisis temporal ───────────────────────────────────────
YEAR_MIN = 2000
YEAR_MAX = 2024