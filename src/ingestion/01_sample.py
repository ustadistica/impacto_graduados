"""
src/ingestion/01_sample.py
==========================
Extrae una muestra aleatoria de SAMPLE_SIZE registros del CSV original
usando DuckDB (eficiente para archivos de varios GB) y la guarda como
Parquet en data/samples/.

Uso:
    python src/ingestion/01_sample.py
"""

import sys
import logging
from pathlib import Path

# ── Importaciones del proyecto ────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.settings import (
    CSV_PATH, SAMPLE_PATH, SAMPLE_SIZE, RANDOM_SEED,
    COL_FECHA_MAT, COL_FECHA_CANCEL, YEAR_MIN, YEAR_MAX
)

import duckdb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)


def build_sample() -> pd.DataFrame:
    """Lee el CSV con DuckDB y extrae la muestra."""

    if not CSV_PATH.exists():
        log.error(f"Archivo no encontrado: {CSV_PATH}")
        log.error("Coloca el CSV original en data/raw/ con el nombre exacto indicado en config/settings.py")
        sys.exit(1)

    log.info(f"Leyendo muestra de {SAMPLE_SIZE:,} registros desde: {CSV_PATH.name}")

    # DuckDB infiere el esquema automáticamente
    query = f"""
        SELECT *
        FROM read_csv_auto(
            '{CSV_PATH}',
            ignore_errors = true,
            sample_size   = 50000
        )
        USING SAMPLE {SAMPLE_SIZE} ROWS (bernoulli, {RANDOM_SEED})
    """

    con = duckdb.connect()
    df  = con.execute(query).df()
    con.close()

    log.info(f"Registros obtenidos: {len(df):,}  |  Columnas: {len(df.columns)}")
    return df


def clean_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parsea fechas y extrae año de matrícula."""
    for col in [COL_FECHA_MAT, COL_FECHA_CANCEL]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    if COL_FECHA_MAT in df.columns:
        df["anio_matricula"] = df[COL_FECHA_MAT].dt.year
        # Filtrar rango de años de interés (conservar NaN para no perder registros sin fecha)
        mask = df["anio_matricula"].between(YEAR_MIN, YEAR_MAX) | df["anio_matricula"].isna()
        before = len(df)
        df = df[mask].copy()
        log.info(f"Registros fuera del rango {YEAR_MIN}-{YEAR_MAX} eliminados: {before - len(df):,}")

    return df


def normalize_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza columnas de texto: mayúsculas, sin espacios extras."""
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip().str.upper()
        df[col] = df[col].replace("NAN", pd.NA)
    return df


def main():
    df = build_sample()
    df = clean_dates(df)
    df = normalize_strings(df)

    SAMPLE_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(SAMPLE_PATH, index=False)
    log.info(f"Muestra guardada en: {SAMPLE_PATH}")

    # Vista previa
    print("\n── Primeras filas ──────────────────────────────────────────")
    print(df.head(3).to_string())
    print(f"\n── Columnas disponibles ({len(df.columns)}) ──────────────────────")
    print(list(df.columns))


if __name__ == "__main__":
    main()
