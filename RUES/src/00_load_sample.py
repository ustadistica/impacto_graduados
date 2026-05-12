"""
src/00_load_sample.py
=====================
PASO 1 — Extrae una muestra aleatoria de 10 000 registros del CSV raw del RUES.
No hace ninguna limpieza: eso lo hace 00b_ingesta.py sobre la muestra.

Salidas:
  outputs/tables/muestra_rues_10000.csv
  outputs/reports/00_resumen_muestra.json
"""

import sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import numpy as np

from config.settings import (
    RUTA_CSV, SAMPLE_N, RANDOM_SEED,
    SAMPLE_PATH, DIR_TABLES, DIR_FIGURES, DIR_REPORTS,
)

for d in [DIR_TABLES, DIR_FIGURES, DIR_REPORTS]:
    d.mkdir(parents=True, exist_ok=True)

def detectar_encoding(path):
    for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            with open(path, encoding=enc, errors="strict") as f:
                f.read(50_000)
            return enc
        except (UnicodeDecodeError, LookupError):
            continue
    return "latin-1"

def detectar_separador(path, enc):
    with open(path, encoding=enc, errors="replace") as f:
        muestra = "".join(f.readline() for _ in range(5))
    conteos = {s: muestra.count(s) for s in (";", ",", "\t", "|")}
    return max(conteos, key=conteos.get)

def contar_lineas(path, enc):
    with open(path, encoding=enc, errors="replace") as f:
        return sum(1 for _ in f) - 1  # -1 cabecera

if __name__ == "__main__":
    print("=" * 55)
    print("  RUES — Muestreo aleatorio")
    print("=" * 55)

    if not RUTA_CSV.exists():
        print(f"\n❌  CSV no encontrado: {RUTA_CSV}")
        print("Edita config/settings.py → RUTA_CSV")
        sys.exit(1)

    enc = detectar_encoding(RUTA_CSV)
    sep = detectar_separador(RUTA_CSV, enc)
    total = contar_lineas(RUTA_CSV, enc)

    print(f"  Encoding   : {enc}")
    print(f"  Separador  : '{sep}'")
    print(f"  Total filas: {total:,}")
    print(f"  Muestra    : {SAMPLE_N:,}")

    # Índices a omitir (excluye la cabecera en índice 0)
    rng = np.random.default_rng(RANDOM_SEED)
    todos = np.arange(1, total + 1)
    elegidos = set(rng.choice(todos, size=min(SAMPLE_N, total), replace=False))
    omitir = set(todos) - elegidos

    df = pd.read_csv(
        RUTA_CSV,
        encoding=enc,
        sep=sep,
        skiprows=omitir,
        on_bad_lines="skip",
        low_memory=False,
        dtype=str,          # todo como string, la limpieza va después
    )

    print(f"  Registros cargados: {len(df):,}")
    print(f"  Columnas          : {list(df.columns)}")

    df.to_csv(SAMPLE_PATH, index=False, encoding="utf-8")
    print(f"\n✅  Muestra guardada: {SAMPLE_PATH}")

    with open(DIR_REPORTS / "00_resumen_muestra.json", "w", encoding="utf-8") as f:
        json.dump({
            "total_raw": total,
            "muestra": len(df),
            "columnas": list(df.columns),
            "seed": RANDOM_SEED,
        }, f, ensure_ascii=False, indent=2)

    print("Siguiente paso → python src/00b_ingesta.py")