"""
SCRIPT 3 - VALIDACIÓN DE DATOS
================================
Valida schema, nulos, rangos y duplicados.
Genera artefacto: validation_report.json
"""

import pandas as pd
import json
import os
from pathlib import Path

# ─────────────────────────────────────────────
BASE        = Path(__file__).parent
RUTA_CSV    = BASE / "output" / "consolidado_2018_2024.csv"
RUTA_REPORT = BASE / "artifacts" / "validation_report.json"

RUTA_REPORT.parent.mkdir(parents=True, exist_ok=True)
# ─────────────────────────────────────────────

# Umbrales configurables
UMBRAL_NULOS   = 0.30   # máximo 30% de nulos por columna
RANGO_PUNTAJES = (0, 300)
PERIODOS_OK    = list(range(20181, 20252))

print("Cargando datos...")
df = pd.read_csv(RUTA_CSV, low_memory=False)
print(f"Filas: {len(df):,} | Columnas: {len(df.columns)}")

errores  = []
warnings = []
ok       = []

# ── 1. Schema ───────────────────────────────
COLS_REQUERIDAS = [
    'periodo', 'inst_nombre_institucion', 'es_usta',
    'punt_global', 'mod_lectura_critica_punt',
    'mod_razona_cuantitat_punt', 'mod_comuni_escrita_punt',
    'mod_ingles_punt', 'mod_competen_ciudada_punt',
]
for col in COLS_REQUERIDAS:
    if col not in df.columns:
        errores.append(f"SCHEMA: Columna requerida ausente -> '{col}'")
    else:
        ok.append(f"SCHEMA OK: '{col}' presente")

# ── 2. Nulos ────────────────────────────────
for col in df.columns:
    pct = df[col].isnull().mean()
    if pct > UMBRAL_NULOS:
        warnings.append(f"NULOS: '{col}' tiene {pct:.1%} de nulos (umbral: {UMBRAL_NULOS:.0%})")
    else:
        ok.append(f"NULOS OK: '{col}' -> {pct:.1%}")

# ── 3. Rangos de puntajes ───────────────────
COLS_PUNTAJE = [c for c in df.columns if '_punt' in c]
for col in COLS_PUNTAJE:
    fuera = df[col].dropna()
    fuera = fuera[(fuera < RANGO_PUNTAJES[0]) | (fuera > RANGO_PUNTAJES[1])]
    if len(fuera) > 0:
        errores.append(f"RANGO: '{col}' tiene {len(fuera):,} valores fuera de {RANGO_PUNTAJES}")
    else:
        ok.append(f"RANGO OK: '{col}' dentro de {RANGO_PUNTAJES}")

# ── 4. Periodos válidos ─────────────────────
periodos_invalidos = df[~df['periodo'].isin(PERIODOS_OK)]['periodo'].unique()
if len(periodos_invalidos) > 0:
    warnings.append(f"PERIODO: Periodos no esperados -> {list(periodos_invalidos)}")
else:
    ok.append("PERIODO OK: Todos los periodos son válidos")

# ── 5. Duplicados ───────────────────────────
n_dup = df.duplicated().sum()
if n_dup > 0:
    warnings.append(f"DUPLICADOS: {n_dup:,} filas duplicadas encontradas")
else:
    ok.append("DUPLICADOS OK: Sin filas duplicadas")

# ── 6. Integridad USTA ──────────────────────
n_usta = df['es_usta'].sum()
if n_usta == 0:
    errores.append("INTEGRIDAD: No hay registros USTA en el dataset")
else:
    ok.append(f"INTEGRIDAD OK: {n_usta:,} registros USTA")

# ── Reporte ─────────────────────────────────
reporte = {
    'total_filas':    int(len(df)),
    'total_columnas': int(len(df.columns)),
    'registros_usta': int(df['es_usta'].sum()),
    'periodos':       sorted(df['periodo'].unique().tolist()),
    'errores':        errores,
    'warnings':       warnings,
    'ok':             ok,
    'resultado':      'FALLO' if errores else ('ADVERTENCIA' if warnings else 'PASADO'),
}

with open(RUTA_REPORT, 'w', encoding='utf-8') as f:
    json.dump(reporte, f, ensure_ascii=False, indent=2)

print(f"\n{'='*50}")
print(f"RESULTADO: {reporte['resultado']}")
print(f"  Errores:      {len(errores)}")
print(f"  Advertencias: {len(warnings)}")
print(f"  Checks OK:    {len(ok)}")
print(f"{'='*50}")
if errores:
    print("\nERRORES:")
    for e in errores: print(f"  ✗ {e}")
if warnings:
    print("\nADVERTENCIAS:")
    for w in warnings: print(f"  ⚠ {w}")

print(f"\nReporte guardado: {RUTA_REPORT}")
print("Script 3 completado!")