"""
SCRIPT 1 - CONSOLIDAR BASES SABER PRO 2018-2024
================================================
Une todos los archivos Saber_pro_YYYY.xlsx en un solo CSV.
"""
 
import pandas as pd
import glob
import os
from pathlib import Path
 
# ─────────────────────────────────────────────
BASE        = Path(__file__).parent          # carpeta raíz del repo
CARPETA     = BASE / "data" / "raw" / "saber_pro"
RUTA_SALIDA = BASE / "src" / "union_bases" / "consolidado_2018_2024.csv"
 
RUTA_SALIDA.parent.mkdir(parents=True, exist_ok=True)
# ─────────────────────────────────────────────
 
# Columnas relevantes para el análisis
COLUMNAS = [
    'periodo', 'inst_nombre_institucion',
    'estu_prgm_academico', 'estu_snies_prgmacademico',
    'estu_nivel_prgm_academico', 'estu_metodo_prgm',
    'estu_inst_departamento', 'estu_inst_municipio',
    'punt_global', 'percentil_global', 'percentil_nbc',
    'mod_lectura_critica_punt', 'mod_lectura_critica_desem',
    'mod_razona_cuantitat_punt', 'mod_razona_cuantitat_desem',
    'mod_comuni_escrita_punt', 'mod_comuni_escrita_desem',
    'mod_ingles_punt', 'mod_ingles_desem',
    'mod_competen_ciudada_punt', 'mod_competen_ciudada_desem',
    'estu_genero', 'fami_estratovivienda',
    'fami_educacionpadre', 'fami_educacionmadre',
    'fami_tieneinternet', 'fami_tienecomputador',
    'estu_horassemanatrabaja',
]
 
archivos = sorted(glob.glob(str(CARPETA / "Saber_pro_20*.xlsx")))
print(f"Archivos encontrados: {len(archivos)}")
for a in archivos:
    print(f"  - {os.path.basename(a)}")
 
if len(archivos) == 0:
    print("\nERROR: No se encontraron archivos. Verifica la CARPETA y que los archivos")
    print("se llamen exactamente Saber_pro_2018.xlsx, Saber_pro_2019.xlsx, etc.")
    exit()
 
dfs = []
for archivo in archivos:
    nombre = os.path.basename(archivo)
    print(f"\nLeyendo: {nombre}...")
    df = pd.read_excel(archivo)
 
    # Seleccionar solo columnas disponibles
    cols = [c for c in COLUMNAS if c in df.columns]
    df = df[cols]
 
    # Marcar si es USTA o no
    df['es_usta'] = df['inst_nombre_institucion'].str.contains('SANTO TOMAS', na=False).astype(int)
 
    print(f"  Filas: {len(df):,} | USTA: {df['es_usta'].sum():,}")
    dfs.append(df)
 
print("\nUniendo todas las bases...")
df_total = pd.concat(dfs, ignore_index=True)
print(f"Total filas: {len(df_total):,}")
print(f"Filas USTA:  {df_total['es_usta'].sum():,}")
print(f"Periodos:    {sorted(df_total['periodo'].unique())}")
 
df_total.to_csv(RUTA_SALIDA, index=False, encoding='utf-8-sig')
print(f"\nArchivo guardado: {RUTA_SALIDA}")
print("Script 1 completado!")