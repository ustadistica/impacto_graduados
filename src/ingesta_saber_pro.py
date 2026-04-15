"""
SCRIPT 0 - INGESTA SABER PRO
==============================
Recibe los archivos xlsx descargados del portal ICFES,
los valida superficialmente y los mueve a data/raw/.
Genera artefacto: ingesta_report.json
"""

import pandas as pd
import glob
import os
import json
import shutil
from pathlib import Path
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIGURACIÓN
BASE           = Path(__file__).parent
CARPETA_ORIGEN = BASE / "Saber pro" / "saber_pro"
CARPETA_RAW    = BASE / "data" / "raw" / "saber_pro"
RUTA_REPORT    = BASE / "artifacts" / "ingesta_saber_pro.json"

CARPETA_RAW.mkdir(parents=True, exist_ok=True)
RUTA_REPORT.parent.mkdir(parents=True, exist_ok=True)
# ─────────────────────────────────────────────

# Campos mínimos esperados en cada archivo
CAMPOS_MINIMOS = [
    'periodo', 'inst_nombre_institucion', 'punt_global',
    'mod_lectura_critica_punt', 'mod_razona_cuantitat_punt',
    'mod_comuni_escrita_punt', 'mod_ingles_punt',
    'mod_competen_ciudada_punt',
]

Path(CARPETA_RAW).mkdir(parents=True, exist_ok=True)
Path(RUTA_REPORT).parent.mkdir(parents=True, exist_ok=True)

archivos = sorted(glob.glob(os.path.join(CARPETA_ORIGEN, "Saber_pro_20*.xlsx")))
print(f"Archivos encontrados: {len(archivos)}")

if len(archivos) == 0:
    print(f"\nERROR: No se encontraron archivos en: {CARPETA_ORIGEN}")
    print("Verifica que los archivos se llamen Saber_pro_2018.xlsx, Saber_pro_2019.xlsx, etc.")
    exit()

reporte = {
    'fuente':       'ICFES - DataIcfes 2.0',
    'url':          'https://icfesgovco.sharepoint.com/sites/DataIcfes2.0',
    'fecha_ingesta': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    'archivos':     [],
    'errores':      [],
    'resultado':    None,
}

for archivo in archivos:
    nombre = os.path.basename(archivo)
    print(f"\nIngestando: {nombre}...")

    info = {
        'archivo':   nombre,
        'ruta_raw':  os.path.join(CARPETA_RAW, nombre),
        'n_filas':   None,
        'n_columnas':None,
        'periodos':  None,
        'n_usta':    None,
        'campos_ok': True,
        'campos_faltantes': [],
    }

    try:
        # Leer solo primeras filas para validar estructura
        df_head = pd.read_excel(archivo, nrows=5)
        faltantes = [c for c in CAMPOS_MINIMOS if c not in df_head.columns]

        if faltantes:
            info['campos_ok'] = False
            info['campos_faltantes'] = faltantes
            reporte['errores'].append(f"{nombre}: campos faltantes -> {faltantes}")
            print(f"  ADVERTENCIA: Campos faltantes -> {faltantes}")
        else:
            print(f"  Estructura OK")

        # Leer completo para métricas
        df = pd.read_excel(archivo)
        info['n_filas']    = len(df)
        info['n_columnas'] = len(df.columns)
        info['periodos']   = sorted(df['periodo'].dropna().unique().tolist()) if 'periodo' in df.columns else []
        info['n_usta']     = int(df['inst_nombre_institucion'].str.contains('SANTO TOMAS', na=False).sum()) if 'inst_nombre_institucion' in df.columns else 0

        print(f"  Filas: {info['n_filas']:,} | USTA: {info['n_usta']:,} | Periodos: {info['periodos']}")

        # Copiar a data/raw/
        shutil.copy2(archivo, info['ruta_raw'])
        print(f"  Copiado a: {info['ruta_raw']}")

    except Exception as e:
        reporte['errores'].append(f"{nombre}: error al leer -> {str(e)}")
        print(f"  ERROR: {e}")

    reporte['archivos'].append(info)

# Resultado final
reporte['total_archivos'] = len(archivos)
reporte['total_errores']  = len(reporte['errores'])
reporte['resultado']      = 'FALLO' if reporte['errores'] else 'OK'

total_filas = sum(a['n_filas'] for a in reporte['archivos'] if a['n_filas'])
total_usta  = sum(a['n_usta']  for a in reporte['archivos'] if a['n_usta'])
reporte['total_filas'] = total_filas
reporte['total_usta']  = total_usta

with open(RUTA_REPORT, 'w', encoding='utf-8') as f:
    json.dump(reporte, f, ensure_ascii=False, indent=2)

print(f"\n{'='*50}")
print(f"INGESTA COMPLETADA: {reporte['resultado']}")
print(f"  Archivos procesados: {len(archivos)}")
print(f"  Total filas:         {total_filas:,}")
print(f"  Total filas USTA:    {total_usta:,}")
print(f"  Errores:             {reporte['total_errores']}")
print(f"{'='*50}")
print(f"\nArtefacto guardado: {RUTA_REPORT}")
print("Script 0 completado! Ahora corre: python 1_consolidar_saber_pro.py")