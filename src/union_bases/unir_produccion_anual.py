"""
UNIR ARCHIVOS DE PRODUCCIÓN ANUAL
Combina todos los archivos Excel de producción por año en uno solo.

INSTRUCCIONES:
1. Pon este script en la misma carpeta donde están los archivos Excel
2. Los archivos deben llamarse exactamente:
      Producción_Grupos_Investigación_2013.xlsx
      Producción_Grupos_Investigación_2014.xlsx
      Producción_Grupos_Investigación_2015.xlsx
      ... (sin 2016 ni 2018)
3. Ejecuta:
      py -3.11 unir_produccion_anual.py

RESULTADO:
   Produccion_Consolidada_2013_2021.xlsx
"""

import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(BASE_DIR, '..', '..', 'data'))

# Años disponibles (sin 2016 ni 2018)
AÑOS = [2017, 2019, 2020, 2021]

NOMBRE_ARCHIVO = "Producción_Grupos_Investigación_{año}.xlsx"
ARCHIVO_SALIDA = os.path.join(DATA_DIR, "Produccion_Consolidada_2017_2021.csv")


def main():
    print("=" * 55)
    print("UNIFICADOR DE PRODUCCIÓN ANUAL - GRUPOS INVESTIGACIÓN")
    print("=" * 55)

    dfs = []
    total_filas = 0

    for año in AÑOS:
        nombre = os.path.join(DATA_DIR, NOMBRE_ARCHIVO.format(año=año))
        if not os.path.exists(nombre):
            print(f"  ⚠ No encontrado: {nombre} — se omite")
            continue
        print(f"  Leyendo {nombre}...", end=' ')
        df = pd.read_excel(nombre)
        filas = len(df)
        total_filas += filas
        dfs.append(df)
        print(f"{filas:,} filas ✓")

    if not dfs:
        print("\n❌ No se encontró ningún archivo. Verifica que estén en la misma carpeta.")
        return

    print(f"\nUniendo {len(dfs)} archivos ({total_filas:,} filas en total)...")
    df_consolidado = pd.concat(dfs, ignore_index=True)

    # Eliminar duplicados exactos
    antes = len(df_consolidado)
    df_consolidado.drop_duplicates(inplace=True)
    duplicados = antes - len(df_consolidado)
    if duplicados > 0:
        print(f"  → {duplicados:,} filas duplicadas eliminadas")

    # Guardar
    print(f"\nGuardando '{ARCHIVO_SALIDA}'...")
    df_consolidado.to_csv(ARCHIVO_SALIDA, index=False, encoding='utf-8-sig')

    print("\n" + "=" * 55)
    print("RESUMEN")
    print("=" * 55)
    print(f"Archivos procesados : {len(dfs)}")
    print(f"Filas totales       : {len(df_consolidado):,}")
    print(f"Años cubiertos      : {df_consolidado['ANO_CONVO'].nunique()}")
    print(f"Grupos únicos       : {df_consolidado['COD_GRUPO_GR'].nunique():,}")
    print(f"Productos únicos    : {df_consolidado['ID_PRODUCTO_PD'].nunique():,}")
    print(f"\n✓ Archivo guardado: {ARCHIVO_SALIDA}")

if __name__ == '__main__':
    main()
