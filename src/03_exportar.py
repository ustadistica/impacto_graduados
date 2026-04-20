"""
PASO 3 — Exportar hallazgos a Excel
Genera hallazgos_usta.xlsx con una hoja por análisis.
"""

import pickle
import pandas as pd

with open("resultados_analisis.pkl", "rb") as f:
    r = pickle.load(f)

OUTPUT = "hallazgos_usta.xlsx"

with pd.ExcelWriter(OUTPUT, engine="openpyxl") as xls:

    r["resumen_camara"].to_excel(xls, sheet_name="1_resumen_camaras",   index=False)
    r["tipo_org"].to_excel(xls,       sheet_name="2_tipo_organizacion",  index=False)
    r["por_anio"].to_excel(xls,       sheet_name="3_creacion_por_anio",  index=False)
    r["ciiu_global"].to_excel(xls,    sheet_name="4_top_ciiu",           index=False)
    r["estado"].to_excel(xls,         sheet_name="5_estado_matricula",   index=False)
    r["supervivencia"].to_excel(xls,  sheet_name="6_supervivencia",      index=False)

print(f"✓ Archivo generado: {OUTPUT}")
print(f"  Hojas incluidas:")
print(f"    1_resumen_camaras    — {len(r['resumen_camara'])} filas")
print(f"    2_tipo_organizacion  — {len(r['tipo_org'])} filas")
print(f"    3_creacion_por_anio  — {len(r['por_anio'])} filas")
print(f"    4_top_ciiu           — {len(r['ciiu_global'])} filas")
print(f"    5_estado_matricula   — {len(r['estado'])} filas")
print(f"    6_supervivencia      — {len(r['supervivencia'])} filas")

# También exportar CSVs individuales por si los necesitas en otra herramienta
for nombre, df in r.items():
    df.to_csv(f"out_{nombre}.csv", index=False)

print(f"\n✓ CSVs individuales guardados como out_*.csv")
print(f"\n  Descarga {OUTPUT} desde el explorador de archivos de Codespace.")
