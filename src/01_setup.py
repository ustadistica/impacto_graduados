"""
PASO 1 — Verificar instalación y cargar el CSV como vista DuckDB
Corre este script primero para confirmar que todo funciona.
"""

import subprocess, sys

# Instalar dependencias si no están
subprocess.check_call([sys.executable, "-m", "pip", "install",
                       "duckdb", "pandas", "openpyxl", "-q"])

import duckdb
import pandas as pd

# ─────────────────────────────────────────────
# AJUSTA ESTA RUTA si subiste el CSV a otra carpeta
# ─────────────────────────────────────────────
CSV = "/workspaces/impacto_graduados/data/raw/Personas_Naturales,_Personas_Jurídicas_y_Entidades_Sin_Animo_de_Lucro_20260325.csv"

CAMARAS_USTA = [
    "BOGOTA", "BUCARAMANGA", "TUNJA",
    "MEDELLIN PARA ANTIOQUIA", "VILLAVICENCIO",
    "MANIZALES", "CUCUTA", "IBAGUE", "HUILA",
    "PASTO", "PEREIRA", "BARRANQUILLA",
    "CARTAGENA", "ARMENIA", "SOGAMOSO",
    "CALI", "SANTA MARTA PARA EL MAGDALENA"
]

lista = ", ".join(f"'{c}'" for c in CAMARAS_USTA)

con = duckdb.connect()

con.execute(f"""
CREATE VIEW empresas AS
SELECT *
FROM read_csv_auto('{CSV}', ignore_errors=True, all_varchar=False)
WHERE camara_comercio IN ({lista})
""")

total = con.execute("SELECT COUNT(*) AS registros FROM empresas").fetchone()[0]
print(f"✓ Vista creada correctamente")
print(f"✓ Registros filtrados (cámaras USTA): {total:,}")

camaras = con.execute("""
    SELECT camara_comercio, COUNT(*) AS n
    FROM empresas
    GROUP BY 1 ORDER BY 2 DESC
""").df()
print("\nDistribución por cámara:")
print(camaras.to_string(index=False))

# Guardar conexión para reutilizar en otros scripts
con.close()
print("\n✓ Listo — ahora corre 02_analisis.py")
