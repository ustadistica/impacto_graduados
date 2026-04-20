"""
PASO 2 — Consultas de análisis para el informe USTA
Genera 6 tablas de hallazgos sobre creación empresarial.
"""

import duckdb
import pandas as pd

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

# ─────────────────────────────────────────────
# 1. Distribución por tipo de organización jurídica
# ─────────────────────────────────────────────
print("Calculando tipo de organización...")
tipo_org = con.execute("""
    SELECT
        organizacion_juridica,
        COUNT(*) AS total,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS porcentaje
    FROM empresas
    GROUP BY 1
    ORDER BY 2 DESC
""").df()

# ─────────────────────────────────────────────
# 2. Empresas creadas por año y cámara (desde 2015)
# ─────────────────────────────────────────────
print("Calculando creación por año...")
por_anio = con.execute("""
    SELECT
        YEAR(TRY_CAST(fecha_matricula AS DATE))   AS anio,
        camara_comercio,
        COUNT(*)                                   AS nuevas_empresas
    FROM empresas
    WHERE YEAR(TRY_CAST(fecha_matricula AS DATE)) >= 2015
      AND YEAR(TRY_CAST(fecha_matricula AS DATE)) <= 2026
    GROUP BY 1, 2
    ORDER BY 1, 3 DESC
""").df()

# ─────────────────────────────────────────────
# 3. Top 30 actividades CIIU (todas las cámaras)
# ─────────────────────────────────────────────
print("Calculando CIIU...")
ciiu_global = con.execute("""
    SELECT
        cod_ciiu_act_econ_pri            AS ciiu,
        COUNT(*)                          AS empresas,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM empresas
    WHERE cod_ciiu_act_econ_pri IS NOT NULL
      AND cod_ciiu_act_econ_pri != ''
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 30
""").df()

# ─────────────────────────────────────────────
# 4. Estado de matrícula por cámara
# ─────────────────────────────────────────────
print("Calculando estado de matrículas...")
estado = con.execute("""
    SELECT
        camara_comercio,
        estado_matricula,
        COUNT(*) AS total
    FROM empresas
    GROUP BY 1, 2
    ORDER BY 1, 3 DESC
""").df()

# ─────────────────────────────────────────────
# 5. Tasa de supervivencia — cohortes 2015-2020
# ─────────────────────────────────────────────
print("Calculando supervivencia...")
supervivencia = con.execute("""
    SELECT
        YEAR(TRY_CAST(fecha_matricula AS DATE)) AS anio_creacion,
        estado_matricula,
        COUNT(*)                                 AS empresas
    FROM empresas
    WHERE YEAR(TRY_CAST(fecha_matricula AS DATE)) BETWEEN 2015 AND 2020
    GROUP BY 1, 2
    ORDER BY 1, 2
""").df()

# ─────────────────────────────────────────────
# 6. Resumen general por cámara (activas + total)
# ─────────────────────────────────────────────
print("Calculando resumen por cámara...")
resumen_camara = con.execute("""
    SELECT
        camara_comercio,
        COUNT(*)                                           AS total_registros,
        SUM(CASE WHEN estado_matricula ILIKE '%activ%'
                 THEN 1 ELSE 0 END)                       AS activas,
        SUM(CASE WHEN estado_matricula ILIKE '%cancel%'
                 THEN 1 ELSE 0 END)                       AS canceladas,
        ROUND(SUM(CASE WHEN estado_matricula ILIKE '%activ%'
                       THEN 1 ELSE 0 END) * 100.0
              / COUNT(*), 1)                              AS pct_activas
    FROM empresas
    GROUP BY 1
    ORDER BY 2 DESC
""").df()

con.close()

# Guardar resultados para usar en el exportador
import pickle
resultados = {
    "tipo_org":       tipo_org,
    "por_anio":       por_anio,
    "ciiu_global":    ciiu_global,
    "estado":         estado,
    "supervivencia":  supervivencia,
    "resumen_camara": resumen_camara,
}

with open("resultados_analisis.pkl", "wb") as f:
    pickle.dump(resultados, f)

print("\n─────────────────────────────────────────")
print("✓ Análisis completo — resultados guardados en resultados_analisis.pkl")
print("✓ Ahora corre 03_exportar.py")
print("─────────────────────────────────────────")

# Preview rápido
print("\n── Resumen por cámara ──")
print(resumen_camara.to_string(index=False))
