"""
convertir_a_parquet.py
======================
Convierte el CSV del RUES a formato Parquet optimizado para el dashboard.
Ejecutar ANTES de correr el dashboard.

Uso:
    python convertir_a_parquet.py
"""

import duckdb
import os

# ─── Rutas ────────────────────────────────────────────────────────────────────
CSV_INPUT  = "data/Personas_Naturales,_Personas_Jurídicas_y_Entidades_Sin_Animo_de_Lucro_20260510.csv"

PARQUET_OUT = "data/rues_data.parquet"

# ─── Verificar que el CSV existe ──────────────────────────────────────────────
if not os.path.exists(CSV_INPUT):
    raise FileNotFoundError(
        f"No se encontró el archivo CSV: {CSV_INPUT}\n"
        "Asegúrate de que el archivo esté en el mismo directorio."
    )

print(f"📂  Leyendo: {CSV_INPUT}")
print("⏳  Esto puede tardar varios minutos con 9M de filas...")

con = duckdb.connect()

# ─── Leer CSV y exportar a Parquet ────────────────────────────────────────────
# DuckDB infiere tipos automáticamente; forzamos columnas críticas a VARCHAR
# para evitar errores de tipo en NITs/cédulas con ceros iniciales.
con.execute(f"""
COPY (
    SELECT
        -- Identificación
        CAST(numero_identificacion   AS VARCHAR) AS numero_identificacion,
        CAST(nit                      AS VARCHAR) AS nit,
        CAST(digito_verificacion      AS VARCHAR) AS digito_verificacion,

        -- Cámara y matrícula
        codigo_camara,
        camara_comercio,
        CAST(matricula               AS VARCHAR) AS matricula,

        -- Razón social y nombre
        razon_social,
        primer_nombre,
        primer_apellido,

        -- Identificación tributaria
        codigo_clase_identificacion,
        clase_identificacion,

        -- Actividad económica
        CAST(cod_ciiu_act_econ_pri   AS VARCHAR) AS cod_ciiu_act_econ_pri,
        CAST(cod_ciiu_act_econ_sec   AS VARCHAR) AS cod_ciiu_act_econ_sec,

        -- Fechas (se leen como VARCHAR y se parsean en el dashboard)
        CAST(fecha_matricula         AS VARCHAR) AS fecha_matricula,
        CAST(fecha_renovacion        AS VARCHAR) AS fecha_renovacion,
        CAST(ultimo_ano_renovado     AS VARCHAR) AS ultimo_ano_renovado,
        CAST(fecha_vigencia          AS VARCHAR) AS fecha_vigencia,
        CAST(fecha_cancelacion       AS VARCHAR) AS fecha_cancelacion,
        CAST(fecha_actualizacion     AS VARCHAR) AS fecha_actualizacion,

        -- Clasificación jurídica
        codigo_tipo_sociedad,
        tipo_sociedad,
        codigo_organizacion_juridica,
        organizacion_juridica,
        codigo_categoria_matricula,
        categoria_matricula,
        codigo_estado_matricula,
        estado_matricula,

        -- Representante legal
        clase_identificacion_RL,
        "Num Identificacion Representante Legal",
        "Representante Legal"

    FROM read_csv_auto('{CSV_INPUT}',
        header       = true,
        ignore_errors= true,
        null_padding = true
    )
)
TO '{PARQUET_OUT}'
(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
""")

con.close()

# ─── Verificación rápida ──────────────────────────────────────────────────────
size_mb = os.path.getsize(PARQUET_OUT) / 1_048_576
print(f"\n✅  Parquet guardado: {PARQUET_OUT}  ({size_mb:.1f} MB)")

verify = duckdb.query(f"SELECT COUNT(*) AS total FROM '{PARQUET_OUT}'").fetchone()
print(f"📊  Total de registros: {verify[0]:,}")
print("\n🚀  Listo. Ahora puedes correr el dashboard con:  streamlit run dashboard_rues.py")