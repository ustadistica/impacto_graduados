# Entregables — Observatorio de Participación de Graduados USTA

Paquete de datos del cruce de los graduados de la Universidad Santo Tomás con
tres registros nacionales: **SECOP** (contratación pública), **RUES** (registro
mercantil) y **CvLAC** (investigación, ScienTI/Minciencias).

## Contenido

- `datasets/` — datasets curados en CSV (UTF-8 con BOM, compatibles con Excel):
  - **01_graduados_integrado.csv** — Graduados integrados: Base unificada de graduados USTA (seis sedes: Bogotá, Bucaramanga, Tunja, Villavicencio, Medellín y VUAD; 1970-2026). Una fila por registro de grado. (197,273 filas).
  - **02_graduados_proveedores_secop.csv** — Graduados proveedores (SECOP): Graduados que figuran como contratistas del Estado en SECOP Integrado. Una fila por cédula proveedora. (42,889 filas).
  - **03_graduados_emprendedores_rues.csv** — Graduados matriculados en el RUES: Graduados con matrícula mercantil (persona natural) en el RUES. Una fila por cédula con matrícula. (37,544 filas).
  - **04_graduados_investigadores_cvlac.csv** — Graduados investigadores (CvLAC): Graduados validados como investigadores en CvLAC por evidencia académica (formación USTA y/o grupo USTA). (3,403 filas).
  - **05_cvlac_usta_no_graduados.csv** — USTA en CvLAC fuera de graduados: Personas con vínculo USTA en CvLAC (docentes, posgrados, egresados no listados) ausentes de las bases de grados. (3,137 filas).
  - **06_impacto_consolidado.csv** — Participación consolidada por persona: TABLA MAESTRA: una fila por cédula con las tres dimensiones (SECOP+RUES+CvLAC) y el índice de participación. (174,685 filas).
- `Impacto_Graduados_USTA.xlsx` — libro Excel con todas las tablas + Resumen + Diccionario.
- `diccionario_datos.csv` — descripción de cada columna de cada dataset.
- `resumen_indicadores.csv` — indicadores principales del impacto.

## Llave de cruce

La llave entre fuentes es la **cédula** (`identificacion`). SECOP y RUES se cruzan
por documento contra el universo completo de cada fuente; CvLAC se valida por
**nombre + evidencia académica** (formación USTA y/o pertenencia a grupo USTA),
por lo que incluye un `score` y un nivel de `confianza`.

## Tabla maestra

**`06_impacto_consolidado.csv`** es la entrega principal: una fila por persona con
las tres dimensiones y el índice `n_dimensiones` (0-3) / `perfil_impacto`.

## Advertencias de uso

- **Datos personales.** Los archivos contienen documentos y nombres; uso interno
  institucional, sujeto a la política de tratamiento de datos de la Universidad.
- **Cobertura asimétrica.** SECOP/RUES cubren su universo por documento; CvLAC es
  cobertura parcial y por evidencia. Las cifras de RUES (solo persona natural) y
  CvLAC son **pisos**, no techos (ver informe de hallazgos).
- **Reproducibilidad.** Generado con `generar_entregables.py` a partir de `salidas/`.

_Consultorio de Estadística · Organización ustadistica._