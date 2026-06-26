# -*- coding: utf-8 -*-
"""Empaqueta los resultados del Observatorio en una carpeta de ENTREGABLES:
   CSVs curados + libro Excel multi-hoja + diccionario de datos + resumen + LÉEME.
   Reproducible: lee de salidas/ y escribe en entregables/.
"""
import shutil
from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parent
SAL = BASE / "salidas"
ENT = BASE / "entregables"
DSET = ENT / "datasets"
ENT.mkdir(exist_ok=True)
DSET.mkdir(exist_ok=True)

# (archivo_origen, archivo_entrega, título, descripción)
DATASETS = [
    ("graduados_integrado.csv", "01_graduados_integrado.csv",
     "Graduados integrados",
     "Base unificada de graduados USTA (seis sedes: Bogotá, Bucaramanga, Tunja, Villavicencio, Medellín y VUAD; 1970-2026). Una fila por registro de grado."),
    ("graduados_proveedores_secop.csv", "02_graduados_proveedores_secop.csv",
     "Graduados proveedores (SECOP)",
     "Graduados que figuran como contratistas del Estado en SECOP Integrado. Una fila por cédula proveedora."),
    ("graduados_emprendedores_rues.csv", "03_graduados_emprendedores_rues.csv",
     "Graduados matriculados en el RUES",
     "Graduados con matrícula mercantil (persona natural) en el RUES. Una fila por cédula con matrícula."),
    ("graduados_en_cvlac.csv", "04_graduados_investigadores_cvlac.csv",
     "Graduados investigadores (CvLAC)",
     "Graduados validados como investigadores en CvLAC por evidencia académica (formación USTA y/o grupo USTA)."),
    ("cvlac_usta_no_graduados.csv", "05_cvlac_usta_no_graduados.csv",
     "USTA en CvLAC fuera de graduados",
     "Personas con vínculo USTA en CvLAC (docentes, posgrados, egresados no listados) ausentes de las bases de grados."),
    ("impacto_consolidado.csv", "06_impacto_consolidado.csv",
     "Participación consolidada por persona",
     "TABLA MAESTRA: una fila por cédula con las tres dimensiones (SECOP+RUES+CvLAC) y el índice de participación."),
]

# ── Diccionario de datos ────────────────────────────────────────────
DICC = {
    # comunes
    "fuente": "Base institucional de origen del registro (Bucaramanga / Tunja / Villavicencio / SPB-CM-CAU).",
    "sede": "Seccional o sede de graduación (cuando la fuente la reporta).",
    "programa": "Nombre del programa académico (texto original).",
    "programa_norm": "Programa normalizado (mayúsculas, sin tildes) para agrupación.",
    "modalidad": "Pregrado / Posgrado (cuando la fuente la reporta).",
    "tipo_identificacion": "Tipo de documento de identidad (cuando aplica).",
    "identificacion": "Número de documento normalizado (solo dígitos). Llave de cruce.",
    "identificacion_raw": "Documento tal como venía en la fuente original.",
    "nombre_completo": "Nombre y apellidos del graduado.",
    "fecha_grado": "Fecha de grado (AAAA-MM-DD).",
    "anio_grado": "Año de grado.",
    # SECOP
    "nombre_secop": "Razón social / nombre del contratista en SECOP.",
    "tipo_doc_secop": "Tipo de documento del proveedor en SECOP.",
    "n_contratos": "Número de contratos del proveedor en SECOP Integrado.",
    "valor_total": "Valor total contratado (COP) sumado en SECOP.",
    "primera_firma": "Fecha del primer contrato firmado.",
    "ultima_firma": "Fecha del último contrato firmado.",
    # RUES
    "razon_social": "Razón social registrada en el RUES.",
    "categoria": "Categoría de matrícula RUES (Persona Natural / Jurídica).",
    "camara_comercio": "Cámara de comercio donde está la matrícula.",
    "ciiu_principal": "Código CIIU de la actividad económica principal.",
    "n_matriculas": "Número de matrículas mercantiles asociadas a la cédula.",
    "n_activas": "Número de matrículas en estado ACTIVA.",
    "tiene_empresa_activa": "Verdadero si tiene al menos una empresa activa.",
    "primera_matricula": "Fecha de la primera matrícula mercantil.",
    "ultima_matricula": "Fecha de la última matrícula mercantil.",
    # CvLAC
    "cod_rh": "Código interno del investigador en ScienTI/CvLAC.",
    "nombre_cvlac": "Nombre del investigador como aparece en CvLAC.",
    "nivel_maximo": "Nivel máximo de formación registrado en CvLAC.",
    "categoria_minciencias": "Categoría de investigador Minciencias (si aplica).",
    "total_productos": "Total de productos de investigación registrados en CvLAC.",
    "flag_usta_form": "1 si el perfil registra a la USTA en su formación académica.",
    "en_grupo_usta": "1 si el perfil es integrante de un grupo de investigación USTA (GrupLAC).",
    "prog_overlap": "Solape (Jaccard 0-1) entre el programa del graduado y el de CvLAC.",
    "n_homonimos": "Nº de perfiles CvLAC que comparten el mismo nombre (control de homónimos).",
    "score": "Score de cruce 0-100 (vínculo USTA + solape de programa).",
    "confianza": "Nivel de confianza de la coincidencia (Alta / Media).",
    "url_cvlac": "URL pública de la hoja de vida CvLAC (verificación manual).",
    # Consolidado
    "nombre": "Nombre representativo de la persona.",
    "programas": "Programas cursados por la persona (separados por ' | ').",
    "sedes": "Sedes asociadas a la persona.",
    "fuentes": "Bases de origen asociadas a la persona.",
    "n_titulos": "Número de registros de grado de la persona.",
    "anio_primer_grado": "Año del primer grado.",
    "anio_ultimo_grado": "Año del último grado.",
    "es_proveedor_secop": "Verdadero si la persona es proveedora del Estado (SECOP).",
    "secop_n_contratos": "Nº de contratos SECOP de la persona.",
    "secop_valor_total": "Valor total contratado (COP) en SECOP.",
    "secop_ultima_firma": "Fecha del último contrato SECOP.",
    "en_rues": "Verdadero si la persona tiene matrícula mercantil (RUES).",
    "rues_n_matriculas": "Nº de matrículas mercantiles.",
    "rues_n_activas": "Nº de matrículas activas.",
    "rues_empresa_activa": "Verdadero si tiene empresa activa hoy.",
    "rues_ciiu": "CIIU principal de la actividad empresarial.",
    "en_cvlac": "Verdadero si la persona está validada como investigadora en CvLAC.",
    "cvlac_nivel": "Nivel máximo de formación (CvLAC).",
    "cvlac_categoria": "Categoría Minciencias (CvLAC).",
    "cvlac_productos": "Total de productos de investigación (CvLAC).",
    "cvlac_score": "Score de validación CvLAC (0-100).",
    "n_dimensiones": "Nº de dimensiones de participación (0-3): SECOP, RUES, CvLAC.",
    "perfil_impacto": "Combinación de dimensiones (p.ej. 'RUES+CvLAC').",
}

print("== Copiando datasets curados ==")
dic_rows = []
copiados = []
for src, dst, titulo, desc in DATASETS:
    s = SAL / src
    if not s.exists():
        print(f"  [FALTA] {src}"); continue
    shutil.copyfile(s, DSET / dst)
    df = pd.read_csv(s, dtype=str)
    print(f"  [ok] {dst:42s} {len(df):>7,} filas x {len(df.columns)} cols")
    copiados.append((dst, titulo, desc, len(df), df))
    for c in df.columns:
        dic_rows.append({"dataset": dst, "columna": c,
                         "descripcion": DICC.get(c, "(sin descripción)")})

# ── Diccionario de datos ────────────────────────────────────────────
dicc = pd.DataFrame(dic_rows)
dicc.to_csv(ENT / "diccionario_datos.csv", index=False, encoding="utf-8-sig")
print(f"\n== Diccionario de datos: {len(dicc)} entradas ==")

# ── Resumen de indicadores ──────────────────────────────────────────
cons = pd.read_csv(SAL / "impacto_consolidado.csv", low_memory=False)
N = len(cons)
n_reg = sum(1 for _ in open(SAL / "graduados_integrado.csv", encoding="utf-8")) - 1
def pct(x): return round(x / N * 100, 1)
ind = [
    ("Personas (cédulas únicas de graduados)", N, ""),
    ("Registros de grado integrados", n_reg, ""),
    ("Proveedores del Estado (SECOP)", int(cons["es_proveedor_secop"].sum()), f"{pct(int(cons['es_proveedor_secop'].sum()))}%"),
    ("Matriculados en el RUES", int(cons["en_rues"].sum()), f"{pct(int(cons['en_rues'].sum()))}%"),
    ("Matriculados con empresa activa", int(cons["rues_empresa_activa"].sum()), f"{pct(int(cons['rues_empresa_activa'].sum()))}%"),
    ("Investigadores validados (CvLAC)", int(cons["en_cvlac"].sum()), f"{pct(int(cons['en_cvlac'].sum()))}%"),
    ("Con al menos una dimensión de participación", int((cons["n_dimensiones"] >= 1).sum()), f"{pct(int((cons['n_dimensiones']>=1).sum()))}%"),
    ("En dos dimensiones", int((cons["n_dimensiones"] == 2).sum()), f"{pct(int((cons['n_dimensiones']==2).sum()))}%"),
    ("En las tres dimensiones", int((cons["n_dimensiones"] == 3).sum()), f"{pct(int((cons['n_dimensiones']==3).sum()))}%"),
]
resumen = pd.DataFrame(ind, columns=["indicador", "valor", "porcentaje"])
resumen.to_csv(ENT / "resumen_indicadores.csv", index=False, encoding="utf-8-sig")
print("== Resumen de indicadores ==")
print(resumen.to_string(index=False))

# ── Libro Excel multi-hoja ──────────────────────────────────────────
xlsx = ENT / "Impacto_Graduados_USTA.xlsx"
SHEETS = {
    "01_graduados_integrado.csv": "Graduados",
    "02_graduados_proveedores_secop.csv": "SECOP",
    "03_graduados_emprendedores_rues.csv": "RUES",
    "04_graduados_investigadores_cvlac.csv": "CvLAC",
    "05_cvlac_usta_no_graduados.csv": "CvLAC_USTA_no_grad",
    "06_impacto_consolidado.csv": "Impacto_consolidado",
}
with pd.ExcelWriter(xlsx, engine="openpyxl") as xw:
    resumen.to_excel(xw, sheet_name="Resumen", index=False)
    dicc.to_excel(xw, sheet_name="Diccionario", index=False)
    for dst, titulo, desc, n, df in copiados:
        hoja = SHEETS.get(dst, dst[:31])
        df.to_excel(xw, sheet_name=hoja[:31], index=False)
print(f"\n== Libro Excel: {xlsx.name} ({xlsx.stat().st_size/1e6:.1f} MB) ==")

# ── LÉEME ───────────────────────────────────────────────────────────
leeme = ENT / "LEEME.md"
lineas = [
    "# Entregables — Observatorio de Participación de Graduados USTA",
    "",
    "Paquete de datos del cruce de los graduados de la Universidad Santo Tomás con",
    "tres registros nacionales: **SECOP** (contratación pública), **RUES** (registro",
    "mercantil) y **CvLAC** (investigación, ScienTI/Minciencias).",
    "",
    "## Contenido",
    "",
    "- `datasets/` — datasets curados en CSV (UTF-8 con BOM, compatibles con Excel):",
]
for dst, titulo, desc, n, df in copiados:
    lineas.append(f"  - **{dst}** — {titulo}: {desc} ({n:,} filas).")
lineas += [
    "- `Impacto_Graduados_USTA.xlsx` — libro Excel con todas las tablas + Resumen + Diccionario.",
    "- `diccionario_datos.csv` — descripción de cada columna de cada dataset.",
    "- `resumen_indicadores.csv` — indicadores principales del impacto.",
    "",
    "## Llave de cruce",
    "",
    "La llave entre fuentes es la **cédula** (`identificacion`). SECOP y RUES se cruzan",
    "por documento contra el universo completo de cada fuente; CvLAC se valida por",
    "**nombre + evidencia académica** (formación USTA y/o pertenencia a grupo USTA),",
    "por lo que incluye un `score` y un nivel de `confianza`.",
    "",
    "## Tabla maestra",
    "",
    "**`06_impacto_consolidado.csv`** es la entrega principal: una fila por persona con",
    "las tres dimensiones y el índice `n_dimensiones` (0-3) / `perfil_impacto`.",
    "",
    "## Advertencias de uso",
    "",
    "- **Datos personales.** Los archivos contienen documentos y nombres; uso interno",
    "  institucional, sujeto a la política de tratamiento de datos de la Universidad.",
    "- **Cobertura asimétrica.** SECOP/RUES cubren su universo por documento; CvLAC es",
    "  cobertura parcial y por evidencia. Las cifras de RUES (solo persona natural) y",
    "  CvLAC son **pisos**, no techos (ver informe de hallazgos).",
    "- **Reproducibilidad.** Generado con `generar_entregables.py` a partir de `salidas/`.",
    "",
    "_Consultorio de Estadística · Organización ustadistica._",
]
leeme.write_text("\n".join(lineas), encoding="utf-8")
print(f"== LÉEME escrito: {leeme} ==")
print("\nENTREGABLES listos en:", ENT)
