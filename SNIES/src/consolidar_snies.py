import pandas as pd
from pathlib import Path

# -----------------------------------------
# CONFIGURACION
# -----------------------------------------

BASE_DIR   = Path(r"C:\Users\aleja\Downloads\proyecto_snies_usta")
RAW_DIR    = BASE_DIR / "data" / "raw"
OUTPUT_DIR = BASE_DIR / "data" / "processed"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

COLUMNAS = {
    "codigo_institucion":  ["Código de la Institución",                "CÓDIGO DE LA INSTITUCIÓN",                "CÓDIGO DE LA INSTITUCIÓN",                "CÓDIGO DE LA INSTITUCIÓN"],
    "ies_padre":           ["IES PADRE",                               "IES PADRE",                               "IES PADRE",                               "IES PADRE"],
    "institucion":         ["Institución de Educación Superior (IES)", "INSTITUCIÓN DE EDUCACIÓN SUPERIOR (IES)", "INSTITUCIÓN DE EDUCACIÓN SUPERIOR (IES)", "INSTITUCIÓN DE EDUCACIÓN SUPERIOR (IES)"],
    "principal_seccional": ["Principal o Seccional",                   "PRINCIPAL O SECCIONAL",                   "PRINCIPAL O SECCIONAL",                   None],
    "id_sector":           ["ID Sector IES",                           "ID SECTOR IES",                           "ID SECTOR IES",                           "ID SECTOR IES"],
    "sector":              ["Sector IES",                              "SECTOR IES",                              "SECTOR IES",                              "SECTOR IES"],
    "id_caracter":         ["ID Caracter",                             "ID CARACTER",                             "ID CARACTER",                             "ID CARÁCTER IES"],
    "caracter":            ["Caracter IES",                            "CARACTER IES",                            "CARACTER IES",                            "CARÁCTER IES"],
    "cod_dpto_ies":        ["Código del departamento (IES)",           "CÓDIGO DEL DEPARTAMENTO (IES)",           "CÓDIGO DEL DEPARTAMENTO (IES)",           "CÓDIGO DEL DEPARTAMENTO (IES)"],
    "dpto_ies":            ["Departamento de domicilio de la IES",     "DEPARTAMENTO DE DOMICILIO DE LA IES",     "DEPARTAMENTO DE DOMICILIO DE LA IES",     "DEPARTAMENTO DE DOMICILIO DE LA IES"],
    "cod_mpio_ies":        ["Código del municipio",                    "CÓDIGO DEL MUNICIPIO",                    "CÓDIGO DEL MUNICIPIO (IES)",               "CÓDIGO DEL MUNICIPIO IES"],
    "mpio_ies":            ["Municipio de domicilio de la IES",        "MUNICIPIO DE DOMICILIO DE LA IES",        "MUNICIPIO DE DOMICILIO DE LA IES",        "MUNICIPIO DE DOMICILIO DE LA IES"],
    "cod_snies_programa":  ["Código SNIES del programa",               "CÓDIGO SNIES DEL PROGRAMA",               "CÓDIGO SNIES DEL PROGRAMA",               "CÓDIGO SNIES DEL PROGRAMA"],
    "programa":            ["Programa Académico",                      "PROGRAMA ACADÉMICO",                      "PROGRAMA ACADÉMICO",                      "PROGRAMA ACADÉMICO"],
    "id_nivel_academico":  ["ID Nivel Académico",                      "ID NIVEL ACADÉMICO",                      "ID NIVEL ACADÉMICO",                      "ID NIVEL ACADÉMICO"],
    "nivel_academico":     ["Nivel Académico",                         "NIVEL ACADÉMICO",                         "NIVEL ACADÉMICO",                         "NIVEL ACADÉMICO"],
    "id_nivel_formacion":  ["ID Nivel de Formación",                   "ID NIVEL DE FORMACIÓN",                   "ID NIVEL DE FORMACIÓN",                   "ID NIVEL DE FORMACIÓN"],
    "nivel_formacion":     ["Nivel de Formación",                      "NIVEL DE FORMACIÓN",                      "NIVEL DE FORMACIÓN",                      "NIVEL DE FORMACIÓN"],
    "id_metodologia":      ["ID Metodología",                          "ID METODOLOGÍA",                          "ID METODOLOGÍA",                          "ID MODALIDAD"],
    "metodologia":         ["Metodología",                             "METODOLOGÍA",                             "METODOLOGÍA",                             "MODALIDAD"],
    "id_area":             ["ID Área",                                 "ID ÁREA",                                 "ID ÁREA",                                 "ID ÁREA"],
    "area_conocimiento":   ["Área de Conocimiento",                    "ÁREA DE CONOCIMIENTO",                    "ÁREA DE CONOCIMIENTO",                    "ÁREA DE CONOCIMIENTO"],
    "id_nbc":              ["ID Núcleo",                               "ID NÚCLEO",                               "ID NÚCLEO",                               "ID NÚCLEO"],
    "nbc":                 ["Núcleo Básico del Conocimiento (NBC)",    "NÚCLEO BÁSICO DEL CONOCIMIENTO (NBC)",    "NÚCLEO BÁSICO DEL CONOCIMIENTO (NBC)",    "NÚCLEO BÁSICO DEL CONOCIMIENTO (NBC)"],
    "cod_dpto_programa":   ["Código del Departamento (Programa)",      "CÓDIGO DEL DEPARTAMENTO (PROGRAMA)",      "CÓDIGO DEL DEPARTAMENTO (PROGRAMA)",      "CÓDIGO DEL DEPARTAMENTO (PROGRAMA)"],
    "dpto_programa":       ["Departamento de Oferta del Programa",     "DEPARTAMENTO DE OFERTA DEL PROGRAMA",     "DEPARTAMENTO DE OFERTA DEL PROGRAMA",     "DEPARTAMENTO DE OFERTA DEL PROGRAMA"],
    "cod_mpio_programa":   ["Código del Municipio (Programa)",         "CÓDIGO DEL MUNICIPIO (PROGRAMA)",         "CÓDIGO DEL MUNICIPIO (PROGRAMA)",         "CÓDIGO DEL MUNICIPIO (PROGRAMA)"],
    "mpio_programa":       ["Municipio de Oferta del Programa",        "MUNICIPIO DE OFERTA DEL PROGRAMA",        "MUNICIPIO DE OFERTA DEL PROGRAMA",        "MUNICIPIO DE OFERTA DEL PROGRAMA"],
    "id_sexo":             ["ID Sexo",                                 "ID SEXO",                                 "ID SEXO",                                 "ID SEXO"],
    "sexo":                ["Sexo",                                    "SEXO",                                    "SEXO",                                    "SEXO"],
    "anio":                ["Año",                                     "AÑO",                                     "AÑO",                                     "AÑO"],
    "semestre":            ["Semestre",                                "SEMESTRE",                                "SEMESTRE",                                "SEMESTRE"],
    "graduados":           ["Graduados",                               "GRADUADOS",                               "GRADUADOS",                               "GRADUADOS"],
}

# Formato por archivo: 0=2018, 1=2019-2020, 2=2021-2022, 3=2023-2024
ARCHIVOS = [
    (RAW_DIR / "Graduados_2018.xlsx", 0),
    (RAW_DIR / "Graduados_2019.xlsx", 1),
    (RAW_DIR / "Graduados_2020.xlsx", 1),
    (RAW_DIR / "Graduados_2021.xlsx", 2),
    (RAW_DIR / "Graduados_2022.xlsx", 2),
    (RAW_DIR / "Graduados_2023.xlsx", 3),
    (RAW_DIR / "Graduados_2024.xlsx", 3),
]

# Excepciones por archivo para columnas con nombre distinto
EXCEPCIONES = {
    "Graduados_2022.xlsx": {"ies_padre": "IES_PADRE", "cod_mpio_programa": "CDIGO DEL MUNICIPIO (PROGRAMA)"},
    "Graduados_2021.xlsx": {"id_area": "ID ÁREA DE CONOCIMIENTO"},
}

# -----------------------------------------
# FUNCIONES
# -----------------------------------------

def estandarizar(df):
    df["sexo"] = df["sexo"].replace({
        "Hombre": "Masculino",
        "Mujer":  "Femenino",
    })
    df["nivel_formacion"] = df["nivel_formacion"].replace({
        "Universitaria":                       "Universitario",
        "Especialización Médico Quirúrgica":   "Especialización médico quirúrgica",
        "Especialización Universitaria":       "Especialización universitaria",
        "Especialización Tecnológica":         "Especialización tecnológica",
        "Especialización Técnico Profesional": "Especialización técnico profesional",
    })
    df["anio"] = pd.to_numeric(
        df["anio"].astype(str).str[:4], errors="coerce"
    ).astype("Int64")
    return df


def leer_archivo(path, formato):
    df = pd.read_excel(path)
    nombre_archivo = path.name
    excepciones = EXCEPCIONES.get(nombre_archivo, {})

    rename = {}
    for nombre_final, opciones in COLUMNAS.items():
        col_original = opciones[formato]
        if nombre_final in excepciones:
            col_original = excepciones[nombre_final]
        if col_original is not None and col_original in df.columns:
            rename[col_original] = nombre_final

    df = df.rename(columns=rename)

    for col in COLUMNAS.keys():
        if col not in df.columns:
            df[col] = None

    return df[list(COLUMNAS.keys())]


# -----------------------------------------
# CONSOLIDACION
# -----------------------------------------

def consolidar():
    dfs = []

    for path, formato in ARCHIVOS:
        print(f"  -> {path.name} (formato {formato})")
        dfs.append(leer_archivo(path, formato))

    print("\nConsolidando y estandarizando...")
    df = pd.concat(dfs, ignore_index=True)
    df = estandarizar(df)

    output_path = OUTPUT_DIR / "SNIES_contexto.xlsx"
    df.to_excel(output_path, index=False)

    print(f"\nListo: {len(df):,} filas x {len(df.columns)} columnas")
    print(f"Guardado en: {output_path}")
    print(f"\nGraduados por año:")
    print(df.groupby("anio")["graduados"].sum().to_string())


if __name__ == "__main__":
    consolidar()