"""
ingest.py
Carga y valida los datos de graduados desde SNIES_contexto.xlsx
"""
import pandas as pd
from pathlib import Path

RAW_PATH = Path("data/processed/SNIES_contexto.xlsx")

EXPECTED_COLUMNS = [
    "codigo_institucion", "ies_padre", "institucion", "principal_seccional",
    "id_sector", "sector", "id_caracter", "caracter",
    "cod_dpto_ies", "dpto_ies", "cod_mpio_ies", "mpio_ies",
    "cod_snies_programa", "programa", "id_nivel_academico", "nivel_academico",
    "id_nivel_formacion", "nivel_formacion", "id_metodologia", "metodologia",
    "id_area", "area_conocimiento", "id_nbc", "nbc",
    "cod_dpto_programa", "dpto_programa", "cod_mpio_programa", "mpio_programa",
    "id_sexo", "sexo", "anio", "semestre", "graduados",
]


def norm_dpto(s):
    """Normaliza nombres de departamentos con inconsistencias de capitalización y tildes."""
    if pd.isna(s):
        return s
    s = str(s).strip().upper()
    s = (s.replace("Á", "A").replace("É", "E").replace("Í", "I")
          .replace("Ó", "O").replace("Ú", "U")
          .replace(",", "").replace(".", "").strip())
    if "BOGOT"     in s:                         return "Bogotá D.C."
    if "ANTIOQUIA" in s:                         return "Antioquia"
    if "VALLE"     in s and "CAUCA" in s:        return "Valle del Cauca"
    if "NORTE"     in s and "SANTANDER" in s:    return "Norte de Santander"
    if "SANTANDER" in s:                         return "Santander"
    if "BOLIVAR"   in s:                         return "Bolívar"
    if "BOYACA"    in s:                         return "Boyacá"
    if "ATLANTICO" in s:                         return "Atlántico"
    if "CORDOBA"   in s:                         return "Córdoba"
    if "CUNDINAMARCA" in s:                      return "Cundinamarca"
    if "NARINO"    in s:                         return "Nariño"
    if "TOLIMA"    in s:                         return "Tolima"
    if "CALDAS"    in s:                         return "Caldas"
    if "RISARALDA" in s:                         return "Risaralda"
    if "HUILA"     in s:                         return "Huila"
    if "CESAR"     in s:                         return "Cesar"
    if "MAGDALENA" in s:                         return "Magdalena"
    if "CAUCA"     in s:                         return "Cauca"
    if "META"      in s:                         return "Meta"
    if "SUCRE"     in s:                         return "Sucre"
    if "QUINDIO"   in s:                         return "Quindío"
    if "CASANARE"  in s:                         return "Casanare"
    if "ARAUCA"    in s:                         return "Arauca"
    if "PUTUMAYO"  in s:                         return "Putumayo"
    if "CHOCO"     in s:                         return "Chocó"
    if "GUAJIRA"   in s:                         return "La Guajira"
    if "NARIÑO"    in s:                         return "Nariño"
    if "AMAZONAS"  in s:                         return "Amazonas"
    if "VAUPES"    in s:                         return "Vaupés"
    if "VICHADA"   in s:                         return "Vichada"
    if "GUAINIA"   in s:                         return "Guainía"
    if "GUAVIARE"  in s:                         return "Guaviare"
    if "CAQUETA"   in s:                         return "Caquetá"
    if "SAN ANDRES" in s:                        return "San Andrés"
    return s.title()


def load_data(path: Path = RAW_PATH) -> pd.DataFrame:
    """Carga SNIES_contexto.xlsx, limpia y valida columnas esperadas."""
    if not path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {path}")

    df = pd.read_excel(path)

    missing = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Columnas faltantes en el dataset: {missing}")

    # ── Limpieza de tipos ──────────────────────────────────────────────────
    df["anio"]      = pd.to_numeric(df["anio"],      errors="coerce").astype("Int64")
    df["semestre"]  = pd.to_numeric(df["semestre"],  errors="coerce").astype("Int64")
    df["graduados"] = pd.to_numeric(df["graduados"], errors="coerce").fillna(0)

    # ── Normalizar sexo ────────────────────────────────────────────────────
    df["sexo"] = df["sexo"].str.strip().str.title()
    df["sexo"] = df["sexo"].replace({
        "Hombre":     "Masculino",
        "Mujer":      "Femenino",
        "No Binario": "No binario",
    })

    # ── Normalizar nivel_academico ─────────────────────────────────────────
    df["nivel_academico"] = df["nivel_academico"].str.strip().str.title()
    df["nivel_academico"] = df["nivel_academico"].replace({
        "Pregrado": "Pregrado",
        "Posgrado": "Posgrado",
    })

    # ── Normalizar nivel_formacion ─────────────────────────────────────────
    df["nivel_formacion"] = df["nivel_formacion"].str.strip().str.title()
    df["nivel_formacion"] = df["nivel_formacion"].replace({
        "Universitaria": "Universitario",
    })

    # ── Normalizar metodologia ─────────────────────────────────────────────
    df["metodologia"] = df["metodologia"].str.strip().str.title()
    df["metodologia"] = df["metodologia"].replace({
        "Distancia (Tradicional)":      "Distancia Tradicional",
        "Distancia (Virtual)":          "Virtual",
        "A Distancia":                  "Distancia Tradicional",
        "Híbrida (Presencial-Virtual)": "Híbrida",
        "Presencial-Virtual":           "Híbrida",
        "Virtual-Dual":                 "Virtual",
        "Virtual-A Distancia":          "Virtual",
        "Presencial-Dual":              "Presencial",
        "Dual":                         "Presencial",
    })

    # ── Normalizar area_conocimiento ───────────────────────────────────────
    df["area_conocimiento"] = df["area_conocimiento"].str.strip().str.title()
    df["area_conocimiento"] = df["area_conocimiento"].replace({
        "Sin Clasificar":  "Sin clasificar",
        "Sin Información": "Sin clasificar",
    })

    # ── Normalizar sector ──────────────────────────────────────────────────
    df["sector"] = df["sector"].str.strip().str.title()
    df["sector"] = df["sector"].replace({
        "Oficial":  "Oficial",
        "Privada":  "Privado",
        "Privado":  "Privado",
    })

    # ── Normalizar departamentos ───────────────────────────────────────────
    df["dpto_programa"] = df["dpto_programa"].apply(norm_dpto)
    df["dpto_ies"]      = df["dpto_ies"].apply(norm_dpto)

    # ── Eliminar filas completamente vacías (las del final del Excel) ──────
    df = df.dropna(subset=["institucion", "programa", "anio", "graduados"])
    df = df.reset_index(drop=True)

    print(f"[ingest] Dataset cargado: {df.shape[0]:,} filas, {df.shape[1]} columnas")
    print(f"[ingest] Años disponibles: {sorted(df['anio'].dropna().unique().tolist())}")
    nulos = df.isnull().sum()
    nulos = nulos[nulos > 0]
    print(f"[ingest] Nulos por columna:\n{nulos if len(nulos) > 0 else '  Ninguno ✓'}")

    return df


def get_usta_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra registros de la Universidad Santo Tomás (USTA).
    Las sedes se distinguen por dpto_ies, no por nombre de institución.
    """
    usta = df[df["institucion"].str.contains("SANTO TOMAS", case=False, na=False)].copy()
    print(f"[ingest] Registros USTA: {usta.shape[0]:,}")
    return usta


if __name__ == "__main__":
    df = load_data()
    usta = get_usta_data(df)
    print(usta[["institucion", "dpto_ies", "programa", "anio", "semestre", "graduados"]].head(10))