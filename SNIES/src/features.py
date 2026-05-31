"""
features.py
Limpieza, encoding y construcción de features para el modelo de graduados.
"""
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from pathlib import Path


CATEGORICAL_COLS = [
    "sector", "caracter", "nivel_academico", "nivel_formacion",
    "metodologia", "area_conocimiento", "sexo", "dpto_programa",
]

NUMERIC_COLS = [
    "anio", "semestre", "id_sector", "id_caracter",
    "id_nivel_academico", "id_nivel_formacion", "id_metodologia",
    "id_area", "id_nbc", "id_sexo",
    "cod_dpto_programa", "cod_mpio_programa",
]

TARGET = "graduados"


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina filas con nulos en columnas clave y filtra valores atípicos."""
    df = df.copy()

    # Convertir numéricas a float (por si vienen como Int64 o con NaN)
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df[TARGET] = pd.to_numeric(df[TARGET], errors="coerce")

    # Eliminar filas sin target o sin columnas numéricas clave
    df = df.dropna(subset=[TARGET] + [c for c in NUMERIC_COLS if c in df.columns])

    # Eliminar graduados negativos o cero
    df = df[df[TARGET] > 0].copy()

    return df


def encode_features(df: pd.DataFrame) -> tuple:
    """
    Aplica Label Encoding a columnas categóricas.
    Retorna el DataFrame transformado y un diccionario con los encoders.
    """
    encoders = {}
    df_enc = df.copy()

    for col in CATEGORICAL_COLS:
        if col in df_enc.columns:
            le = LabelEncoder()
            # Rellenar nulos con 'Desconocido' antes de encodear
            df_enc[col] = df_enc[col].fillna("Desconocido").astype(str)
            df_enc[col + "_enc"] = le.fit_transform(df_enc[col])
            encoders[col] = le

    return df_enc, encoders


def build_feature_matrix(df: pd.DataFrame) -> tuple:
    """
    Construye la matriz X de features y el vector y (target).
    """
    df_clean = clean_data(df)
    df_enc, encoders = encode_features(df_clean)

    # Features numéricas + categóricas codificadas
    num_disponibles  = [c for c in NUMERIC_COLS if c in df_enc.columns]
    cat_encoded_cols = [c + "_enc" for c in CATEGORICAL_COLS if c in df.columns]
    feature_cols     = num_disponibles + cat_encoded_cols

    X = df_enc[feature_cols].copy()
    y = df_enc[TARGET].copy()

    # Asegurar que todo sea float (sklearn no acepta Int64)
    X = X.apply(pd.to_numeric, errors="coerce").astype(float)
    y = y.astype(float)

    # Eliminar filas con NaN que hayan quedado
    mask = X.notna().all(axis=1) & y.notna()
    X = X[mask].reset_index(drop=True)
    y = y[mask].reset_index(drop=True)

    print(f"[features] X shape: {X.shape}")
    print(f"[features] y shape: {y.shape}, media graduados: {y.mean():.2f}")

    return X, y, encoders


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from ingest import load_data
    df = load_data()
    X, y, encoders = build_feature_matrix(df)
    print(X.head())