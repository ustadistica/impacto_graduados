"""
SCRIPT 4 - FEATURE ENGINEERING
================================
Prepara y transforma las variables para el modelo.
Genera: features.parquet, targets.parquet, preprocessor.joblib
"""
 
import pandas as pd
import numpy as np
import os
import joblib
from pathlib import Path
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OrdinalEncoder
from sklearn.impute import SimpleImputer
 
# ─────────────────────────────────────────────
BASE        = Path(__file__).parent
RUTA_CSV    = BASE / "output" / "consolidado_2018_2024.csv"
CARPETA_OUT = BASE / "artifacts" / "data_procesada"
 
CARPETA_OUT.mkdir(parents=True, exist_ok=True)
# ─────────────────────────────────────────────
 
print("Cargando datos...")
df = pd.read_csv(RUTA_CSV, low_memory=False)
print(f"Filas: {len(df):,}")
 
# ── 1. Definir features y target ────────────
TARGET = 'punt_global'
 
NUM_COLS = [
    'mod_lectura_critica_punt',
    'mod_razona_cuantitat_punt',
    'mod_comuni_escrita_punt',
    'mod_ingles_punt',
    'mod_competen_ciudada_punt',
    'percentil_global',
]
 
CAT_COLS = [
    'estu_genero',
    'fami_estratovivienda',
    'fami_educacionpadre',
    'fami_educacionmadre',
    'fami_tieneinternet',
    'fami_tienecomputador',
    'estu_metodo_prgm',
    'estu_nivel_prgm_academico',
    'estu_horassemanatrabaja',
]
 
NUM_COLS = [c for c in NUM_COLS if c in df.columns]
CAT_COLS = [c for c in CAT_COLS if c in df.columns]
 
print(f"Features numéricas:   {NUM_COLS}")
print(f"Features categóricas: {CAT_COLS}")
 
# ── 2. Limpiar dataset ──────────────────────
df_model = df[NUM_COLS + CAT_COLS + [TARGET, 'es_usta', 'periodo']].copy()
df_model = df_model.dropna(subset=[TARGET])
print(f"Filas tras eliminar nulos en target: {len(df_model):,}")
 
# ── 3. Separar features y target ────────────
X = df_model[NUM_COLS + CAT_COLS + ['es_usta', 'periodo']]
y = df_model[TARGET]
 
# ── 4. Pipeline de transformación ───────────
num_pipeline = Pipeline([
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler',  StandardScaler()),
])
 
cat_pipeline = Pipeline([
    ('imputer', SimpleImputer(strategy='most_frequent')),
    ('encoder', OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1)),
])
 
preprocessor = ColumnTransformer([
    ('num', num_pipeline, NUM_COLS),
    ('cat', cat_pipeline, CAT_COLS),
], remainder='passthrough')
 
# ── 5. Ajustar y transformar ─────────────────
print("\nAjustando preprocessor...")
X_transformed = preprocessor.fit_transform(X)
 
cols_out = NUM_COLS + CAT_COLS + ['es_usta', 'periodo']
df_features = pd.DataFrame(X_transformed, columns=cols_out)
df_features['punt_global'] = y.values
 
print(f"Shape final: {df_features.shape}")
 
# ── 6. Guardar artefactos ────────────────────
ruta_features     = CARPETA_OUT / "features.parquet"
ruta_targets      = CARPETA_OUT / "targets.parquet"
ruta_preprocessor = CARPETA_OUT / "preprocessor.joblib"
 
df_features[cols_out].to_parquet(ruta_features, index=False)
df_features[['punt_global']].to_parquet(ruta_targets, index=False)
joblib.dump(preprocessor, ruta_preprocessor)
 
print(f"\nArtefactos guardados en: {CARPETA_OUT}")
print(f"  features.parquet    -> {df_features.shape}")
print(f"  targets.parquet     -> {len(y):,} registros")
print(f"  preprocessor.joblib -> listo")
print("Script 4 completado!")