"""
train.py
Entrena un modelo de regresión para predecir el número de graduados.
Guarda el modelo y los artefactos en la carpeta models/.
"""
import pandas as pd
import numpy as np
import joblib
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

MODELS_DIR = Path("models")
ARTIFACTS_DIR = Path("artifacts")


def train_model(X: pd.DataFrame, y: pd.Series, model_type: str = "random_forest"):
    """
    Entrena el modelo seleccionado.
    model_type: 'random_forest' | 'gradient_boosting' | 'ridge'
    """
    MODELS_DIR.mkdir(exist_ok=True)
    ARTIFACTS_DIR.mkdir(exist_ok=True)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    models = {
        "random_forest": RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1),
        "gradient_boosting": GradientBoostingRegressor(n_estimators=100, random_state=42),
        "ridge": Ridge(alpha=1.0),
    }

    model = models.get(model_type)
    if model is None:
        raise ValueError(f"Modelo no reconocido: {model_type}")

    print(f"[train] Entrenando {model_type} con {X_train.shape[0]:,} muestras...")
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    metrics = {
        "MAE": mean_absolute_error(y_test, y_pred),
        "RMSE": np.sqrt(mean_squared_error(y_test, y_pred)),
        "R2": r2_score(y_test, y_pred),
    }

    print(f"[train] Métricas en test:")
    for k, v in metrics.items():
        print(f"        {k}: {v:.4f}")

    # Guardar modelo
    model_path = MODELS_DIR / f"{model_type}_graduados.pkl"
    joblib.dump(model, model_path)
    print(f"[train] Modelo guardado en: {model_path}")

    # Guardar split para evaluación
    joblib.dump((X_test, y_test), ARTIFACTS_DIR / "test_split.pkl")

    return model, metrics, X_test, y_test


if __name__ == "__main__":
    from ingest import load_data
    from features import build_feature_matrix

    df = load_data()
    X, y, encoders = build_feature_matrix(df)
    model, metrics, X_test, y_test = train_model(X, y, model_type="random_forest")