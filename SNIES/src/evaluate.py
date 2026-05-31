"""
evaluate.py
Genera métricas detalladas y visualizaciones del modelo de graduados.
"""
import pandas as pd
import numpy as np
import joblib
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")  # para entornos sin pantalla
from pathlib import Path
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

MODELS_DIR = Path("models")
ARTIFACTS_DIR = Path("artifacts")


def load_model_and_data(model_type: str = "random_forest"):
    model = joblib.load(MODELS_DIR / f"{model_type}_graduados.pkl")
    X_test, y_test = joblib.load(ARTIFACTS_DIR / "test_split.pkl")
    return model, X_test, y_test


def print_metrics(y_true, y_pred):
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2 = r2_score(y_true, y_pred)
    mape = np.mean(np.abs((y_true - y_pred) / np.clip(y_true, 1, None))) * 100

    print("=" * 40)
    print("      MÉTRICAS DE EVALUACIÓN")
    print("=" * 40)
    print(f"  MAE  : {mae:.2f}")
    print(f"  RMSE : {rmse:.2f}")
    print(f"  R²   : {r2:.4f}")
    print(f"  MAPE : {mape:.2f}%")
    print("=" * 40)
    return {"MAE": mae, "RMSE": rmse, "R2": r2, "MAPE": mape}


def plot_predictions(y_true, y_pred, save_path: Path = ARTIFACTS_DIR / "pred_vs_real.png"):
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Real vs predicho
    axes[0].scatter(y_true, y_pred, alpha=0.3, color="#2196F3", s=10)
    lims = [min(y_true.min(), y_pred.min()), max(y_true.max(), y_pred.max())]
    axes[0].plot(lims, lims, "r--", linewidth=1.5, label="Predicción perfecta")
    axes[0].set_xlabel("Graduados reales")
    axes[0].set_ylabel("Graduados predichos")
    axes[0].set_title("Real vs Predicho")
    axes[0].legend()

    # Distribución de errores
    errors = y_pred - y_true
    axes[1].hist(errors, bins=50, color="#4CAF50", alpha=0.7, edgecolor="black")
    axes[1].axvline(0, color="red", linestyle="--")
    axes[1].set_xlabel("Error (predicho - real)")
    axes[1].set_ylabel("Frecuencia")
    axes[1].set_title("Distribución de errores")

    plt.tight_layout()
    save_path.parent.mkdir(exist_ok=True)
    plt.savefig(save_path, dpi=150)
    print(f"[evaluate] Gráfica guardada en: {save_path}")
    plt.close()


def plot_feature_importance(model, feature_names: list, save_path: Path = ARTIFACTS_DIR / "feature_importance.png"):
    if not hasattr(model, "feature_importances_"):
        print("[evaluate] El modelo no soporta feature importances.")
        return

    importances = pd.Series(model.feature_importances_, index=feature_names)
    top = importances.sort_values(ascending=False).head(15)

    plt.figure(figsize=(10, 6))
    top.sort_values().plot(kind="barh", color="#FF5722")
    plt.title("Top 15 Features por Importancia")
    plt.xlabel("Importancia")
    plt.tight_layout()
    plt.savefig(save_path, dpi=150)
    print(f"[evaluate] Feature importance guardada en: {save_path}")
    plt.close()


if __name__ == "__main__":
    model, X_test, y_test = load_model_and_data("random_forest")
    y_pred = model.predict(X_test)

    print_metrics(y_test, y_pred)
    plot_predictions(np.array(y_test), y_pred)
    plot_feature_importance(model, X_test.columns.tolist())