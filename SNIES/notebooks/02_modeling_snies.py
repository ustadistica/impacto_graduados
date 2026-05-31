"""
02_modeling_snies.py
Modelado para predecir el número de graduados — Graduados SNIES
Ejecutar desde la raíz del proyecto: python notebooks/02_modeling_snies.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'src'))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import joblib
import warnings
warnings.filterwarnings("ignore")
from pathlib import Path

from sklearn.model_selection import train_test_split, cross_val_score, KFold
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import Ridge, Lasso
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from ingest import load_data
from features import build_feature_matrix

# ── Config ────────────────────────────────────────────────────────────────────
sns.set_theme(style="whitegrid", font_scale=1.1)
plt.rcParams["axes.spines.top"] = False
plt.rcParams["axes.spines.right"] = False
fmt_miles = mticker.FuncFormatter(lambda x, _: f"{int(x):,}")

Path("models").mkdir(exist_ok=True)
Path("artifacts").mkdir(exist_ok=True)


def seccion(titulo):
    print(f"\n{'='*55}")
    print(f"  {titulo}")
    print(f"{'='*55}")


# ── 1. CARGA Y PREPARACIÓN ────────────────────────────────────────────────────
seccion("1. CARGA Y PREPARACIÓN DE DATOS")

df = load_data()
X, y, encoders = build_feature_matrix(df)

print(f"  Features  : {X.shape[1]}")
print(f"  Muestras  : {X.shape[0]:,}")
print(f"  Target    : graduados — media={y.mean():.2f}, std={y.std():.2f}")
print(f"\n  Features usadas:")
for col in X.columns:
    print(f"    - {col}")

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)
print(f"\n  Train : {X_train.shape[0]:,} muestras")
print(f"  Test  : {X_test.shape[0]:,} muestras")


# ── 2. FUNCIÓN DE EVALUACIÓN ──────────────────────────────────────────────────
def evaluar(nombre, y_true, y_pred):
    mae  = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2   = r2_score(y_true, y_pred)
    mape = np.mean(np.abs((y_true - y_pred) / np.clip(y_true, 1, None))) * 100
    print(f"\n  [{nombre}]")
    print(f"    MAE  : {mae:.2f}  graduados de error promedio")
    print(f"    RMSE : {rmse:.2f}")
    print(f"    R²   : {r2:.4f}  ({r2*100:.1f}% varianza explicada)")
    print(f"    MAPE : {mape:.2f}%")
    return {"modelo": nombre, "MAE": mae, "RMSE": rmse, "R2": r2, "MAPE": mape}


# ── 3. ENTRENAMIENTO DE MODELOS ───────────────────────────────────────────────
seccion("2. ENTRENAMIENTO DE MODELOS")

modelos = {
    "Ridge":             Ridge(alpha=1.0),
    "Lasso":             Lasso(alpha=0.1),
    "Random Forest":     RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1),
    "Gradient Boosting": GradientBoostingRegressor(n_estimators=100, random_state=42),
}

resultados = []
modelos_entrenados = {}

for nombre, modelo in modelos.items():
    print(f"\n  Entrenando {nombre}...")
    modelo.fit(X_train, y_train)
    y_pred = modelo.predict(X_test)
    metricas = evaluar(nombre, y_test, y_pred)
    resultados.append(metricas)
    modelos_entrenados[nombre] = (modelo, y_pred)


# ── 4. COMPARACIÓN DE MODELOS ─────────────────────────────────────────────────
seccion("3. COMPARACIÓN DE MODELOS")

df_res = pd.DataFrame(resultados).set_index("modelo")
print(df_res.round(4).to_string())

colores = ["#EF5350", "#AB47BC", "#42A5F5", "#26A69A"]
fig, axes = plt.subplots(1, 3, figsize=(15, 5))

for ax, metric in zip(axes, ["MAE", "RMSE", "R2"]):
    bars = ax.bar(df_res.index, df_res[metric], color=colores, edgecolor="white")
    ax.set_title(f"{metric}", fontweight="bold", fontsize=13)
    ax.set_ylabel(metric)
    for bar, val in zip(bars, df_res[metric]):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.01,
                f"{val:.3f}", ha="center", fontsize=9)
    plt.setp(ax.get_xticklabels(), rotation=20, ha="right")

plt.suptitle("Comparación de Modelos — Test Set", fontsize=14, fontweight="bold")
plt.tight_layout()
plt.savefig("artifacts/modeling_01_comparacion.png", dpi=150)
plt.show()


# ── 5. ANÁLISIS DEL MEJOR MODELO ─────────────────────────────────────────────
seccion("4. ANÁLISIS DEL MEJOR MODELO")

mejor_nombre = df_res["R2"].idxmax()
mejor_modelo, y_pred_mejor = modelos_entrenados[mejor_nombre]
print(f"  Mejor modelo : {mejor_nombre}")
print(f"  R²           : {df_res.loc[mejor_nombre, 'R2']:.4f}")
print(f"  MAE          : {df_res.loc[mejor_nombre, 'MAE']:.2f}")

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

axes[0].scatter(y_test, y_pred_mejor, alpha=0.3, color="#1565C0", s=12)
lim = [0, max(float(y_test.max()), float(y_pred_mejor.max())) * 1.05]
axes[0].plot(lim, lim, "r--", lw=1.5, label="Predicción perfecta")
axes[0].set_xlabel("Graduados Reales")
axes[0].set_ylabel("Graduados Predichos")
axes[0].set_title(f"{mejor_nombre} — Real vs Predicho", fontweight="bold")
axes[0].legend()

errores = y_pred_mejor - y_test
axes[1].hist(errores, bins=50, color="#43A047", alpha=0.85, edgecolor="white")
axes[1].axvline(0, color="red", linestyle="--", lw=1.5, label="Error = 0")
axes[1].axvline(errores.mean(), color="orange", linestyle="--", lw=1.5,
                label=f"Media error: {errores.mean():.2f}")
axes[1].set_xlabel("Error (Predicho − Real)")
axes[1].set_ylabel("Frecuencia")
axes[1].set_title("Distribución de Errores", fontweight="bold")
axes[1].legend()

plt.suptitle(f"Análisis del Mejor Modelo: {mejor_nombre}", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.savefig("artifacts/modeling_02_predicciones.png", dpi=150)
plt.show()


# ── 6. FEATURE IMPORTANCE ─────────────────────────────────────────────────────
seccion("5. FEATURE IMPORTANCE")

if hasattr(mejor_modelo, "feature_importances_"):
    importancias = pd.Series(mejor_modelo.feature_importances_, index=X.columns)
    top15 = importancias.sort_values(ascending=True).tail(15)

    fig, ax = plt.subplots(figsize=(11, 7))
    bars = ax.barh(top15.index, top15.values, color="#5E35B1", edgecolor="white")
    for bar, val in zip(bars, top15.values):
        ax.text(bar.get_width() + 0.001, bar.get_y() + bar.get_height() / 2,
                f"{val:.4f}", va="center", fontsize=8)

    ax.set_title(f"Top 15 Features — {mejor_nombre}", fontsize=13, fontweight="bold")
    ax.set_xlabel("Importancia")
    plt.tight_layout()
    plt.savefig("artifacts/modeling_03_feature_importance.png", dpi=150)
    plt.show()

    print("\n  Top 10 features más importantes:")
    print(importancias.sort_values(ascending=False).head(10).round(4).to_string())
else:
    print("  El modelo seleccionado no soporta feature_importances_")


# ── 7. VALIDACIÓN CRUZADA ─────────────────────────────────────────────────────
seccion("6. VALIDACIÓN CRUZADA (5-FOLD)")

kf = KFold(n_splits=5, shuffle=True, random_state=42)
cv_resultados = []

for nombre, modelo in modelos.items():
    scores_r2  = cross_val_score(modelo, X, y, cv=kf, scoring="r2", n_jobs=-1)
    scores_mae = cross_val_score(modelo, X, y, cv=kf, scoring="neg_mean_absolute_error", n_jobs=-1)
    cv_resultados.append({
        "modelo":   nombre,
        "R2_mean":  scores_r2.mean(),
        "R2_std":   scores_r2.std(),
        "MAE_mean": -scores_mae.mean(),
        "MAE_std":  scores_mae.std(),
    })
    print(f"  {nombre:<22} R²: {scores_r2.mean():.4f} ± {scores_r2.std():.4f}  |  MAE: {-scores_mae.mean():.2f} ± {scores_mae.std():.2f}")

df_cv = pd.DataFrame(cv_resultados).set_index("modelo")

fig, axes = plt.subplots(1, 2, figsize=(13, 5))

axes[0].bar(df_cv.index, df_cv["R2_mean"],
            yerr=df_cv["R2_std"], capsize=5,
            color=colores, edgecolor="white")
axes[0].set_title("R² — Validación Cruzada 5-fold", fontweight="bold")
axes[0].set_ylabel("R²")
plt.setp(axes[0].get_xticklabels(), rotation=20, ha="right")

axes[1].bar(df_cv.index, df_cv["MAE_mean"],
            yerr=df_cv["MAE_std"], capsize=5,
            color=colores, edgecolor="white")
axes[1].set_title("MAE — Validación Cruzada 5-fold", fontweight="bold")
axes[1].set_ylabel("MAE")
plt.setp(axes[1].get_xticklabels(), rotation=20, ha="right")

plt.suptitle("Validación Cruzada — Todos los Modelos", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.savefig("artifacts/modeling_04_cross_validation.png", dpi=150)
plt.show()


# ── 8. GUARDAR MEJOR MODELO ───────────────────────────────────────────────────
seccion("7. GUARDAR MODELO")

model_filename = mejor_nombre.lower().replace(" ", "_") + "_graduados.pkl"
model_path = Path("models") / model_filename
joblib.dump(mejor_modelo, model_path)
joblib.dump((X_test, y_test), "artifacts/test_split.pkl")
joblib.dump(encoders,         "artifacts/encoders.pkl")

print(f"  Modelo guardado   : {model_path}")
print(f"  Test split        : artifacts/test_split.pkl")
print(f"  Encoders          : artifacts/encoders.pkl")
print(f"\n✅  Modelado completado. Gráficas guardadas en artifacts/")