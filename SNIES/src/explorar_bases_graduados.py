import pandas as pd
from pathlib import Path

BASE_DIR = Path(r"C:\Users\aleja\Downloads\proyecto_snies_usta")
RAW_DIR  = BASE_DIR / "data" / "raw"

ARCHIVOS = [
    RAW_DIR / "Graduados_2018.xlsx",
    RAW_DIR / "Graduados_2019.xlsx",
    RAW_DIR / "Graduados_2020.xlsx",
    RAW_DIR / "Graduados_2021.xlsx",
    RAW_DIR / "Graduados_2022.xlsx",
    RAW_DIR / "Graduados_2023.xlsx",
    RAW_DIR / "Graduados_2024.xlsx",
]

for path in ARCHIVOS:
    df = pd.read_excel(path)
    print(f"\n=== {path.name} ({len(df.columns)} columnas) ===")
    for c in df.columns:
        print(repr(c))
    