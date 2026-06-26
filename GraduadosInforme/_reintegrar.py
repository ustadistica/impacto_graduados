# -*- coding: utf-8 -*-
"""Re-integra la base. Bucaramanga ahora viene con Pregrado+Posgrado (con modalidad),
   desde 'GRADUADOS SANTOTO BUCARAMANGA - POSGRADO.xlsx' (hojas PREGRADO y POSGRADO)."""
import re, unicodedata
from pathlib import Path
import numpy as np, pandas as pd

D = Path("datos"); SAL = Path("salidas"); SAL.mkdir(exist_ok=True)
F_TUNJA = D / "GRADUADOS UNIVERSIDAD SANTO TOMÁS SECCIONAL TUNJA.xls"
F_VILLA = D / "Base de Graduados Villavicencio para proyecto exclusivo con el programa de Estadística.xls"
F_BUC   = D / "GRADUADOS SANTOTO BUCARAMANGA - POSGRADO.xlsx"   # 2 hojas: PREGRADO, POSGRADO
F_NEW   = D / "Lista de Graduados SPB-CM-CAU_Vr.22_06_2026.xlsx"

def qa(t):
    if not isinstance(t,str): return t
    return "".join(c for c in unicodedata.normalize("NFKD",t) if not unicodedata.combining(c))
def norm_prog(t):
    if pd.isna(t): return np.nan
    return re.sub(r"\s+"," ",qa(str(t)).upper().strip())
def limpiar_id(v):
    if pd.isna(v): return np.nan
    s=re.sub(r"\D","",str(v)); return s if s else np.nan
def componer(pn,sn,pa,sa):
    p=[str(x).strip() for x in (pn,sn,pa,sa) if pd.notna(x) and str(x).strip()]
    return re.sub(r"\s+"," "," ".join(p)).strip() or np.nan

COLS=["fuente","sede","programa","programa_norm","modalidad","tipo_identificacion",
      "identificacion","identificacion_raw","nombre_completo","fecha_grado","anio_grado"]

def cargar_11col(ruta,fuente,sede):
    b=pd.read_excel(ruta); c=b.columns; o=pd.DataFrame(index=b.index)
    o["fuente"]=fuente; o["sede"]=sede
    o["programa"]=b[c[1]].astype("string").str.strip(); o["programa_norm"]=o["programa"].map(norm_prog)
    o["modalidad"]=b[c[2]].astype("string").str.strip()
    o["tipo_identificacion"]=b[c[3]].astype("string").str.strip()
    o["identificacion_raw"]=b[c[4]].astype("string").str.strip()
    o["identificacion"]=o["identificacion_raw"].map(limpiar_id)
    o["nombre_completo"]=[componer(pn,sn,pa,sa) for pn,sn,pa,sa in zip(b[c[7]],b[c[8]],b[c[5]],b[c[6]])]
    o["fecha_grado"]=pd.to_datetime(b[c[10]],errors="coerce")
    anio=pd.to_numeric(b[c[9]],errors="coerce"); o["anio_grado"]=anio.fillna(o["fecha_grado"].dt.year).astype("Int64")
    return o[COLS]

def cargar_bucaramanga(ruta):
    partes=[]
    for sh,mod in [("PREGRADO","Pregrado"),("POSGRADO","Posgrado")]:
        b=pd.read_excel(ruta,sheet_name=sh,header=1)
        b.columns=["tipo_id","num_doc","nombre","programa","fecha"][:len(b.columns)]
        o=pd.DataFrame(index=b.index)
        o["fuente"]="Bucaramanga"; o["sede"]="Bucaramanga"
        o["programa"]=b["programa"].astype("string").str.strip(); o["programa_norm"]=o["programa"].map(norm_prog)
        o["modalidad"]=mod
        o["tipo_identificacion"]=b["tipo_id"].astype("string").str.strip()
        o["identificacion_raw"]=b["num_doc"].astype("string").str.strip()
        o["identificacion"]=o["identificacion_raw"].map(limpiar_id)
        o["nombre_completo"]=b["nombre"].astype("string").str.replace(r"\s+"," ",regex=True).str.strip()
        o["fecha_grado"]=pd.to_datetime(b["fecha"],errors="coerce")
        o["anio_grado"]=o["fecha_grado"].dt.year.astype("Int64")
        partes.append(o[COLS])
    return pd.concat(partes,ignore_index=True)

def sede_canonica(s):
    sl=qa(str(s)).lower().strip()
    if "bogota" in sl and "principal" in sl: return "Bogotá"
    if "medellin" in sl: return "Medellín"
    if sl.startswith("v-") or sl.startswith("v "): return "VUAD"
    return str(s).strip()

def cargar_nuevo(ruta):
    b=pd.read_excel(ruta); b.columns=["identificacion_raw","nombre_completo","programa","modalidad","sede_det","anio_grado"]
    o=pd.DataFrame(index=b.index)
    o["fuente"]="SPB-CM-CAU"; o["sede"]=b["sede_det"].map(sede_canonica)
    o["programa"]=b["programa"].astype("string").str.strip(); o["programa_norm"]=o["programa"].map(norm_prog)
    o["modalidad"]=b["modalidad"].astype("string").str.strip(); o["tipo_identificacion"]=pd.NA
    o["identificacion_raw"]=b["identificacion_raw"].astype("string").str.strip()
    o["identificacion"]=o["identificacion_raw"].map(limpiar_id)
    o["nombre_completo"]=b["nombre_completo"].astype("string").str.replace(r"\s+"," ",regex=True).str.strip()
    o["fecha_grado"]=pd.NaT; o["anio_grado"]=pd.to_numeric(b["anio_grado"],errors="coerce").astype("Int64")
    return o[COLS]

print("Cargando...")
dt=cargar_11col(F_TUNJA,"Tunja","Tunja")
dv=cargar_11col(F_VILLA,"Villavicencio","Villavicencio")
db=cargar_bucaramanga(F_BUC)
dn=cargar_nuevo(F_NEW)
g=pd.concat([db,dt,dv,dn],ignore_index=True)
g.to_csv(SAL/"graduados_integrado.csv",index=False,encoding="utf-8-sig")
print("Registros:",f"{len(g):,}","| Cédulas únicas:",f"{g['identificacion'].nunique():,}")
print("\nRegistros por fuente:"); print(g["fuente"].value_counts().to_string())
print("\nCédulas únicas por sede:")
print(g.dropna(subset=["identificacion"]).groupby("sede")["identificacion"].nunique().sort_values(ascending=False).to_string())
print("\nBucaramanga por modalidad (registros):")
print(g[g["sede"]=="Bucaramanga"]["modalidad"].value_counts().to_string())
print("Modalidad global (registros):"); print(g["modalidad"].value_counts(dropna=False).to_string())
print("Años:",int(g["anio_grado"].min()),"-",int(g["anio_grado"].max()))
