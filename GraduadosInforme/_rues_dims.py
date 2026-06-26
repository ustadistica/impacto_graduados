# -*- coding: utf-8 -*-
"""Extrae dimensiones ricas de RUES por cédula: organización jurídica, tipo de sociedad,
   estado de la matrícula y CIIU. Guarda rues_dims.csv."""
import time, pandas as pd, requests
API="https://www.datos.gov.co/resource/c82u-588k.json"
SES=requests.Session(); SAL="salidas"
g=pd.read_csv(f"{SAL}/graduados_integrado.csv", usecols=["identificacion"], dtype=str)
ced=g["identificacion"].dropna().str.strip(); ced=sorted(ced[ced.str.fullmatch(r"\d{4,12}")].unique())
print("cédulas:", len(ced), flush=True)
SEL=("numero_identificacion, categoria_matricula, organizacion_juridica, estado_matricula,"
     "cod_ciiu_act_econ_pri, count(1) as n")
GRP="numero_identificacion, categoria_matricula, organizacion_juridica, estado_matricula, cod_ciiu_act_econ_pri"
def lote(cs, intentos=5):
    inlist=",".join("'%s'"%c for c in cs)
    p={"$select":SEL,"$where":f"numero_identificacion in ({inlist})","$group":GRP,"$limit":50000}
    for k in range(intentos):
        try:
            r=SES.get(API, params=p, timeout=180); r.raise_for_status(); return r.json()
        except Exception:
            if k==intentos-1: raise
            time.sleep(2**k)
    return []
res=[]; B=300; t0=time.time(); tot=(len(ced)+B-1)//B
for i in range(0,len(ced),B):
    res.extend(lote(ced[i:i+B])); j=i//B+1
    if j%25==0 or j==tot: print(f"lote {j}/{tot} | filas {len(res)} | {time.time()-t0:.0f}s", flush=True)
    time.sleep(0.1)
df=pd.DataFrame(res).rename(columns={"numero_identificacion":"identificacion","cod_ciiu_act_econ_pri":"ciiu"})
df["n"]=pd.to_numeric(df["n"],errors="coerce")
df.to_csv(f"{SAL}/rues_dims.csv", index=False, encoding="utf-8-sig")
print("FIN | filas:", len(df), "| cédulas:", df["identificacion"].nunique(), flush=True)
