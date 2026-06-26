# -*- coding: utf-8 -*-
"""Re-consulta SECOP para obtener el departamento de la entidad contratante por
   cada graduado proveedor. Guarda salidas/secop_regional.csv."""
import time
import pandas as pd
import requests

API="https://www.datos.gov.co/resource/rpmr-utcd.json"
SES=requests.Session()
SAL="salidas"

g=pd.read_csv(f"{SAL}/graduados_integrado.csv", usecols=["identificacion"], dtype=str)
ced=g["identificacion"].dropna().str.strip()
ced=sorted(ced[ced.str.fullmatch(r"\d{4,12}")].unique())
print("cédulas:", len(ced), flush=True)

SEL=("documento_proveedor, departamento_entidad,"
     "count(1) as n_contratos, sum(valor_contrato) as valor")
def lote(cs, intentos=4):
    inlist=",".join("'%s'"%c for c in cs)
    p={"$select":SEL,"$where":f"documento_proveedor in ({inlist})",
       "$group":"documento_proveedor, departamento_entidad","$limit":50000}
    for k in range(intentos):
        try:
            r=SES.get(API, params=p, timeout=120); r.raise_for_status(); return r.json()
        except Exception:
            if k==intentos-1: raise
            time.sleep(2**k)
    return []

res=[]; B=250; t0=time.time(); tot=(len(ced)+B-1)//B
for i in range(0,len(ced),B):
    res.extend(lote(ced[i:i+B]))
    j=i//B+1
    if j%20==0 or j==tot:
        print(f"lote {j}/{tot} | filas {len(res)} | {time.time()-t0:.0f}s", flush=True)
    time.sleep(0.1)

df=pd.DataFrame(res).rename(columns={"documento_proveedor":"identificacion",
                                     "departamento_entidad":"departamento"})
df["n_contratos"]=pd.to_numeric(df["n_contratos"],errors="coerce")
df["valor"]=pd.to_numeric(df["valor"],errors="coerce")
df["departamento"]=df["departamento"].fillna("(sin dato)").str.strip()
df.to_csv(f"{SAL}/secop_regional.csv", index=False, encoding="utf-8-sig")
print("FIN | filas:", len(df), "| cédulas:", df["identificacion"].nunique(), flush=True)
