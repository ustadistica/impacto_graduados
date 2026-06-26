# -*- coding: utf-8 -*-
"""Extrae, por graduado proveedor, las dimensiones del contrato en SECOP:
   tipo_de_contrato, modalidad_de_contrataci_n, nivel_entidad. Guarda secop_dims.csv."""
import time
import pandas as pd
import requests

API="https://www.datos.gov.co/resource/rpmr-utcd.json"
SES=requests.Session(); SAL="salidas"
g=pd.read_csv(f"{SAL}/graduados_integrado.csv", usecols=["identificacion"], dtype=str)
ced=g["identificacion"].dropna().str.strip(); ced=sorted(ced[ced.str.fullmatch(r"\d{4,12}")].unique())
print("cédulas:", len(ced), flush=True)

SEL=("documento_proveedor, tipo_de_contrato, modalidad_de_contrataci_n, nivel_entidad,"
     "count(1) as n_contratos, sum(valor_contrato) as valor")
GRP="documento_proveedor, tipo_de_contrato, modalidad_de_contrataci_n, nivel_entidad"
def lote(cs, intentos=5):
    inlist=",".join("'%s'"%c for c in cs)
    p={"$select":SEL,"$where":f"documento_proveedor in ({inlist})","$group":GRP,"$limit":50000}
    for k in range(intentos):
        try:
            r=SES.get(API, params=p, timeout=180); r.raise_for_status(); return r.json()
        except Exception:
            if k==intentos-1: raise
            time.sleep(2**k)
    return []

res=[]; B=200; t0=time.time(); tot=(len(ced)+B-1)//B
for i in range(0,len(ced),B):
    res.extend(lote(ced[i:i+B])); j=i//B+1
    if j%25==0 or j==tot: print(f"lote {j}/{tot} | filas {len(res)} | {time.time()-t0:.0f}s", flush=True)
    time.sleep(0.1)

df=pd.DataFrame(res).rename(columns={"documento_proveedor":"identificacion",
                                     "modalidad_de_contrataci_n":"modalidad"})
df["n_contratos"]=pd.to_numeric(df["n_contratos"],errors="coerce")
df["valor"]=pd.to_numeric(df["valor"],errors="coerce")
for c in ["tipo_de_contrato","modalidad","nivel_entidad"]:
    df[c]=df[c].fillna("(sin dato)").str.strip()
df.to_csv(f"{SAL}/secop_dims.csv", index=False, encoding="utf-8-sig")
print("FIN | filas:", len(df), "| cédulas:", df["identificacion"].nunique(), flush=True)
