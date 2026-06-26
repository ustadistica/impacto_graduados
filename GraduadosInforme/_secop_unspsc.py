# -*- coding: utf-8 -*-
"""Cruza los graduados con SECOP II (jbjy-vk9h) para traer el clasificador UNSPSC
   (codigo_de_categoria_principal) de sus contratos. Guarda secop_unspsc.csv."""
import time, pandas as pd, requests
API="https://www.datos.gov.co/resource/jbjy-vk9h.json"
SES=requests.Session(); SAL="salidas"
g=pd.read_csv(f"{SAL}/graduados_integrado.csv", usecols=["identificacion"], dtype=str)
ced=g["identificacion"].dropna().str.strip(); ced=sorted(ced[ced.str.fullmatch(r"\d{4,12}")].unique())
print("cédulas:", len(ced), flush=True)
SEL=("documento_proveedor, codigo_de_categoria_principal,"
     "count(1) as n, sum(valor_del_contrato) as valor")
GRP="documento_proveedor, codigo_de_categoria_principal"
def lote(cs, intentos=5):
    inlist=",".join("'%s'"%c for c in cs)
    p={"$select":SEL,"$where":f"documento_proveedor in ({inlist})","$group":GRP,"$limit":50000}
    for k in range(intentos):
        try:
            r=SES.get(API, params=p, timeout=120); r.raise_for_status(); return r.json()
        except Exception:
            if k==intentos-1: raise
            time.sleep(2**k)
    return []
res=[]; B=300; t0=time.time(); tot=(len(ced)+B-1)//B
for i in range(0,len(ced),B):
    res.extend(lote(ced[i:i+B])); j=i//B+1
    if j%25==0 or j==tot: print(f"lote {j}/{tot} | filas {len(res)} | {time.time()-t0:.0f}s", flush=True)
    time.sleep(0.1)
df=pd.DataFrame(res).rename(columns={"documento_proveedor":"identificacion",
                                     "codigo_de_categoria_principal":"unspsc"})
df["n"]=pd.to_numeric(df["n"],errors="coerce"); df["valor"]=pd.to_numeric(df["valor"],errors="coerce")
df.to_csv(f"{SAL}/secop_unspsc.csv", index=False, encoding="utf-8-sig")
print("FIN | filas:", len(df), "| cédulas:", df["identificacion"].nunique(), "| contratos II:", int(df["n"].sum()), flush=True)
