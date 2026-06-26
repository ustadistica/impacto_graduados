# -*- coding: utf-8 -*-
"""Categoriza las revistas (ISSN) de los artículos CvLAC vía OpenAlex."""
import time, pandas as pd, requests
SAL="salidas"; MAIL="cizaineam@gmail.com"; SES=requests.Session()
df=pd.read_csv(f"{SAL}/_cvlac_issn_tmp.csv",dtype=str)
issns=sorted(df["issn"].dropna().unique())
print("ISSN únicos:",len(issns),flush=True)
out=[]; t0=time.time()
for i,issn in enumerate(issns,1):
    rec={"issn":issn,"found":0,"name":"","country":"","doaj":"","oa":"","field":"","domain":"","works":0}
    try:
        r=SES.get(f"https://api.openalex.org/sources/issn:{issn}",params={"mailto":MAIL},timeout=30)
        if r.status_code==200:
            d=r.json(); rec["found"]=1; rec["name"]=d.get("display_name") or ""
            rec["country"]=d.get("country_code") or ""; rec["doaj"]=int(bool(d.get("is_in_doaj")))
            rec["oa"]=int(bool(d.get("is_oa"))); rec["works"]=d.get("works_count") or 0
            tp=(d.get("topics") or [])
            if tp:
                rec["field"]=tp[0].get("field",{}).get("display_name") or ""
                rec["domain"]=tp[0].get("domain",{}).get("display_name") or ""
    except Exception:
        pass
    out.append(rec)
    if i%200==0 or i==len(issns): print(f"{i}/{len(issns)} | {time.time()-t0:.0f}s",flush=True)
    time.sleep(0.07)
pd.DataFrame(out).to_csv(f"{SAL}/cvlac_issn_openalex.csv",index=False,encoding="utf-8-sig")
print("FIN | encontradas:",sum(r["found"] for r in out),"de",len(out),flush=True)
