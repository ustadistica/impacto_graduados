# -*- coding: utf-8 -*-
"""Capítulo CvLAC: tablas LaTeX y figuras de los cruces:
   nivel de formación, categoría Minciencias, producción por tipo, gran área x carrera."""
import json, re, unicodedata
from pathlib import Path
import numpy as np, pandas as pd
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt

BASE=Path(__file__).resolve().parent
SAL=BASE.parent.parent/"GraduadosInforme"/"salidas"
CVDIR=Path(r"c:\Users\cizai\Dropbox\Documentos\Doctorado Matemáticas\scraper_scienti\data\cvlac")
FIG=BASE/"figs_cruce"; FIG.mkdir(exist_ok=True)
TAB=BASE/"_build_cruce"; TAB.mkdir(exist_ok=True)
AZUL="#1F3A93"; VERDE="#1B998B"; ROJO="#B22222"; DORADO="#E8A317"; MORADO="#5C415D"
plt.rcParams.update({"figure.dpi":130,"savefig.dpi":130,"font.size":9,"axes.grid":True,
                     "grid.alpha":0.25,"axes.spines.top":False,"axes.spines.right":False,"figure.autolayout":True})
def save(n): p=FIG/f"{n}.png"; plt.savefig(p,bbox_inches="tight"); plt.close(); return p.name
def esc(s): return str(s).replace("&","\\&").replace("_","\\_").replace("%","\\%").replace("#","\\#")
def mil(x): return f"{int(x):,}".replace(",", ".")
def pc(x): return f"{x:.1f}".replace(".", ",")+"\\%"
def tex_table(df, caption, label, colfmt, header, path, note=""):
    L=[r"\begin{table}[H]\centering\footnotesize",rf"\caption{{{caption}}}\label{{{label}}}",
       rf"\begin{{tabular}}{{{colfmt}}}",r"\toprule",
       r"\rowcolor{ustaazul}"+" & ".join(rf"\textbf{{\color{{white}}{h}}}" for h in header)+r" \\",r"\midrule"]
    for _,r in df.iterrows(): L.append(" & ".join(str(x) for x in r.values)+r" \\")
    L+=[r"\bottomrule",r"\end{tabular}"]
    if note: L.append(rf"\\[2pt]{{\scriptsize\color{{ustagris}} {note}}}")
    L.append(r"\end{table}"); Path(path).write_text("\n".join(L),encoding="utf-8")

S={}
gi=pd.read_csv(SAL/"graduados_integrado.csv", dtype={"identificacion":"string"})
ced_sec=gi.dropna(subset=["identificacion"]).groupby("identificacion")["sede"].first()
val=pd.read_csv(SAL/"graduados_en_cvlac.csv", dtype=str)
val["total_productos"]=pd.to_numeric(val["total_productos"],errors="coerce").fillna(0)
prof=val.drop_duplicates("cod_rh").copy()   # un perfil por cod_rh
NPROF=len(prof)
S["n_validados"]=int(NPROF); S["n_registros"]=int(len(val))
SECC_LBL={s:s for s in ["Bogotá","Bucaramanga","Tunja","Villavicencio","Medellín","VUAD"]}

# ── Leer JSON: producción por tipo + gran área ──────────────────────
PROD_LBL={"articulos":"Artículos","libros":"Libros","capitulos_libro":"Capítulos de libro",
 "notas_cientificas":"Notas científicas","otra_prod_biblio":"Documentos de trabajo","software":"Software",
 "patentes":"Patentes","innovacion_proceso":"Innovación de proceso","demas_trabajos":"Demás trabajos",
 "textos_no_cientificos":"Textos no científicos","otra_prod_bibliografica":"Otra prod. bibliográfica",
 "informes_tecnicos":"Informes técnicos","informes_investigacion":"Informes de investigación","consultorias":"Consultorías"}
def gran_area(d):
    a=d.get("areas_actuacion") or []
    for line in a:
        parte=str(line).split("--")[0].strip()
        parte=re.sub(r"\s+"," ",parte)
        if len(parte)>3: return parte
    return "(sin área)"

prod_tot={k:0 for k in PROD_LBL}; area_de={}
for c in prof["cod_rh"].dropna():
    p=CVDIR/f"{str(c).zfill(10)}.json"
    if not p.exists(): continue
    try: d=json.load(open(p,encoding="utf-8"))
    except Exception: continue
    for k,v in (d.get("produccion") or {}).items():
        if k in prod_tot: prod_tot[k]+=int(v.get("total",0) or 0)
    area_de[str(c)]=gran_area(d)
prof["gran_area"]=prof["cod_rh"].map(area_de).fillna("(sin área)")

# ── T1 nivel de formación ───────────────────────────────────────────
niv=prof["nivel_maximo"].fillna("No identificado").replace("","No identificado").value_counts()
nt=niv.reset_index(); nt.columns=["nivel","n"]; nt["pct"]=(nt["n"]/NPROF*100).round(1)
df1=pd.DataFrame({"Nivel máximo de formación":nt["nivel"].map(esc),"Investigadores":nt["n"].map(mil),"\\% ":nt["pct"].map(pc)})
tex_table(df1,"Nivel máximo de formación de los graduados validados en CvLAC.","tab:cvlac_nivel","lrr",
          ["Nivel máximo de formación","Investigadores","\\%"],TAB/"tab_cvlac_nivel.tex")
S["nivel"]={str(r["nivel"]):int(r["n"]) for _,r in nt.iterrows()}
S["pct_posgrado"]=round(prof["nivel_maximo"].isin(["Doctorado","Maestría","Especialización"]).mean()*100,1)

# ── T2 categoría Minciencias ────────────────────────────────────────
cat=prof["categoria_minciencias"].fillna("").replace("","Sin categoría").value_counts()
ct=cat.reset_index(); ct.columns=["cat","n"]; ct["pct"]=(ct["n"]/NPROF*100).round(1)
df2=pd.DataFrame({"Categoría Minciencias":ct["cat"].map(esc),"Investigadores":ct["n"].map(mil),"\\% ":ct["pct"].map(pc)})
tex_table(df2,"Categoría de investigador (Minciencias) de los graduados validados.","tab:cvlac_categoria","lrr",
          ["Categoría Minciencias","Investigadores","\\%"],TAB/"tab_cvlac_categoria.tex")
S["categoria"]={str(r["cat"]):int(r["n"]) for _,r in ct.iterrows()}
S["n_categorizados"]=int(NPROF-cat.get("Sin categoría",0))

# ── T3 producción por tipo ──────────────────────────────────────────
ps=pd.Series(prod_tot).sort_values(ascending=False); TOTP=ps.sum()
pt=ps[ps>0].head(10).reset_index(); pt.columns=["k","n"]; pt["lbl"]=pt["k"].map(PROD_LBL); pt["pct"]=(pt["n"]/TOTP*100).round(1)
df3=pd.DataFrame({"Tipo de producto":pt["lbl"].map(esc),"Productos":pt["n"].map(mil),"\\% ":pt["pct"].map(pc)})
tex_table(df3,"Producción científica de los graduados validados, por tipo de producto (CvLAC).","tab:cvlac_prod","lrr",
          ["Tipo de producto","Productos","\\%"],TAB/"tab_cvlac_prod.tex",
          f"Total de {mil(TOTP)} productos registrados por los {mil(NPROF)} investigadores validados.")
S["prod_total"]=int(TOTP); S["prod_top"]={PROD_LBL[r['k']]:int(r['n']) for _,r in pt.iterrows()}

# ── T4 por carrera ──────────────────────────────────────────────────
val["prog"]=val["programa"].fillna("")  # programa del graduado
vv=val.drop_duplicates(["identificacion","cod_rh"]).copy()
vv["prog_norm"]=vv["identificacion"].map(gi.dropna(subset=["identificacion"]).groupby("identificacion")["programa_norm"].first())
vv["posg"]=vv["nivel_maximo"].isin(["Doctorado","Maestría","Especialización"])
vv["doc"]=vv["nivel_maximo"]=="Doctorado"
cp=(vv.groupby("prog_norm").agg(investigadores=("cod_rh","nunique"),
        prod_med=("total_productos","median"),
        pct_doc=("doc","mean")).sort_values("investigadores",ascending=False).head(12).reset_index())
df4=pd.DataFrame({"Programa (carrera)":cp["prog_norm"].map(lambda x: esc(str(x).title()[:30])),
                  "Investigadores":cp["investigadores"].map(mil),
                  "\\% doctorado":cp["pct_doc"].map(lambda x: pc(x*100)),
                  "Productos (mediana)":cp["prod_med"].map(lambda x: mil(round(x)))})
tex_table(df4,"Investigación por \\textbf{carrera}: programas con más graduados validados en CvLAC.","tab:cvlac_carrera","lrrr",
          ["Programa (carrera)","Investigadores","\\% doctorado","Productos (mediana)"],TAB/"tab_cvlac_carrera.tex",
          "Programa de origen del graduado. Ordenado por nº de investigadores.")
S["carrera_top"]=[{"prog":r["prog_norm"],"investigadores":int(r["investigadores"]),"pct_doc":round(float(r["pct_doc"])*100,1)} for _,r in cp.iterrows()]

# ── FIG C1 nivel + categoría ────────────────────────────────────────
fig,ax=plt.subplots(1,2,figsize=(10,3.8))
niv2=niv.reindex([x for x in ["Doctorado","Maestría","Especialización","Pregrado","No identificado"] if x in niv.index])
ax[0].barh(niv2.index[::-1], niv2.values[::-1], color=MORADO); ax[0].set_title("Nivel máximo de formación")
cc=cat.drop(labels=["Sin categoría"],errors="ignore")
ax[1].barh(cc.index[::-1], cc.values[::-1], color=DORADO); ax[1].set_title("Categoría Minciencias (categorizados)")
save("C1_cvlac_perfil")

# ── FIG C2 producción por tipo ──────────────────────────────────────
plt.figure(figsize=(7.2,4.0)); pp=ps[ps>0].head(8)
plt.barh([PROD_LBL.get(k,k) for k in pp.index[::-1]], pp.values[::-1], color=VERDE)
plt.xlabel("Productos"); plt.title("Producción científica por tipo de producto (CvLAC)"); save("C2_cvlac_prod")

# ── FIG C3 gran área × carrera (firma disciplinar investigativa) ────
topprogs=cp["prog_norm"].head(7).tolist()
prof2=prof.merge(vv[["cod_rh","prog_norm"]].drop_duplicates("cod_rh"), on="cod_rh", how="left")
topar=prof2["gran_area"].value_counts().drop(labels=["(sin área)"],errors="ignore").head(6).index.tolist()
he=prof2[prof2["prog_norm"].isin(topprogs) & prof2["gran_area"].isin(topar)]
pv=he.pivot_table(index="prog_norm",columns="gran_area",values="cod_rh",aggfunc="nunique",fill_value=0).reindex(topprogs)
cols=[c for c in topar if c in pv.columns]; pv=pv[cols]
pvp=(pv.div(pv.sum(axis=1).replace(0,np.nan),axis=0)*100)
plt.figure(figsize=(9.2,4.2)); plt.imshow(pvp.values,cmap="Purples",aspect="auto")
plt.yticks(range(len(topprogs)),[str(p).title()[:22] for p in topprogs],fontsize=8)
plt.xticks(range(len(cols)),[esc(c)[:18] for c in cols],rotation=25,ha="right",fontsize=8)
for i in range(len(topprogs)):
    for j in range(len(cols)):
        v=pvp.values[i,j]
        if v==v: plt.text(j,i,f"{v:.0f}",ha="center",va="center",fontsize=7,color="white" if v>50 else "black")
plt.colorbar(label="% de investigadores"); plt.title("Gran área de conocimiento por carrera (CvLAC)"); save("C3_cvlac_carrera_area")
S["carrera_area"]={str(p):{c:(round(float(pvp.loc[p,c]),1) if pvp.loc[p,c]==pvp.loc[p,c] else 0) for c in cols} for p in topprogs}

# ── por seccional ───────────────────────────────────────────────────
vv["seccional"]=vv["identificacion"].map(ced_sec)
ss=vv[vv["seccional"].isin(["Bogotá","Bucaramanga","Tunja","Villavicencio","Medellín","VUAD"])].groupby("seccional")["cod_rh"].nunique()
S["validados_seccional"]={SECC_LBL[k]:int(v) for k,v in ss.items()}
S["gran_area_top"]={k:int(v) for k,v in prof2["gran_area"].value_counts().drop(labels=["(sin área)"],errors="ignore").head(7).items()}

(TAB/"cvlac_stats.json").write_text(json.dumps(S,ensure_ascii=False,indent=2),encoding="utf-8")
print("Tablas y figuras CvLAC generadas.")
print("validados:",NPROF,"| %posgrado:",S["pct_posgrado"],"| categorizados:",S["n_categorizados"])
print("prod total:",S["prod_total"],"| top:",list(S["prod_top"].items())[:5])
print("grandes áreas:",list(S["gran_area_top"].items()))
print("firma área:")
for p,d in S["carrera_area"].items(): print("  ",p[:22],"->",", ".join(f"{c} {v:.0f}" for c,v in sorted(d.items(),key=lambda x:-x[1])[:2]))
print("carrera:",[(c["prog"],c["investigadores"],c["pct_doc"]) for c in S["carrera_top"][:6]])
print("seccional:",S["validados_seccional"])
