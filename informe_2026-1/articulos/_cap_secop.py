# -*- coding: utf-8 -*-
"""Capítulo SECOP: genera tablas LaTeX (booktabs) y figuras de cruces:
   tipo de contrato, modalidad, nivel de entidad, carrera x tipo, carrera x región."""
import json, re, unicodedata
from pathlib import Path
import numpy as np, pandas as pd
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt

BASE=Path(__file__).resolve().parent
SAL=BASE.parent.parent/"GraduadosInforme"/"salidas"
FIG=BASE/"figs_cruce"; FIG.mkdir(exist_ok=True)
TAB=BASE/"_build_cruce"; TAB.mkdir(exist_ok=True)
AZUL="#1F3A93"; VERDE="#1B998B"; DORADO="#E8A317"; MORADO="#5C415D"
plt.rcParams.update({"figure.dpi":130,"savefig.dpi":130,"font.size":9,"axes.grid":True,
                     "grid.alpha":0.25,"axes.spines.top":False,"axes.spines.right":False,"figure.autolayout":True})
def na(s): s=unicodedata.normalize("NFKD",str(s)); return "".join(c for c in s if not unicodedata.combining(c))
def save(n): p=FIG/f"{n}.png"; plt.savefig(p,bbox_inches="tight"); plt.close(); return p.name
def cop(v):
    v=float(v)
    if v>=1e12: return f"\\${v/1e12:,.1f} bill."
    if v>=1e9: return f"\\${v/1e9:,.1f} mil mill."
    if v>=1e6: return f"\\${v/1e6:,.1f} mill."
    return f"\\${v:,.0f}"
def esc(s): return str(s).replace("&","\\&").replace("_","\\_").replace("%","\\%").replace("#","\\#")
def tex_table(df, caption, label, colfmt, header, path, note=""):
    lines=[r"\begin{table}[H]\centering\footnotesize",
           rf"\caption{{{caption}}}\label{{{label}}}",
           rf"\begin{{tabular}}{{{colfmt}}}", r"\toprule",
           r"\rowcolor{ustaazul}"+" & ".join(rf"\textbf{{\color{{white}}{h}}}" for h in header)+r" \\", r"\midrule"]
    for _,r in df.iterrows():
        lines.append(" & ".join(str(x) for x in r.values)+r" \\")
    lines+=[r"\bottomrule", r"\end{tabular}"]
    if note: lines.append(rf"\\[2pt]{{\scriptsize\color{{ustagris}} {note}}}")
    lines.append(r"\end{table}")
    Path(path).write_text("\n".join(lines), encoding="utf-8")

S={}
gi=pd.read_csv(SAL/"graduados_integrado.csv", dtype={"identificacion":"string"})
prov=pd.read_csv(SAL/"graduados_proveedores_secop.csv", dtype={"identificacion":"string"})
reg=pd.read_csv(SAL/"secop_regional.csv", dtype={"identificacion":"string"})
dims=pd.read_csv(SAL/"secop_dims.csv", dtype={"identificacion":"string"})
# Consolidar categorías duplicadas por mayúsculas/tildes
for c in ["tipo_de_contrato","modalidad","nivel_entidad"]:
    dims[c]=dims[c].fillna("(sin dato)").str.strip().str.title()
dims["nivel_entidad"]=dims["nivel_entidad"].replace({"Corporación Autónoma":"Territorial","No Definido":"(sin dato)"})

# cédula -> programa primario y seccional
ced_prog=gi.dropna(subset=["identificacion"]).groupby("identificacion")["programa_norm"].agg(lambda s: s.value_counts().index[0])
ced_sec=gi.dropna(subset=["identificacion"]).groupby("identificacion")["fuente"].first()
SECC={"General":"Bucaramanga","Tunja":"Tunja","Villavicencio":"Villavicencio"}

TOTV=pd.to_numeric(prov["valor_total"],errors="coerce").sum()
TOTC=pd.to_numeric(prov["n_contratos"],errors="coerce").sum()
NPROV=prov["identificacion"].nunique()
S["n_prov"]=int(NPROV); S["n_contratos"]=int(TOTC); S["valor_total"]=float(TOTV)

# ── T1 tipo de contrato ─────────────────────────────────────────────
t=(dims.groupby("tipo_de_contrato").agg(contratos=("n_contratos","sum"),valor=("valor","sum"),
        proveedores=("identificacion","nunique")).sort_values("contratos",ascending=False))
t["%c"]=(t["contratos"]/t["contratos"].sum()*100).round(1)
top=t.head(10).reset_index()
dft=pd.DataFrame({"Tipo de contrato":top["tipo_de_contrato"].map(esc),
                  "Proveedores":top["proveedores"].map(lambda x:f"{int(x):,}"),
                  "Contratos":top["contratos"].map(lambda x:f"{int(x):,}"),
                  "\\% contr.":top["%c"].map(lambda x:f"{x:.1f}"),
                  "Valor":top["valor"].map(cop)})
tex_table(dft,"Contratación de los graduados por \\textbf{tipo de contrato} (SECOP Integrado).",
          "tab:secop_tipo","lrrrr",["Tipo de contrato","Proveedores","Contratos","\\% contr.","Valor"],
          TAB/"tab_secop_tipo.tex","Ordenado por número de contratos. El valor está sesgado por pocos megacontratos de fiducia.")
S["tipo_top"]={r["tipo_de_contrato"]:{"contratos":int(r["contratos"]),"valor":float(r["valor"]),"pct_c":float(r["%c"])} for _,r in top.iterrows()}

# ── T2 modalidad ────────────────────────────────────────────────────
m=(dims.groupby("modalidad").agg(contratos=("n_contratos","sum"),valor=("valor","sum"),
        proveedores=("identificacion","nunique")).sort_values("valor",ascending=False))
m["%valor"]=(m["valor"]/m["valor"].sum()*100).round(1); topm=m.head(8).reset_index()
dfm=pd.DataFrame({"Modalidad de contratación":topm["modalidad"].map(esc),
                  "Proveedores":topm["proveedores"].map(lambda x:f"{int(x):,}"),
                  "Contratos":topm["contratos"].map(lambda x:f"{int(x):,}"),
                  "Valor":topm["valor"].map(cop),"\\% valor":topm["%valor"].map(lambda x:f"{x:.1f}")})
tex_table(dfm,"Contratación de los graduados por \\textbf{modalidad de contratación}.",
          "tab:secop_modalidad","lrrrr",["Modalidad","Proveedores","Contratos","Valor","\\% valor"],
          TAB/"tab_secop_modalidad.tex")
S["modalidad_top"]={r["modalidad"]:{"valor":float(r["valor"]),"pct":float(r["%valor"])} for _,r in topm.iterrows()}

# ── T3 nivel entidad ────────────────────────────────────────────────
nv=(dims.groupby("nivel_entidad").agg(contratos=("n_contratos","sum"),valor=("valor","sum")).sort_values("valor",ascending=False))
nv["%c"]=(nv["contratos"]/nv["contratos"].sum()*100).round(1); nv["%v"]=(nv["valor"]/nv["valor"].sum()*100).round(1)
topn=nv.reset_index()
dfn=pd.DataFrame({"Nivel de la entidad":topn["nivel_entidad"].map(esc),
                  "Contratos":topn["contratos"].map(lambda x:f"{int(x):,}"),
                  "\\% contratos":topn["%c"].map(lambda x:f"{x:.1f}"),
                  "Valor":topn["valor"].map(cop),"\\% valor":topn["%v"].map(lambda x:f"{x:.1f}")})
tex_table(dfn,"Contratación de los graduados por \\textbf{nivel de la entidad} contratante.",
          "tab:secop_nivel","lrrrr",["Nivel de la entidad","Contratos","\\% contratos","Valor","\\% valor"],
          TAB/"tab_secop_nivel.tex")
S["nivel"]={r["nivel_entidad"]:{"pct_c":float(r["%c"]),"pct_v":float(r["%v"])} for _,r in topn.iterrows()}

# ── Atípico: el valor agregado está dominado por pocos megacontratos ─
prov["val"]=pd.to_numeric(prov["valor_total"],errors="coerce")
vmax=float(prov["val"].max()); vtot=float(prov["val"].sum())
S["outlier"]={"valor_max":vmax,"valor_total":vtot,"valor_total_sin_top1":vtot-vmax,
              "valor_mediana":float(prov["val"].median()),
              "pct_top10_valor":round(prov["val"].sort_values(ascending=False).head(10).sum()/vtot*100,1)}

# ── T4 carrera (top programas) — medidas ROBUSTAS ───────────────────
prov["prog"]=prov["identificacion"].map(ced_prog)
cp=(prov.assign(nc=pd.to_numeric(prov["n_contratos"],errors="coerce"))
    .groupby("prog").agg(proveedores=("identificacion","nunique"),contratos=("nc","sum"),
                         valor=("val","sum"),vmed=("val","median"))
    .sort_values("proveedores",ascending=False).head(12).reset_index())
dfc=pd.DataFrame({"Programa (carrera)":cp["prog"].map(lambda x: esc(str(x).title()[:32])),
                  "Proveedores":cp["proveedores"].map(lambda x:f"{int(x):,}"),
                  "Contratos":cp["contratos"].map(lambda x:f"{int(x):,}"),
                  "Valor (mediana)":cp["vmed"].map(cop),"Valor total":cp["valor"].map(cop)})
tex_table(dfc,"Contratación pública por \\textbf{carrera}: programas con más graduados proveedores.",
          "tab:secop_carrera","lrrrr",["Programa (carrera)","Proveedores","Contratos","Valor (mediana)","Valor total"],
          TAB/"tab_secop_carrera.tex","Programa principal de cada proveedor. Ordenado por nº de proveedores. La mediana evita el sesgo de los megacontratos.")
S["carrera_top"]=[{ "prog":r["prog"],"proveedores":int(r["proveedores"]),"contratos":int(r["contratos"]),
                    "vmed":float(r["vmed"]),"valor":float(r["valor"]) } for _,r in cp.iterrows()]

# ── FIG tipo / modalidad / nivel ────────────────────────────────────
plt.figure(figsize=(7.2,4.0)); tt=t.head(8)
plt.barh([esc(x)[:26] for x in tt.index[::-1]], (tt["valor"][::-1]/1e9), color=AZUL)
plt.xlabel("Valor contratado (miles de millones COP)"); plt.title("Valor por tipo de contrato (SECOP)"); save("S1_secop_tipo")
plt.figure(figsize=(7.2,3.8)); mm=m.head(7)
plt.barh([esc(x)[:26] for x in mm.index[::-1]], (mm["valor"][::-1]/1e9), color=VERDE)
plt.xlabel("Valor contratado (miles de millones COP)"); plt.title("Valor por modalidad de contratación (SECOP)"); save("S2_secop_modalidad")
plt.figure(figsize=(5.6,3.8))
plt.pie(nv["contratos"], labels=[esc(x)[:18] for x in nv.index], autopct="%1.0f%%", colors=[AZUL,VERDE,DORADO,MORADO,"#aaa"][:len(nv)])
plt.title("Contratos por nivel de entidad"); save("S3_secop_nivel")

# ── UNSPSC (objeto del contrato, SECOP II) ──────────────────────────
topprogs=cp["prog"].head(8).tolist()
uns=pd.read_csv(SAL/"secop_unspsc.csv", dtype={"identificacion":"string"})
uns["n"]=pd.to_numeric(uns["n"],errors="coerce")
uns["code"]=uns["unspsc"].astype(str).str.extract(r"(\d{8})")[0]
uns["prog"]=uns["identificacion"].map(ced_prog)
def categoria(code):
    if not isinstance(code,str) or len(code)<2: return "Otros"
    seg=code[:2]; fam=code[:4]
    if fam=="8012": return "Jurídico"
    if seg in ("93","94","92"): return "Cívico y político"
    if seg in ("85","42"): return "Salud"
    if seg=="81": return "Ingeniería y TI"
    if seg in ("72","95","30","31"): return "Construcción y obra"
    if seg=="84": return "Financiero y contable"
    if seg=="77": return "Ambiental"
    if seg=="86": return "Educación"
    if seg in ("70","71"): return "Agro y recursos"
    if seg=="78": return "Transporte"
    if seg=="80": return "Apoyo profesional y gestión"
    return "Otros"
uns["cat"]=uns["code"].map(categoria)
catall=uns.groupby("cat")["n"].sum().sort_values(ascending=False)
TOTU=catall.sum(); S["unspsc_n_contratos"]=int(TOTU)
S["unspsc_generico_pct"]=round(float(catall.get("Apoyo profesional y gestión",0))/TOTU*100,1)
S["unspsc_categorias"]={k:int(v) for k,v in catall.items()}
# Tabla: categorías de objeto (UNSPSC)
ct=catall.head(9).reset_index(); ct.columns=["cat","n"]; ct["pct"]=(ct["n"]/TOTU*100).round(1)
dfu=pd.DataFrame({"Categoría de objeto (UNSPSC)":ct["cat"].map(esc),
                  "Contratos (SECOP II)":ct["n"].map(lambda x:f"{int(x):,}".replace(",", ".")),
                  "\\% ":ct["pct"].map(lambda x:f"{x:.1f}".replace(".", ",")+"\\%")})
tex_table(dfu,"Objeto contratado por los graduados según el clasificador \\textbf{UNSPSC} (SECOP II).",
          "tab:secop_unspsc","lrr",["Categoría de objeto (UNSPSC)","Contratos (SECOP II)","\\%"],
          TAB/"tab_secop_unspsc.tex","Solo contratos electrónicos (SECOP II), ~38\\% del total. El código 80111600 (apoyo profesional) domina la categoría genérica.")
# Heatmap carrera x categoría ESPECIALIZADA (excluye el apoyo genérico)
ESPEC=["Jurídico","Salud","Ingeniería y TI","Construcción y obra","Financiero y contable","Cívico y político","Ambiental","Educación"]
he=uns[uns["prog"].isin(topprogs) & uns["cat"].isin(ESPEC)]
pv=he.pivot_table(index="prog",columns="cat",values="n",aggfunc="sum",fill_value=0).reindex(topprogs).fillna(0)
cols=[c for c in ESPEC if c in pv.columns]; pv=pv[cols]
pvp=pv.div(pv.sum(axis=1).replace(0,np.nan),axis=0)*100
plt.figure(figsize=(9.0,4.4)); plt.imshow(pvp.values,cmap="Blues",aspect="auto")
plt.yticks(range(len(topprogs)),[str(p).title()[:24] for p in topprogs],fontsize=8)
plt.xticks(range(len(cols)),[esc(c)[:16] for c in cols],rotation=30,ha="right",fontsize=8)
for i in range(len(topprogs)):
    for j in range(len(cols)):
        v=pvp.values[i,j]
        if v==v: plt.text(j,i,f"{v:.0f}",ha="center",va="center",fontsize=7,color="white" if v>50 else "black")
plt.colorbar(label="% (entre contratos especializados)"); plt.title("Firma disciplinar: carrera × objeto especializado (UNSPSC, SECOP II)"); save("S6_secop_unspsc")
S["carrera_unspsc"]={str(p):{c:(round(float(pvp.loc[p,c]),1) if pvp.loc[p,c]==pvp.loc[p,c] else 0) for c in cols} for p in topprogs}

# ── carrera x región (heatmap, % contratos por fila) ────────────────
reg["prog"]=reg["identificacion"].map(ced_prog)
reg["dep"]=reg["departamento"].map(lambda s: na(s).strip()).replace({"Distrito Capital de Bogota":"Bogota D.C."})
topdeps=reg.groupby("dep")["n_contratos"].sum().sort_values(ascending=False)
topdeps=topdeps[topdeps.index!="(sin dato)"].head(7).index.tolist()
r2=reg[reg["prog"].isin(topprogs)].assign(dep2=lambda d: d["dep"].where(d["dep"].isin(topdeps),"Otros"))
pr=r2.pivot_table(index="prog",columns="dep2",values="n_contratos",aggfunc="sum",fill_value=0).reindex(topprogs)
cols2=[c for c in topdeps if c in pr.columns]+(["Otros"] if "Otros" in pr.columns else [])
pr=pr[cols2]; prp=pr.div(pr.sum(axis=1),axis=0)*100
plt.figure(figsize=(8.6,4.4)); plt.imshow(prp.values,cmap="Greens",aspect="auto")
plt.yticks(range(len(topprogs)),[str(p).title()[:24] for p in topprogs],fontsize=8)
plt.xticks(range(len(cols2)),[esc(c)[:13] for c in cols2],rotation=30,ha="right",fontsize=8)
for i in range(len(topprogs)):
    for j in range(len(cols2)): plt.text(j,i,f"{prp.values[i,j]:.0f}",ha="center",va="center",fontsize=7,color="white" if prp.values[i,j]>50 else "black")
plt.colorbar(label="% de contratos"); plt.title("Carrera × región del contrato (SECOP)"); save("S5_secop_carrera_region")
S["carrera_region"]={str(p):{esc(c):round(float(prp.loc[p,c]),1) for c in cols2} for p in topprogs}

(TAB/"secop_stats.json").write_text(json.dumps(S,ensure_ascii=False,indent=2),encoding="utf-8")
print("Tablas y figuras SECOP generadas.")
print("tipos:", list(S["tipo_top"].keys())[:5])
print("nivel:", S["nivel"])
print("carrera_top:", [(c["prog"],round(c["valor"]/1e9,1)) for c in S["carrera_top"][:6]])
