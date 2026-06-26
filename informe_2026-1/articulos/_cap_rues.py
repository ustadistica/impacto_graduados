# -*- coding: utf-8 -*-
"""Capítulo RUES: tablas LaTeX (booktabs) y figuras de los cruces:
   organización jurídica, estado/supervivencia, sector CIIU, carrera×sector, supervivencia."""
import json, re, unicodedata
from pathlib import Path
import numpy as np, pandas as pd
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt

BASE=Path(__file__).resolve().parent
SAL=BASE.parent.parent/"GraduadosInforme"/"salidas"
FIG=BASE/"figs_cruce"; FIG.mkdir(exist_ok=True)
TAB=BASE/"_build_cruce"; TAB.mkdir(exist_ok=True)
AZUL="#1F3A93"; VERDE="#1B998B"; ROJO="#B22222"; DORADO="#E8A317"; MORADO="#5C415D"
SC=[AZUL,VERDE,DORADO,ROJO,MORADO,"#6C8EBF"]
plt.rcParams.update({"figure.dpi":130,"savefig.dpi":130,"font.size":9,"axes.grid":True,
                     "grid.alpha":0.25,"axes.spines.top":False,"axes.spines.right":False,"figure.autolayout":True})
def na(s): s=unicodedata.normalize("NFKD",str(s)); return "".join(c for c in s if not unicodedata.combining(c))
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

def sector_ciiu(code):
    s=re.sub(r"\D","",str(code))
    if len(s)<2: return "(sin dato)"
    d=int(s[:2])
    if 1<=d<=3: return "Agropecuario"
    if 5<=d<=9: return "Minería"
    if 10<=d<=33: return "Industria manufacturera"
    if d==35: return "Energía"
    if 36<=d<=39: return "Agua y saneamiento"
    if 41<=d<=43: return "Construcción"
    if 45<=d<=47: return "Comercio"
    if 49<=d<=53: return "Transporte y logística"
    if 55<=d<=56: return "Alojamiento y comida"
    if 58<=d<=63: return "Información y comunicaciones"
    if 64<=d<=66: return "Financieras y seguros"
    if d==68: return "Inmobiliarias"
    if 69<=d<=75: return "Profesionales y técnicas"
    if 77<=d<=82: return "Servicios administrativos"
    if d==84: return "Administración pública"
    if d==85: return "Educación"
    if 86<=d<=88: return "Salud y asistencia social"
    if 90<=d<=93: return "Arte y entretenimiento"
    if 94<=d<=96: return "Otros servicios"
    return "(sin/otro)"

S={}
gi=pd.read_csv(SAL/"graduados_integrado.csv", dtype={"identificacion":"string"})
emp=pd.read_csv(SAL/"graduados_emprendedores_rues.csv", dtype={"identificacion":"string"})
dims=pd.read_csv(SAL/"rues_dims.csv", dtype={"identificacion":"string"})
dims["n"]=pd.to_numeric(dims["n"],errors="coerce")
ced_prog=gi.dropna(subset=["identificacion"]).groupby("identificacion")["programa_norm"].agg(lambda s:s.value_counts().index[0])
ced_sec=gi.dropna(subset=["identificacion"]).groupby("identificacion")["sede"].first()
SECC=["Bogotá","Bucaramanga","Tunja","Villavicencio","Medellín","VUAD"]; SECC_LBL={s:s for s in ["Bogotá","Bucaramanga","Tunja","Villavicencio","Medellín","VUAD"]}

emp["prog"]=emp["identificacion"].map(ced_prog)
emp["activa"]=emp["tiene_empresa_activa"].astype(str).str.lower().isin(["true","1","verdadero"])
emp["sector"]=emp["ciiu_principal"].map(sector_ciiu)
NEMP=emp["identificacion"].nunique()
S["n_emprendedores"]=int(NEMP); S["n_activa"]=int(emp["activa"].sum())
S["pct_activa"]=round(emp["activa"].mean()*100,1)

# ── T1 organización jurídica (tipo de empresa) ──────────────────────
org=(dims.groupby("organizacion_juridica")["n"].sum().sort_values(ascending=False))
org=org/org.sum()*100
oj=org.head(7).reset_index(); oj.columns=["org","pct"]
df1=pd.DataFrame({"Organización jurídica":oj["org"].map(lambda x: esc(str(x).title()[:34])),
                  "\\% de matrículas":oj["pct"].map(lambda x: pc(x))})
tex_table(df1,"Matrículas de los graduados por \\textbf{organización jurídica} (RUES).",
          "tab:rues_org","lr",["Organización jurídica","\\% de matrículas"],TAB/"tab_rues_org.tex",
          "El cruce por cédula capta sobre todo persona natural; las sociedades con NIT no se enlazan por la cédula del socio.")
S["org_top"]={str(r["org"]):round(float(r["pct"]),1) for _,r in oj.iterrows()}

# ── T2 estado / supervivencia ───────────────────────────────────────
est=(dims.groupby("estado_matricula")["n"].sum().sort_values(ascending=False))
estp=est/est.sum()*100; et=estp.head(6).reset_index(); et.columns=["estado","pct"]
df2=pd.DataFrame({"Estado de la matrícula":et["estado"].map(lambda x: esc(str(x).title())),
                  "\\% de matrículas":et["pct"].map(lambda x: pc(x))})
tex_table(df2,"Estado de las matrículas de los graduados (RUES).","tab:rues_estado","lr",
          ["Estado de la matrícula","\\% de matrículas"],TAB/"tab_rues_estado.tex")
S["estado_top"]={str(r["estado"]):round(float(r["pct"]),1) for _,r in et.iterrows()}

# ── T3 sector económico (CIIU) ──────────────────────────────────────
sec=(emp.groupby("sector")["identificacion"].nunique().sort_values(ascending=False))
sec=sec[~sec.index.isin(["(sin dato)","(sin/otro)"])]
secp=(sec/NEMP*100); st=sec.head(10).reset_index(); st.columns=["sector","n"]; st["pct"]=(st["n"]/NEMP*100).round(1)
df3=pd.DataFrame({"Sector económico (CIIU)":st["sector"].map(esc),
                  "Matriculados":st["n"].map(mil),"\\% ":st["pct"].map(lambda x: pc(x))})
tex_table(df3,"Sector económico de las empresas de los graduados, según \\textbf{CIIU} (RUES).",
          "tab:rues_sector","lrr",["Sector económico (CIIU)","Matriculados","\\%"],TAB/"tab_rues_sector.tex",
          "Sector de la actividad principal (división CIIU de la matrícula representativa de cada cédula).")
S["sector_top"]=[{"sector":r["sector"],"n":int(r["n"]),"pct":float(r["pct"])} for _,r in st.iterrows()]

# ── T4 carrera ──────────────────────────────────────────────────────
cp=(emp.groupby("prog").agg(emprendedores=("identificacion","nunique"),
                            matriculas=("n_matriculas",lambda s: pd.to_numeric(s,errors="coerce").sum()),
                            activa=("activa","mean")).sort_values("emprendedores",ascending=False).head(12).reset_index())
df4=pd.DataFrame({"Programa (carrera)":cp["prog"].map(lambda x: esc(str(x).title()[:30])),
                  "Matriculados":cp["emprendedores"].map(mil),
                  "Matrículas":cp["matriculas"].map(mil),
                  "\\% con empresa activa":cp["activa"].map(lambda x: pc(x*100))})
tex_table(df4,"Matrícula mercantil por \\textbf{carrera}: programas con más graduados matriculados en el RUES.",
          "tab:rues_carrera","lrrr",["Programa (carrera)","Matriculados","Matrículas","\\% activa"],
          TAB/"tab_rues_carrera.tex","Programa principal de cada graduado matriculado. Ordenado por nº de matriculados.")
S["carrera_top"]=[{"prog":r["prog"],"emprendedores":int(r["emprendedores"]),"pct_activa":round(float(r["activa"])*100,1)} for _,r in cp.iterrows()]

# ── FIG R1 sectores ─────────────────────────────────────────────────
plt.figure(figsize=(7.2,4.2)); ss=sec.head(10)
plt.barh([esc(x)[:26] for x in ss.index[::-1]], ss.values[::-1], color=VERDE)
plt.xlabel("Matriculados en RUES"); plt.title("Sectores económicos de los graduados matriculados en el RUES (CIIU)"); save("R1_rues_sector")

# ── FIG R2 carrera × sector (firma sectorial) ───────────────────────
topprogs=cp["prog"].head(8).tolist()
topsec=sec.head(7).index.tolist()
he=emp[emp["prog"].isin(topprogs) & emp["sector"].isin(topsec)]
pv=he.pivot_table(index="prog",columns="sector",values="identificacion",aggfunc="nunique",fill_value=0).reindex(topprogs)
cols=[c for c in topsec if c in pv.columns]; pv=pv[cols]
pvp=(pv.div(pv.sum(axis=1).replace(0,np.nan),axis=0)*100)
plt.figure(figsize=(9.2,4.4)); plt.imshow(pvp.values,cmap="Greens",aspect="auto")
plt.yticks(range(len(topprogs)),[str(p).title()[:22] for p in topprogs],fontsize=8)
plt.xticks(range(len(cols)),[esc(c)[:15] for c in cols],rotation=30,ha="right",fontsize=8)
for i in range(len(topprogs)):
    for j in range(len(cols)):
        v=pvp.values[i,j]
        if v==v: plt.text(j,i,f"{v:.0f}",ha="center",va="center",fontsize=7,color="white" if v>50 else "black")
plt.colorbar(label="% de matriculados"); plt.title("Sector económico por carrera (RUES)"); save("R2_rues_carrera_sector")
S["carrera_sector"]={str(p):{c:(round(float(pvp.loc[p,c]),1) if pvp.loc[p,c]==pvp.loc[p,c] else 0) for c in cols} for p in topprogs}

# ── FIG R3 supervivencia por carrera ────────────────────────────────
def prog_label(x):
    s=str(x).title().replace("Especializacion En ","Esp. en ").replace("Especializacion","Especialización")
    s=s.replace("Ingenieria","Ingeniería").replace("Administracion","Administración")
    return s[:30]
sup=(emp.groupby("prog").agg(n=("identificacion","nunique"),act=("activa","mean")))
sup=sup[sup["n"]>=100].sort_values("act").tail(12)          # ascendente: barh deja el mayor arriba
plt.figure(figsize=(8.0,4.6))
b=plt.barh([prog_label(x) for x in sup.index], sup["act"].values*100, color=VERDE)
for r,v in zip(b, sup["act"].values*100):
    plt.text(v+0.6, r.get_y()+r.get_height()/2, f"{v:.0f}%", va="center", fontsize=8)
plt.xlabel("% de matrícula activa"); plt.xlim(0, sup["act"].max()*100*1.12)
plt.title("Matrícula activa por carrera (%)")
save("R3_rues_superv_carrera")
S["superv_carrera"]={str(i):round(float(r["act"])*100,1) for i,r in sup.sort_values("act",ascending=False).iterrows()}

# ── FIG R4 supervivencia por cohorte (año de matrícula) ─────────────
emp["anio_mat"]=pd.to_datetime(emp["primera_matricula"],errors="coerce").dt.year
co=emp.dropna(subset=["anio_mat"]); co=co[(co["anio_mat"]>=1990)&(co["anio_mat"]<=2024)]
coh=co.groupby((co["anio_mat"]//5*5).astype(int)).agg(n=("identificacion","nunique"),act=("activa","mean"))
coh=coh[coh["n"]>=50]
plt.figure(figsize=(7.4,3.8)); plt.plot(coh.index, coh["act"]*100, marker="o", color=VERDE)
plt.xlabel("Quinquenio de la primera matrícula"); plt.ylabel("% activa hoy"); plt.title("Matrícula activa según antigüedad (RUES)"); save("R4_rues_cohorte")
S["superv_cohorte"]={int(i):round(float(r["act"])*100,1) for i,r in coh.iterrows()}

# ── supervivencia por seccional (cruda y controlada por antigüedad) ─
emp["seccional"]=emp["identificacion"].map(ced_sec)
ss=emp[emp["seccional"].isin(SECC)].groupby("seccional")["activa"].mean().reindex(SECC)*100
S["superv_seccional"]={SECC_LBL[s]:round(float(ss[s]),1) for s in SECC}

# FIG R5: supervivencia por seccional dentro de cada cohorte (controla antigüedad)
cc=emp.dropna(subset=["anio_mat"]).copy()
cc=cc[cc["seccional"].isin(SECC) & cc["anio_mat"].between(2005,2024)]
cc["quin"]=(cc["anio_mat"]//5*5).astype(int)
g=cc.groupby(["quin","seccional"]).agg(n=("identificacion","nunique"),act=("activa","mean"))
quins=[q for q in sorted(cc["quin"].unique()) if (g.loc[q]["n"]>=30).all()]
piv=(g["act"].unstack("seccional").reindex(columns=SECC).loc[quins]*100)
x=np.arange(len(quins))
plt.figure(figsize=(7.6,4.0))
for k,s in enumerate(SECC):
    plt.plot(x, piv[s].values, marker="o", ms=5, lw=1.8, color=SC[k], label=SECC_LBL[s])
plt.xticks(x,[f"{q}–{q+4}" for q in quins]); plt.ylabel("% de matrícula activa hoy")
plt.xlabel("Cohorte (quinquenio de la primera matrícula)")
plt.title("Matrícula activa por sede, dentro de cada cohorte"); plt.legend(ncol=2, fontsize=8)
plt.ylim(0, None)
save("R5_rues_superv_seccional_cohorte")
S["superv_seccional_cohorte"]={int(q):{SECC_LBL[s]:round(float(piv.loc[q,s]),1) for s in SECC if piv.loc[q,s]==piv.loc[q,s]} for q in quins}

(TAB/"rues_stats.json").write_text(json.dumps(S,ensure_ascii=False,indent=2),encoding="utf-8")
print("Tablas y figuras RUES generadas.")
print("emprendedores:",NEMP,"| % activa:",S["pct_activa"])
print("org:",list(S["org_top"].items())[:3])
print("sectores:",[(x['sector'],x['n']) for x in S['sector_top'][:6]])
print("firma sectorial:")
for p,d in S["carrera_sector"].items(): print("  ",p[:22],"->",", ".join(f"{c} {v:.0f}" for c,v in sorted(d.items(),key=lambda x:-x[1])[:2]))
print("superv carrera:",list(S["superv_carrera"].items())[:5])
print("superv seccional:",S["superv_seccional"])
