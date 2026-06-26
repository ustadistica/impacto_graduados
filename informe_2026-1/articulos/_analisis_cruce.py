# -*- coding: utf-8 -*-
"""Figuras y stats.json del artículo. Dimensión SEDE (6): Bogotá, Bucaramanga, Tunja,
   Villavicencio, Medellín, VUAD. Incluye cruce territorial (SECOP departamento, RUES cámara)."""
import json, re, unicodedata
from pathlib import Path
import numpy as np, pandas as pd
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt

BASE=Path(__file__).resolve().parent
SAL=BASE.parent.parent/"GraduadosInforme"/"salidas"
FIG=BASE/"figs_cruce"; FIG.mkdir(exist_ok=True)
OUT=BASE/"_build_cruce"; OUT.mkdir(exist_ok=True)
AZUL="#1F3A93"; ROJO="#B22222"; DORADO="#E8A317"; VERDE="#1B998B"; MORADO="#5C415D"; GRIS="#888888"
plt.rcParams.update({"figure.dpi":130,"savefig.dpi":130,"font.size":9.5,"axes.grid":True,
                     "grid.alpha":0.25,"axes.spines.top":False,"axes.spines.right":False,"figure.autolayout":True})
SEDES=["Bogotá","Bucaramanga","Tunja","Villavicencio","Medellín","VUAD"]
SC=[AZUL,VERDE,DORADO,ROJO,MORADO,"#6C8EBF"]
def na(s): s=unicodedata.normalize("NFKD",str(s)); return "".join(c for c in s if not unicodedata.combining(c))
def save(name): p=FIG/f"{name}.png"; plt.savefig(p,bbox_inches="tight"); plt.close(); return p.name

S={}
gi=pd.read_csv(SAL/"graduados_integrado.csv", dtype={"identificacion":"string"}, low_memory=False)
cons=pd.read_csv(SAL/"impacto_consolidado.csv", dtype={"identificacion":"string"}, low_memory=False)
secop=pd.read_csv(SAL/"graduados_proveedores_secop.csv", dtype={"identificacion":"string"})
rues=pd.read_csv(SAL/"graduados_emprendedores_rues.csv", dtype={"identificacion":"string"})
cvl=pd.read_csv(SAL/"graduados_en_cvlac.csv", dtype={"identificacion":"string"})
sreg=pd.read_csv(SAL/"secop_regional.csv", dtype={"identificacion":"string"})

S["n_registros"]=int(len(gi)); S["n_personas"]=int(len(cons))
S["anio_min"]=int(gi["anio_grado"].min()); S["anio_max"]=int(gi["anio_grado"].max())
ced_sede=cons.set_index("identificacion")["sede_principal"]
pe=cons.rename(columns={"sede_principal":"sede"}).copy()
pe=pe[pe["sede"].isin(SEDES)]

# ── 1 graduados por sede ────────────────────────────────────────────
g_sec=gi.dropna(subset=["identificacion"]).groupby("sede")["identificacion"].nunique().reindex(SEDES)
reg_sec=gi.groupby("sede").size().reindex(SEDES)
S["grad_por_sede"]={k:int(v) for k,v in g_sec.items()}
S["registros_por_sede"]={k:int(v) for k,v in reg_sec.items()}
plt.figure(figsize=(7.2,3.9)); b=plt.bar(SEDES,g_sec.values,color=SC)
for r,v in zip(b,g_sec.values): plt.text(r.get_x()+r.get_width()/2,v,f"{int(v):,}",ha="center",va="bottom",fontsize=8)
plt.ylabel("Graduados (cédulas únicas)"); plt.title("Graduados por sede"); plt.xticks(rotation=15); save("01_grad_seccional")

# ── 2 evolución por sede (hasta 2025) ───────────────────────────────
ev=(gi.dropna(subset=["anio_grado"]).pivot_table(index="anio_grado",columns="sede",values="identificacion",aggfunc="size",fill_value=0).reindex(columns=SEDES).sort_index())
ev=ev[ev.index<=2025]
plt.figure(figsize=(7.8,3.9))
for s,c in zip(SEDES,SC): plt.plot(ev.index,ev[s],marker="o",ms=2,lw=1.4,color=c,label=s)
plt.legend(ncol=2,fontsize=8); plt.xlabel("Año de grado"); plt.ylabel("Graduados"); plt.title("Evolución anual de graduados por sede (1979–2025)"); save("02_evolucion_seccional")
S["pico_anual"]={s:{"anio":int(ev[s].idxmax()),"n":int(ev[s].max())} for s in SEDES}

# ── 3 top programas ─────────────────────────────────────────────────
topp=gi["programa_norm"].value_counts().head(15); S["top_programas"]={k:int(v) for k,v in topp.items()}
plt.figure(figsize=(7.4,4.8)); plt.barh([str(x).title()[:40] for x in topp.index[::-1]],topp.values[::-1],color=AZUL)
plt.xlabel("Graduados"); plt.title("Top 15 programas por número de graduados"); save("03_top_programas")

# ── 4 modalidad por sede ────────────────────────────────────────────
mod=(gi[gi["modalidad"].isin(["Pregrado","Posgrado"])].pivot_table(index="sede",columns="modalidad",values="identificacion",aggfunc="size",fill_value=0).reindex(SEDES))
S["modalidad_sede"]={s:{m:int(mod.loc[s,m]) for m in mod.columns} for s in mod.index}
plt.figure(figsize=(7.0,3.7)); mod.plot(kind="bar",stacked=True,color=[AZUL,DORADO],ax=plt.gca())
plt.xticks(rotation=15); plt.ylabel("Registros de grado"); plt.title("Modalidad por sede"); plt.legend(title="Modalidad"); save("04_modalidad_seccional")

# ── tasas por sede ──────────────────────────────────────────────────
def rate(col):
    n=pe.groupby("sede")["identificacion"].nunique().reindex(SEDES)
    k=pe[pe[col]==True].groupby("sede")["identificacion"].nunique().reindex(SEDES).fillna(0)
    return n.astype(int),k.astype(int),(k/n*100).round(1)
n_sec,secop_k,secop_p=rate("es_proveedor_secop"); _,rues_k,rues_p=rate("en_rues"); _,cvl_k,cvl_p=rate("en_cvlac")
S["tasas_sede"]={s:{"n":int(n_sec[s]),"secop_pct":float(secop_p[s]),"rues_pct":float(rues_p[s]),"cvlac_pct":float(cvl_p[s])} for s in SEDES}
x=np.arange(len(SEDES)); w=0.26
plt.figure(figsize=(8.2,4.0))
plt.bar(x-w,secop_p.values,w,label="SECOP",color=AZUL); plt.bar(x,rues_p.values,w,label="RUES",color=VERDE); plt.bar(x+w,cvl_p.values,w,label="CvLAC",color=MORADO)
plt.xticks(x,SEDES,rotation=15); plt.ylabel("% de graduados"); plt.title("Participación por dimensión y sede (%)"); plt.legend(); save("05_tasas_seccional")

# ── 6,7 dimensiones ─────────────────────────────────────────────────
nd=cons["n_dimensiones"].value_counts().sort_index(); S["dist_dimensiones"]={int(k):int(v) for k,v in nd.items()}
S["con_al_menos_una"]=int((cons["n_dimensiones"]>=1).sum()); S["pct_al_menos_una"]=round((cons["n_dimensiones"]>=1).mean()*100,1)
plt.figure(figsize=(5.8,3.7)); b=plt.bar([str(i) for i in nd.index],nd.values,color=[GRIS,AZUL,VERDE,ROJO])
for r,v in zip(b,nd.values): plt.text(r.get_x()+r.get_width()/2,v,f"{int(v):,}",ha="center",va="bottom",fontsize=8)
plt.xlabel("Nº de dimensiones con participación"); plt.ylabel("Personas"); plt.title("Personas por nº de dimensiones"); save("06_dimensiones")
combo=cons[cons["n_dimensiones"]>=1]["perfil_impacto"].value_counts(); S["combinaciones"]={k:int(v) for k,v in combo.items()}
plt.figure(figsize=(6.6,3.9)); plt.barh(combo.index[::-1],combo.values[::-1],color=DORADO)
for i,v in enumerate(combo.values[::-1]): plt.text(v,i,f" {int(v):,}",va="center",fontsize=8)
plt.xlabel("Personas"); plt.title("Combinaciones de dimensiones (con ≥1)"); save("07_combinaciones")

# ── 8 dimensiones por sede ──────────────────────────────────────────
md=(pe.pivot_table(index="sede",columns="n_dimensiones",values="identificacion",aggfunc="nunique",fill_value=0).reindex(SEDES))
mdp=md.div(md.sum(axis=1),axis=0)*100; S["dimensiones_por_sede"]={s:{int(c):int(md.loc[s,c]) for c in md.columns} for s in md.index}
plt.figure(figsize=(7.6,3.8)); bottom=np.zeros(len(SEDES)); cols=[GRIS,AZUL,VERDE,ROJO]
for j,c in enumerate(md.columns):
    plt.bar(SEDES,mdp[c].values,bottom=bottom,color=cols[j%4],label=f"{c} dim."); bottom+=mdp[c].values
plt.ylabel("% de graduados"); plt.title("Perfil de dimensiones por sede"); plt.legend(ncol=4,fontsize=8); plt.xticks(rotation=15); save("08_dimensiones_seccional")

# ── 9 cohorte ───────────────────────────────────────────────────────
c2=cons.dropna(subset=["anio_primer_grado"]).copy(); c2["decada"]=(c2["anio_primer_grado"]//10*10).astype(int)
dec=(c2.groupby("decada").agg(n=("identificacion","size"),secop=("es_proveedor_secop","sum"),rues=("en_rues","sum"),cvlac=("en_cvlac","sum")))
dec=dec[dec["n"]>=300]
for col in ["secop","rues","cvlac"]: dec[col+"_p"]=(dec[col]/dec["n"]*100).round(1)
S["impacto_por_decada"]={int(k):{"n":int(r["n"]),"secop_pct":float(r["secop_p"]),"rues_pct":float(r["rues_p"]),"cvlac_pct":float(r["cvlac_p"])} for k,r in dec.iterrows()}
plt.figure(figsize=(7.6,4.0))
plt.plot(dec.index,dec["secop_p"],marker="o",color=AZUL,label="SECOP"); plt.plot(dec.index,dec["rues_p"],marker="s",color=VERDE,label="RUES"); plt.plot(dec.index,dec["cvlac_p"],marker="^",color=MORADO,label="CvLAC")
plt.xlabel("Década del primer grado"); plt.ylabel("% con participación"); plt.title("Participación según cohorte de graduación"); plt.legend(); save("09_impacto_cohorte")

# ── 10,11 SECOP ─────────────────────────────────────────────────────
v=pd.to_numeric(secop["valor_total"],errors="coerce").dropna(); v=v[v>0]
S["secop"]={"n_proveedores":int(len(secop)),"n_contratos":int(pd.to_numeric(secop["n_contratos"],errors="coerce").sum()),
            "valor_total":float(v.sum()),"valor_mediana":float(v.median()),"contratos_mediana":float(pd.to_numeric(secop["n_contratos"],errors="coerce").median())}
plt.figure(figsize=(7.0,3.7)); plt.hist(np.log10(v),bins=40,color=AZUL,edgecolor="white")
plt.xlabel("log10(valor total contratado, COP)"); plt.ylabel("Proveedores"); plt.title("Distribución del valor contratado (SECOP)"); save("10_secop_valor")
spp=secop["programa"].fillna("").str.upper().str.strip(); spp=spp[spp.str.len()>0].value_counts().head(12); S["secop_top_programas"]={k:int(v) for k,v in spp.items()}
plt.figure(figsize=(7.6,4.4)); plt.barh([str(x).title()[:34] for x in spp.index[::-1]],spp.values[::-1],color=AZUL); plt.xlabel("Proveedores"); plt.title("Top programas por nº de graduados proveedores (SECOP)"); save("11_secop_programas")

# ── 12,13 RUES ──────────────────────────────────────────────────────
rr=rues.copy(); rr["sede"]=rr["identificacion"].map(ced_sede)
rr2=rr[rr["sede"].isin(SEDES)]
act=(rr2.assign(activa=rr2["tiene_empresa_activa"].astype(str).str.lower().isin(["true","1","verdadero"])).pivot_table(index="sede",columns="activa",values="identificacion",aggfunc="nunique",fill_value=0).reindex(SEDES))
act.columns=["Cancelada/otro" if c==False else "Activa" for c in act.columns]
S["rues"]={"n_emprendedores":int(len(rues)),"n_activa":int(pd.to_numeric(rues["n_activas"],errors="coerce").gt(0).sum())}
plt.figure(figsize=(7.2,3.8)); act.plot(kind="bar",stacked=True,color=[VERDE,ROJO],ax=plt.gca())
plt.xticks(rotation=15); plt.ylabel("Matriculados en RUES"); plt.title("Supervivencia de la matrícula (RUES) por sede"); plt.legend(); save("12_rues_seccional")
ci=rues["ciiu_principal"].astype(str).str.replace(r"\.0$","",regex=True); ci=ci[ci.str.fullmatch(r"\d+")].value_counts().head(12); S["rues_top_ciiu"]={k:int(v) for k,v in ci.items()}
plt.figure(figsize=(7.0,4.2)); plt.barh(ci.index[::-1],ci.values[::-1],color=VERDE); plt.xlabel("Matriculados en RUES"); plt.ylabel("Código CIIU"); plt.title("Top CIIU de los graduados matriculados en el RUES"); save("13_rues_ciiu")

# ── 14 CvLAC ────────────────────────────────────────────────────────
cv1=cvl.drop_duplicates("cod_rh"); niv=cv1["nivel_maximo"].fillna("(sin dato)").replace("","(sin dato)").value_counts(); cat=cv1["categoria_minciencias"].fillna("").replace("","(sin categoría)").value_counts()
S["cvlac"]={"n_validados_personas":int(cv1["cod_rh"].nunique()),"n_validados_registros":int(len(cvl)),
            "por_formacion":int((cvl["flag_usta_form"]==1).sum()),"por_grupo":int((cvl["en_grupo_usta"]==1).sum())}
fig,ax=plt.subplots(1,2,figsize=(10,3.8)); ax[0].barh(niv.index[::-1],niv.values[::-1],color=MORADO); ax[0].set_title("Nivel máximo (CvLAC)")
cc=cat.drop(labels=["(sin categoría)"],errors="ignore"); ax[1].barh(cc.index[::-1],cc.values[::-1],color=DORADO); ax[1].set_title("Categoría Minciencias (validados)"); save("14_cvlac_perfil")

# ── 15 overlaps ─────────────────────────────────────────────────────
A=cons["es_proveedor_secop"].astype(bool); B=cons["en_rues"].astype(bool); C=cons["en_cvlac"].astype(bool)
S["overlaps"]={"secop_y_rues":int((A&B).sum()),"secop_y_cvlac":int((A&C).sum()),"rues_y_cvlac":int((B&C).sum()),"las_tres":int((A&B&C).sum()),
               "cvlac_tambien_secop_pct":round((A&C).sum()/max(C.sum(),1)*100,1),"cvlac_tambien_rues_pct":round((B&C).sum()/max(C.sum(),1)*100,1)}
M=pd.DataFrame({"SECOP":[A.sum(),(A&B).sum(),(A&C).sum()],"RUES":[(A&B).sum(),B.sum(),(B&C).sum()],"CvLAC":[(A&C).sum(),(B&C).sum(),C.sum()]},index=["SECOP","RUES","CvLAC"])
plt.figure(figsize=(5.2,4.4)); plt.imshow(M.values,cmap="Blues"); plt.xticks(range(3),M.columns); plt.yticks(range(3),M.index)
for i in range(3):
    for j in range(3): plt.text(j,i,f"{int(M.values[i,j]):,}",ha="center",va="center",color="white" if M.values[i,j]>M.values.max()*0.5 else "black",fontsize=9)
plt.title("Co-ocurrencia entre dimensiones (personas)"); save("15_overlaps")

# ── 16 heatmap programa × dimensión ─────────────────────────────────
ce=cons.assign(prog=cons["programas"].fillna("").str.split(" | ",regex=False)).explode("prog"); ce["prog"]=ce["prog"].str.upper().str.strip()
tops=ce["prog"].value_counts().head(12).index; H=[]
for p in tops:
    d=ce[ce["prog"]==p]; H.append([d["es_proveedor_secop"].mean()*100,d["en_rues"].mean()*100,d["en_cvlac"].mean()*100])
H=pd.DataFrame(H,index=tops,columns=["SECOP","RUES","CvLAC"]); S["heatmap_programa"]={p:{"secop":round(H.loc[p,"SECOP"],1),"rues":round(H.loc[p,"RUES"],1),"cvlac":round(H.loc[p,"CvLAC"],1)} for p in tops}
plt.figure(figsize=(6.2,5.6)); plt.imshow(H.values,cmap="YlOrRd",aspect="auto"); plt.xticks(range(3),H.columns); plt.yticks(range(len(tops)),[str(t).title()[:30] for t in tops],fontsize=8)
for i in range(len(tops)):
    for j in range(3): plt.text(j,i,f"{H.values[i,j]:.0f}",ha="center",va="center",fontsize=8,color="black" if H.values[i,j]<40 else "white")
plt.colorbar(label="% con participación"); plt.title("Participación por programa y dimensión (%)"); save("16_heatmap_programa")

# ── TERRITORIAL ─────────────────────────────────────────────────────
HOME_DEP={"Bogotá":"Bogota D.C.","Bucaramanga":"Santander","Tunja":"Boyaca","Villavicencio":"Meta","Medellín":"Antioquia"}
HOME_CAM={"Bogotá":"Bogota","Bucaramanga":"Bucaramanga","Tunja":"Tunja","Villavicencio":"Villavicencio","Medellín":"Medellin"}
sreg["dep"]=sreg["departamento"].map(lambda s: na(s).strip()).replace({"Distrito Capital de Bogota":"Bogota D.C."})
sreg["sede"]=sreg["identificacion"].map(ced_sede)
dep_tot=sreg.groupby("dep")["n_contratos"].sum().sort_values(ascending=False); dep_tot=dep_tot[dep_tot.index!="(sin dato)"].head(12)
S["secop_top_departamento"]={k:int(v) for k,v in dep_tot.items()}
plt.figure(figsize=(7.2,4.4)); plt.barh(dep_tot.index[::-1],dep_tot.values[::-1],color=AZUL); plt.xlabel("Contratos"); plt.title("Departamentos donde contratan los graduados (SECOP)"); save("17_secop_departamento")
cam=rues["camara_comercio"].fillna("").map(lambda s: na(s).strip().title()); cam_tot=cam[cam.str.len()>0].value_counts().head(12); S["rues_top_camara"]={k:int(v) for k,v in cam_tot.items()}
plt.figure(figsize=(7.2,4.4)); plt.barh(cam_tot.index[::-1],cam_tot.values[::-1],color=VERDE); plt.xlabel("Empresas"); plt.title("Cámaras de comercio de los graduados matriculados en el RUES"); save("18_rues_camara")
# seccional x departamento (SECOP)
SEDES_T=[s for s in SEDES if s!="VUAD"]
topdeps=list(HOME_DEP.values())+["Otros"]
piv=(sreg[sreg["sede"].isin(SEDES_T)].assign(dep2=lambda d: d["dep"].where(d["dep"].isin(list(HOME_DEP.values())),"Otros")).pivot_table(index="sede",columns="dep2",values="n_contratos",aggfunc="sum",fill_value=0).reindex(SEDES_T))
order=[d for d in topdeps if d in piv.columns]; piv=piv[order]; pivp=piv.div(piv.sum(axis=1),axis=0)*100
S["territorial_secop"]={s:{c:round(float(pivp.loc[s,c]),1) for c in pivp.columns} for s in SEDES_T}
plt.figure(figsize=(8.4,3.8)); plt.imshow(pivp.values,cmap="Blues",aspect="auto")
plt.yticks(range(len(SEDES_T)),SEDES_T); plt.xticks(range(len(order)),[c[:12] for c in order],rotation=25,ha="right",fontsize=8)
for i in range(len(SEDES_T)):
    for j in range(len(order)): plt.text(j,i,f"{pivp.values[i,j]:.0f}",ha="center",va="center",fontsize=8,color="white" if pivp.values[i,j]>50 else "black")
plt.colorbar(label="% de contratos"); plt.title("Departamento del contrato por sede (SECOP)"); save("19_secop_seccional_region")
# seccional x cámara (RUES)
rc=rues.copy(); rc["sede"]=rc["identificacion"].map(ced_sede); rc["cam"]=rc["camara_comercio"].fillna("").map(lambda s: na(s).strip().title())
topcam=list(dict.fromkeys(HOME_CAM.values()))+["Otras"]
pc=(rc[rc["sede"].isin(SEDES_T) & (rc["cam"].str.len()>0)].assign(cam2=lambda d: d["cam"].where(d["cam"].isin(list(HOME_CAM.values())),"Otras")).pivot_table(index="sede",columns="cam2",values="identificacion",aggfunc="nunique",fill_value=0).reindex(SEDES_T))
order2=[c for c in topcam if c in pc.columns]; pc=pc[order2]; pcp=pc.div(pc.sum(axis=1),axis=0)*100
S["territorial_rues"]={s:{c:round(float(pcp.loc[s,c]),1) for c in pcp.columns} for s in SEDES_T}
plt.figure(figsize=(8.4,3.8)); plt.imshow(pcp.values,cmap="Greens",aspect="auto")
plt.yticks(range(len(SEDES_T)),SEDES_T); plt.xticks(range(len(order2)),[c[:12] for c in order2],rotation=25,ha="right",fontsize=8)
for i in range(len(SEDES_T)):
    for j in range(len(order2)): plt.text(j,i,f"{pcp.values[i,j]:.0f}",ha="center",va="center",fontsize=8,color="white" if pcp.values[i,j]>50 else "black")
plt.colorbar(label="% de empresas"); plt.title("Cámara de comercio por sede (RUES)"); save("20_rues_seccional_camara")

# ── SÍNTESIS por sede ───────────────────────────────────────────────
top_prog_sede={s: gi[gi["sede"]==s]["programa_norm"].value_counts().index[0] for s in SEDES}
pct_imp={s: round((md.loc[s,[c for c in md.columns if c>=1]].sum()/md.loc[s].sum())*100,1) for s in SEDES}
def posg(s):
    m=S["modalidad_sede"].get(s)
    if not m: return "n/d"
    tot=sum(m.values()); return f"{m.get('Posgrado',0)/tot*100:.0f}\\%"
def mil(x): return f"{int(x):,}".replace(",", ".")
def pc_(x): return f"{x:.1f}".replace(".", ",")+"\\%"
def texesc(x): return str(x).replace("&","\\&").replace("%","\\%").replace("_","\\_")
filas=[
 ("Graduados (personas)", [mil(S['grad_por_sede'][s]) for s in SEDES]),
 ("Posgrado (\\% títulos)", [posg(s) for s in SEDES]),
 ("Programa principal", [texesc(str(top_prog_sede[s]).title()[:16]) for s in SEDES]),
 ("Proveedor (SECOP)", [pc_(S['tasas_sede'][s]['secop_pct']) for s in SEDES]),
 ("Matriculado en RUES", [pc_(S['tasas_sede'][s]['rues_pct']) for s in SEDES]),
 ("Investigador (CvLAC)", [pc_(S['tasas_sede'][s]['cvlac_pct']) for s in SEDES]),
 ("Con $\\geq$1 dimensión", [pc_(pct_imp[s]) for s in SEDES]),
]
tl=[r"\begin{table}[H]\centering\scriptsize",
    r"\caption{Síntesis comparativa por sede: tamaño, formación y participación.}",r"\label{tab:seccional_sintesis}",
    r"\begin{tabular}{l"+"r"*len(SEDES)+"}",r"\toprule",
    r"\rowcolor{ustaazul}\textbf{\color{white}Indicador} & "+" & ".join(rf"\textbf{{\color{{white}}{s}}}" for s in SEDES)+r" \\",r"\midrule"]
for nombre,vals in filas: tl.append(f"{nombre} & "+" & ".join(vals)+r" \\")
tl+=[r"\bottomrule",r"\end{tabular}",
     r"\\[2pt]{\scriptsize\color{ustagris} VUAD agrupa los centros de atención universitaria (educación a distancia).}",r"\end{table}"]
(OUT/"tab_seccional_sintesis.tex").write_text("\n".join(tl),encoding="utf-8")

FIGS={f"{i:02d}":"" for i in range(1,21)}
S["figuras"]=FIGS
(OUT/"stats.json").write_text(json.dumps(S,ensure_ascii=False,indent=2),encoding="utf-8")
print("Figuras (20) + síntesis regeneradas para 6 sedes.")
print("grad_por_sede:",S["grad_por_sede"])
print("tasas:",{s:(S['tasas_sede'][s]['secop_pct'],S['tasas_sede'][s]['rues_pct'],S['tasas_sede'][s]['cvlac_pct']) for s in SEDES})
print("territorial_secop:",{s:max(d.items(),key=lambda x:x[1]) for s,d in S["territorial_secop"].items()})
print("pct_al_menos_una:",S["pct_al_menos_una"],"| combos:",S["combinaciones"],"| overlaps:",S["overlaps"])
print("secop:",S["secop"]["n_proveedores"],"med",S["secop"]["valor_mediana"]/1e6)
