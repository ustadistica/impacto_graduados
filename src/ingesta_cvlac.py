"""
SCRAPING CvLAC - INTEGRANTES GRUPOS USTA (2017-2021)
=====================================================
1. Filtra la base consolidada por los 123 grupos USTA
2. Extrae IDs únicos de personas (ID_PERSONA_PD)
3. Construye URLs de CvLAC y hace scraping de:
   - Nombre
   - Formación académica (nivel, institución, año)
   - Si es egresado USTA
   - Producción registrada
4. Genera Excel con resultados

REQUISITOS:
    py -3.11 -m pip install requests beautifulsoup4 lxml openpyxl pandas

EJECUCIÓN:
    py -3.11 scraping_cvlac_usta.py
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(BASE_DIR, '..', 'data'))

# ── CONFIGURACIÓN ─────────────────────────────────────────────────────────────
ARCHIVO_PRODUCCION = os.path.join(DATA_DIR, "Produccion_Consolidada_2017_2021.csv")
ARCHIVO_SALIDA     = os.path.join(BASE_DIR, "USTA_Integrantes_Formacion_Produccion.xlsx")
DELAY              = 1.5  # segundos entre cada consulta

# 123 grupos USTA identificados desde GrupLAC
GRUPOS_USTA = [
    'COL0049373','COL0113483','COL0107323','COL0163968','COL0067228',
    'COL0144354','COL0030193','COL0048287','COL0218144','COL0041633',
    'COL0019955','COL0042971','COL0048142','COL0028417','COL0169389',
    'COL0069741','COL0028373','COL0044297','COL0003374','COL0121162',
    'COL0188359','COL0101713','COL0192674','COL0065241','COL0027329',
    'COL0090745','COL0030578','COL0083034','COL0032385','COL0026065',
    'COL0027714','COL0080195','COL0046756','COL0082859','COL0102499',
    'COL0198444','COL0102639','COL0145665','COL0121224','COL0140865',
    'COL0187001','COL0116369','COL0021292','COL0165701','COL0003688',
    'COL0085979','COL0025021','COL0040396','COL0179609','COL0130644',
    'COL0193849','COL0135589','COL0032625','COL0155311','COL0057689',
    'COL0064262','COL0027062','COL0120619','COL0044205','COL0031799',
    'COL0061233','COL0042926','COL0069231','COL0001173','COL0027349',
    'COL0028112','COL0030649','COL0113474','COL0178728','COL0042532',
    'COL0082707','COL0095339','COL0124539','COL0051872','COL0205656',
    'COL0043253','COL0051077','COL0044484','COL0114678','COL0153209',
    'COL0201675','COL0123505','COL0120399','COL0109309','COL0124469',
    'COL0047987','COL0051498','COL0091509','COL0084274','COL0008889',
    'COL0119665','COL0085567','COL0007542','COL0024599','COL0083319',
    'COL0019739','COL0087098','COL0095554','COL0197957','COL0034236',
    'COL0202162','COL0207169','COL0020749','COL0195059','COL0191908',
    'COL0033954','COL0071579','COL0036974','COL0044958','COL0196789',
    'COL0186479','COL0202986','COL0129169','COL0159731','COL0177103',
    'COL0120844','COL0186499','COL0128501','COL0142699','COL0142654',
    'COL0208219','COL0197438','COL0204523',
]
# ─────────────────────────────────────────────────────────────────────────────

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
URL_CVLAC = "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh={}"

def get_soup(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return BeautifulSoup(r.content, 'lxml')
    except Exception as e:
        print(f"    ⚠ Error: {e}")
        return None

def extraer_nombre(soup):
    try:
        for tag in ['h2', 'h1', 'h3']:
            h = soup.find(tag)
            if h and len(h.text.strip()) > 3:
                return h.text.strip()
        span = soup.find('span', class_='nombre')
        if span:
            return span.text.strip()
    except:
        pass
    return ''

def extraer_formacion(soup):
    formacion = []
    try:
        niveles = ['DOCTORADO','MAESTRÍA','MAESTRIA','MAGÍSTER','MAGISTER',
                   'ESPECIALIZACIÓN','ESPECIALIZACION','PREGRADO','LICENCIATURA',
                   'POSDOCTORADO','PHD','PH.D']
        for tr in soup.find_all('tr'):
            texto = tr.get_text(separator=' | ').strip()
            texto_up = texto.upper()
            if any(n in texto_up for n in niveles):
                if any(inst in texto_up for inst in ['UNIVERSIDAD','INSTITUTO','COLLEGE','SCHOOL']):
                    if 20 < len(texto) < 600:
                        formacion.append(texto)
    except:
        pass
    return formacion

def nivel_maximo(formacion_lista):
    texto = ' '.join(formacion_lista).upper()
    if 'POSDOCTORADO' in texto: return 'Posdoctorado'
    if any(x in texto for x in ['DOCTORADO','PHD','PH.D']): return 'Doctorado'
    if any(x in texto for x in ['MAESTRÍA','MAESTRIA','MAGÍSTER','MAGISTER']): return 'Maestría'
    if any(x in texto for x in ['ESPECIALIZACIÓN','ESPECIALIZACION']): return 'Especialización'
    if any(x in texto for x in ['PREGRADO','LICENCIATURA']): return 'Pregrado'
    return 'No identificado'

def es_egresado_usta(formacion_lista):
    texto = ' '.join(formacion_lista).upper()
    return 'SANTO TOMAS' in texto or 'USTA' in texto

def institucion_grado_max(formacion_lista):
    prioridad = ['DOCTORADO','PHD','MAESTRÍA','MAESTRIA','MAGÍSTER','MAGISTER',
                 'ESPECIALIZACIÓN','ESPECIALIZACION','PREGRADO']
    for nivel in prioridad:
        for f in formacion_lista:
            if nivel in f.upper():
                match = re.search(
                    r'(UNIVERSIDAD[^\|,\n]{3,60}|INSTITUTO[^\|,\n]{3,40})',
                    f.upper()
                )
                if match:
                    return match.group(0).strip().title()
    return ''

def contar_produccion(soup):
    conteos = {'articulos': 0, 'libros': 0, 'cap_libros': 0, 'ponencias': 0, 'otros': 0}
    categorias = {
        'articulos':  ['Artículos publicados', 'Artículo'],
        'libros':     ['Libros publicados'],
        'cap_libros': ['Capítulos de libro'],
        'ponencias':  ['Ponencias', 'Trabajos en eventos'],
        'otros':      ['Otros productos'],
    }
    try:
        for key, nombres in categorias.items():
            for nombre in nombres:
                td = soup.find('td', string=re.compile(nombre, re.IGNORECASE))
                if td:
                    tabla = td.find_parent('table')
                    if tabla:
                        n = max(0, len(tabla.find_all('tr')) - 1)
                        conteos[key] = max(conteos[key], n)
                    break
    except:
        pass
    return conteos, sum(conteos.values())

# ── PASO 1: Cargar y filtrar base de producción ───────────────────────────────
def paso1_filtrar_produccion():
    print("\n📂 PASO 1: Cargando base de producción...")
    if not os.path.exists(ARCHIVO_PRODUCCION):
        print(f"  ❌ No se encontró '{ARCHIVO_PRODUCCION}'")
        print("     Asegúrate de que esté en la misma carpeta que este script.")
        return None

    print(f"  Leyendo {ARCHIVO_PRODUCCION} (puede tardar unos segundos)...")
    df = pd.read_csv(ARCHIVO_PRODUCCION, low_memory=False, encoding='utf-8-sig')
    print(f"  Total filas cargadas: {len(df):,}")

    # Normalizar código de grupo
    df['COD_GRUPO_GR'] = df['COD_GRUPO_GR'].astype(str).str.strip().str.upper()
    grupos_usta_norm = [g.upper().strip() for g in GRUPOS_USTA]

    df_usta = df[df['COD_GRUPO_GR'].isin(grupos_usta_norm)].copy()
    print(f"  Filas de grupos USTA: {len(df_usta):,}")
    print(f"  Grupos USTA encontrados: {df_usta['COD_GRUPO_GR'].nunique()}")

    # Guardar producción USTA filtrada
    df_usta.to_csv("Produccion_USTA_2017_2021.csv", index=False, encoding='utf-8-sig')
    print(f"  ✓ Guardado: Produccion_USTA_2017_2021.csv")

    return df_usta

# ── PASO 2: Extraer IDs únicos ────────────────────────────────────────────────
def paso2_ids_unicos(df_usta):
    print("\n👥 PASO 2: Extrayendo IDs únicos de personas...")
    df_usta['ID_PERSONA_PD'] = df_usta['ID_PERSONA_PD'].astype(str).str.strip().str.zfill(10)
    df_usta = df_usta[df_usta['ID_PERSONA_PD'].str.match(r'^\d{10}$')]

    # Una fila por persona-grupo (el más reciente)
    df_personas = (
        df_usta
        .sort_values('ANO_CONVO', ascending=False)
        .drop_duplicates(subset=['ID_PERSONA_PD', 'COD_GRUPO_GR'])
        [['ID_PERSONA_PD', 'COD_GRUPO_GR', 'NME_GRUPO_GR', 'NME_CONVOCATORIA']]
        .reset_index(drop=True)
    )
    print(f"  Personas únicas (por grupo): {len(df_personas):,}")
    print(f"  IDs únicos totales         : {df_personas['ID_PERSONA_PD'].nunique():,}")
    return df_personas

# ── PASO 3: Scraping CvLAC ────────────────────────────────────────────────────
def paso3_scraping(df_personas):
    print(f"\n🔍 PASO 3: Scraping CvLAC ({len(df_personas):,} registros)...")
    print("   (aprox.", round(len(df_personas) * DELAY / 60, 1), "minutos)\n")

    resultados = []
    total = len(df_personas)

    for i, row in df_personas.iterrows():
        id_persona = str(row['ID_PERSONA_PD']).zfill(10)
        cod_grupo  = row['COD_GRUPO_GR']
        nme_grupo  = row['NME_GRUPO_GR']
        url        = URL_CVLAC.format(id_persona)

        print(f"  [{i+1}/{total}] ID: {id_persona} | Grupo: {cod_grupo}", end=' ')

        soup = get_soup(url)
        if not soup:
            print("→ ERROR")
            resultados.append({
                'id_persona': id_persona, 'cod_grupo': cod_grupo,
                'nombre_grupo': nme_grupo, 'url_cvlac': url,
                'nombre': 'ERROR', 'nivel_maximo': '', 'institucion_grado': '',
                'egresado_usta': '', 'articulos': 0, 'libros': 0,
                'cap_libros': 0, 'ponencias': 0, 'otros': 0,
                'total_productos': 0, 'formacion_detalle': ''
            })
            time.sleep(DELAY)
            continue

        nombre    = extraer_nombre(soup)
        formacion = extraer_formacion(soup)
        nivel     = nivel_maximo(formacion)
        egresado  = es_egresado_usta(formacion)
        inst      = institucion_grado_max(formacion)
        conteos, total_prod = contar_produccion(soup)

        print(f"→ {nombre or '?'} | {nivel} | USTA: {'✓' if egresado else '-'} | Prod: {total_prod}")

        resultados.append({
            'id_persona':      id_persona,
            'cod_grupo':       cod_grupo,
            'nombre_grupo':    nme_grupo,
            'url_cvlac':       url,
            'nombre':          nombre,
            'nivel_maximo':    nivel,
            'institucion_grado': inst,
            'egresado_usta':   'SÍ' if egresado else 'NO',
            'articulos':       conteos['articulos'],
            'libros':          conteos['libros'],
            'cap_libros':      conteos['cap_libros'],
            'ponencias':       conteos['ponencias'],
            'otros':           conteos['otros'],
            'total_productos': total_prod,
            'formacion_detalle': ' || '.join(formacion[:4])
        })

        time.sleep(DELAY)

    return pd.DataFrame(resultados)

# ── PASO 4: Guardar resultados ────────────────────────────────────────────────
def paso4_guardar(df_resultado, df_usta):
    print(f"\n💾 PASO 4: Guardando resultados...")

    # Hoja 1: Integrantes con formación
    # Hoja 2: Resumen por grupo
    resumen = df_resultado.groupby(['cod_grupo','nombre_grupo']).agg(
        total_integrantes=('id_persona','count'),
        egresados_usta=('egresado_usta', lambda x: (x=='SÍ').sum()),
        con_doctorado=('nivel_maximo', lambda x: (x=='Doctorado').sum()),
        con_maestria=('nivel_maximo', lambda x: (x=='Maestría').sum()),
        total_productos=('total_productos','sum'),
    ).reset_index()
    resumen['pct_egresados'] = (resumen['egresados_usta'] / resumen['total_integrantes'] * 100).round(1)

    # Hoja 3: Producción USTA filtrada (resumen por tipo)
    prod_tipo = df_usta.groupby('ID_TIPO_PD_MED').agg(
        total=('ID_PRODUCTO_PD','count'),
        grupos=('COD_GRUPO_GR','nunique'),
        personas=('ID_PERSONA_PD','nunique')
    ).reset_index().sort_values('total', ascending=False)

    with pd.ExcelWriter(ARCHIVO_SALIDA, engine='openpyxl') as writer:
        df_resultado.to_excel(writer, sheet_name='Integrantes_Formacion', index=False)
        resumen.to_excel(writer, sheet_name='Resumen_por_Grupo', index=False)
        prod_tipo.to_excel(writer, sheet_name='Produccion_por_Tipo', index=False)

    print(f"  ✓ Guardado: {ARCHIVO_SALIDA}")
    print(f"    - Hoja 'Integrantes_Formacion' : {len(df_resultado):,} registros")
    print(f"    - Hoja 'Resumen_por_Grupo'     : {len(resumen):,} grupos")
    print(f"    - Hoja 'Produccion_por_Tipo'   : {len(prod_tipo):,} tipos")

    # Resumen en pantalla
    print("\n" + "="*55)
    print("RESUMEN FINAL")
    print("="*55)
    print(f"Integrantes procesados   : {len(df_resultado):,}")
    print(f"Egresados USTA detectados: {(df_resultado['egresado_usta']=='SÍ').sum():,}")
    print(f"Con doctorado            : {(df_resultado['nivel_maximo']=='Doctorado').sum():,}")
    print(f"Con maestría             : {(df_resultado['nivel_maximo']=='Maestría').sum():,}")
    print(f"Total productos (CvLAC)  : {df_resultado['total_productos'].sum():,}")
    print(f"\n✓ Archivo final: {ARCHIVO_SALIDA}")

# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print("="*55)
    print("SCRAPING CvLAC - GRUPOS USTA 2017-2021")
    print("="*55)

    df_usta     = paso1_filtrar_produccion()
    if df_usta is None:
        exit()

    df_personas = paso2_ids_unicos(df_usta)
    df_resultado = paso3_scraping(df_personas)
    paso4_guardar(df_resultado, df_usta)
