# -*- coding: utf-8 -*-
"""
Re-scrape DIRIGIDO de CvLAC para los integrantes de grupos de investigación USTA.

Este paso de adquisición enriqueció la validación CvLAC del Observatorio: en lugar de
quedarnos solo con el barrido secuencial de `cod_rh` (el dump local de ~54k perfiles es
parcial y no está dirigido a la USTA), buscamos explícitamente a los investigadores
vinculados a la Universidad a través de sus grupos en GrupLAC y descargamos sus hojas
de vida que aún no teníamos. El flujo es:

  1. Filtrar, entre los GrupLAC ya descargados localmente, los grupos cuya institución
     avaladora es la Universidad Santo Tomás (regex `santo\\s*tomas` sobre `instituciones`).
     -> 259 grupos USTA.
  2. Reunir el `cod_rh` de todos sus integrantes -> roster de 4.546 personas, guardado
     en `salidas/cvlac_grupos_usta.csv` (la misma tabla que consume el cuaderno de CvLAC).
  3. Detectar qué integrantes aún NO tienen hoja de vida CvLAC descargada y listarlos
     en `salidas/_cvlac_faltantes.txt`.
  4. Descargar en paralelo esas hojas de vida faltantes con el scraper de ScienTI
     (paquete externo `scraper_scienti`), guardándolas como JSON en `data/cvlac/`.

El scraper de ScienTI vive fuera de este repositorio (proyecto «Doctorado Matemáticas»);
este script deja documentada y reproducible la operación desde el lado del Observatorio
de Graduados. Es idempotente: re-ejecutarlo solo descarga lo que falte.

Uso:
    python _scrape_cvlac_usta.py                # roster + faltantes + descarga
    python _scrape_cvlac_usta.py --solo-roster  # solo (1)-(3), sin tocar la red
"""
import os
import re
import csv
import sys
import glob
import json
import time
import argparse
import unicodedata
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Rutas (ajusta si mueves el scraper de ScienTI) ──────────────────
SCRAPER  = Path(r"c:\Users\cizai\Dropbox\Documentos\Doctorado Matemáticas\scraper_scienti")
GRUPLAC  = SCRAPER / "data" / "gruplac"   # GrupLAC ya descargados (JSON)
CVLAC    = SCRAPER / "data" / "cvlac"     # hojas de vida CvLAC (JSON) = destino
OUT      = Path(__file__).resolve().parent / "salidas"
WORKERS  = 8
RE_USTA  = re.compile(r"santo\s*tomas")

OUT.mkdir(exist_ok=True)


def na(s: str) -> str:
    """Minúsculas sin tildes/diacríticos (NFKD)."""
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in s if not unicodedata.combining(c)).lower()


def name_key(s: str) -> str:
    """Clave de nombre canónica: tokens ordenados, sin acentos ni puntuación."""
    s = na(s).upper().replace(",", " ")
    s = re.sub(r"[^A-Z\s]", " ", s)
    return " ".join(sorted(t for t in s.split() if len(t) > 1))


# ── 1-2. Roster de integrantes de grupos USTA ───────────────────────
def construir_roster() -> dict:
    files = glob.glob(str(GRUPLAC / "*.json"))
    print(f"GrupLAC locales: {len(files):,}")
    if not files:
        raise SystemExit(f"No hay GrupLAC en {GRUPLAC}. ¿Ruta del scraper correcta?")
    roster, usta_groups = {}, 0
    for f in files:
        try:
            d = json.load(open(f, encoding="utf-8"))
        except Exception:
            continue
        insts = " || ".join(d.get("instituciones", []) or [])
        if not RE_USTA.search(na(insts)):
            continue
        usta_groups += 1
        for m in d.get("integrantes", []) or []:
            cr = m.get("cod_rh")
            if not cr:
                continue
            cr = str(cr)
            if cr not in roster:
                roster[cr] = {"cod_rh": cr, "nombre": m.get("nombre", ""),
                              "name_key": name_key(m.get("nombre", "")), "n_grupos": 0}
            roster[cr]["n_grupos"] += 1
    print(f"Grupos USTA (avaladora): {usta_groups} | integrantes únicos con cod_rh: {len(roster):,}")
    out = OUT / "cvlac_grupos_usta.csv"
    with open(out, "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["cod_rh", "nombre", "name_key", "n_grupos"])
        w.writeheader()
        for r in roster.values():
            w.writerow(r)
    print(f"  -> {out}")
    return roster


# ── 3. cod_rh faltantes (sin JSON CvLAC local) ──────────────────────
def detectar_faltantes(roster: dict) -> list:
    have = set(os.path.splitext(os.path.basename(p))[0].zfill(10)
               for p in glob.glob(str(CVLAC / "*.json")))
    miss = [c for c in roster if c.zfill(10) not in have]
    out = OUT / "_cvlac_faltantes.txt"
    out.write_text("\n".join(miss), encoding="utf-8")
    print(f"Ya descargados: {len(roster) - len(miss):,} | faltantes por descargar: {len(miss):,}")
    print(f"  -> {out}")
    return miss


# ── 4. Descarga en paralelo vía el scraper de ScienTI ───────────────
def descargar(faltantes: list) -> None:
    pend = [c.zfill(10) for c in faltantes if not (CVLAC / f"{c.zfill(10)}.json").exists()]
    if not pend:
        print("Nada que descargar: todas las hojas de vida ya están en data/cvlac/.")
        return
    sys.path.insert(0, str(SCRAPER))
    try:
        from src.parallel import configure_utf8_stdio  # evita UnicodeEncodeError en prints
        configure_utf8_stdio()
    except Exception:
        pass
    from src.cvlac import scrape_and_save  # paquete externo scraper_scienti

    print(f"Descargando {len(pend):,} hojas de vida CvLAC con {WORKERS} hilos...")
    ok = err = 0
    t0 = time.time()

    def trabajo(cod):
        try:
            scrape_and_save(cod, overwrite=False)
            return True
        except Exception:
            return False

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(trabajo, c): c for c in pend}
        for i, fut in enumerate(as_completed(futs), 1):
            r = fut.result()
            ok += int(r)
            err += int(not r)
            if i % 200 == 0 or i == len(pend):
                el = time.time() - t0
                eta = el / i * (len(pend) - i)
                print(f"  {i}/{len(pend)} | ok={ok} err={err} | {el:.0f}s | ETA {eta:.0f}s", flush=True)
    print(f"FIN | descargados ok={ok} err={err} | {time.time() - t0:.0f}s")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--solo-roster", action="store_true",
                    help="Solo construir el roster y la lista de faltantes; no descargar (sin red).")
    args = ap.parse_args()

    roster = construir_roster()
    faltantes = detectar_faltantes(roster)
    if args.solo_roster:
        print("\nModo --solo-roster: no se descargó nada.")
    else:
        descargar(faltantes)
