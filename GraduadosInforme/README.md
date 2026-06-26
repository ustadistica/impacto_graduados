# Cruce integrado de graduados USTA — informe 2026-1

Este aporte ejecuta el **cruce integrado** entre los graduados de la Universidad Santo
Tomás y tres registros nacionales —**SECOP** (contratación pública), **RUES** (registro
mercantil) y **CvLAC** (investigación, ScienTI/Minciencias)— sobre una base unificada de
**174.685 personas** (197.273 registros de grado, 1970–2026), con análisis por seccional.

> **Datos personales — no se publican.** Los insumos (`datos/`) y los resultados con
> cédulas y nombres (`salidas/`, `entregables/datasets/`, los `.csv` y `.xlsx`) **se
> excluyen deliberadamente** de este repositorio público por la política de tratamiento
> de datos de la Universidad. El `.gitignore` los bloquea. Todo se **regenera localmente**
> ejecutando el pipeline sobre las bases institucionales.

## Pipeline (orden de ejecución)

1. **`_reintegrar.py`** — unifica las cuatro bases institucionales (Tunja, Villavicencio,
   Bucaramanga con Pregrado+Posgrado y SPB-CM-CAU = Bogotá/Medellín/VUAD) en
   `salidas/graduados_integrado.csv`. Documentado en `Integracion_Graduados_USTA.ipynb`.
2. **`Cruce_Graduados_SECOP.ipynb`** y **`Cruce_Graduados_RUES.ipynb`** — enlazan por cédula
   contra SECOP Integrado (`rpmr-utcd`) y RUES (`c82u-588k`) vía API Socrata (SoQL por lotes,
   `documento IN (...)`), sin descargar el universo. Cache-first: leen de `salidas/` si existe.
3. **`_scrape_cvlac_usta.py`** — raspado dirigido: filtra los grupos USTA en GrupLAC, reúne
   sus integrantes y descarga sus hojas de vida CvLAC faltantes vía el scraper de ScienTI.
4. **`Validacion_Graduados_CvLAC.ipynb`** — valida coincidencias CvLAC por **evidencia
   académica** (formación USTA y/o pertenencia a grupo USTA), con un *score* 0–100.
5. **`_secop_*.py`, `_rues_dims.py`, `_cvlac_openalex.py`** — extracciones ampliadas
   (territorial, UNSPSC, CIIU, supervivencia, revistas vía OpenAlex).
6. **`Impacto_Consolidado_Graduados.ipynb`** — consolida una fila por persona con las tres
   dimensiones y el índice de participación.
7. **`generar_entregables.py`** — empaqueta los datasets curados, el diccionario y el resumen.

## Artículo

El informe de hallazgos está en `../informe_2026-1/articulos/articulo_impacto_completo_2026-1.tex`
(PDF incluido). Las tablas y figuras se generan con `_analisis_cruce.py` y `_cap_{secop,rues,cvlac}.py`,
y todas las cifras son **agregadas** (sin datos individuales).

## Reproducibilidad

- Llave de cruce: la **cédula** (`identificacion`, solo dígitos). CvLAC se valida por nombre
  + evidencia (no expone documento). Ver `entregables/Guia_de_cruce_de_tablas.pdf`.
- Requisitos: `pandas`, `numpy`, `matplotlib`, `requests`, `beautifulsoup4`, `lxml`,
  `openpyxl`, `nbformat`/`nbconvert`. El scraper de ScienTI es un paquete externo.
