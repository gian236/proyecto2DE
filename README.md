# proyecto2DE

## Resumen
Pipeline local (sin nube) para *Wikipedia Clickstream* en capas *Bronze → Silver → Gold → GOLD_Specifics* usando *Python + DuckDB + Parquet (Hive partitioning). GOLD evita duplicar títulos mediante un **diccionario por idioma (ID↔título)*; GOLD_Specifics produce tops, tendencias y rutas listas para análisis.

## Árbol de datos
data/
├─ bronze/
├─ silver/  lang=XX/year=YYYY/month=MM/data.parquet
├─ gold/
│  ├─ article_popularity/  ├─ article_top/  ├─ referrers_top/  ├─ edges_monthly/
│  └─ dimensions/articles/lang=XX/dict.parquet   # diccionario ID↔título
└─ GOLD_Specifics/
   ├─ top10_by_year/   ├─ all_time_top/
   ├─ edges_6m/        ├─ trending_mom/     └─ referrers_6m/
config/settings.yaml

## Scripts
- *download_all*.py → Descarga datos bronce (Wikipedia Clickstream).
- *etl_duckdb.py* → ETL Bronze → Silver:
  - Limpia, filtra, transforma a parquet y particiona por (lang,year,month).
- *aggregate_gold.py* → Genera GOLD por (lang,year,month) con IDs:
  - article_popularity, article_top, referrers_top (opc.), edges_monthly (opc.).
- *build_gold_specifics.py* → Deriva insights listos:
  - top10_by_year, all_time_top, edges_6m (6 meses), trending_mom (MoM), referrers_6m.

## Requisitos
```bash
pip install duckdb pyyaml aiohttp tqdm