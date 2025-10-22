#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
etl_duckdb_fixed.py
ETL optimizado con DuckDB para Wikipedia Clickstream.
Procesa TSV.gz a Parquet particionado por lang/year/month.
Incluye limpieza avanzada y control de calidad.
"""

import os
import yaml
import duckdb
import argparse
from pathlib import Path


def load_config(config_path="config/settings.yaml"):
    """Carga configuración desde YAML."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def ensure_dir(path):
    """Crea directorio si no existe."""
    os.makedirs(path, exist_ok=True)


def parse_filename(filename):
    parts = filename.replace(".tsv.gz", "").replace(".tsv", "").split("-")
    if len(parts) < 4:
        raise ValueError(f"Formato de nombre inesperado: {filename}")
    lang = parts[1].replace("wiki", "")
    year = int(parts[2])
    month = int(parts[3])
    return lang, year, month


def parse_filepath(filepath):
    parent_dir = filepath.parent.name
    if "-" in parent_dir:
        year_month = parent_dir.split("-")
        year = int(year_month[0])
        month = int(year_month[1])
    else:
        return parse_filename(filepath.name)

    filename = filepath.name
    parts = filename.replace(".tsv.gz", "").replace(".tsv", "").split("-")
    if len(parts) >= 2 and "wiki" in parts[1]:
        lang = parts[1].replace("wiki", "")
    else:
        raise ValueError(f"Formato inesperado: {filename}")
    return lang, year, month


def process_file(con, filepath, silver_dir, lang, year, month, skip_existing=False):
    """Procesa un solo archivo TSV → Parquet limpio."""
    partition_path = silver_dir / f"lang={lang}" / f"year={year}" / f"month={month:02d}"
    output_file = partition_path / "data.parquet"

    if skip_existing and output_file.exists():
        print(f"[SKIP] {lang}-{year}-{month:02d} ya existe")
        return None

    print(f"[INFO] Procesando {lang}-{year}-{month:02d} ...")

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW tmp_raw AS
        SELECT 
            prev AS prev_title,
            curr AS curr_title,
            type,
            n
        FROM read_csv(
            '{filepath}',
            delim = '\t',
            header = false,
            columns = {{
                'prev':'VARCHAR',
                'curr':'VARCHAR',
                'type':'VARCHAR',
                'n':'INTEGER'
            }},
            ignore_errors = true,
            null_padding = true
        )
        WHERE curr IS NOT NULL 
          AND n IS NOT NULL
          AND n > 0;
    """)

    count = con.execute("SELECT COUNT(*) FROM tmp_raw").fetchone()[0]
    if count == 0:
        print(f"[WARN] No se encontraron registros válidos en {filepath.name}")
        return False
    print(f"[INFO] Registros válidos: {count:,}")

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW tmp_clean AS
        SELECT 
            '{lang}' AS lang,
            {year} AS year,
            {month} AS month,
            TRIM(REPLACE(prev_title, '_', ' ')) AS prev_title,
            TRIM(REPLACE(curr_title, '_', ' ')) AS curr_title,
            LOWER(TRIM(type)) AS type,
            n
        FROM tmp_raw
        WHERE type IN ('link', 'external', 'other')
          AND LENGTH(curr_title) > 0
          AND curr_title NOT LIKE 'Special:%'
          AND curr_title NOT LIKE 'User:%'
          AND curr_title NOT LIKE 'File:%'
          AND curr_title NOT LIKE 'Talk:%'
          AND curr_title NOT LIKE 'Category:%'
          AND curr_title NOT LIKE '%404%'
          AND curr_title != prev_title;
    """)

    con.execute("""
        CREATE OR REPLACE TEMP VIEW tmp_dedup AS
        SELECT 
            lang, year, month, prev_title, curr_title, type, MAX(n) AS n
        FROM tmp_clean
        GROUP BY ALL;
    """)

    ensure_dir(partition_path)

    con.execute(f"""
        COPY (SELECT * FROM tmp_dedup)
        TO '{output_file}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 250000);
    """)

    print(f"[OK] Guardado en: {output_file}")
    return True


def update_catalog_table(con, silver_dir):
    """Actualiza la vista 'clickstream_silver' con todos los Parquet existentes."""
    print("[INFO] Creando tabla catálogo (vista sobre Parquet)...")
    con.execute("DROP VIEW IF EXISTS clickstream_silver;")

    # 🔹 Ruta absoluta para evitar errores en DataGrip o CLI
    abs_pattern = Path(silver_dir).resolve().as_posix() + "/lang=*/year=*/month=*/data.parquet"

    # 🔹 Crear vista basada en patrón, no en lista (más eficiente)
    con.execute(f"""
        CREATE VIEW clickstream_silver AS
        SELECT *
        FROM read_parquet('{abs_pattern}', hive_partitioning = true);
    """)

    try:
        count = con.execute("SELECT COUNT(*) FROM clickstream_silver").fetchone()[0]
        print(f"[OK] Vista clickstream_silver creada: {count:,} registros")
    except Exception as e:
        print(f"[WARN] No se pudo contar registros de la vista: {e}")

    print("[INFO] Nota: la vista no duplica datos, solo apunta a los Parquet")


def main():
    parser = argparse.ArgumentParser(description="ETL Wikipedia Clickstream con DuckDB")
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    parser.add_argument("--lang", type=str)
    parser.add_argument("--config", default="config/settings.yaml")
    parser.add_argument("--skip-catalog", action="store_true")
    parser.add_argument("--skip-existing", action="store_true")
    args = parser.parse_args()

    cfg = load_config(args.config)
    bronze_dir = Path(cfg["bronze_dir"])
    silver_dir = Path(cfg["silver_dir"])
    db_path = cfg["duckdb_path"]

    if not bronze_dir.exists():
        print(f"[ERROR] Directorio bronze no existe: {bronze_dir}")
        return

    ensure_dir(silver_dir)
    ensure_dir(Path(db_path).parent)

    con = duckdb.connect(db_path)
    con.execute("SET enable_progress_bar = true;")

    all_files = sorted(bronze_dir.rglob("*.tsv.gz")) or sorted(bronze_dir.rglob("*.tsv"))
    if not all_files:
        print(f"[ERROR] No se encontraron archivos TSV/TSV.gz en {bronze_dir}")
        con.close()
        return

    print(f"[INFO] Encontrados {len(all_files)} archivos en bronze")

    processed = skipped = 0
    for filepath in all_files:
        try:
            lang, year, month = parse_filepath(filepath)
        except Exception as e:
            print(f"[WARN] Nombre inesperado '{filepath.name}': {e}")
            skipped += 1
            continue

        if args.year and year != args.year:
            skipped += 1
            continue
        if args.month and month != args.month:
            skipped += 1
            continue
        if args.lang and lang != args.lang:
            skipped += 1
            continue

        try:
            result = process_file(con, filepath, silver_dir, lang, year, month, args.skip_existing)
            if result is True:
                processed += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"[ERROR] Fallo procesando {filepath.name}: {e}")
            skipped += 1
            continue

    print(f"\n[RESUMEN] Procesados: {processed}, Omitidos: {skipped}")

    if processed > 0 and not args.skip_catalog:
        try:
            update_catalog_table(con, silver_dir)
        except Exception as e:
            print(f"[ERROR] Fallo actualizando catálogo: {e}")

    con.close()
    print("[DONE] ETL completado exitosamente.")


if __name__ == "__main__":
    main()
