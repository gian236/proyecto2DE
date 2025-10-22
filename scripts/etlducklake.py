#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
02_etl_silver_attach.py
ETL Bronze → Silver usando DuckDB + DuckLake con sintaxis ATTACH.
Compatible con DuckDB 1.3.0+
"""

import os
import yaml
import duckdb
import argparse
from pathlib import Path
from datetime import datetime

def load_config(config_path="config/settings.yaml"):
    """Carga configuración desde YAML."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def ensure_dir(path):
    """Crea directorio si no existe."""
    os.makedirs(path, exist_ok=True)

def parse_filepath(filepath):
    """Extrae idioma, año y mes de la ruta y nombre del archivo."""
    parent_dir = filepath.parent.name
    
    if '-' in parent_dir:
        year_month = parent_dir.split('-')
        year = int(year_month[0])
        month = int(year_month[1])
    else:
        raise ValueError(f"Directorio inesperado: {parent_dir}")
    
    filename = filepath.name
    parts = filename.replace(".tsv.gz", "").replace(".tsv", "").split("-")
    
    if len(parts) >= 2 and "wiki" in parts[1]:
        lang = parts[1].replace("wiki", "")
    else:
        raise ValueError(f"Formato de archivo inesperado: {filename}")
    
    return lang, year, month

def setup_ducklake_attach(con, ducklake_name, data_files_path, metadata_path):
    """Inicializa DuckLake usando sintaxis ATTACH."""
    
    print("[INFO] Intentando cargar extensión DuckLake con ATTACH...")
    
    # Crear directorios
    ensure_dir(Path(metadata_path).parent)
    ensure_dir(data_files_path)
    
    try:
        # Instalar extensión
        print("  → Instalando extensión...")
        con.execute("INSTALL ducklake;")
        con.execute("LOAD ducklake;")
        print("  ✓ Extensión DuckLake cargada")
        
        # Usar ATTACH en lugar de CREATE DUCKLAKE
        print(f"  → Attachando DuckLake: {ducklake_name}")
        con.execute(f"""
            ATTACH 'ducklake:{metadata_path}' AS {ducklake_name}
            (DATA_PATH '{data_files_path}');
        """)
        
        # Cambiar a ese schema
        con.execute(f"USE {ducklake_name};")
        
        # Crear tabla (sin PARTITIONED BY aquí)
        print("  → Creando tabla clickstream_silver...")
        con.execute("""
            CREATE TABLE IF NOT EXISTS clickstream_silver (
                lang VARCHAR,
                year INTEGER,
                month INTEGER,
                prev_title VARCHAR,
                curr_title VARCHAR,
                type VARCHAR,
                n INTEGER
            );
        """)
        
        # Configurar particionamiento con ALTER TABLE
        print("  → Configurando particionamiento...")
        con.execute("""
            ALTER TABLE clickstream_silver 
            SET PARTITIONED BY (lang, year, month);
        """)
        
        print("  ✓ DuckLake configurado exitosamente con particionamiento")
        return True
        
    except Exception as e:
        print(f"[WARN] DuckLake no disponible: {e}")
        print("[INFO] Continuando con tabla regular + Parquet particionado...")
        return False

def setup_regular_table(con, silver_dir):
    """Crea tabla regular si DuckLake no está disponible."""
    
    print("[INFO] Creando tabla regular sin DuckLake...")
    con.execute("""
        CREATE TABLE IF NOT EXISTS clickstream_silver (
            lang VARCHAR,
            year INTEGER,
            month INTEGER,
            prev_title VARCHAR,
            curr_title VARCHAR,
            type VARCHAR,
            n INTEGER
        );
    """)
    print("  ✓ Tabla clickstream_silver creada")
    print(f"  → Los datos se guardarán como Parquet particionado en: {silver_dir}")

def process_file(con, filepath, lang, year, month, silver_dir=None, use_ducklake=True):
    """Procesa un archivo TSV.gz individual."""
    
    print(f"\n[PROCESSING] {lang}-{year}-{month:02d} ({filepath.name})")
    start_time = datetime.now()
    
    # Leer TSV.gz
    try:
        # Primero intentar detectar el formato del archivo
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW tmp_raw AS
            SELECT 
                column0 AS prev_title,
                column1 AS curr_title,
                column2 AS type,
                TRY_CAST(column3 AS INTEGER) AS n
            FROM read_csv(
                '{filepath}',
                delim = '\t',
                header = false,
                columns = {{
                    'column0': 'VARCHAR',
                    'column1': 'VARCHAR', 
                    'column2': 'VARCHAR',
                    'column3': 'VARCHAR'
                }},
                ignore_errors = true,
                null_padding = true,
                max_line_size = 1048576,
                sample_size = 50000
            )
            WHERE column1 IS NOT NULL 
              AND column3 IS NOT NULL
              AND column1 != 'curr_id';
        """)
    except Exception as e:
        error_msg = str(e)
        if "Could not convert" in error_msg or "Invalid Input Error" in error_msg:
            print(f"  ✗ Error de formato en archivo (posiblemente corrupto)")
            print(f"     Detalle: {error_msg[:100]}")
        else:
            print(f"  ✗ Error leyendo archivo: {e}")
        return False
    
    count_raw = con.execute("SELECT COUNT(*) FROM tmp_raw").fetchone()[0]
    if count_raw == 0:
        print(f"  ✗ No se encontraron registros válidos")
        return False
    
    print(f"  → Registros leídos: {count_raw:,}")
    
    # Limpiar datos
    con.execute("""
        CREATE OR REPLACE TEMP VIEW tmp_clean AS
        SELECT 
            prev_title,
            curr_title,
            type,
            n
        FROM tmp_raw
        WHERE n > 0
          AND type IN ('link', 'external', 'other')
          AND length(curr_title) > 0
          AND prev_title IS NOT NULL;
    """)
    
    count_clean = con.execute("SELECT COUNT(*) FROM tmp_clean").fetchone()[0]
    filtered = count_raw - count_clean
    print(f"  → Registros válidos: {count_clean:,} (filtrados: {filtered:,})")
    
    if count_clean == 0:
        print(f"  ✗ Todos los registros fueron filtrados")
        return False
    
    # Verificar duplicados
    existing = con.execute("""
        SELECT COUNT(*) FROM clickstream_silver
        WHERE lang = ? AND year = ? AND month = ?
    """, [lang, year, month]).fetchone()[0]
    
    if existing > 0:
        print(f"  ⚠ Ya existen {existing:,} registros - eliminando...")
        con.execute("""
            DELETE FROM clickstream_silver
            WHERE lang = ? AND year = ? AND month = ?
        """, [lang, year, month])
    
    # Insertar datos
    print(f"  → Insertando en clickstream_silver...")
    con.execute(f"""
        INSERT INTO clickstream_silver
        SELECT 
            '{lang}' AS lang,
            {year} AS year,
            {month} AS month,
            prev_title,
            curr_title,
            type,
            n
        FROM tmp_clean;
    """)
    
    # SOLO guardar Parquet si NO usamos DuckLake
    # (DuckLake ya maneja el almacenamiento en Parquet automáticamente)
    if not use_ducklake and silver_dir:
        partition_dir = Path(silver_dir) / f"lang={lang}" / f"year={year}" / f"month={month:02d}"
        ensure_dir(partition_dir)
        parquet_file = partition_dir / "data.parquet"
        
        print(f"  → Guardando Parquet: {parquet_file.name}")
        con.execute(f"""
            COPY (
                SELECT * FROM clickstream_silver
                WHERE lang = '{lang}' AND year = {year} AND month = {month}
            )
            TO '{parquet_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
        """)
    elif use_ducklake:
        print(f"  → DuckLake maneja el almacenamiento automáticamente")
    
    elapsed = (datetime.now() - start_time).total_seconds()
    rate = int(count_clean / elapsed) if elapsed > 0 else 0
    print(f"  ✓ Completado en {elapsed:.2f}s ({rate:,} rows/s)")
    
    return True

def get_stats(con):
    """Muestra estadísticas de la tabla."""
    
    print("\n" + "="*60)
    print("ESTADÍSTICAS: clickstream_silver")
    print("="*60)
    
    total = con.execute("SELECT COUNT(*) FROM clickstream_silver").fetchone()[0]
    print(f"\n📊 Total registros: {total:,}")
    
    if total == 0:
        print("\n[WARN] Tabla vacía")
        return
    
    # Por idioma
    print("\n🌍 Por idioma:")
    result = con.execute("""
        SELECT lang, 
               COUNT(*) as records, 
               SUM(n) as total_clicks,
               COUNT(DISTINCT curr_title) as unique_articles
        FROM clickstream_silver
        GROUP BY lang
        ORDER BY records DESC
    """).fetchall()
    
    for row in result:
        print(f"  {row[0]:>4}: {row[1]:>12,} records | {row[2]:>15,} clicks | {row[3]:>10,} articles")
    
    # Rango temporal
    print("\n📅 Rango temporal:")
    result = con.execute("""
        SELECT 
            MIN(year) as min_year, MAX(year) as max_year,
            MIN(month) as min_month, MAX(month) as max_month,
            COUNT(DISTINCT year || '-' || LPAD(CAST(month AS VARCHAR), 2, '0')) as num_months
        FROM clickstream_silver
    """).fetchone()
    print(f"  De {result[0]}-{result[2]:02d} a {result[1]}-{result[3]:02d}")
    print(f"  Total meses únicos: {result[4]}")
    
    # Por tipo
    print("\n🔗 Por tipo de referral:")
    result = con.execute("""
        SELECT type, COUNT(*) as count, SUM(n) as total_clicks
        FROM clickstream_silver
        GROUP BY type
        ORDER BY count DESC
    """).fetchall()
    
    for row in result:
        pct = (row[1] / total) * 100
        print(f"  {row[0]:>10}: {row[1]:>12,} ({pct:>5.1f}%) | {row[2]:>15,} clicks")
    
    # Top 10 artículos
    print("\n🔥 Top 10 artículos (global):")
    result = con.execute("""
        SELECT curr_title, SUM(n) as total_clicks, COUNT(DISTINCT lang) as languages
        FROM clickstream_silver
        GROUP BY curr_title
        ORDER BY total_clicks DESC
        LIMIT 10
    """).fetchall()
    
    for i, row in enumerate(result, 1):
        title = row[0][:50] if len(row[0]) > 50 else row[0]
        print(f"  {i:>2}. {title:50} | {row[1]:>12,} clicks | {row[2]} langs")
    
    print("\n" + "="*60)

def main():
    parser = argparse.ArgumentParser(
        description="ETL Bronze → Silver con DuckLake (sintaxis ATTACH)"
    )
    parser.add_argument("--year", type=int, help="Filtrar por año")
    parser.add_argument("--month", type=int, help="Filtrar por mes")
    parser.add_argument("--lang", type=str, help="Filtrar por idioma")
    parser.add_argument("--config", default="config/settings.yaml")
    parser.add_argument("--stats", action="store_true")
    parser.add_argument("--no-ducklake", action="store_true")
    parser.add_argument("--limit", type=int, help="Limitar N archivos")
    args = parser.parse_args()

    print("="*60)
    print("ETL: Bronze → Silver Layer (DuckLake ATTACH)")
    print("="*60)

    # Cargar config
    cfg = load_config(args.config)
    bronze_dir = Path(cfg["bronze_dir"])
    silver_dir = Path(cfg["silver_dir"])
    db_path = cfg["duckdb_path"]
    ducklake_name = cfg.get("ducklake_name", "wikipedia_lakehouse")
    metadata_path = cfg.get("ducklake_metadata", "data/metadata.ducklake")

    ensure_dir(silver_dir)
    ensure_dir(Path(db_path).parent)

    print(f"\n[INFO] Conectando a DuckDB: {db_path}")
    con = duckdb.connect(db_path)
    
    # Verificar versión DuckDB
    version = con.execute("SELECT version()").fetchone()[0]
    print(f"[INFO] DuckDB version: {version}")
    
    # Setup DuckLake o tabla regular
    if not args.no_ducklake:
        use_ducklake = setup_ducklake_attach(con, ducklake_name, 
                                            silver_dir.as_posix(), 
                                            metadata_path)
    else:
        use_ducklake = False
    
    if not use_ducklake:
        setup_regular_table(con, silver_dir)
    
    if args.stats:
        get_stats(con)
        con.close()
        return
    
    if not bronze_dir.exists():
        print(f"\n[ERROR] Directorio bronze no existe: {bronze_dir}")
        con.close()
        return

    print(f"\n[INFO] Escaneando archivos en: {bronze_dir}")
    all_files = sorted(bronze_dir.rglob("*.tsv.gz"))
    
    if not all_files:
        all_files = sorted(bronze_dir.rglob("*.tsv"))
    
    if not all_files:
        print(f"[ERROR] No se encontraron archivos")
        con.close()
        return

    print(f"[INFO] Encontrados {len(all_files)} archivos")

    if args.limit:
        all_files = all_files[:args.limit]
        print(f"[INFO] Limitando a {args.limit} archivos")

    processed = 0
    skipped = 0
    errors = 0

    print("\n" + "="*60)
    for i, filepath in enumerate(all_files, 1):
        print(f"\n[{i}/{len(all_files)}]", end=" ")
        
        try:
            lang, year, month = parse_filepath(filepath)
        except Exception as e:
            print(f"✗ Error parseando: {e}")
            errors += 1
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
            success = process_file(con, filepath, lang, year, month, 
                                 silver_dir=silver_dir, use_ducklake=use_ducklake)
            if success:
                processed += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"  ✗ ERROR: {e}")
            errors += 1

    print("\n" + "="*60)
    print("RESUMEN")
    print("="*60)
    print(f"  ✓ Procesados: {processed}")
    print(f"  ⊘ Omitidos:   {skipped}")
    print(f"  ✗ Errores:    {errors}")
    
    # Mostrar tamaño de archivos
    if processed > 0:
        # Tamaño del .duckdb
        if Path(db_path).exists():
            db_size_mb = Path(db_path).stat().st_size / (1024*1024)
            print(f"\n💾 Tamaño base datos: {db_size_mb:.1f} MB")
        
        # Tamaño del directorio silver
        if use_ducklake:
            # DuckLake guarda en data_files
            silver_size = sum(f.stat().st_size for f in Path(silver_dir).rglob('*') if f.is_file())
            silver_size_mb = silver_size / (1024*1024)
            print(f"💾 Tamaño Silver (Parquet): {silver_size_mb:.1f} MB")
        else:
            # Tabla regular: Parquet manual
            silver_size = sum(f.stat().st_size for f in Path(silver_dir).rglob('*.parquet') if f.is_file())
            silver_size_mb = silver_size / (1024*1024)
            print(f"💾 Tamaño Silver (Parquet): {silver_size_mb:.1f} MB")
    
    print("="*60)

    if processed > 0:
        get_stats(con)

    con.close()
    print("\n[DONE] ETL completado.")

if __name__ == "__main__":
    main()