#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
aggregate_gold.py — GOLD sin duplicación de textos
- Guarda títulos una sola vez (dimensión por idioma) y usa IDs en todos los outputs.
- Paraleliza por idioma; dentro de cada idioma es secuencial (evita carreras al construir el diccionario).
- ZSTD + ROW_GROUP_SIZE 250k. Skip por archivo. Umbrales y switches para outputs pesados.
- FIX Windows: sin lambda en ProcessPoolExecutor.
- FIX Binder Error: UNION/UNION ALL con columnas explícitas y tipos casteados.
"""

from pathlib import Path
import argparse, os, yaml, duckdb, time
from typing import Dict, List, Tuple, DefaultDict
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict

# ---------------- util ----------------

def load_config(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def parse_partition_from_path(silver_file: Path) -> Tuple[str,int,int]:
    parts = {kv.split("=")[0]: kv.split("=")[1] for kv in silver_file.parent.parts[-3:]}
    return parts["lang"], int(parts["year"]), int(parts["month"])

def atomic_copy_query_to_parquet(con: duckdb.DuckDBPyConnection, query: str, out_file: Path, label: str):
    ensure_dir(out_file.parent)
    tmp = out_file.with_suffix(out_file.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    t0 = time.time()
    con.execute(
        f"COPY ({query}) TO '{tmp.as_posix()}' "
        f"(FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 250000)"
    )
    if out_file.exists():
        out_file.unlink()
    tmp.rename(out_file)
    print(f"[WRITE] {label:<18} → {out_file.as_posix()} ({time.time()-t0:0.2f}s)")

def outputs_for(lang:str, year:int, month:int, gold_root:Path) -> Dict[str,Path]:
    mm = f"{month:02d}"
    base = lambda name: gold_root / name / f"lang={lang}" / f"year={year}" / f"month={mm}" / "data.parquet"
    return {
        "article_popularity": base("article_popularity"),
        "article_top":        base("article_top"),
        "referrers_top":      base("referrers_top"),
        "edges_monthly":      base("edges_monthly"),
    }

def dim_path_for(lang: str, gold_root: Path) -> Path:
    return gold_root / "dimensions" / "articles" / f"lang={lang}" / "dict.parquet"

def missing_outputs(paths: Dict[str,Path]) -> List[str]:
    miss=[]
    for k,p in paths.items():
        try:
            if not p.exists() or os.path.getsize(p)<=0:
                miss.append(k)
        except OSError:
            miss.append(k)
    return miss

def pick_latest_month(files: List[Path]) -> List[Path]:
    if not files:
        return files
    ym = [parse_partition_from_path(f)[1:3] for f in files]
    my, mm = max(ym)
    return sorted([f for f in files if parse_partition_from_path(f)[1:3]==(my,mm)],
                  key=lambda p: parse_partition_from_path(p)[0])

# ---------------- dimensión por idioma (IDs) ----------------

def load_or_init_dim(con: duckdb.DuckDBPyConnection, lang: str, dim_file: Path):
    """
    Carga la dimensión si existe, casteando y nombrando columnas explícitamente.
    Si no existe, crea la tabla temporal vacía con schema (id BIGINT, title VARCHAR).
    """
    if dim_file.exists() and os.path.getsize(dim_file) > 0:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE dim AS
            SELECT
              CAST(id AS BIGINT)   AS id,
              CAST(title AS VARCHAR) AS title
            FROM read_parquet('{dim_file.as_posix()}')
        """)
    else:
        con.execute("CREATE OR REPLACE TEMP TABLE dim (id BIGINT, title VARCHAR)")
    # Índice simple para joins (misma forma/orden)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE dim_idx AS
        SELECT CAST(id AS BIGINT) AS id, CAST(title AS VARCHAR) AS title FROM dim
    """)

def update_dim_with_partition(con: duckdb.DuckDBPyConnection, lang: str, dim_file: Path):
    """
    Construye el set de títulos del mes (prev+curr), detecta nuevos vs dim existente,
    asigna IDs incrementales y persiste el diccionario con columnas explícitas.
    """
    # 1) Títulos del mes (en una sola columna y con tipo fijo)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE part_titles_all AS
        SELECT CAST(curr_title AS VARCHAR) AS title FROM s WHERE curr_title IS NOT NULL
        UNION ALL
        SELECT CAST(prev_title AS VARCHAR) AS title FROM s WHERE prev_title IS NOT NULL
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE part_titles AS
        SELECT DISTINCT title FROM part_titles_all
    """)

    # 2) Detectar títulos nuevos (antijoin por título)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE new_titles AS
        SELECT p.title
        FROM part_titles p
        LEFT JOIN dim_idx d ON p.title = d.title
        WHERE d.id IS NULL
    """)

    # 3) Asignar IDs consecutivos a los nuevos y unir con la dimensión existente
    base = con.execute("SELECT COALESCE(MAX(id), 0) FROM dim").fetchone()[0]
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dim_new AS
        SELECT
          (ROW_NUMBER() OVER (ORDER BY title)) + {base} AS id,
          title
        FROM (SELECT DISTINCT title FROM new_titles) t
    """)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE dim_merged AS
        SELECT CAST(id AS BIGINT) AS id, CAST(title AS VARCHAR) AS title FROM dim
        UNION ALL
        SELECT CAST(id AS BIGINT) AS id, CAST(title AS VARCHAR) AS title FROM dim_new
    """)

    atomic_copy_query_to_parquet(con, "SELECT id, title FROM dim_merged", dim_file, f"article_dim[{lang}]")

    # Refrescar tablas temporales para esta conexión
    con.execute("CREATE OR REPLACE TEMP TABLE dim AS SELECT id, title FROM dim_merged")
    con.execute("CREATE OR REPLACE TEMP TABLE dim_idx AS SELECT id, title FROM dim")

# ---------------- outputs ----------------

def compute_and_write_outputs(con, outs: Dict[str,Path], topn:int, topm:int,
                              min_edges:int, min_ref:int, write_edges:bool, write_ref:bool):
    # Mapear a IDs (cast explícito por seguridad)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE s_ids AS
        SELECT
          s.lang,
          s.year,
          s.month,
          CAST(dp.id AS BIGINT)  AS prev_id,
          CAST(dc.id AS BIGINT)  AS curr_id,
          CAST(s.n AS BIGINT)    AS n
        FROM s
        JOIN dim_idx dc ON dc.title = s.curr_title
        LEFT JOIN dim_idx dp ON dp.title = s.prev_title
    """)

    # Popularidad (curr_id) y Top-N
    q_pop = """
        SELECT lang, year, month, curr_id, SUM(n) AS total_clicks
        FROM s_ids
        GROUP BY 1,2,3,4
    """
    q_top = f"""
        WITH pop AS ({q_pop})
        SELECT lang, year, month, curr_id, total_clicks,
               ROW_NUMBER() OVER (PARTITION BY lang,year,month ORDER BY total_clicks DESC) AS rn
        FROM pop
        QUALIFY rn <= {topn}
    """
    atomic_copy_query_to_parquet(con, q_pop, outs["article_popularity"], "article_popularity")
    atomic_copy_query_to_parquet(con, q_top, outs["article_top"], "article_top")

    # Referrers (prev_id → curr_id)
    if write_ref:
        q_ref = f"""
            WITH agg AS (
              SELECT lang, year, month, curr_id, prev_id, SUM(n) AS clicks_from_prev
              FROM s_ids
              WHERE prev_id IS NOT NULL
              GROUP BY 1,2,3,4,5
            )
            SELECT lang, year, month, curr_id, prev_id, clicks_from_prev,
                   ROW_NUMBER() OVER (PARTITION BY lang,year,month,curr_id ORDER BY clicks_from_prev DESC) AS rn
            FROM agg
            WHERE clicks_from_prev >= {min_ref}
            QUALIFY rn <= {topm}
        """
        atomic_copy_query_to_parquet(con, q_ref, outs["referrers_top"], "referrers_top")

    # Edges (mensual)
    if write_edges:
        q_edges = f"""
            SELECT lang, year, month, prev_id, curr_id, SUM(n) AS total_transitions
            FROM s_ids
            WHERE prev_id IS NOT NULL
            GROUP BY 1,2,3,4,5
            HAVING SUM(n) >= {min_edges}
        """
        atomic_copy_query_to_parquet(con, q_edges, outs["edges_monthly"], "edges_monthly")

def run_one_partition(con, silver_file: Path, gold_root: Path,
                      topn:int, topm:int, min_edges:int, min_ref:int,
                      write_edges:bool, write_ref:bool, skip_existing:bool=True):
    lang, year, month = parse_partition_from_path(silver_file)
    outs = outputs_for(lang, year, month, gold_root)
    to_write = list(outs.keys()) if not skip_existing else missing_outputs(outs)
    if not write_edges and "edges_monthly" in to_write:
        to_write.remove("edges_monthly")
    if not write_ref   and "referrers_top" in to_write:
        to_write.remove("referrers_top")
    if skip_existing and not to_write:
        print(f"[SKIP] GOLD ya existe para lang={lang} year={year} month={month:02d}")
        return "skipped"

    src = silver_file.as_posix()
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE s AS
        SELECT
          lang,
          year,
          month,
          CAST(prev_title AS VARCHAR) AS prev_title,
          CAST(curr_title AS VARCHAR) AS curr_title,
          CAST(n AS BIGINT)           AS n
        FROM read_parquet('{src}')
        WHERE n > 0
    """)

    dim_file = dim_path_for(lang, gold_root)
    load_or_init_dim(con, lang, dim_file)
    update_dim_with_partition(con, lang, dim_file)

    print(f"\n[START] GOLD lang={lang} year={year} month={month:02d} (a escribir: {', '.join(to_write) if to_write else 'nada'})")
    t0 = time.time()
    compute_and_write_outputs(con, outs, topn, topm, min_edges, min_ref, write_edges, write_ref)
    print(f"[OK] GOLD lang={lang} year={year} month={month:02d} ({time.time()-t0:0.2f}s)")
    return "processed"

# ---------------- ejecución por idioma ----------------

def process_lang(lang: str, files: List[Path], gold_dir: Path,
                 topn:int, topm:int, min_edges:int, min_ref:int,
                 write_edges:bool, write_ref:bool, skip_existing:bool, threads:int, tmpdir:str, mem_limit:str):
    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"PRAGMA threads={threads}")
        con.execute("PRAGMA enable_object_cache=true")
        con.execute("PRAGMA preserve_insertion_order=false")
        con.execute(f"PRAGMA temp_directory='{tmpdir}'")
        con.execute(f"PRAGMA memory_limit='{mem_limit if mem_limit!='auto' else '24GB'}'")
        processed=skipped=0
        for f in sorted(files, key=lambda p: (parse_partition_from_path(p)[1], parse_partition_from_path(p)[2])):
            try:
                status = run_one_partition(con, f, gold_dir, topn, topm, min_edges, min_ref,
                                           write_edges, write_ref, skip_existing)
                processed += (status=="processed"); skipped += (status!="processed")
            except Exception as e:
                y,m = parse_partition_from_path(f)[1:3]
                print(f"[ERROR] lang={lang} year={y} month={m:02d}: {e}")
                skipped+=1
        return processed, skipped
    finally:
        con.close()


def _process_lang_star(args):
    return process_lang(*args)

# ---------------- main ----------------

def main():
    ap = argparse.ArgumentParser(description="GOLD (IDs + dimensión por idioma, sin duplicar títulos)")
    ap.add_argument("--config", default="config/settings.yaml")
    ap.add_argument("--topn", type=int, default=1000)
    ap.add_argument("--topm-referrers", type=int, default=50)
    ap.add_argument("--threads", type=int, default=6)
    ap.add_argument("--workers", type=int, default=2, help="Paralelo por idioma")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--skip-existing", dest="skip_existing", action="store_true", default=True)
    g.add_argument("--no-skip-existing", dest="skip_existing", action="store_false")
    ap.add_argument("--latest-month", action="store_true")
    ap.add_argument("--lang", type=str)
    ap.add_argument("--year", type=int)
    ap.add_argument("--month", type=int)
    ap.add_argument("--memory-limit", type=str, default="26 GB")
    ap.add_argument("--min-clicks-edges", type=int, default=1)
    ap.add_argument("--min-clicks-referrers", type=int, default=1)
    ap.add_argument("--no-edges", action="store_true")
    ap.add_argument("--no-referrers", action="store_true")
    args = ap.parse_args()

    cfg = load_config(args.config)
    silver_dir = Path(cfg["silver_dir"]).expanduser().resolve()
    gold_dir   = Path(cfg.get("gold_dir","data/gold")).expanduser().resolve()

    pattern = "lang=*/year=*/month=*/data.parquet"
    files = sorted(silver_dir.glob(pattern))
    if args.lang:
        files = [f for f in files if f.parent.parent.parent.name == f"lang={args.lang}"]
    if args.year:
        files = [f for f in files if f.parent.parent.name == f"year={args.year}"]
    if args.month:
        files = [f for f in files if f.parent.name == f"month={args.month:02d}"]
    if args.latest_month and files:
        files = pick_latest_month(files)
    if not files:
        print(f"❌ No hay archivos SILVER en {silver_dir.as_posix()}/{pattern} con los filtros dados.")
        return

    ensure_dir(gold_dir)
    tmp_root = Path("tmp_duck") if Path("tmp_duck").exists() else Path("duckdb_tmp")
    tmpdir = tmp_root.expanduser().resolve()
    ensure_dir(tmpdir)

    # Agrupar por idioma; se corre secuencialmente por idioma, en paralelo entre idiomas
    by_lang: DefaultDict[str, List[Path]] = defaultdict(list)
    for f in files:
        by_lang[parse_partition_from_path(f)[0]].append(f)

    print("🚀 Starting GOLD (IDs, no duplicated titles)")
    print(f"📚 SILVER root : {silver_dir.as_posix()}")
    print(f"📂 GOLD root   : {gold_dir.as_posix()}")
    print(f"🧮 Langs: {len(by_lang)} | Particiones totales: {len(files)}")
    print(f"🧵 Threads DuckDB: {args.threads} | 🧰 Workers (por idioma): {args.workers}")
    print(f"💾 memory_limit: {args.memory_limit if args.memory_limit!='auto' else '24GB (default)'}  |  temp_directory: {tmpdir.as_posix()}")
    if args.no_edges:
        print("⚠️  edges_monthly: DESACTIVADO")
    if args.no_referrers:
        print("⚠️  referrers_top: DESACTIVADO")

    jobs = []
    for lang, fs in by_lang.items():
        jobs.append((lang, fs, gold_dir, args.topn, args.topm_referrers,
                     args.min_clicks_edges, args.min_clicks_referrers,
                     not args.no_edges, not args.no_referrers,
                     args.skip_existing, args.threads, tmpdir.as_posix(), args.memory_limit))

    processed=skipped=0
    if args.workers>1:
        with ProcessPoolExecutor(max_workers=args.workers) as ex:
            for (p,s) in ex.map(_process_lang_star, jobs):
                processed += p
                skipped   += s
    else:
        for j in jobs:
            p,s = process_lang(*j)
            processed += p
            skipped   += s

    print(f"\n[RESUMEN] Procesadas: {processed}  Omitidas: {skipped}")
    print("✅ GOLD completado.")

if __name__ == "__main__":
    main()
