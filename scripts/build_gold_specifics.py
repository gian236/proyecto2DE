#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_gold_specifics.py
Crea una carpeta GOLD_Specifics con agregados "interesantes" listos para análisis:

- top10_by_year/lang=XX/year=YYYY/data.parquet
- all_time_top/lang=XX/data.parquet
- edges_6m/lang=XX/year=YYYY/month=MM/data.parquet
- trending_mom/lang=XX/year=YYYY/month=MM/data.parquet
- referrers_6m/lang=XX/year=YYYY/month=MM/data.parquet

Aprovecha particionado Hive y projection pushdown. Skip por defecto (no reescribe),
o usa --no-skip para forzar overwrite.

Cambios:
- Procesar por defecto **todos** los idiomas detectados en GOLD (union de article_popularity y edges_monthly).
- Robustez de esquema: si no existe *_title, usa *_name; si no, CAST(*_id AS VARCHAR).
"""

from pathlib import Path
import argparse
import os
import time
import yaml
import duckdb
from typing import List, Tuple, Optional, Set

# -------------------- util --------------------

def load_config(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def atomic_copy_query_to_parquet(con: duckdb.DuckDBPyConnection, query: str, out_file: Path, label: str):
    ensure_dir(out_file.parent)
    tmp_file = out_file.with_suffix(out_file.suffix + ".tmp")
    if tmp_file.exists():
        tmp_file.unlink()
    t0 = time.time()
    con.execute(f"COPY ({query}) TO '{tmp_file.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    if out_file.exists():
        out_file.unlink()
    tmp_file.rename(out_file)
    dt = time.time() - t0
    sz = out_file.stat().st_size
    human = f"{sz/1024/1024:,.2f} MB" if sz >= 1024*1024 else f"{sz/1024:,.1f} KB"
    print(f"[WRITE] {label:<18} → {out_file.as_posix()}  ({human}, {dt:0.2f}s)")

def list_langs(gold_dir: Path, topic: str) -> List[str]:
    base = gold_dir / topic
    if not base.exists():
        return []
    langs = []
    for p in base.glob("lang=*"):
        if p.is_dir():
            langs.append(p.name.split("=",1)[1])
    return sorted(langs)

def list_langs_union(gold_dir: Path, topics: List[str]) -> List[str]:
    out: Set[str] = set()
    for t in topics:
        out |= set(list_langs(gold_dir, t))
    return sorted(out)

def latest_period(con: duckdb.DuckDBPyConnection, path_glob: str) -> Tuple[Optional[int], Optional[int]]:
    # retorna (year, month) máximo existente en un subárbol parquet hive
    sql = f"""
    SELECT MAX(make_date(CAST(year AS INT), CAST(month AS INT), 1)) AS maxp
    FROM read_parquet('{path_glob}', hive_partitioning=true)
    """
    maxp = con.execute(sql).fetchone()[0]
    if maxp is None:
        return None, None
    y, m = con.execute("SELECT YEAR(?), MONTH(?)", [maxp, maxp]).fetchone()
    return int(y), int(m)

def parquet_columns(con: duckdb.DuckDBPyConnection, src_glob: str) -> Set[str]:
    # Lee solo el header para obtener columnas del parquet (sin tocar datos)
    cur = con.execute(f"SELECT * FROM read_parquet('{src_glob}', hive_partitioning=true) LIMIT 0")
    return {d[0] for d in cur.description or []}

def title_expr(cols: Set[str], prefix: str) -> str:
    """
    Devuelve una expresión SQL para una columna de "título":
    - <prefix>_title si existe
    - <prefix>_name si existe
    - CAST(<prefix>_id AS VARCHAR) si existe
    """
    if f"{prefix}_title" in cols:
        return f"{prefix}_title"
    if f"{prefix}_name" in cols:
        return f"{prefix}_name"
    if f"{prefix}_id" in cols:
        return f"CAST({prefix}_id AS VARCHAR)"
    # Si no hay ninguna, devolvemos un literal para no romper, pero avisamos después.
    return f"NULL"

# -------------------- builders --------------------

def build_top10_by_year(con, gold_dir: Path, out_root: Path, lang: str, overwrite: bool):
    src = (gold_dir / "article_popularity" / f"lang={lang}" / "year=*" / "month=*" / "data.parquet").as_posix()


    years = [r[0] for r in con.execute(
        f"SELECT DISTINCT CAST(year AS INT) FROM read_parquet('{src}', hive_partitioning=true) ORDER BY 1"
    ).fetchall()]

    if not years:
        print(f"[WARN] top10_by_year: no data for lang={lang}")
        return

    cols = parquet_columns(con, src)
    curr_title_sql = title_expr(cols, "curr")
    if curr_title_sql == "NULL":
        print(f"[WARN] top10_by_year: columnas de título no encontradas (curr_*). lang={lang}; se omite.")
        return

    # consulta base (filtraremos por año al escribir)
    q = f"""
    WITH base AS (
      SELECT
        CAST(year AS INT)  AS y,
        {curr_title_sql}   AS curr_title,
        CAST(total_clicks AS BIGINT) AS total_clicks
      FROM read_parquet('{src}', hive_partitioning=true)
    ),
    agg AS (
      SELECT y, curr_title, SUM(total_clicks) AS clicks_year
      FROM base GROUP BY y, curr_title
    ),
    ranked AS (
      SELECT y, curr_title, clicks_year,
             ROW_NUMBER() OVER (PARTITION BY y ORDER BY clicks_year DESC) AS rn
      FROM agg
    )
    SELECT y AS year, curr_title, clicks_year, rn
    FROM ranked
    """

    for y in years:
        out = out_root / "top10_by_year" / f"lang={lang}" / f"year={y:04d}" / "data.parquet"
        if out.exists() and not overwrite:
            print(f"[SKIP] top10_by_year lang={lang} year={y}")
            continue
        atomic_copy_query_to_parquet(
            con,
            f"SELECT * FROM ({q}) WHERE year={y} AND rn<=10 ORDER BY year, rn",
            out,
            f"top10_by_year {lang}-{y}"
        )

def build_all_time_top(con, gold_dir: Path, out_root: Path, lang: str, topn: int, overwrite: bool):
    src = (gold_dir / "article_popularity" / f"lang={lang}" / "year=*" / "month=*" / "data.parquet").as_posix()
    out = out_root / "all_time_top" / f"lang={lang}" / "data.parquet"
    if out.exists() and not overwrite:
        print(f"[SKIP] all_time_top lang={lang}")
        return

    # si no hay datos, saltar silenciosamente
    exists = con.execute(f"SELECT COUNT(*)>0 FROM read_parquet('{src}', hive_partitioning=true)").fetchone()[0]
    if not exists:
        print(f"[WARN] all_time_top: no data for lang={lang}")
        return

    cols = parquet_columns(con, src)
    curr_title_sql = title_expr(cols, "curr")
    if curr_title_sql == "NULL":
        print(f"[WARN] all_time_top: columnas de título no encontradas (curr_*). lang={lang}; se omite.")
        return

    q = f"""
    WITH base AS (
      SELECT {curr_title_sql} AS curr_title, CAST(total_clicks AS BIGINT) AS total_clicks
      FROM read_parquet('{src}', hive_partitioning=true)
    )
    SELECT curr_title, SUM(total_clicks) AS clicks_all_time
    FROM base
    GROUP BY curr_title
    ORDER BY clicks_all_time DESC
    LIMIT {topn}
    """
    atomic_copy_query_to_parquet(con, q, out, f"all_time_top {lang}")

def build_edges_6m(con, gold_dir: Path, out_root: Path, lang: str, topn_pairs: int, overwrite: bool):
    src = (gold_dir / "edges_monthly" / f"lang={lang}" / "year=*" / "month=*" / "data.parquet").as_posix()
    y, m = latest_period(con, src)
    if y is None:
        print(f"[WARN] edges_6m: no data for lang={lang}")
        return
    out = out_root / "edges_6m" / f"lang={lang}" / f"year={y:04d}" / f"month={m:02d}" / "data.parquet"
    if out.exists() and not overwrite:
        print(f"[SKIP] edges_6m lang={lang} period={y}-{m:02d}")
        return

    cols = parquet_columns(con, src)
    prev_title_sql = title_expr(cols, "prev")
    curr_title_sql = title_expr(cols, "curr")
    if prev_title_sql == "NULL" or curr_title_sql == "NULL":
        print(f"[WARN] edges_6m: columnas de título no encontradas (prev_*/curr_*). lang={lang}; se omite.")
        return

    q = f"""
    WITH last6 AS (
      SELECT CAST(YEAR(gs) AS INT) y, CAST(MONTH(gs) AS INT) m
      FROM generate_series(make_date({y},{m},1) - INTERVAL 5 MONTH,
                           make_date({y},{m},1), INTERVAL 1 MONTH) t(gs)
    )
    SELECT
      {prev_title_sql} AS prev_title,
      {curr_title_sql} AS curr_title,
      SUM(CAST(d.total_transitions AS BIGINT)) AS transitions_6m
    FROM read_parquet('{src}', hive_partitioning=true) d
    WHERE (CAST(d.year AS INT), CAST(d.month AS INT)) IN (SELECT y,m FROM last6)
    GROUP BY prev_title, curr_title
    ORDER BY transitions_6m DESC
    LIMIT {topn_pairs}
    """
    atomic_copy_query_to_parquet(con, q, out, f"edges_6m {lang}-{y}-{m:02d}")

def build_trending_mom(con, gold_dir: Path, out_root: Path, lang: str, topn: int, overwrite: bool):
    src = (gold_dir / "article_popularity" / f"lang={lang}" / "year=*" / "month=*" / "data.parquet").as_posix()
    y, m = latest_period(con, src)
    if y is None:
        print(f"[WARN] trending_mom: no data for lang={lang}")
        return
    out = out_root / "trending_mom" / f"lang={lang}" / f"year={y:04d}" / f"month={m:02d}" / "data.parquet"
    if out.exists() and not overwrite:
        print(f"[SKIP] trending_mom lang={lang} period={y}-{m:02d}")
        return

    cols = parquet_columns(con, src)
    curr_title_sql = title_expr(cols, "curr")
    if curr_title_sql == "NULL":
        print(f"[WARN] trending_mom: columnas de título no encontradas (curr_*). lang={lang}; se omite.")
        return

    q = f"""
    WITH two AS (
      SELECT make_date({y},{m},1) AS nowp,
             make_date({y},{m},1) - INTERVAL 1 MONTH AS prevp
    ),
    base AS (
      SELECT
        make_date(CAST(year AS INT), CAST(month AS INT), 1) AS period,
        {curr_title_sql} AS curr_title,
        CAST(total_clicks AS BIGINT) AS clicks
      FROM read_parquet('{src}', hive_partitioning=true)
      WHERE make_date(CAST(year AS INT), CAST(month AS INT), 1)
            IN ((SELECT nowp FROM two), (SELECT prevp FROM two))
    ),
    pv AS (
      SELECT
        curr_title,
        MAX(CASE WHEN period = (SELECT nowp  FROM two) THEN clicks END) AS clicks_now,
        MAX(CASE WHEN period = (SELECT prevp FROM two) THEN clicks END) AS clicks_prev
      FROM base
      GROUP BY curr_title
    )
    SELECT
      curr_title,
      clicks_now,
      clicks_prev,
      (clicks_now - clicks_prev) AS abs_delta,
      CASE WHEN clicks_prev > 0 THEN (clicks_now - clicks_prev) * 100.0 / clicks_prev END AS pct_delta
    FROM pv
    WHERE clicks_now IS NOT NULL AND clicks_prev IS NOT NULL
    ORDER BY pct_delta DESC NULLS LAST, abs_delta DESC
    LIMIT {topn}
    """
    atomic_copy_query_to_parquet(con, q, out, f"trending_mom {lang}-{y}-{m:02d}")

def build_referrers_6m(con, gold_dir: Path, out_root: Path, lang: str, topm_referrers: int, overwrite: bool):
    # Usamos edges_monthly (no truncado) para sumar 6m por destino
    src = (gold_dir / "edges_monthly" / f"lang={lang}" / "year=*" / "month=*" / "data.parquet").as_posix()
    y, m = latest_period(con, src)
    if y is None:
        print(f"[WARN] referrers_6m: no data for lang={lang}")
        return
    out = out_root / "referrers_6m" / f"lang={lang}" / f"year={y:04d}" / f"month={m:02d}" / "data.parquet"
    if out.exists() and not overwrite:
        print(f"[SKIP] referrers_6m lang={lang} period={y}-{m:02d}")
        return

    cols = parquet_columns(con, src)
    curr_title_sql = title_expr(cols, "curr")
    prev_title_sql = title_expr(cols, "prev")
    if curr_title_sql == "NULL" or prev_title_sql == "NULL":
        print(f"[WARN] referrers_6m: columnas de título no encontradas (prev_*/curr_*). lang={lang}; se omite.")
        return

    q = f"""
    WITH last6 AS (
      SELECT CAST(YEAR(gs) AS INT) y, CAST(MONTH(gs) AS INT) m
      FROM generate_series(make_date({y},{m},1) - INTERVAL 5 MONTH,
                           make_date({y},{m},1), INTERVAL 1 MONTH) t(gs)
    ),
    agg AS (
      SELECT
        {curr_title_sql} AS curr_title,
        {prev_title_sql} AS prev_title,
        SUM(CAST(d.total_transitions AS BIGINT)) AS clicks_6m
      FROM read_parquet('{src}', hive_partitioning=true) d
      WHERE (CAST(d.year AS INT), CAST(d.month AS INT)) IN (SELECT y,m FROM last6)
      GROUP BY curr_title, prev_title
    ),
    ranked AS (
      SELECT curr_title, prev_title, clicks_6m,
             ROW_NUMBER() OVER (PARTITION BY curr_title ORDER BY clicks_6m DESC) AS rn
      FROM agg
    )
    SELECT curr_title, prev_title, clicks_6m, rn
    FROM ranked
    WHERE rn <= {topm_referrers}
    ORDER BY curr_title, rn
    """
    atomic_copy_query_to_parquet(con, q, out, f"referrers_6m {lang}-{y}-{m:02d}")

# -------------------- main --------------------

def main():
    ap = argparse.ArgumentParser(description="Genera GOLD_Specifics (insights listos) a partir de GOLD.")
    ap.add_argument("--config", default="config/settings.yaml", help="YAML con rutas (usa gold_dir).")
    ap.add_argument("--lang", type=str, help="Idioma específico (ej: es, en, de).")
    ap.add_argument("--langs", type=str,
                    help="CSV de idiomas a procesar (p.ej. 'en,fr,de') o 'all' para todos los presentes en GOLD.")
    ap.add_argument("--topn-alltime", type=int, default=500, help="Top N para all_time_top.")
    ap.add_argument("--topn-trending", type=int, default=1000, help="Top N para trending_mom.")
    ap.add_argument("--topn-edges", type=int, default=100000, help="Top N pares en edges_6m.")
    ap.add_argument("--topm-referrers", type=int, default=50, help="Top M referidores por destino en referrers_6m.")
    ap.add_argument("--no-skip", action="store_true", help="Sobrescribe salidas si existen.")
    args = ap.parse_args()

    cfg = load_config(args.config)
    gold_dir = Path(cfg.get("gold_dir", "data/gold")).expanduser().resolve()
    specifics_root = gold_dir.parent / "GOLD_Specifics"  # mismo nivel que GOLD
    ensure_dir(specifics_root)

    con = duckdb.connect(database=":memory:")
    try:
        # PRAGMA conservadores y efectivos
        threads = max(1, (os.cpu_count() or 8)//2)
        con.execute(f"PRAGMA threads = {threads}")
        con.execute("PRAGMA enable_object_cache = true")
        con.execute("PRAGMA memory_limit = '24GB'")

        # selección de idiomas
        if args.lang and args.langs:
            print("❌ Usa --lang o --langs, pero no ambos.")
            raise SystemExit(1)

        if args.lang:
            langs = [args.lang]
        elif args.langs:
            if args.langs.strip().lower() == "all":
                # union de fuentes relevantes
                langs = list_langs_union(gold_dir, ["article_popularity", "edges_monthly"])
            else:
                langs = [s.strip() for s in args.langs.split(",") if s.strip()]
        else:
            # por defecto: TODOS los idiomas presentes en cualquiera de las fuentes
            langs = list_langs_union(gold_dir, ["article_popularity", "edges_monthly"])

        if not langs:
            print("❌ No se detectaron idiomas en GOLD (article_popularity ni edges_monthly).")
            raise SystemExit(1)

        overwrite = bool(args.no_skip)

        for lang in langs:
            print(f"\n=== Idioma: {lang} ===")
            # Builders basados en article_popularity
            build_top10_by_year(con, gold_dir, specifics_root, lang, overwrite)
            build_all_time_top(con, gold_dir, specifics_root, lang, args.topn_alltime, overwrite)
            build_trending_mom(con, gold_dir, specifics_root, lang, args.topn_trending, overwrite)
            # Builders basados en edges_monthly
            build_edges_6m(con, gold_dir, specifics_root, lang, args.topn_edges, overwrite)
            build_referrers_6m(con, gold_dir, specifics_root, lang, args.topm_referrers, overwrite)

        print("\n✅ GOLD_Specifics listo.")
    finally:
        con.close()

if __name__ == "__main__":
    main()
