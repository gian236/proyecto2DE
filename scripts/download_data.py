#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
download_data.py
Descarga los dumps recientes de Wikipedia Clickstream (TSV.GZ)
en ./data/bronze/<YYYY-MM>/

✅ Versión standalone con CLI
Requiere: aiohttp, aiofiles, async-timeout, tqdm

Uso (PowerShell/Windows):
# 1) Todos los meses y todos los idiomas
python .\download_data.py --latest 0 --langs all --workers 8

# 2) Todos los meses pero solo ciertos idiomas
python .\download_data.py --latest 0 --langs "enwiki, eswiki, dewiki" --workers 8

# 3) Desde una fecha específica (inclusive)
python .\download_data.py --latest 0 --langs all --since 2023-01 --workers 6
"""

import asyncio
import argparse
import aiohttp
import aiofiles
import async_timeout
import re
from pathlib import Path
from tqdm import tqdm

BASE_URL = "https://dumps.wikimedia.org/other/clickstream/"
RE_MONTH = re.compile(r'href="(\d{4}-\d{2})/?"')
RE_FILE = re.compile(r'href="(clickstream-([a-z-]+)-(\d{4}-\d{2})\.tsv\.gz)"')


# --------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------
async def fetch_text(session, url: str) -> str:
    async with async_timeout.timeout(30):
        async with session.get(url) as r:
            r.raise_for_status()
            return await r.text()


async def list_all_months(session) -> list[str]:
    html = await fetch_text(session, BASE_URL)
    # Devolvemos meses únicos ordenados, p. ej. ["2019-01", "2019-02", ..., "2025-09"]
    return sorted(set(RE_MONTH.findall(html)))


async def list_files_for_month(session, month: str, langs) -> list[tuple[str, str, str, str]]:
    """
    Retorna lista de tuplas (url, lang, month, filename) para un mes concreto.
    Si langs es None/Falsey, no filtra por idioma.
    """
    html = await fetch_text(session, f"{BASE_URL}{month}/")
    files = []
    for href, lang, mm in RE_FILE.findall(html):
        if langs and lang not in langs:
            continue
        if mm == month:
            files.append((f"{BASE_URL}{month}/{href}", lang, month, href))
    return files


async def download_file(session, url: str, dest: Path, sem: asyncio.Semaphore, idx: int, total: int):
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".part")
    pbar = None
    try:
        async with sem:
            async with async_timeout.timeout(1800):
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    total_bytes = int(resp.headers.get("Content-Length", 0))
                    pbar = tqdm(
                        total=total_bytes,
                        unit="B",
                        unit_scale=True,
                        desc=dest.name,
                        leave=False,
                    )
                    async with aiofiles.open(tmp, "wb") as f:
                        async for chunk in resp.content.iter_chunked(1 << 20):
                            await f.write(chunk)
                            if pbar:
                                pbar.update(len(chunk))
        tmp.replace(dest)
        tqdm.write(f"[{idx}/{total}] ✅ {dest.name}")
    except Exception as e:
        tqdm.write(f"[{idx}/{total}] ❌ {dest.name}: {e}")
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
    finally:
        if pbar:
            pbar.close()


# --------------------------------------------------------------------
# Main logic con CLI
# --------------------------------------------------------------------
async def main(langs=("enwiki", "eswiki"), latest: int | None = None, workers: int = 4, since: str | None = None):
    """
    langs:
      - tuple/list de idiomas (e.g., ("enwiki","eswiki"))
      - None -> sin filtro, todos los idiomas
    latest:
      - None o 0 -> todos los meses
      - N > 0    -> últimos N meses
    since (opcional):
      - "YYYY-MM" -> filtra meses >= since
    """
    out_dir = Path("data") / "bronze"
    sem = asyncio.Semaphore(workers)
    timeout = aiohttp.ClientTimeout(total=None, connect=20, sock_read=1200)
    connector = aiohttp.TCPConnector(limit=workers)

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers={"User-Agent": "wikilake-downloader/1.0"},
    ) as session:
        tqdm.write(f"🌐 Connecting to {BASE_URL} ...")
        all_months = await list_all_months(session)

        # Filtro por fecha mínima
        if since:
            all_months = [m for m in all_months if m >= since]

        # Selección de meses
        if latest in (None, 0):
            months = all_months
        else:
            months = all_months[-int(latest):]

        # Construcción del plan (omite lo que ya existe)
        plan: list[tuple[str, Path]] = []
        for m in months:
            for url, lang, month, fname in await list_files_for_month(session, m, langs):
                dest = out_dir / month / fname
                if dest.exists():
                    tqdm.write(f"⏩ Skipping (already exists): {dest.name}")
                    continue
                plan.append((url, dest))

        if not plan:
            tqdm.write("✅ All files already downloaded.")
            return

        tqdm.write(f"⬇️  Downloading {len(plan)} files ...")
        tasks = [download_file(session, url, dest, sem, i, len(plan))
                 for i, (url, dest) in enumerate(plan, 1)]
        await asyncio.gather(*tasks)
        tqdm.write("✅ All downloads complete.")


def _parse_langs(s):
    """
    Convierte el argumento --langs en:
      - None si es "all", "*" o vacío => sin filtro (todos los idiomas)
      - tuple de idiomas si se pasan separados por coma o espacios
    """
    if not s or str(s).lower().strip() in ("all", "*"):
        return None
    # acepta coma o espacios
    parts = [x.strip() for x in re.split(r"[,\s]+", str(s)) if x.strip()]
    return tuple(parts) if parts else None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Wikipedia Clickstream downloader")
    parser.add_argument("--langs", default=None,
                        help='Idiomas (p.ej. "enwiki,eswiki,dewiki") o "all" para todos')
    parser.add_argument("--latest", type=int, default=0,
                        help="Cuántos meses más recientes (0 = todos)")
    parser.add_argument("--workers", type=int, default=6,
                        help="Concurrencia (sugerido 4–10). Baja si ves 429/403.")
    parser.add_argument("--since", default=None,
                        help='Descargar desde este mes inclusive (YYYY-MM), opcional')
    args = parser.parse_args()

    asyncio.run(
        main(
            langs=_parse_langs(args.langs),
            latest=args.latest,
            workers=args.workers,
            since=args.since,
        )
    )
