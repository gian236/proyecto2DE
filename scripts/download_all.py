#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
download_all.py
Descarga *todos* los archivos disponibles del dataset Wikipedia Clickstream
desde https://dumps.wikimedia.org/other/clickstream/

- Detecta automáticamente todos los años/meses.
- Descarga todos los idiomas o los especificados.
- Evita duplicados.
"""

import os
import re
import time
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import yaml


def load_config(config_path="config/settings.yaml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def list_available_months(base_url):
    """Lista todos los subdirectorios (ej: 2024-09, 2024-10) disponibles."""
    response = requests.get(base_url, timeout=10)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    months = [
        a["href"].strip("/")
        for a in soup.find_all("a", href=True)
        if re.match(r"^\d{4}-\d{2}/$", a["href"])
    ]
    return sorted(months)


def list_available_files(base_url, year_month):
    """Lista todos los archivos .tsv.gz para un mes dado."""
    url = f"{base_url}{year_month}/"
    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        return []
    soup = BeautifulSoup(response.text, "html.parser")
    return [
        a["href"]
        for a in soup.find_all("a", href=True)
        if a["href"].endswith(".tsv.gz") and a["href"].startswith("clickstream-")
    ]


def download_file(url, dest_path):
    """Descarga un archivo con barra de progreso."""
    try:
        with requests.get(url, stream=True, timeout=30) as r:
            if r.status_code == 404:
                print(f"[WARN] No existe: {url}")
                return False
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            with open(dest_path, "wb") as f, tqdm(
                total=total, unit="B", unit_scale=True, desc=os.path.basename(dest_path)
            ) as bar:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        bar.update(len(chunk))
        return True
    except Exception as e:
        print(f"[ERROR] Descarga fallida: {e}")
        return False


def main():
    cfg = load_config()
    base_url = cfg["base_url"]
    bronze_dir = cfg["bronze_dir"]

    print("[INFO] Listando todos los meses disponibles...")
    months = list_available_months(base_url)
    print(f"[INFO] Encontrados {len(months)} meses.")

    for ym in months:
        print(f"\n=== Procesando {ym} ===")
        files = list_available_files(base_url, ym)
        if not files:
            print(f"[WARN] No hay archivos para {ym}")
            continue

        dest_dir = os.path.join(bronze_dir, ym)
        ensure_dir(dest_dir)

        for f in files:
            dest_file = os.path.join(dest_dir, f)
            url = f"{base_url}{ym}/{f}"

            if os.path.exists(dest_file):
                print(f"[SKIP] Ya existe: {f}")
                continue

            print(f"[INFO] Descargando {f}")
            if download_file(url, dest_file):
                print(f"[OK] Guardado: {dest_file}")
            else:
                print(f"[FAIL] Error descargando {f}")
                time.sleep(1)


if __name__ == "__main__":
    main()
