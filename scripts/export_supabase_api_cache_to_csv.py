#!/usr/bin/env python3
"""
Exportiert alle Einträge aus der Supabase-Tabelle `api_cache` in einzelne CSV-Dateien.

Usage:
  python export_supabase_api_cache_to_csv.py

Das Skript liest `SUPABASE_URL` und `SUPABASE_KEY` aus der Umgebung oder einer .env-Datei
und schreibt CSVs nach `../docs/supabase_exports/` (relativ zum Skript).
"""

import csv
import json
import os
from collections import defaultdict
from pathlib import Path

import requests
from dotenv import load_dotenv


def load_env():
    # Search upward for a .env file until filesystem root
    p = Path(__file__).resolve()
    for _ in range(20):
        p = p.parent
        candidate = p / ".env"
        if candidate.exists():
            load_dotenv(candidate)
            return
    # fallback: rely on existing environment
    return


def fetch_api_cache_rows(supabase_url, supabase_key):
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }
    base = supabase_url.rstrip("/") + "/rest/v1/api_cache"
    # first fetch keys only
    params = {"select": "key", "limit": 5000}
    resp = requests.get(base, headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    items = resp.json()
    rows = []
    for it in items:
        key = it.get("key")
        if not key:
            continue
        # fetch the full row for this key
        try:
            resp2 = requests.get(
                base,
                headers=headers,
                params={"select": "key,data", "key": f"eq.{key}"},
                timeout=60,
            )
            resp2.raise_for_status()
            data = resp2.json()
            if data:
                rows.extend(data)
        except Exception:
            # try with encoded key as fallback
            from urllib.parse import quote

            resp2 = requests.get(
                f"{base}?select=key,data&key=eq.{quote(key, safe='')}",
                headers=headers,
                timeout=60,
            )
            try:
                resp2.raise_for_status()
                data = resp2.json()
                if data:
                    rows.extend(data)
            except Exception:
                continue
    return rows


def flatten(obj, parent_key="", sep="."):
    items = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.update(flatten(v, new_key, sep=sep))
    elif isinstance(obj, list):
        # For lists, try to flatten list of dicts by enumerating
        if all(isinstance(i, dict) for i in obj):
            for idx, it in enumerate(obj):
                items.update(
                    flatten(
                        it,
                        f"{parent_key}{sep}{idx}" if parent_key else str(idx),
                        sep=sep,
                    )
                )
        else:
            items[parent_key] = json.dumps(obj, ensure_ascii=False)
    else:
        items[parent_key] = obj
    return items


def reconstruct_for_base(rows_for_base):
    # rows_for_base: list of dicts with keys 'key' and 'data'
    by_key = {r["key"]: r["data"] for r in rows_for_base}
    base = rows_for_base[0]["key"].split("#")[0]
    # prefer direct entry
    if base in by_key:
        return by_key[base]

    # collect chunk entries
    chunks = []
    for k in sorted(by_key.keys()):
        if k.startswith(base + "#chunk"):
            # append in order by index
            try:
                idx = int(k.split("#chunk")[-1])
            except Exception:
                idx = 0
            chunks.append((idx, by_key[k]))
    if chunks:
        chunks_sorted = [c[1] for c in sorted(chunks, key=lambda x: x[0])]
        # combine depending on chunk type
        first = chunks_sorted[0]
        if isinstance(first, dict):
            combined = {}
            for c in chunks_sorted:
                if isinstance(c, dict):
                    combined.update(c)
            return combined
        elif isinstance(first, list):
            combined = []
            for c in chunks_sorted:
                if isinstance(c, list):
                    combined.extend(c)
                else:
                    combined.append(c)
            return combined
        else:
            # fallback: collect all pieces into a list
            combined = []
            for c in chunks_sorted:
                combined.append(c)
            return combined

    # fallback: if only meta exists or unknown format, return merged data values
    # try to merge all 'data' fields
    merged = None
    for v in by_key.values():
        if merged is None:
            merged = v
        elif isinstance(merged, list) and isinstance(v, list):
            merged.extend(v)
        elif isinstance(merged, dict) and isinstance(v, dict):
            merged.update(v)
    return merged or {}


def write_csv_for_endpoint(base, data, outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)
    fname = outdir / f"{base.replace('/', '_')}.csv"

    rows = []
    if isinstance(data, dict):
        # treat dict as mapping id->value; each row will include the key as `_id`
        for k, v in data.items():
            if isinstance(v, (dict, list)):
                flat = flatten(v)
            else:
                flat = {"value": v}
            flat["_id"] = k
            rows.append(flat)
    elif isinstance(data, list):
        for v in data:
            if isinstance(v, (dict, list)):
                flat = flatten(v)
            else:
                flat = {"value": v}
            rows.append(flat)
    else:
        # primitive
        rows = [{"value": data}]

    # determine all columns
    columns = set()
    for r in rows:
        columns.update(r.keys())
    columns = sorted(columns)

    with fname.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for r in rows:
            writer.writerow(
                {
                    k: (
                        json.dumps(v, ensure_ascii=False)
                        if isinstance(v, (dict, list))
                        else v
                    )
                    for k, v in r.items()
                }
            )

    print(f"Wrote {fname}")
    return fname


def main():
    load_env()
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_ACCESS_TOKEN")
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("SUPABASE_URL or SUPABASE_KEY not set in environment or .env")
        return

    rows = fetch_api_cache_rows(SUPABASE_URL, SUPABASE_KEY)
    if not rows:
        print("Keine Einträge in api_cache gefunden")
        return

    # group by base key
    groups = defaultdict(list)
    for r in rows:
        base = r["key"].split("#")[0]
        groups[base].append(r)

    outdir = Path(__file__).resolve().parent.parent / "docs" / "supabase_exports"
    generated = []
    for base, rs in groups.items():
        data = reconstruct_for_base(rs)
        if not data:
            print(f"No data for {base}, skipping")
            continue
        fname = write_csv_for_endpoint(base, data, outdir)
        generated.append(str(fname))

    print("\nGenerated files:")
    for g in generated:
        print(" -", g)


if __name__ == "__main__":
    main()
