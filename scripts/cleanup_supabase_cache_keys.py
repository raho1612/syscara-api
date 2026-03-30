#!/usr/bin/env python3
"""Safe backup -> delete -> verify workflow for Supabase api_cache keys."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

LEGACY_KEYS = {"ads", "equipment", "vehicles", "orders"}


def load_env() -> None:
    current = Path(__file__).resolve()
    for _ in range(20):
        current = current.parent
        candidate = current / ".env"
        if candidate.exists():
            load_dotenv(candidate)
            return


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("bases", nargs="+", help="Base cache keys to process")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete keys after backup. Default is dry-run.",
    )
    parser.add_argument(
        "--allow-non-legacy",
        action="store_true",
        help="Allow keys outside the known legacy set.",
    )
    parser.add_argument(
        "--audit-after",
        action="store_true",
        help="Run the audit script after the cleanup step finishes.",
    )
    return parser.parse_args()


def build_headers(supabase_key: str) -> dict[str, str]:
    return {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }


def fetch_inventory(supabase_url: str, headers: dict[str, str]) -> list[dict]:
    base_url = supabase_url.rstrip("/") + "/rest/v1/api_cache"
    response = requests.get(
        base_url,
        headers=headers,
        params={"select": "key,updated_at", "limit": 5000},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def keys_for_base(inventory: list[dict], base: str) -> list[str]:
    return sorted(row["key"] for row in inventory if row["key"].split("#")[0] == base)


def fetch_rows_for_keys(
    supabase_url: str, headers: dict[str, str], keys: list[str]
) -> list[dict]:
    base_url = supabase_url.rstrip("/") + "/rest/v1/api_cache"
    rows = []
    for key in keys:
        response = requests.get(
            base_url,
            headers=headers,
            params={"select": "key,data,updated_at", "key": f"eq.{key}"},
            timeout=60,
        )
        response.raise_for_status()
        rows.extend(response.json())
    return rows


def write_backup(base: str, rows: list[dict]) -> Path:
    backup_dir = Path(__file__).resolve().parent.parent / "docs" / "supabase_backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out_path = backup_dir / f"backup_{base.replace('/', '_')}_{stamp}.json"
    out_path.write_text(
        json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return out_path


def delete_keys(supabase_url: str, headers: dict[str, str], keys: list[str]) -> None:
    base_url = supabase_url.rstrip("/") + "/rest/v1/api_cache"
    delete_headers = {**headers, "Prefer": "return=minimal"}
    for key in keys:
        response = requests.delete(
            base_url,
            headers=delete_headers,
            params={"key": f"eq.{key}"},
            timeout=60,
        )
        response.raise_for_status()


def run_audit() -> None:
    audit_script = Path(__file__).resolve().parent / "audit_supabase_cache.py"
    subprocess.run([sys.executable, str(audit_script)], check=True)


def main() -> None:
    args = parse_args()
    load_env()
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_ACCESS_TOKEN")
    if not supabase_url or not supabase_key:
        raise SystemExit("SUPABASE_URL or SUPABASE_KEY is missing")

    if not args.allow_non_legacy:
        invalid = [base for base in args.bases if base not in LEGACY_KEYS]
        if invalid:
            raise SystemExit(
                "Refusing non-legacy bases without --allow-non-legacy: "
                + ", ".join(sorted(invalid))
            )

    headers = build_headers(supabase_key)
    inventory = fetch_inventory(supabase_url, headers)

    for base in args.bases:
        keys = keys_for_base(inventory, base)
        print(f"BASE {base}")
        print(f"MATCHING_KEYS {len(keys)}")
        for key in keys:
            print(f"  {key}")

        if not keys:
            print("STATUS nothing-to-do")
            continue

        rows = fetch_rows_for_keys(supabase_url, headers, keys)
        backup_path = write_backup(base, rows)
        print(f"BACKUP {backup_path}")

        if not args.execute:
            print("STATUS dry-run")
            continue

        delete_keys(supabase_url, headers, keys)
        refreshed_inventory = fetch_inventory(supabase_url, headers)
        remaining = keys_for_base(refreshed_inventory, base)
        if remaining:
            raise SystemExit(
                "Verification failed, keys still present: " + ", ".join(remaining)
            )
        print("STATUS deleted-and-verified")

    if args.audit_after:
        run_audit()
        print("STATUS audit-refreshed")


if __name__ == "__main__":
    main()
