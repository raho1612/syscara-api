#!/usr/bin/env python3
"""Audit current Supabase api_cache base keys and active payload fingerprints."""

from __future__ import annotations

import hashlib
import json
import os
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

ACTIVE_KEYS = {
    "sale/ads",
    "sale/equipment",
    "sale/lists",
    "sale/orders",
    "sale/orders_full",
    "sale/vehicles",
    "sale/vehicles_full",
}

LEGACY_KEYS = {
    "ads",
    "equipment",
    "vehicles",
    "orders",
}


def load_env() -> None:
    current = Path(__file__).resolve()
    for _ in range(20):
        current = current.parent
        candidate = current / ".env"
        if candidate.exists():
            load_dotenv(candidate)
            return


def classify_base(base: str) -> str:
    if base in ACTIVE_KEYS:
        return "active"
    if base in LEGACY_KEYS:
        return "legacy"
    return "unknown"


def fetch_key_inventory(supabase_url: str, supabase_key: str) -> list[dict]:
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }
    base_url = supabase_url.rstrip("/") + "/rest/v1/api_cache"
    response = requests.get(
        base_url,
        headers=headers,
        params={"select": "key,updated_at", "limit": 5000},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def fetch_rows_for_base(
    supabase_url: str,
    supabase_key: str,
    inventory_rows: list[dict],
    base: str,
) -> list[dict]:
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }
    base_url = supabase_url.rstrip("/") + "/rest/v1/api_cache"
    matching_keys = [
        row["key"] for row in inventory_rows if row["key"].split("#")[0] == base
    ]
    if not matching_keys:
        return []

    rows = []
    with requests.Session() as session:
        session.headers.update(headers)
        for key in matching_keys:
            response = session.get(
                base_url,
                params={"select": "key,data,updated_at", "key": f"eq.{key}"},
                timeout=60,
            )
            response.raise_for_status()
            rows.extend(response.json())

    return rows


def reconstruct_for_base(rows_for_base: list[dict]):
    by_key = {row["key"]: row.get("data") for row in rows_for_base}
    base = rows_for_base[0]["key"].split("#")[0]

    if base in by_key:
        return by_key[base]

    chunks = []
    for key, value in by_key.items():
        if key.startswith(base + "#chunk"):
            try:
                index = int(key.split("#chunk")[-1])
            except ValueError:
                index = 0
            chunks.append((index, value))

    if not chunks:
        return None

    ordered = [value for _, value in sorted(chunks, key=lambda item: item[0])]
    first = ordered[0]

    if isinstance(first, dict):
        merged = {}
        for value in ordered:
            if isinstance(value, dict):
                merged.update(value)
            else:
                merged[str(len(merged))] = value
        return merged

    if isinstance(first, list):
        merged = []
        for value in ordered:
            if isinstance(value, list):
                merged.extend(value)
            else:
                merged.append(value)
        return merged

    return ordered


def summarize_payload(payload) -> tuple[str, int]:
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.md5(blob.encode("utf-8")).hexdigest(), len(blob)


def write_report(
    rows: list[dict],
    active_payloads: dict[str, dict[str, str | int]],
    out_path: Path,
) -> None:
    counts = Counter(row["key"].split("#")[0] for row in rows)
    latest_updates = defaultdict(str)
    stored_key_samples = defaultdict(list)
    hashes_to_bases = defaultdict(list)

    for row in rows:
        base = row["key"].split("#")[0]
        updated_at = str(row.get("updated_at") or "")
        if updated_at > latest_updates[base]:
            latest_updates[base] = updated_at
        if len(stored_key_samples[base]) < 4:
            stored_key_samples[base].append(row["key"])

    for base, payload_info in active_payloads.items():
        hashes_to_bases[str(payload_info["md5"])].append(base)

    duplicate_groups = [bases for bases in hashes_to_bases.values() if len(bases) > 1]

    lines = []
    lines.append("# Supabase Cache Audit")
    lines.append("")
    lines.append(f"Generated: {datetime.now(UTC).isoformat()}")
    lines.append("")
    lines.append("## Cleanup Model")
    lines.append("")
    lines.append(
        "- `active`: current production `sale/...` keys used by `syscara-api-python`"
    )
    lines.append("- `legacy`: old unprefixed keys from `api/index.py`")
    lines.append("- `unknown`: manual review required before cleanup")
    lines.append("")
    lines.append("## Current Base Inventory")
    lines.append("")
    lines.append(
        "| Base Key | Status | Stored Rows | Latest Updated At | Sample Keys |"
    )
    lines.append("| --- | --- | ---: | --- | --- |")
    for base in sorted(counts):
        sample_keys = ", ".join(stored_key_samples[base])
        lines.append(
            f"| {base} | {classify_base(base)} | {counts[base]} | {latest_updates[base]} | {sample_keys} |"
        )

    lines.append("")
    lines.append("## Active Payload Fingerprints")
    lines.append("")
    lines.append("| Base Key | Payload Size | MD5 |")
    lines.append("| --- | ---: | --- |")
    for base in sorted(active_payloads):
        payload_info = active_payloads[base]
        lines.append(f"| {base} | {payload_info['size']} | {payload_info['md5']} |")

    lines.append("")
    lines.append("## Duplicate Active Payloads")
    lines.append("")
    if duplicate_groups:
        for bases in duplicate_groups:
            lines.append(f"- {', '.join(sorted(bases))}")
    else:
        lines.append("- No duplicate payloads detected across active `sale/...` keys.")

    lines.append("")
    lines.append("## Cleanup Notes")
    lines.append("")
    lines.append("- Delete only `legacy` keys after backup.")
    lines.append("- Keep all `active` `sale/...` keys unchanged.")
    lines.append(
        "- Re-run this script after each cleanup step to confirm the new state."
    )

    out_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    load_env()
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_ACCESS_TOKEN")
    if not supabase_url or not supabase_key:
        raise SystemExit("SUPABASE_URL or SUPABASE_KEY is missing")

    rows = fetch_key_inventory(supabase_url, supabase_key)
    active_payloads = {}
    for base in sorted(ACTIVE_KEYS):
        rows_for_base = fetch_rows_for_base(supabase_url, supabase_key, rows, base)
        if not rows_for_base:
            continue
        payload = reconstruct_for_base(rows_for_base)
        if payload is None:
            continue
        payload_hash, payload_size = summarize_payload(payload)
        active_payloads[base] = {"md5": payload_hash, "size": payload_size}

    out_path = (
        Path(__file__).resolve().parent.parent / "docs" / "supabase_cache_audit.md"
    )
    write_report(rows, active_payloads, out_path)
    print(out_path)


if __name__ == "__main__":
    main()
