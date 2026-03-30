#!/usr/bin/env python3
"""Kombiniert alle CSV-Dateien in `docs/supabase_exports/` zu einer einzigen .xlsx mit je einem Sheet pro CSV."""

import csv
from pathlib import Path

try:
    from openpyxl import Workbook
except Exception:
    print(
        "Fehler: openpyxl ist nicht installiert. Installiere mit: pip install openpyxl"
    )
    raise


def csv_to_sheet(wb, csv_path: Path, sheet_name: str):
    ws = wb.create_sheet(title=sheet_name[:31])
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for r in reader:
            ws.append(r)


def main():
    base = Path(__file__).resolve().parent.parent / "docs" / "supabase_exports"
    if not base.exists():
        print("Kein Ordner mit CSV-Exports gefunden:", base)
        return

    csv_files = sorted([p for p in base.iterdir() if p.suffix.lower() == ".csv"])
    if not csv_files:
        print("Keine CSV-Dateien im Ordner gefunden:", base)
        return

    wb = Workbook()
    # remove default sheet created by Workbook
    default = wb.active
    wb.remove(default)

    for p in csv_files:
        name = p.stem
        print("Adding", p.name)
        try:
            csv_to_sheet(wb, p, name)
        except Exception as e:
            print("Fehler beim Lesen von", p, e)

    out = base / "supabase_api_cache_export.xlsx"
    wb.save(out)
    print("Wrote", out)


if __name__ == "__main__":
    main()
