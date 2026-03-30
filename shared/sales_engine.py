"""
sales_engine.py
===============
Zentrale, app-weite Engine zur korrekten Netto-Berechnung von Fahrzeug-Verkäufen.

Regeln (aus fachlicher Anforderung):
  1. Ein Fahrzeug gilt als (Netto-)Verkaufserfolg, sobald eine Auftragsbestätigung (AB/ORDER)
     erstellt wurde – unabhängig davon, ob danach eine Rechnung (RE) folgt.
  2. Wird für denselben Kunden dasselbe Fahrzeug (gleiche VIN/ID) mehrfach bestätigt,
     zählt es NUR EINMAL (Zubehör-/Ergänzungs-ABs).
  3. Wird ein Auftrag storniert (CANCELLATION / ST), zählt das betroffene Fahrzeug
     NICHT als Verkauf.
  4. Tauscht ein Kunde Fahrzeug A gegen Fahrzeug B (Storno A + neue AB B),
     zählt genau 1 Verkauf (Fahrzeug B).
  5. Fahrzeuge ohne VIN (Vorlauf/Bestellung auf Produktion) werden über ihre
     interne Auftrags-ID getrackt, bis eine VIN bekannt ist.

Liefert konsistente Zahlen für:
  - Mitarbeiter-Performance-Tab
  - KI-Analyst
  - BI-Kontext / Dashboard-Kacheln
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any


# ---------------------------------------------------------------------------
# Status-Klassifikation
# ---------------------------------------------------------------------------

# Statuskürzel, die einen positiven Verkaufsabschluss signalisieren
POSITIVE_STATUSES = {"ORDER", "AB", "CONTRACT", "VT", "RE"}

# Statuskürzel, die einen Storno/Auflösung signalisieren
CANCEL_STATUSES = {"CANCELLATION", "CANCELLED", "ST", "STORNO"}


def _norm_status(status_field: Any) -> str:
    """Extrahiert und normalisiert den Status-Key aus einem Auftrags-Objekt."""
    if isinstance(status_field, dict):
        raw = status_field.get("key") or status_field.get("label") or ""
    else:
        raw = str(status_field or "")
    return raw.strip().upper()


# ---------------------------------------------------------------------------
# Fahrzeug-Identitäts-Extraktion aus einem Auftrag
# ---------------------------------------------------------------------------

def _get_vehicle_key_from_order(order: dict) -> str | None:
    """
    Versucht, eine stabile Fahrzeug-Identität aus einem Auftrag zu extrahieren.
    Reihenfolge: VIN (Fahrgestellnr.) > interne ID > externe UID > Auftrags-interne Fahrzeug-ID.
    Gibt None zurück, wenn kein Fahrzeug zugeordnet ist.
    """
    # 1. Identifier-Block im Auftrag (oft verschachtelt)
    identifier = order.get("identifier") or {}
    if isinstance(identifier, list):
        identifier = identifier[0] if identifier else {}

    vin = str(identifier.get("vin") or "").strip().upper()
    if vin:
        return f"VIN:{vin}"

    internal = str(identifier.get("internal") or "").strip()
    if internal:
        return f"INT:{internal}"

    uid = str(identifier.get("uid") or "").strip()
    if uid:
        return f"UID:{uid}"

    # 2. Direktes Fahrzeug-Objekt im Auftrag
    vehicle = order.get("vehicle") or {}
    if isinstance(vehicle, dict):
        v_id = str(vehicle.get("id") or vehicle.get("uid") or "").strip()
        if v_id:
            return f"VID:{v_id}"

    # 3. Fahrzeug-Referenz als einfache ID
    vehicle_id = str(order.get("vehicle_id") or order.get("article_id") or "").strip()
    if vehicle_id:
        return f"VID:{vehicle_id}"

    return None


def _get_customer_key(order: dict) -> str | None:
    """Extrahiert eine stabile Kunden-ID aus dem Auftrag."""
    customer = order.get("customer") or {}
    if isinstance(customer, dict):
        cid = str(customer.get("id") or customer.get("number") or "").strip()
        if cid and cid not in ("0", ""):
            return f"CID:{cid}"
    return None


def _get_order_datetime(order: dict) -> datetime | None:
    """Extrahiert das Datum aus einem Auftrag (mehrere mögliche Felder)."""
    date_field = order.get("date")
    candidates = []

    if isinstance(date_field, str):
        candidates.append(date_field)
    elif isinstance(date_field, dict):
        for key in ("order", "create", "created", "created_at", "createAt", "update"):
            val = date_field.get(key)
            if isinstance(val, str) and val:
                candidates.append(val)

    for key in ("created_at", "created", "order_date", "contract_date"):
        val = order.get(key)
        if isinstance(val, str) and val:
            candidates.append(val)

    for val in candidates:
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except Exception:
            try:
                return datetime.strptime(val.split("T")[0], "%Y-%m-%d")
            except Exception:
                continue
    return None


def _get_order_price(order: dict) -> float:
    """Extrahiert den relevanten Umsatzwert aus einem Auftrag."""
    p = order.get("prices") or {}
    # Priorisierung: Angebotspreis > Basispreis > Brutto (falls vorhanden)
    return float(p.get("offer") or p.get("basic") or p.get("brutto") or 0)


# ---------------------------------------------------------------------------
# Kern-Algorithmus: Netto-Verkaufs-Berechnung
# ---------------------------------------------------------------------------

def calculate_net_sales(
    orders: list[dict],
    *,
    year_min: int | None = None,
    year_max: int | None = None,
    month_min: int | None = None,
    month_max: int | None = None,
    employee_id: str | None = None,
    employee_name: str | None = None,
) -> dict[str, Any]:
    """
    Berechnet den *echten* Netto-Verkaufserfolg aus einer Auftrags-Liste.

    Rückgabe:
      {
        "netto_verkauft": int,      # Eindeutige Fahrzeuge mit pos. AB (nach Storno-Abzug)
        "netto_umsatz": float,      # Summe des Umsatzes aller Netto-Verkäufe
        "brutto_ab_count": int,     # Rohe Auftragsbestätigungen
        "storno_count": int,        # Stornierungen im Zeitraum
        "ohne_fahrzeug_ref": int,   # ABs ohne erkennbare Fahrzeug-Zuordnung
        "fahrzeuge": [              # Detaillierte Liste der gezählten Fahrzeuge
            {
              "fahrzeug_key": str,
              "kunde_key": str | None,
              "status_final": str,
              "datum_ab": str,
              "revenue": float,
              "employee_ids": list[str],
            }
        ],
        "storni_detail": [...],     # Stornierte Fahrzeuge
        "debug_unmatched": int,
      }

    Logik:
      - Wir bauen pro Fahrzeug-Key eine Timeline aller zugehörigen Auftrags-Events.
      - Am Ende "gewinnt" der letzte positive Status, außer wenn danach ein Storno folgte.
      - Tausch (Storno A + neues Fahrzeug B) → A = 0, B = 1.
    """
    # Schritt 1: Alle relevanten Aufträge filtern und nach Fahrzeug gruppieren
    # Key: fahrzeug_key -> list of events {dt, status, order, employee}
    vehicle_timeline: dict[str, list[dict]] = defaultdict(list)
    no_vehicle_ref_ab_count = 0
    brutto_ab_count = 0

    for order in orders:
        if not isinstance(order, dict):
            continue

        dt = _get_order_datetime(order)
        if dt is None:
            continue

        # Zeitraum-Filter
        if year_min and dt.year < year_min:
            continue
        if year_max and dt.year > year_max:
            continue
        if month_min and dt.month < month_min:
            continue
        if month_max and dt.month > month_max:
            continue

        status_raw = _norm_status(order.get("status"))

        # Mitarbeiter-Filter
        if employee_id or employee_name:
            user = order.get("user") or {}
            u_ids = []
            u_names = []
            for k in ("order", "update", "id"):
                v = user.get(k)
                if v:
                    u_ids.append(str(v).split(".")[0])
            for k in ("full_name", "name", "display_name", "username"):
                v = user.get(k)
                if isinstance(v, str) and v.strip():
                    u_names.append(v.strip().lower())

            if employee_id and employee_id not in u_ids:
                continue
            if employee_name and not any(employee_name.lower() in n for n in u_names):
                continue

        # Nur relevante Statuse einbeziehen
        is_positive = status_raw in POSITIVE_STATUSES
        is_cancel = status_raw in CANCEL_STATUSES
        if not (is_positive or is_cancel):
            continue

        if is_positive:
            brutto_ab_count += 1

        vkey = _get_vehicle_key_from_order(order)
        ckey = _get_customer_key(order)

        # Fahrzeuge ohne erkennbare Referenz → separat zählen, nicht in Timeline
        if vkey is None:
            if is_positive:
                no_vehicle_ref_ab_count += 1
            continue

        # Zusammengesetzter Schlüssel: Fahrzeug + Kunde (für Tausch-Erkennung)
        # Ein Fahrzeug kann von verschiedenen Kunden bestellt worden sein (selten)
        compound_key = f"{vkey}|{ckey or 'NO_CID'}"

        user = order.get("user") or {}
        emp_id = str(user.get("order") or user.get("id") or "").split(".")[0]
        price = _get_order_price(order)

        vehicle_timeline[compound_key].append({
            "dt": dt,
            "status": status_raw,
            "is_positive": is_positive,
            "is_cancel": is_cancel,
            "emp_id": emp_id,
            "price": price,
            "order_ref": order.get("id"),
        })

    # Schritt 2: Für jedes Fahrzeug den finalen Status ermitteln
    net_sold: list[dict] = []
    cancelled: list[dict] = []
    total_net_revenue = 0.0

    for compound_key, events in vehicle_timeline.items():
        # Chronologisch sortieren
        events_sorted = sorted(events, key=lambda e: e["dt"])

        # Das letzte Event bestimmt den Endstatus
        last_event = events_sorted[-1]
        vkey, ckey = compound_key.split("|", 1)
        emp_ids = list({e["emp_id"] for e in events_sorted if e["emp_id"]})

        if last_event["is_positive"]:
            # Bei Zubehör-ABs (mehrere positive Events) nehmen wir den Preis des NEUESTEN Events,
            # da dieser meist den Gesamtpreis inkl. Anhängerkupplung etc. enthält.
            fzg_revenue = last_event["price"]
            total_net_revenue += fzg_revenue

            net_sold.append({
                "fahrzeug_key": vkey,
                "kunde_key": ckey if ckey != "NO_CID" else None,
                "status_final": last_event["status"],
                "datum_ab": last_event["dt"].strftime("%Y-%m-%d"),
                "revenue": fzg_revenue,
                "employee_ids": emp_ids,
                "events_count": len(events_sorted),
            })
        elif last_event["is_cancel"]:
            # Nur als Storno erfassen, wenn vorher eine AB gab
            had_positive = any(e["is_positive"] for e in events_sorted[:-1])
            if had_positive:
                cancelled.append({
                    "fahrzeug_key": vkey,
                    "kunde_key": ckey if ckey != "NO_CID" else None,
                    "datum_storno": last_event["dt"].strftime("%Y-%m-%d"),
                    "employee_ids": emp_ids,
                })

    return {
        "netto_verkauft": len(net_sold),
        "netto_umsatz": round(total_net_revenue, 2),
        "brutto_ab_count": brutto_ab_count,
        "storno_count": len(cancelled),
        "ohne_fahrzeug_ref": no_vehicle_ref_ab_count,
        "fahrzeuge": net_sold,
        "storni_detail": cancelled,
    }


def calculate_net_sales_by_employee(
    orders: list[dict],
    *,
    year_min: int | None = None,
    year_max: int | None = None,
) -> dict[str, dict[str, Any]]:
    """
    Berechnet Netto-Verkäufe aufgeschlüsselt nach Mitarbeiter-ID.
    Gibt ein Dict {employee_id: {netto_verkauft, brutto_ab, storno, ...}} zurück.
    """
    # Zuerst alle Netto-Fahrzeuge ermitteln (ohne Mitarbeiter-Filter)
    all_net = calculate_net_sales(orders, year_min=year_min, year_max=year_max)

    # Aufschlüsselung nach Mitarbeiter
    by_employee: dict[str, dict] = defaultdict(lambda: {
        "netto_verkauft": 0,
        "netto_umsatz": 0.0,
        "brutto_ab": 0,
        "storno": 0,
    })

    for fzg in all_net["fahrzeuge"]:
        rev = fzg.get("revenue", 0)
        for emp_id in (fzg["employee_ids"] or ["UNBEKANNT"]):
            by_employee[emp_id]["netto_verkauft"] += 1
            by_employee[emp_id]["netto_umsatz"] += rev

    # Brutto zählen: alle positiven Aufträge pro Mitarbeiter
    for order in orders:
        if not isinstance(order, dict):
            continue
        dt = _get_order_datetime(order)
        if not dt:
            continue
        if year_min and dt.year < year_min:
            continue
        if year_max and dt.year > year_max:
            continue
        status_raw = _norm_status(order.get("status"))
        user = order.get("user") or {}
        emp_id = str(user.get("order") or user.get("id") or "UNBEKANNT").split(".")[0]
        if status_raw in POSITIVE_STATUSES:
            by_employee[emp_id]["brutto_ab"] += 1
        elif status_raw in CANCEL_STATUSES:
            by_employee[emp_id]["storno"] += 1

    return dict(by_employee)
