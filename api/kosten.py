"""
API-Routen für Deckungsbeitrag-Analyse.
Liefert Fahrzeugliste mit VK/EK-Preisen und berechnet DB-Positionen.
"""

import os

from core.config import SYSCARA_BASE
from core.database import get_cached_or_fetch, iter_items
from flask import jsonify, request

# Typische Standtage je Fahrzeugtyp (Erfahrungswerte)
STANDTAGE_DEFAULTS = {
    "integriert": 45,
    "teilintegriert": 38,
    "alkofen": 42,
    "kastenwagen": 28,
    "wohnwagen": 52,
    "default": 40,
}

STANDKOSTEN_ZINS = float(os.getenv("STANDKOSTEN_ZINS", "0.05"))


def _classify_typ(v: dict) -> str:
    raw = str(v.get("typeof", "")).lower()
    if "alkov" in raw:
        return "alkofen"
    if "teilintegriert" in raw:
        return "teilintegriert"
    if "integriert" in raw:
        return "integriert"
    if "kastenwagen" in raw or "camper" in raw:
        return "kastenwagen"
    if v.get("type") == "Caravan":
        return "wohnwagen"
    return "default"


def _extract_order_date(order: dict) -> str:
    """Gibt das Datum eines Auftrags als 'YYYY-MM-DD'-String zurück, oder ''.

    Syscara liefert das Datum verschachtelt unter order['date']['created'],
    deshalb wird der dict-Wert zuerst geprüft, danach flache Felder als Fallback.
    """
    # Primär: verschachteltes date-Objekt (Syscara: order.date.created)
    date_obj = order.get("date")
    if isinstance(date_obj, dict):
        for sub_key in ("created", "delivery", "updated"):
            val = date_obj.get(sub_key)
            if val and isinstance(val, str) and len(val) >= 7:
                return val[:10]
    # Fallback: flache Schlüssel (andere API-Varianten)
    for key in ("created_at", "created", "create_date", "createdAt", "order_date"):
        val = order.get(key)
        if val and isinstance(val, str) and len(val) >= 7:
            return val[:10]
    return ""


def _load_orders() -> list:
    # sale/orders_full = vollständiger Bulk-Sync (2022–2026), kein Datumslimit
    orders_raw = get_cached_or_fetch(
        "sale/orders_full", f"{SYSCARA_BASE}/sale/orders/?update=2022-01-01"
    )
    if isinstance(orders_raw, dict) and "orders" in orders_raw:
        return orders_raw["orders"]
    if isinstance(orders_raw, list):
        return orders_raw
    if hasattr(orders_raw, "values"):
        return [o for o in orders_raw.values() if isinstance(o, dict)]
    return []


def _load_employee_names() -> dict:
    """Lädt die Mitarbeiter-ID → Name Zuordnung aus der lokalen JSON-Datei."""
    import json as _json
    from core.config import CURRENT_DIR
    path = CURRENT_DIR / "employee_names.json"
    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                return _json.load(f)
        except Exception:
            pass
    return {}


def _order_join_keys(order: dict) -> list[str]:
    identifier = order.get("identifier") or {}
    if isinstance(identifier, list):
        identifier = identifier[0] if identifier else {}
    keys = [
        str(identifier.get("internal") or "").strip(),
        str(identifier.get("vin") or "").strip().upper(),
        str(identifier.get("uid") or "").strip(),
    ]
    return [key for key in keys if key]


def _vehicle_join_keys(vehicle: dict) -> list[str]:
    identifier = vehicle.get("identifier") or {}
    if isinstance(identifier, list):
        identifier = identifier[0] if identifier else {}
    keys = [
        str(vehicle.get("id") or vehicle.get("uid") or "").strip(),
        str(identifier.get("internal") or "").strip(),
        str(identifier.get("vin") or "").strip().upper(),
        str(identifier.get("uid") or "").strip(),
        str(identifier.get("serial") or "").strip(),
    ]
    unique_keys = []
    seen = set()
    for key in keys:
        if key and key not in seen:
            seen.add(key)
            unique_keys.append(key)
    return unique_keys


def register_kosten_routes(app):

    @app.route("/api/kosten/fahrzeuge", methods=["GET"])
    def api_kosten_fahrzeuge():
        """
        Liefert verkaufte Fahrzeuge inkl. VK/EK-Preise für die DB-Kalkulation.

        Query-Parameter:
          von   – YYYY-MM  (Zeitraum von, inkl.)
          bis   – YYYY-MM  (Zeitraum bis, inkl.)
          art   – alle | integriert | teilintegriert | alkofen | kastenwagen
          top_n – 0=alle, 3|10|30|50 = Top-N nach DB%-Ranking
        """
        von_monat = (request.args.get("von") or "").strip()[:7]  # "YYYY-MM"
        bis_monat = (request.args.get("bis") or "").strip()[:7]  # "YYYY-MM"
        art_filter = (request.args.get("art") or "alle").lower()
        top_n = max(0, int(request.args.get("top_n") or 0))

        raw = get_cached_or_fetch("sale/vehicles_full", f"{SYSCARA_BASE}/sale/vehicles/")
        orders = _load_orders()
        employee_names = _load_employee_names()

        # --- Datum-Index, Kundendaten, VK aus Order, Verkäufer ---
        vehicle_sale_date: dict = {}
        vehicle_expenses: dict = {}
        vehicle_customer_info: dict = {}
        vehicle_vk_from_order: dict = {}
        vehicle_verkaeufer: dict = {}
        for o in orders:
            join_keys = _order_join_keys(o)
            if not join_keys:
                continue

            # --- Kundendaten: korrekter Feldname laut Syscara-Struktur ---
            customer = o.get("customer") or {}
            if isinstance(customer, list):
                customer = customer[0] if customer else {}
            company = str(customer.get("company_name") or "").strip()
            first = str(customer.get("first_name") or "").strip()
            last = str(customer.get("last_name") or "").strip()
            c_name = company if company else f"{first} {last}".strip()
            c_zip = str(customer.get("zipcode") or customer.get("zip") or "").strip()
            c_city = str(customer.get("city") or "").strip()
            if c_name or c_city:
                for join_key in join_keys:
                    if join_key not in vehicle_customer_info:
                        vehicle_customer_info[join_key] = {
                            "name": c_name,
                            "ort": f"{c_zip} {c_city}".strip(),
                        }

            # --- Tatsächlicher VK aus dem Auftrag (prices.offer) ---
            prices_obj = o.get("prices") or {}
            if isinstance(prices_obj, list):
                prices_obj = prices_obj[0] if prices_obj else {}
            vk_order = float(prices_obj.get("offer") or 0)
            if vk_order > 0:
                for join_key in join_keys:
                    if join_key not in vehicle_vk_from_order:
                        vehicle_vk_from_order[join_key] = vk_order

            # --- Werkstattkosten ---
            expenses = float(
                prices_obj.get("expenses") or prices_obj.get("internal_costs") or 0
            )
            if expenses > 0:
                for join_key in join_keys:
                    vehicle_expenses[join_key] = (
                        vehicle_expenses.get(join_key, 0.0) + expenses
                    )

            # --- Verkäufer aus user.order → Name ---
            user_obj = o.get("user") or {}
            seller_id = str(user_obj.get("order") or user_obj.get("seller") or "").strip()
            if seller_id and seller_id != "0":
                seller_name = employee_names.get(seller_id, f"ID {seller_id}")
                for join_key in join_keys:
                    if join_key not in vehicle_verkaeufer:
                        vehicle_verkaeufer[join_key] = seller_name

            # --- Verkaufsdatum ---
            order_date = _extract_order_date(o)
            if order_date:
                for join_key in join_keys:
                    existing = vehicle_sale_date.get(join_key, "")
                    if not existing or order_date > existing:
                        vehicle_sale_date[join_key] = order_date

        # --- Fahrzeuge in Zeitraum bestimmen ---
        if von_monat or bis_monat:
            if not vehicle_sale_date:
                # Fallback: Wenn Orders (und damit die vehicle_sale_date Map) leer sind 
                # (weil z.B. Supabase Cache leer ist und Syscara Timeout liefert),
                # deaktivieren wir den Datumsfilter vollständig, um zumindest Fahrzeuge anzuzeigen.
                vehicles_in_range = None
            else:
                vehicles_in_range = set()
                for vid, d in vehicle_sale_date.items():
                    ym = d[:7]
                    if von_monat and ym < von_monat:
                        continue
                    if bis_monat and ym > bis_monat:
                        continue
                    vehicles_in_range.add(vid)
        else:
            vehicles_in_range = None  # kein Datumsfilter

        result = []
        for v in iter_items(raw):
            if not v or not isinstance(v, dict):
                continue

            status = str(v.get("status", "")).upper()
            if status != "RE":
                continue

            model = v.get("model", {}) or {}
            prices = v.get("prices", {}) or {}
            engine = v.get("engine", {}) or {}
            identifier = v.get("identifier", {}) or {}

            if isinstance(model, list): model = model[0] if model else {}
            if isinstance(prices, list): prices = prices[0] if prices else {}
            if isinstance(engine, list): engine = engine[0] if engine else {}
            if isinstance(identifier, list): identifier = identifier[0] if identifier else {}

            vk = float(
                prices.get("offer") or prices.get("list") or prices.get("basic") or 0
            )
            ek = float(prices.get("purchase") or 0)
            if vk <= 0:
                continue

            v_id = str(v.get("id") or v.get("uid") or "")
            join_keys = _vehicle_join_keys(v)
            sale_date = next(
                (
                    vehicle_sale_date[key]
                    for key in join_keys
                    if key in vehicle_sale_date
                ),
                "",
            )

            # Datumsfilter
            if vehicles_in_range is not None and not any(
                key in vehicles_in_range for key in join_keys
            ):
                continue

            typ = _classify_typ(v)

            # Fahrzeugtypfilter
            if art_filter != "alle" and typ != art_filter:
                continue

            standtage_default = STANDTAGE_DEFAULTS.get(
                typ, STANDTAGE_DEFAULTS["default"]
            )
            wk_kosten = next(
                (vehicle_expenses[key] for key in join_keys if key in vehicle_expenses),
                0.0,
            )
            dimensions = v.get("dimensions", {}) or {}
            if isinstance(dimensions, list): dimensions = dimensions[0] if dimensions else {}
            laenge_cm = int(dimensions.get("length", 0) or 0)

            # Schnelles DB-Vorschau für Ranking
            # Standkosten: EK * Zins / 365 * Standtage
            # VK: bevorzuge tatsächlichen Auftragspreis, Fallback auf Fahrzeug-Angebotspreis
            vk_order = next(
                (vehicle_vk_from_order[key] for key in join_keys if key in vehicle_vk_from_order),
                0.0,
            )
            vk_final = vk_order if vk_order > 0 else vk

            standkosten_quick = ek * (STANDKOSTEN_ZINS / 365.0) * standtage_default if ek > 0 else 0.0
            db_quick = vk_final - ek - wk_kosten - standkosten_quick if ek > 0 else 0.0

            # Kundendaten
            kd_info = next(
                (vehicle_customer_info[key] for key in join_keys if key in vehicle_customer_info),
                {"name": "", "ort": ""},
            )

            # Verkäufer
            verkaeufer = next(
                (vehicle_verkaeufer[key] for key in join_keys if key in vehicle_verkaeufer),
                "",
            )

            # Einstandsdatum (vehicle.date.incoming) + Rechnungsdatum (vehicle.date.invoice)
            v_date = v.get("date") or {}
            if isinstance(v_date, list):
                v_date = v_date[0] if v_date else {}
            einstandsdatum = str(v_date.get("incoming") or "")
            rechnungsdatum = str(v_date.get("invoice") or "")

            db_pct_quick = (db_quick / vk_final * 100) if vk_final > 0 else 0.0

            result.append(
                {
                    "id": v_id,
                    "vin": identifier.get("vin", ""),
                    "hersteller": model.get("producer", "-"),
                    "modell": model.get("model", "-"),
                    "serie": model.get("series", ""),
                    "modelljahr": model.get("modelyear", "-"),
                    "typ": typ,
                    "zustand": str(v.get("condition", "")).upper(),
                    "vk_brutto": vk_final,
                    "vk_quelle": "auftrag" if vk_order > 0 else "fahrzeug",
                    "ek_brutto": ek,
                    "laenge_m": f"{laenge_cm / 100:.2f}" if laenge_cm else "-",
                    "ps": int(engine.get("ps", 0) or 0),
                    "standtage_vorschlag": standtage_default,
                    "standkosten_zins": STANDKOSTEN_ZINS,
                    "werkstattkosten_vorschlag": wk_kosten,
                    "verkauf_datum": sale_date,
                    "kunden_name": kd_info["name"],
                    "kunden_ort": kd_info["ort"],
                    "verkaeufer": verkaeufer,
                    "einstandsdatum": einstandsdatum,
                    "rechnungsdatum": rechnungsdatum,
                    "_db_quick": db_pct_quick,
                }
            )

        # Nach DB% absteigend sortieren (beste Marge zuerst)
        result.sort(key=lambda x: x["_db_quick"], reverse=True)

        # _db_quick nicht ans Frontend weitergeben
        for item in result:
            item.pop("_db_quick", None)

        # TopN begrenzen
        if top_n > 0:
            result = result[:top_n]

        return jsonify({"success": True, "count": len(result), "vehicles": result})

    @app.route("/api/kosten/deckungsbeitrag", methods=["POST"])
    def api_kosten_deckungsbeitrag():
        """
        Berechnet den Deckungsbeitrag für ein Fahrzeug anhand der übergebenen Positionen.

        Body:
          vehicle_id: str | None        – wenn leer → gibt nur Fahrzeugliste zurück
          filters: dict                 – optionale Filter (art, marke, laenge)
          settings: dict                – Kostenparameter
          positionen: list[dict]        – [{ label, betrag, typ, aktiv }]
        """
        body = request.get_json(silent=True) or {}

        # --- Fahrzeugliste holen und ggf. filtern ---
        raw = get_cached_or_fetch("sale/vehicles", f"{SYSCARA_BASE}/sale/vehicles/")

        # --- Aufträge / Werkstattkosten (Bulk) laden ---
        orders_raw = get_cached_or_fetch(
            "sale/orders", f"{SYSCARA_BASE}/sale/orders/?update=2024-01-01"
        )
        orders = []
        if isinstance(orders_raw, dict) and "orders" in orders_raw:
            orders = orders_raw["orders"]
        elif isinstance(orders_raw, list):
            orders = orders_raw
        elif hasattr(orders_raw, "values"):
            orders = [o for o in orders_raw.values() if isinstance(o, dict)]

        vehicle_expenses = {}
        for o in orders:
            join_keys = _order_join_keys(o)
            if not join_keys:
                continue
            prices_obj = o.get("prices") or {}
            if isinstance(prices_obj, list): prices_obj = prices_obj[0] if prices_obj else {}
            expenses = float(
                prices_obj.get("expenses")
                or prices_obj.get("internal_costs")
                or 0
            )
            if expenses > 0:
                for join_key in join_keys:
                    vehicle_expenses[join_key] = (
                        vehicle_expenses.get(join_key, 0.0) + expenses
                    )

        filters = body.get("filters", {})
        art_filter = str(filters.get("art", "alle")).lower()
        marke_filter = str(filters.get("marke", "alle")).lower()
        laenge_filter = str(filters.get("laenge", "alle")).lower()

        vehicles = []
        for v in iter_items(raw):
            if not v or not isinstance(v, dict):
                continue
            model = v.get("model", {}) or {}
            prices = v.get("prices", {}) or {}
            engine = v.get("engine", {}) or {}
            identifier = v.get("identifier", {}) or {}
            
            if isinstance(model, list): model = model[0] if model else {}
            if isinstance(prices, list): prices = prices[0] if prices else {}
            if isinstance(engine, list): engine = engine[0] if engine else {}
            if isinstance(identifier, list): identifier = identifier[0] if identifier else {}
            typ = _classify_typ(v)
            dimensions = v.get("dimensions", {}) or {}
            if isinstance(dimensions, list): dimensions = dimensions[0] if dimensions else {}
            laenge_cm = int(dimensions.get("length", 0) or 0)
            vk = float(
                prices.get("offer") or prices.get("list") or prices.get("basic") or 0
            )
            ek = float(prices.get("purchase") or 0)
            if vk <= 0:
                continue

            status = str(v.get("status", "")).upper()
            if status != "RE":
                continue

            make = str(model.get("producer", "")).lower()

            if art_filter != "alle" and art_filter not in typ:
                continue
            if marke_filter != "alle" and marke_filter not in make:
                continue
            if laenge_filter != "alle":
                if laenge_filter == "< 6m" and laenge_cm >= 600:
                    continue
                if laenge_filter == "6-7m" and not (600 <= laenge_cm < 700):
                    continue
                if laenge_filter == "7-7.5m" and not (700 <= laenge_cm < 750):
                    continue
                if laenge_filter == "7.5-8m" and not (750 <= laenge_cm < 800):
                    continue
                if laenge_filter == "> 8m" and laenge_cm < 800:
                    continue

            standtage_default = STANDTAGE_DEFAULTS.get(
                typ, STANDTAGE_DEFAULTS["default"]
            )
            v_id = str(v.get("id") or v.get("uid") or "")
            join_keys = _vehicle_join_keys(v)
            wk_kosten = next(
                (vehicle_expenses[key] for key in join_keys if key in vehicle_expenses),
                0.0,
            )

            vehicles.append(
                {
                    "id": v_id,
                    "vin": identifier.get("vin", ""),
                    "hersteller": model.get("producer", "-"),
                    "modell": model.get("model", "-"),
                    "serie": model.get("series", ""),
                    "modelljahr": model.get("modelyear", "-"),
                    "typ": typ,
                    "zustand": str(v.get("condition", "")).upper(),
                    "vk_brutto": vk,
                    "ek_brutto": ek,
                    "laenge_m": f"{laenge_cm / 100:.2f}" if laenge_cm else "-",
                    "ps": int(engine.get("ps", 0) or 0),
                    "standtage_vorschlag": standtage_default,
                    "standkosten_zins": STANDKOSTEN_ZINS,
                    "werkstattkosten_vorschlag": wk_kosten,
                }
            )

        # --- Wenn kein konkretes Fahrzeug: Ranking über alle ---
        settings = body.get("settings", {})
        positionen = body.get("positionen", [])
        vehicle_id = body.get("vehicle_id")

        if not vehicle_id:
            # Ranking-Modus: ø DB je Typ
            typ_buckets: dict = {}
            for vh in vehicles:
                vk = vh["vk_brutto"]
                ek = vh["ek_brutto"]
                standtage = vh["standtage_vorschlag"]
                standkosten = ek * (STANDKOSTEN_ZINS / 365.0) * standtage if ek > 0 else 0.0

                extra = (
                    float(settings.get("batterie", 0))
                    + float(settings.get("solar", 0))
                    + float(settings.get("dachklima", 0))
                )
                finanz = (
                    float(settings.get("finanzierung", {}).get("betrag", 0))
                    if settings.get("finanzierung", {}).get("aktiv")
                    else 0.0
                )
                annahme = float(settings.get("annahme", 200))
                transport = float(settings.get("transport", 0))
                rabatt_ek = ek * float(settings.get("rabatt_ek_prozent", 0)) / 100.0

                gesamtkosten = (
                    ek + standkosten + extra + finanz + annahme + transport - rabatt_ek
                )
                db = vk - gesamtkosten

                t = vh["typ"]
                if t not in typ_buckets:
                    typ_buckets[t] = {
                        "typ": t,
                        "anzahl": 0,
                        "db_sum": 0.0,
                        "best_db": -99999999,
                        "top_vehicle": None,
                    }
                typ_buckets[t]["anzahl"] += 1
                typ_buckets[t]["db_sum"] += db
                if db > typ_buckets[t]["best_db"]:
                    typ_buckets[t]["best_db"] = db
                    typ_buckets[t]["top_vehicle"] = {
                        "make": vh["hersteller"],
                        "model": vh["modell"],
                        "db": round(db, 2),
                        "vk": vk,
                        "ek": ek,
                    }

            ranked = []
            for t, bucket in typ_buckets.items():
                avg_db = (
                    bucket["db_sum"] / bucket["anzahl"] if bucket["anzahl"] > 0 else 0
                )
                ranked.append(
                    {
                        "typ": t,
                        "anzahl": bucket["anzahl"],
                        "avg_db": round(avg_db, 2),
                        "best_db": round(bucket["best_db"], 2),
                        "top_vehicle": bucket["top_vehicle"],
                    }
                )
            ranked.sort(key=lambda x: x["avg_db"], reverse=True)

            return jsonify(
                {
                    "success": True,
                    "ranked_results": ranked,
                    "total_items": len(vehicles),
                    "config": {
                        "extra_costs": float(settings.get("batterie", 0))
                        + float(settings.get("solar", 0))
                        + float(settings.get("dachklima", 0))
                    },
                }
            )

        # --- Einzelfahrzeug-Modus: detaillierte DB-Berechnung ---
        target = next((vh for vh in vehicles if str(vh["id"]) == str(vehicle_id)), None)
        if not target:
            # Fallback: unkalibriertes Fahrzeug aus Rohdaten suchen
            return jsonify({"success": False, "error": "Fahrzeug nicht gefunden."}), 404

        vk = target["vk_brutto"]
        ek = target["ek_brutto"]
        standtage = float(settings.get("standtage", target["standtage_vorschlag"]))

        # Standard-Positionen aus settings + Positions-Liste
        abzuege = []
        zuschlaege = []

        # Standkosten (automatisch)
        standkosten_total = ek * (STANDKOSTEN_ZINS / 365.0) * standtage if ek > 0 else 0.0
        abzuege.append(
            {
                "label": "Standtage",
                "betrag": round(standkosten_total, 2),
                "auto": True,
                "detail": f"{standtage:.0f} Tage bei {STANDKOSTEN_ZINS*100:.1f}% p.a.",
            }
        )

        # Freie Positionen aus dem Body
        for pos in positionen:
            if not pos.get("aktiv", True):
                continue
            betrag = float(pos.get("betrag", 0) or 0)
            if pos.get("typ") == "abzug":
                abzuege.append(
                    {
                        "label": pos.get("label", "Position"),
                        "betrag": betrag,
                        "auto": False,
                    }
                )
            elif pos.get("typ") == "zuschlag":
                zuschlaege.append(
                    {
                        "label": pos.get("label", "Position"),
                        "betrag": betrag,
                        "auto": False,
                    }
                )

        # EK Rabatt als Zuschlag
        rabatt_prozent = float(settings.get("rabatt_ek_prozent", 0))
        if rabatt_prozent > 0:
            zuschlaege.append(
                {
                    "label": f"Rabatt auf EK ({rabatt_prozent:.1f}%)",
                    "betrag": round(ek * rabatt_prozent / 100, 2),
                    "auto": True,
                }
            )

        total_abzuege = sum(p["betrag"] for p in abzuege) + ek
        total_zuschlaege = sum(p["betrag"] for p in zuschlaege)
        db = vk - total_abzuege + total_zuschlaege
        db_prozent = (db / vk * 100) if vk else 0

        return jsonify(
            {
                "success": True,
                "vehicle": target,
                "kalkulation": {
                    "vk_brutto": vk,
                    "ek_brutto": ek,
                    "abzuege": abzuege,
                    "zuschlaege": zuschlaege,
                    "total_abzuege": round(total_abzuege, 2),
                    "total_zuschlaege": round(total_zuschlaege, 2),
                    "deckungsbeitrag": round(db, 2),
                    "db_prozent": round(db_prozent, 2),
                },
            }
        )
