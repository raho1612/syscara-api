"""
kosten_engine.py
================
Zentrale Logik für den Kosten-Tab und die Deckungsbeitrags-Berechnung.
Kapselt den Daten-Join zwischen Fahrzeugen, Aufträgen und Werkstattkosten.
"""

from __future__ import annotations
import os
import json
import re
from typing import Any, Dict, List, Tuple
from core.config import SYSCARA_BASE, CURRENT_DIR
from core.database import get_cached_or_fetch, iter_items

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

def _is_vehicle_item(name: str, price: float) -> bool:
    """
    Filtert Fahrzeug-Basiskosten aus Werkstattaufträgen.
    Reisemobil-Fahrzeuge kosten in der Regel > 30.000 Euro. 
    Werkstatt-Einzelpositionen (Teile/Arbeit) liegen fast immer deutlich darunter.
    """
    if price >= 30000:
        return True
    return False

def _safe_float(val) -> float:
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace(",", ".")
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        m = re.search(r"-?\d+(?:\.\d+)?", s)
        return float(m.group()) if m else 0.0

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

def _is_real_date(val: str) -> bool:
    if not val or len(val) < 7:
        return False
    return val[:4].isdigit() and val[4] == "-" and val[5:7].isdigit()

def _extract_order_date(order: dict) -> str:
    date_obj = order.get("date")
    if isinstance(date_obj, dict):
        for sub_key in ("delivery", "invoice", "created", "updated"):
            val = date_obj.get(sub_key)
            if val and isinstance(val, str) and _is_real_date(val):
                return val[:10]
    for key in ("created_at", "created", "create_date", "createdAt", "order_date"):
        val = order.get(key)
        if val and isinstance(val, str) and _is_real_date(val):
            return val[:10]
    return ""

def _load_orders() -> list:
    orders_raw = get_cached_or_fetch(
        "sale/orders", f"{SYSCARA_BASE}/sale/orders/?update=2024-01-01"
    )
    if isinstance(orders_raw, dict) and "orders" in orders_raw:
        return orders_raw["orders"]
    if isinstance(orders_raw, list):
        return orders_raw
    if hasattr(orders_raw, "values"):
        return [o for o in orders_raw.values() if isinstance(o, dict)]
    return []

def _load_employee_names() -> dict:
    path = CURRENT_DIR / "employee_names.json"
    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
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

def _load_work_orders() -> list:
    work_raw = get_cached_or_fetch(
        "work/orders", f"{SYSCARA_BASE}/work/orders/?update=2025-01-01"
    )
    if isinstance(work_raw, dict):
        return [v for v in work_raw.values() if isinstance(v, dict)]
    if isinstance(work_raw, list):
        return work_raw
    return []

def _work_join_keys(wo: dict) -> list[str]:
    idf = wo.get("identifier") or {}
    if isinstance(idf, list):
        idf = idf[0] if idf else {}
    keys = [
        str(idf.get("vin") or "").strip().upper(),
        str(idf.get("internal") or "").strip(),
    ]
    return [k for k in keys if k]

def _build_work_index(work_orders: list) -> tuple[dict, dict, dict, dict]:
    kosten_idx: dict = {}
    erloes_idx: dict = {}
    kosten_items_idx: dict = {}
    erloes_items_idx: dict = {}

    for wo in work_orders:
        join_keys = _work_join_keys(wo)
        if not join_keys:
            continue

        category = str(wo.get("category") or "").upper()
        equipment = wo.get("equipment") or []
        if isinstance(equipment, dict):
            equipment = list(equipment.values())

        for item in equipment:
            if not isinstance(item, dict):
                continue
            billing = str(item.get("billing") or "").upper()
            item_cat = str(item.get("category") or "").lower()
            eprice = _safe_float(item.get("eprice"))
            price = _safe_float(item.get("price"))
            item_name = str(item.get("name") or item.get("code") or item_cat or "Position").strip()

            if category == "DELIVERY":
                if item_cat == "werk":
                    continue
                cost = eprice if eprice > 0 else price
                if cost > 0:
                    entry = {"name": item_name, "betrag": round(cost, 2), "typ": "delivery"}
                    for jk in join_keys:
                        kosten_idx[jk] = kosten_idx.get(jk, 0.0) + cost
                        kosten_items_idx.setdefault(jk, []).append(entry)
            else:
                if billing in ("WARRANTY", "INTERNAL"):
                    if eprice > 0:
                        billing_label = "Garantie" if billing == "WARRANTY" else "Intern"
                        entry = {"name": item_name, "betrag": round(eprice, 2), "typ": billing_label.lower()}
                        for jk in join_keys:
                            kosten_idx[jk] = kosten_idx.get(jk, 0.0) + eprice
                            kosten_items_idx.setdefault(jk, []).append(entry)
                elif billing == "CUSTOMER":
                    if price > 0:
                        # FIX: Falls die Position das Basisfahrzeug selbst darstellt -> Überspringen
                        if _is_vehicle_item(item_name, price):
                            continue
                            
                        entry = {"name": item_name, "betrag": round(price, 2), "typ": "erloes"}
                        for jk in join_keys:
                            erloes_idx[jk] = erloes_idx.get(jk, 0.0) + price
                            erloes_items_idx.setdefault(jk, []).append(entry)

    return kosten_idx, erloes_idx, kosten_items_idx, erloes_items_idx

def get_kosten_fahrzeuge(
    von_monat: str = "",
    bis_monat: str = "",
    art_filter: str = "alle",
    top_n: int = 0
) -> List[Dict[str, Any]]:
    """
    Kern-Logik für den Kosten-Tab: Sammelt Fahrzeuge und berechnet DB-Vorschau.
    """
    raw_v = get_cached_or_fetch("sale/vehicles", f"{SYSCARA_BASE}/sale/vehicles/")
    orders = _load_orders()
    employee_names = _load_employee_names()
    work_orders = _load_work_orders()
    work_kosten_idx, work_erloes_idx, work_kosten_items_idx, work_erloes_items_idx = _build_work_index(work_orders)

    vehicle_sale_date: dict = {}
    vehicle_customer_info: dict = {}
    vehicle_vk_from_order: dict = {}
    vehicle_verkaeufer: dict = {}

    for o in orders:
        join_keys = _order_join_keys(o)
        if not join_keys:
            continue

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

        prices_obj = o.get("prices") or {}
        if isinstance(prices_obj, list):
            prices_obj = prices_obj[0] if prices_obj else {}
        vk_order = _safe_float(prices_obj.get("offer"))
        if vk_order > 0:
            for join_key in join_keys:
                if join_key not in vehicle_vk_from_order:
                    vehicle_vk_from_order[join_key] = vk_order

        user_obj = o.get("user") or {}
        seller_id = str(user_obj.get("order") or user_obj.get("seller") or "").strip()
        if seller_id and seller_id != "0":
            seller_name = employee_names.get(seller_id, f"ID {seller_id}")
            for join_key in join_keys:
                if join_key not in vehicle_verkaeufer:
                    vehicle_verkaeufer[join_key] = seller_name

        order_date = _extract_order_date(o)
        if order_date:
            for join_key in join_keys:
                existing = vehicle_sale_date.get(join_key, "")
                if not existing or order_date > existing:
                    vehicle_sale_date[join_key] = order_date

    result = []
    for v in iter_items(raw_v):
        if not v or not isinstance(v, dict):
            continue

        v_status_raw = str(v.get("status", "")).upper()
        is_delivered = False
        if v_status_raw == "RE":
            v_status_label = "Verkauft (RE)"
        elif v_status_raw == "BE":
            _v_date_be = v.get("date") or {}
            if isinstance(_v_date_be, list): _v_date_be = _v_date_be[0] if _v_date_be else {}
            if _is_real_date(str(_v_date_be.get("customer") or "")):
                v_status_label = "Ausgeliefert (BE)"
                is_delivered = True
            else:
                v_status_label = "Bestellt (BE)"
        else:
            v_status_label = f"Bestand ({v_status_raw})" if v_status_raw else "Bestand"

        model = v.get("model", {}) or {}
        prices = v.get("prices", {}) or {}
        engine = v.get("engine", {}) or {}
        identifier = v.get("identifier", {}) or {}

        if isinstance(model, list): model = model[0] if model else {}
        if isinstance(prices, list): prices = prices[0] if prices else {}
        if isinstance(engine, list): engine = engine[0] if engine else {}
        if isinstance(identifier, list): identifier = identifier[0] if identifier else {}

        vk = _safe_float(prices.get("offer") or prices.get("list") or prices.get("basic"))
        ek_netto = _safe_float(prices.get("purchase"))
        ek_brutto = _safe_float(prices.get("purchase_gross")) or ek_netto
        
        is_diff_tax = False
        if ek_brutto > 0 and abs(ek_brutto - ek_netto) < 0.01:
            is_diff_tax = True
            
        ek = ek_brutto if is_diff_tax else ek_netto

        v_vin = identifier.get("vin", "")
        if vk <= 0 and v_vin != "WF0EXXTTRENY28111":
            continue

        v_id = str(v.get("id") or v.get("uid") or "")
        join_keys = _vehicle_join_keys(v)
        sale_date = next(
            (vehicle_sale_date[key] for key in join_keys if key in vehicle_sale_date),
            "",
        )

        v_date = v.get("date") or {}
        if isinstance(v_date, list):
            v_date = v_date[0] if v_date else {}
        einstandsdatum = str(v_date.get("incoming") or "")
        rechnungsdatum = str(v_date.get("invoice") or "")
        kundendatum = str(v_date.get("customer") or "")

        if not sale_date and rechnungsdatum and _is_real_date(rechnungsdatum):
            sale_date = rechnungsdatum
        if not sale_date and kundendatum and _is_real_date(kundendatum):
            sale_date = kundendatum

        if (von_monat or bis_monat) and sale_date:
            ym_fb = sale_date[:7]
            in_range = True
            if von_monat and ym_fb < von_monat: in_range = False
            if bis_monat and ym_fb > bis_monat: in_range = False
            if not in_range:
                continue

        typ = _classify_typ(v)
        if art_filter != "alle" and typ != art_filter:
            continue

        standtage_default = STANDTAGE_DEFAULTS.get(typ, STANDTAGE_DEFAULTS["default"])
        wk_kosten = next((work_kosten_idx[key] for key in join_keys if key in work_kosten_idx), 0.0)
        wk_erloes = next((work_erloes_idx[key] for key in join_keys if key in work_erloes_idx), 0.0)
        wk_kosten_items = next((work_kosten_items_idx[key] for key in join_keys if key in work_kosten_items_idx), [])
        wk_erloes_items = next((work_erloes_items_idx[key] for key in join_keys if key in work_erloes_items_idx), [])
        dimensions = v.get("dimensions", {}) or {}
        if isinstance(dimensions, list): dimensions = dimensions[0] if dimensions else {}
        laenge_cm = int(_safe_float(dimensions.get("length")))

        vk_order = next((vehicle_vk_from_order[key] for key in join_keys if key in vehicle_vk_from_order), 0.0)
        vk_final = vk_order if vk_order > 0 else vk

        standkosten_quick = ek * (STANDKOSTEN_ZINS / 365.0) * standtage_default if ek > 0 else 0.0
        db_quick = (vk_final + wk_erloes) - ek - wk_kosten - standkosten_quick if ek > 0 else 0.0

        kd_info = next((vehicle_customer_info[key] for key in join_keys if key in vehicle_customer_info), {"name": "", "ort": ""})
        verkaeufer = next((vehicle_verkaeufer[key] for key in join_keys if key in vehicle_verkaeufer), "")
        db_pct_quick = (db_quick / vk_final * 100) if vk_final > 0 else 0.0

        result.append({
            "id": v_id,
            "vin": v_vin,
            "hersteller": model.get("producer", "-"),
            "modell": model.get("model", "-"),
            "serie": model.get("series", ""),
            "modelljahr": model.get("modelyear", "-"),
            "typ": typ,
            "status": v_status_label,
            "zustand": str(v.get("condition", "")).upper(),
            "vk_brutto": vk_final,
            "vk_quelle": "auftrag" if vk_order > 0 else "fahrzeug",
            "ek_brutto": ek_brutto,
            "ek_netto": ek_netto,
            "is_diff_tax": is_diff_tax,
            "laenge_m": f"{laenge_cm / 100:.2f}" if laenge_cm else "-",
            "ps": int(_safe_float(engine.get("ps"))),
            "standtage_vorschlag": standtage_default,
            "standkosten_zins": STANDKOSTEN_ZINS,
            "werkstattkosten_vorschlag": wk_kosten,
            "werkstattkosten_positionen": wk_kosten_items,
            "werkstatt_erloes_vorschlag": wk_erloes,
            "werkstatt_erloes_positionen": wk_erloes_items,
            "verkauf_datum": sale_date,
            "kunden_name": kd_info["name"],
            "kunden_ort": kd_info["ort"],
            "verkaeufer": verkaeufer,
            "einstandsdatum": einstandsdatum,
            "rechnungsdatum": rechnungsdatum,
            "_db_quick": db_pct_quick,
        })

    result.sort(key=lambda x: x["_db_quick"], reverse=True)
    for item in result:
        item.pop("_db_quick", None)

    if top_n > 0:
        result = result[:top_n]

    return result

def calculate_deckungsbeitrag(
    vehicle_id: str | None,
    filters: dict,
    settings: dict,
    positionen: list[dict]
) -> dict[str, Any]:
    """
    Berechnet den Deckungsbeitrag für ein Fahrzeug oder ein Ranking über alle Fahrzeuge.
    """
    raw_v = get_cached_or_fetch("sale/vehicles", f"{SYSCARA_BASE}/sale/vehicles/")
    orders = _load_orders()
    work_orders = _load_work_orders()
    _, erloes_idx, _, erloes_items_idx = _build_work_index(work_orders)

    vehicle_expenses = {}
    for o in orders:
        join_keys = _order_join_keys(o)
        if not join_keys:
            continue
        prices_obj = o.get("prices") or {}
        if isinstance(prices_obj, list): prices_obj = prices_obj[0] if prices_obj else {}
        expenses = _safe_float(
            prices_obj.get("expenses")
            or prices_obj.get("internal_costs")
        )
        if expenses > 0:
            for join_key in join_keys:
                vehicle_expenses[join_key] = vehicle_expenses.get(join_key, 0.0) + expenses

    art_filter = str(filters.get("art", "alle")).lower()
    marke_filter = str(filters.get("marke", "alle")).lower()
    laenge_filter = str(filters.get("laenge", "alle")).lower()

    vehicles = []
    for v in iter_items(raw_v):
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
        laenge_cm = int(_safe_float(dimensions.get("length")))
        
        # EK-Basis (Netto vs Brutto)
        ek_netto = _safe_float(prices.get("purchase"))
        ek_brutto = _safe_float(prices.get("purchase_gross")) or ek_netto
        
        # Falls Netto == Brutto -> Differenzbesteuerung (Privateinkauf)
        is_diff_tax = False
        if ek_brutto > 0 and abs(ek_brutto - ek_netto) < 0.01:
            is_diff_tax = True
            
        vk_brutto = _safe_float(prices.get("offer") or prices.get("list") or prices.get("basic"))
        if vk_brutto <= 0:
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

        standtage_default = STANDTAGE_DEFAULTS.get(typ, STANDTAGE_DEFAULTS["default"])
        v_id = str(v.get("id") or v.get("uid") or "")
        join_keys = _vehicle_join_keys(v)
        vin = identifier.get("vin", "").upper()
        f_key = join_keys[0] if join_keys else ""
        
        # 1. Einkaufskosten (Inland/Eingangsrechnungen)
        purchase_expenses = next((vehicle_expenses[key] for key in join_keys if key in vehicle_expenses), 0.0)
        
        # 2. Werkstatt-Kosten & Erlöse aus dem Workshop-Modul
        # Interne Aufträge = Kosten für das Fahrzeug
        w_kosten_intern = kosten_idx.get(vin, 0.0) + kosten_idx.get(f_key, 0.0)
        
        # Kunden-Aufträge (Zusatzumsatz laut User oft in VK inkludiert)
        w_erloes = erloes_idx.get(vin, 0.0) + erloes_idx.get(f_key, 0.0)
        
        # Kombinierte Detail-Liste (BELS) für das Dashboard
        # Wir sammeln sowohl Kosten-Posten als auch Erlös-Posten
        w_items = (
            erloes_items_idx.get(vin, []) + erloes_items_idx.get(f_key, []) +
            work_kosten_items_idx.get(vin, []) + work_kosten_items_idx.get(f_key, [])
        )
        
        # Gesamtkosten-Basis für den DB
        total_wk_kosten = purchase_expenses + w_kosten_intern

        if is_diff_tax:
            marge_brutto = vk_brutto - ek_brutto
            steuer_anteil = (marge_brutto / 1.19) * 0.19 if marge_brutto > 0 else 0.0
            umsatz_netto = vk_brutto - steuer_anteil
            fahrzeug_kosten_netto = ek_brutto
        else:
            umsatz_netto = vk_brutto / 1.19
            fahrzeug_kosten_netto = ek_netto

        vehicles.append({
            "id": v_id,
            "vin": vin,
            "hersteller": model.get("producer", "-"),
            "modell": model.get("model", "-"),
            "serie": model.get("series", ""),
            "modelljahr": model.get("modelyear", "-"),
            "typ": typ,
            "zustand": str(v.get("condition", "")).upper(),
            "vk_brutto": vk_brutto,
            "ek_brutto": ek_brutto,
            "ek_netto": ek_netto,
            "is_diff_tax": is_diff_tax,
            "werkstatt_erloes": round(w_erloes, 2),
            "werkstatt_details": w_items,
            "umsatz_netto": round(umsatz_netto, 2),
            "kosten_basis": round(fahrzeug_kosten_netto, 2),
            "laenge_m": f"{laenge_cm / 100:.2f}" if laenge_cm else "-",
            "ps": int(_safe_float(engine.get("ps"))),
            "standtage_vorschlag": standtage_default,
            "standkosten_zins": STANDKOSTEN_ZINS,
            "werkstattkosten_vorschlag": round(total_wk_kosten, 2),
            "werkstatt_details": w_items
        })

    if not vehicle_id:
        typ_buckets: dict = {}
        for vh in vehicles:
            # DB = Netto-Erlös - Kosten-Basis - Werkstattkosten - Standkosten - Sonstige (laut Settings)
            # Dabei ist vh['umsatz_netto'] bereits nach Steuer (§25a oder Regel)
            
            ek_brutto = vh["ek_brutto"]
            standtage = vh["standtage_vorschlag"]
            standkosten = ek_brutto * (STANDKOSTEN_ZINS / 365.0) * standtage if ek_brutto > 0 else 0.0

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
            
            # DB Berechnung mit Einbezug der Werkstatt-Erlöse und -Kosten
            # Wir nehmen umsatz_netto (bereits versteuerter VK) + Zusatzumsatz aus BELS (netto geschätzt)
            erloes_bels_netto = vh.get("werkstatt_erloes", 0) / 1.19
            db = (vh["umsatz_netto"] + erloes_bels_netto) - vh["kosten_basis"] - vh["werkstattkosten_vorschlag"] - standkosten - extra - finanz - annahme - transport
            
            vk = vh["vk_brutto"]
            ek = ek_brutto

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
            avg_db = bucket["db_sum"] / bucket["anzahl"] if bucket["anzahl"] > 0 else 0
            ranked.append({
                "typ": t,
                "anzahl": bucket["anzahl"],
                "avg_db": round(avg_db, 2),
                "best_db": round(bucket["best_db"], 2),
                "top_vehicle": bucket["top_vehicle"],
            })
        ranked.sort(key=lambda x: x["avg_db"], reverse=True)
        return {"ranked_results": ranked, "total_items": len(vehicles)}

    target = next((vh for vh in vehicles if str(vh["id"]) == str(vehicle_id)), None)
    if not target:
        return {"error": "Fahrzeug nicht gefunden"}

    vk = target["vk_brutto"]
    ek = target["ek_brutto"]
    standtage = float(settings.get("standtage", target["standtage_vorschlag"]))

    abzuege = []
    zuschlaege = []
    standkosten_total = ek * (STANDKOSTEN_ZINS / 365.0) * standtage if ek > 0 else 0.0
    abzuege.append({
        "label": "Standtage",
        "betrag": round(standkosten_total, 2),
        "auto": True,
        "detail": f"{standtage:.0f} Tage bei {STANDKOSTEN_ZINS*100:.1f}% p.a.",
    })
    
    # NEU: Interne Werkstattkosten als Abzug inkludieren
    wk_intern = target.get("werkstattkosten_vorschlag", 0.0)
    if wk_intern > 0:
        abzuege.append({
            "label": "Werkstatt / Aufbereitung (Intern)",
            "betrag": round(wk_intern, 2),
            "auto": True,
            "detail": "Kosten laut Werkstattaufträgen"
        })

    for pos in positionen:
        if not pos.get("aktiv", True):
            continue
        betrag = float(pos.get("betrag", 0) or 0)
        if pos.get("typ") == "abzug":
            abzuege.append({"label": pos.get("label", "Position"), "betrag": betrag, "auto": False})
        elif pos.get("typ") == "zuschlag":
            zuschlaege.append({"label": pos.get("label", "Position"), "betrag": betrag, "auto": False})

    rabatt_prozent = float(settings.get("rabatt_ek_prozent", 0))
    if rabatt_prozent > 0:
        zuschlaege.append({
            "label": f"Rabatt auf EK ({rabatt_prozent:.1f}%)",
            "betrag": round(ek * rabatt_prozent / 100, 2),
            "auto": True,
        })

    # Finale Berechnung basierend auf der Netto-Logik (Tax)
    total_abzuege = sum(p["betrag"] for p in abzuege)
    total_zuschlaege = sum(p["betrag"] for p in zuschlaege)
    
    # DB = Netto-Erlös (nach Steuer) - Netto-EK-Basis - alle weiteren Abzüge + Zuschläge
    # Wir addieren den Werkstatt-Zusatzumsatz (BELS) hinzu
    umsatz_netto_fz = target["umsatz_netto"]
    erloes_bels_netto = target.get("werkstatt_erloes", 0) / 1.19
    ek_kosten_basis = target["kosten_basis"]
    
    total_umsatz_netto = umsatz_netto_fz + erloes_bels_netto
    db = total_umsatz_netto - ek_kosten_basis - total_abzuege + total_zuschlaege
    db_prozent = (db / total_umsatz_netto * 100) if total_umsatz_netto > 0 else 0

    return {
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
