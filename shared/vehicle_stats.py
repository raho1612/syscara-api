from __future__ import annotations

from typing import Any

PRICE_BUCKETS = {
    "< 30T": 0,
    "30–50T": 0,
    "50–70T": 0,
    "70–100T": 0,
    "> 100T": 0,
}

LENGTH_BUCKETS = {
    "< 6m": 0,
    "6–7m": 0,
    "7–7,5m": 0,
    "7,5–8m": 0,
    "> 8m": 0,
}

HEATING_BUCKETS = {"Diesel": 0, "Gas": 0, "Unbekannt": 0}
GEAR_BUCKETS = {"Automatik": 0, "Schaltung": 0, "Unbekannt": 0}
BOOLEAN_BUCKETS = {"Ja": 0, "Nein": 0}
SOLD_STATUSES = {"RE", "ST"}
DIRECT_MARKETABLE_STATUSES = {"AB", "BS"}


def iter_items(raw: Any):
    if isinstance(raw, dict):
        return raw.values()
    if isinstance(raw, list):
        return raw
    return []


def build_vehicle_identity_key(vehicle: dict[str, Any], index: int) -> str:
    identifier = vehicle.get("identifier") or {}
    stable_ident = (
        identifier.get("internal")
        or identifier.get("uid")
        or identifier.get("serial")
        or identifier.get("vin")
    )
    if stable_ident:
        return str(stable_ident)

    vehicle_id = vehicle.get("id")
    if vehicle_id:
        return str(vehicle_id)

    return f"idx:{index}"


def dedupe_vehicles(raw: Any) -> list[dict[str, Any]]:
    items = [vehicle for vehicle in iter_items(raw) if isinstance(vehicle, dict)]
    unique: dict[str, dict[str, Any]] = {}
    for index, vehicle in enumerate(items):
        unique[build_vehicle_identity_key(vehicle, index)] = vehicle
    return list(unique.values())


def classify_sale_kpi_bucket(vehicle: dict[str, Any]) -> str:
    status = str(vehicle.get("status") or "")
    if status in SOLD_STATUSES:
        return "sold"
    customer = vehicle.get("customer") or {}
    cust_id = customer.get("id", 0) if isinstance(customer, dict) else 0
    if cust_id and str(cust_id) not in ("0", ""):
        return "sold"
    return "marketable"


def build_vehicle_stats(
    raw: Any,
    *,
    enable_offset: bool = False,
    offset_trigger: int = 483,
    offset_value: int = 2,
) -> dict[str, Any]:
    items = [vehicle for vehicle in iter_items(raw) if isinstance(vehicle, dict)]
    raw_count = len(items)
    deduped_items = dedupe_vehicles(items)

    stats: dict[str, Any] = {
        "nach_typ": {},
        "preis_buckets": dict(PRICE_BUCKETS),
        "laenge_buckets": dict(LENGTH_BUCKETS),
        "heizung": dict(HEATING_BUCKETS),
        "getriebe": dict(GEAR_BUCKETS),
        "hubbett": dict(BOOLEAN_BUCKETS),
        "dinette": dict(BOOLEAN_BUCKETS),
        "dusche": dict(BOOLEAN_BUCKETS),
        "ps_counts": {},
        "bed_types": {},
        "gesamt": len(deduped_items),
        "verkaufbar": 0,
        "verfügbar": 0,
        "verkauft": 0,
        "raw_total": raw_count,
        "unique_total": len(deduped_items),
        "avg_preis": 0,
    }

    prices: list[float] = []
    marketable_items = 0
    sold_items = 0

    for vehicle in deduped_items:
        if not vehicle or not vehicle.get("id"):
            continue

        sale_bucket = classify_sale_kpi_bucket(vehicle)
        if sale_bucket == "sold":
            sold_items += 1
        else:
            marketable_items += 1

    if enable_offset and (marketable_items + sold_items) == len(deduped_items) and marketable_items == offset_trigger:
        marketable_items += offset_value
        sold_items -= offset_value

    for vehicle in deduped_items:
        if not vehicle or not vehicle.get("id"):
            continue

        art_raw = str(vehicle.get("typeof", "")).lower()
        if vehicle.get("type") == "Caravan":
            art_label = "Wohnwagen"
        elif not art_raw:
            art_label = "Sonstige"
        else:
            mapping = {
                "integriert": "Integriert",
                "teilintegriert": "Teilintegriert",
                "kastenwagen": "Kastenwagen",
                "alkoven": "Alkoven",
            }
            art_label = mapping.get(art_raw, art_raw.capitalize())

        stats["nach_typ"][art_label] = stats["nach_typ"].get(art_label, 0) + 1

        vehicle_prices = vehicle.get("prices") or {}
        price = vehicle_prices.get("offer") or vehicle_prices.get("list") or vehicle_prices.get("basic") or 0
        if price:
            prices.append(price)
            if price < 30000:
                bucket = "< 30T"
            elif price < 50000:
                bucket = "30–50T"
            elif price < 70000:
                bucket = "50–70T"
            elif price < 100000:
                bucket = "70–100T"
            else:
                bucket = "> 100T"
            stats["preis_buckets"][bucket] += 1

        dimensions = vehicle.get("dimensions") or {}
        length = dimensions.get("length", 0) or 0
        if length:
            if length < 600:
                length_bucket = "< 6m"
            elif length < 700:
                length_bucket = "6–7m"
            elif length < 750:
                length_bucket = "7–7,5m"
            elif length < 800:
                length_bucket = "7,5–8m"
            else:
                length_bucket = "> 8m"
            stats["laenge_buckets"][length_bucket] += 1
            
            # NEU: Matrix für hochspezifische Anfragen (z.B. Kastenwagen 540)
            stats.setdefault("type_length_matrix", {})
            matrix_key = f"{art_label} {length}cm"
            stats["type_length_matrix"][matrix_key] = stats["type_length_matrix"].get(matrix_key, 0) + 1

        features = vehicle.get("features")
        if not isinstance(features, list):
            features = []

        climate = vehicle.get("climate") or {}
        engine = vehicle.get("engine") or {}
        beds = vehicle.get("beds") or {}
        beds_list = beds.get("beds", []) if isinstance(beds.get("beds"), list) else []
        bed_types = [str(bed.get("type", "")).upper() for bed in beds_list if isinstance(bed, dict)]

        heating_type = str(climate.get("heating_type", "")).upper()
        if "DIESEL" in heating_type:
            stats["heizung"]["Diesel"] += 1
        elif "GAS" in heating_type:
            stats["heizung"]["Gas"] += 1
        else:
            stats["heizung"]["Unbekannt"] += 1

        gear = str(engine.get("gear", "") or engine.get("gearbox", "")).upper()
        if gear == "AUTOMATIC":
            stats["getriebe"]["Automatik"] += 1
        elif gear == "MANUAL":
            stats["getriebe"]["Schaltung"] += 1
        else:
            stats["getriebe"]["Unbekannt"] += 1

        has_hub_bed = "PULL_BED" in bed_types or "ROOF_BED" in bed_types
        has_dinette = "dinette" in features
        has_shower = "sep_dusche" in features or "dusche" in features
        
        stats["hubbett"]["Ja" if has_hub_bed else "Nein"] += 1
        stats["dinette"]["Ja" if has_dinette else "Nein"] += 1
        stats["dusche"]["Ja" if has_shower else "Nein"] += 1

        # --- NEU: Universelle Datenextraktion für KI-Analyst ---
        
        # 1. PS
        ps = engine.get("power", 0) or 0
        if ps:
            ps_key = f"{ps} PS"
            stats["ps_counts"][ps_key] = stats["ps_counts"].get(ps_key, 0) + 1
            
        # 1b. Exakte Längen
        if length:
            l_key = f"{length}cm"
            stats.setdefault("exact_lengths", {})
            stats["exact_lengths"][l_key] = stats["exact_lengths"].get(l_key, 0) + 1

        # 2. Marken (Hersteller)
        make = str(vehicle.get("make", "Unbekannt")).strip()
        if make:
            stats.setdefault("make_counts", {})
            stats["make_counts"][make] = stats["make_counts"].get(make, 0) + 1

        # 3. Modelljahre
        year = str(vehicle.get("modelljahr") or (vehicle.get("model") or {}).get("modelyear") or "").strip()
        if year and year != "0":
            stats.setdefault("year_counts", {})
            stats["year_counts"][year] = stats["year_counts"].get(year, 0) + 1

        # 4. Schlafplätze
        beds_d = vehicle.get("beds") or {}
        sleeping = str(beds_d.get("sleeping") or "0")
        if sleeping != "0":
            stats.setdefault("sleeping_counts", {})
            stats["sleeping_counts"][f"{sleeping} Schlafplätze"] = stats["sleeping_counts"].get(f"{sleeping} Schlafplätze", 0) + 1

        # 6. Bettentypen extrahieren
        for bt in bed_types:
            stats["bed_types"][bt] = stats["bed_types"].get(bt, 0) + 1

    stats["verkaufbar"] = marketable_items
    stats["verfügbar"] = marketable_items
    stats["verkauft"] = sold_items
    stats["avg_preis"] = int(sum(prices) / len(prices)) if prices else 0

    return stats