import logging
from typing import Any, Dict, List, Optional
from core.utils import fmt_preis, iter_items

# Setup logging
logger = logging.getLogger(__name__)

def _extract_vehicle_features(v: dict) -> dict:
    """Helper to extract vehicle features for mapping."""
    def _d(key):
        res = v.get(key)
        return res if isinstance(res, dict) else {}

    model = _d("model")
    engine = _d("engine")
    dimensions = _d("dimensions")
    prices = _d("prices")
    beds_d = _d("beds")

    art_raw = str(v.get("typeof", "")).lower()
    art_label = "Wohnwagen" if v.get("type") == "Caravan" else art_raw.capitalize()
    
    # Spezifische Mappings für Reisemobile-MKK
    if "integriert" in art_raw:
        art_label = "Integriert"
    if "teilintegriert" in art_raw:
        art_label = "Teilintegriert"
    if "kastenwagen" in art_raw or "camper" in art_raw:
        art_label = "Kastenwagen"

    ps = engine.get("ps", 0) or engine.get("power", 0) or 0
    laenge = dimensions.get("length", 0) or 0
    
    # Syscara Preis-Logik (Fallback Kette)
    # WICHTIG: Laut PROJECT_MAP.md können Preisfelder fehlen oder leere Listen sein.
    preis = prices.get("offer") or prices.get("list") or prices.get("basic") or 0
    ek_preis = prices.get("purchase") or 0
    
    modelljahr = model.get("modelyear", 0) or 0
    schlafplaetze = beds_d.get("sleeping", 0) or 0
    
    features = v.get("features", [])
    if not isinstance(features, list):
        features = []

    beds_list = beds_d.get("beds", []) if isinstance(beds_d.get("beds"), list) else []
    bed_types = [str(bed.get("type", "")).upper() for bed in beds_list if isinstance(bed, dict)]
    
    has_hubbett = "PULL_BED" in bed_types or "ROOF_BED" in bed_types
    has_dusche = "sep_dusche" in features or "dusche" in features
    
    gear_raw = str(engine.get("gear", "") or engine.get("gearbox", "")).upper()
    has_auto = any(x in gear_raw for x in ["AUTOMATIC", "AUT", "AUTOMATIK"])
    condition = str(v.get("condition", "")).upper()

    return {
        "art_raw": art_raw,
        "art_label": art_label,
        "ps": ps,
        "laenge": laenge,
        "preis": preis,
        "ek_preis": ek_preis,
        "modelljahr": modelljahr,
        "schlafplaetze": schlafplaetze,
        "has_hubbett": has_hubbett,
        "has_dusche": has_dusche,
        "has_auto": has_auto,
        "condition": condition,
        "producer": model.get("producer", "-"),
        "model_name": model.get("model", "-"),
    }

def _apply_filters(v_data: dict, raw_v: dict, filters: dict) -> bool:
    """Helper to apply all filters to a vehicle."""
    if not filters:
        return True
    
    # Basic Filters
    f_art = str(filters.get("art", "alle")).lower()
    if f_art != "alle" and f_art != v_data["art_label"].lower():
        return False

    f_stat = filters.get("status")
    if f_stat and str(raw_v.get("status")).upper() != f_stat.upper():
        return False

    f_cond = filters.get("zustand")
    if f_cond and f_cond.lower() not in ("alle", "all") and f_cond.upper() != v_data["condition"]:
        return False

    getriebe = filters.get("getriebe")
    if getriebe and getriebe.lower() not in ("alle", "all"):
        if getriebe == "automatik" and not v_data["has_auto"]:
            return False
        if getriebe == "schaltung" and v_data["has_auto"]:
            return False

    # Range Filters
    try:
        ps_min = filters.get("psMin")
        if ps_min and v_data["ps"] < int(ps_min): return False
        ps_max = filters.get("psMax")
        if ps_max and v_data["ps"] > int(ps_max): return False

        pr_min = filters.get("preisMin")
        if pr_min and v_data["preis"] < int(pr_min): return False
        pr_max = filters.get("preisMax")
        if pr_max and v_data["preis"] > int(pr_max): return False

        yr_min = filters.get("jahrMin")
        if yr_min and v_data["modelljahr"] < int(yr_min): return False
        yr_max = filters.get("jahrMax")
        if yr_max and v_data["modelljahr"] > int(yr_max): return False

        l_min = filters.get("laengeMin")
        if l_min and v_data["laenge"] < float(l_min) * 100: return False
        l_max = filters.get("laengeMax")
        if l_max and v_data["laenge"] > float(l_max) * 100: return False

        slp_min = filters.get("schlafplaetzeMin")
        if slp_min and v_data["schlafplaetze"] < int(slp_min): return False
    except (ValueError, TypeError):
        # Bei ungültigen Filterwerten (Strings statt Zahlen) ignorieren wir den Filter
        pass

    if filters.get("hubbett") is True and not v_data["has_hubbett"]: return False
    if filters.get("dusche") is True and not v_data["has_dusche"]: return False

    return True

def map_and_filter(raw_data: Any, filters: dict) -> List[dict]:
    """Map raw vehicle data to UI format and apply filters."""
    vehicles = []
    for v in iter_items(raw_data):
        if not v or not isinstance(v, dict):
            continue
            
        v_id = v.get("id") or v.get("uid") or v.get("internal")
        if not v_id:
            continue

        f = _extract_vehicle_features(v)
        if not _apply_filters(f, v, filters):
            continue

        media = v.get("media", []) or []
        images = [m.get("url") for m in media if isinstance(m, dict) and m.get("group") == "image" and m.get("url")]

        vehicles.append({
            "id": v_id,
            "hersteller": f["producer"],
            "modell": f["model_name"],
            "preis": f["preis"],
            "ek_preis": f["ek_preis"],
            "preis_format": fmt_preis(f["preis"]),
            "ps": f["ps"],
            "laenge_m": f"{f['laenge']/100:.2f}",
            "laenge_cm": f["laenge"],
            "modelljahr": f["modelljahr"],
            "getriebe": "Automatik" if f["has_auto"] else "Schaltung",
            "zustand": f["condition"],
            "typ": f["art_label"],
            "schlafplaetze": f["schlafplaetze"],
            "has_hubbett": f["has_hubbett"],
            "has_dusche": f["has_dusche"],
            "thumb": images[0] if images else None,
            "media_ids": [m.get("id") for m in media if isinstance(m, dict) and m.get("group") == "image"],
        })
    return vehicles
