import os
import json
import datetime as _dt
import re as _re
import time
from collections import Counter
from core.config import SYSCARA_BASE
from core.database import get_cached_or_fetch, iter_items, _MEM_CACHE
from core.utils import normalize_collection_items, extract_order_datetime, _extract_order_nr, fmt_preis, _load_employee_names
from shared.vehicle_stats import build_vehicle_stats

def _get_orders() -> list:
    raw = get_cached_or_fetch('sale/orders', f"{SYSCARA_BASE}/sale/orders/?update=2024-01-01")
    if isinstance(raw, dict) and isinstance(raw.get('orders'), list):
        return raw['orders']
    items = list(iter_items(raw))
    if len(items) == 1 and isinstance(items[0], list):
        return items[0]
    return [o for o in items if isinstance(o, dict)]

def map_and_filter(raw, filters, with_photos=False):
    vehicles = []
    for v in iter_items(raw):
        if not v or not isinstance(v, dict) or not v.get('id'): continue
        def _d(key): r = v.get(key); return r if isinstance(r, dict) else {}
        model = _d('model'); engine = _d('engine'); dimensions = _d('dimensions')
        prices = _d('prices'); weights = _d('weights'); beds_d = _d('beds'); climate = _d('climate')
        
        art_raw = str(v.get('typeof', '')).lower()
        art_label = 'wohnwagen' if v.get('type') == 'Caravan' else art_raw
        ps = engine.get('ps', 0) or engine.get('power', 0) or 0
        laenge = dimensions.get('length', 0) or 0
        preis = prices.get('offer') or prices.get('list') or prices.get('basic') or 0
        ek_preis = prices.get('purchase') or 0
        modelljahr = model.get('modelyear', 0) or 0
        gewicht_kg = weights.get('allowed', 0) or weights.get('total', 0) or 0
        schlafplaetze = beds_d.get('sleeping', 0) or 0
        features = v.get('features', [])
        if not isinstance(features, list): features = []
        
        has_dusche = 'sep_dusche' in features or 'dusche' in features
        gear_raw = str(engine.get('gear', '') or engine.get('gearbox', '')).upper()
        has_auto = any(x in gear_raw for x in ["AUTOMATIC", "AUT", "AUTOMATIK"])
        condition = str(v.get('condition', '')).upper()

        if filters:
            if filters.get('art') and filters.get('art').lower() not in art_label: continue
            if filters.get('zustand') and filters.get('zustand').upper() != condition: continue
            if filters.get('psMin') and ps < int(filters.get('psMin')): continue
            if filters.get('psMax') and ps > int(filters.get('psMax')): continue
            if filters.get('preisMin') and preis < int(filters.get('preisMin')): continue
            if filters.get('preisMax') and preis > int(filters.get('preisMax')): continue
            if filters.get('jahrMin') and modelljahr < int(filters.get('jahrMin')): continue
            if filters.get('jahrMax') and modelljahr > int(filters.get('jahrMax')): continue
            if filters.get('laengeMin') and laenge < float(filters.get('laengeMin')) * 100: continue
            if filters.get('laengeMax') and laenge > float(filters.get('laengeMax')) * 100: continue
            if filters.get('getriebe'):
                if filters.get('getriebe') == 'automatik' and not has_auto: continue
                if filters.get('getriebe') == 'schaltung' and has_auto: continue
            if filters.get('schlafplaetzeMin') and schlafplaetze < int(filters.get('schlafplaetzeMin')): continue

        vehicles.append({
            "id": v.get('id'), "hersteller": model.get('producer', '-'), "modell": model.get('model', '-'),
            "preis": preis, "ek_preis": ek_preis, "preis_format": fmt_preis(preis), "ps": ps, "laenge_m": f"{laenge/100:.2f}",
            "modelljahr": modelljahr, "getriebe": "Automatik" if has_auto else "Schaltung"
        })
    return vehicles

_BI_CONTEXT_CACHE = {'ts': 0, 'data': None}
_BI_CONTEXT_TTL = 300 

def _build_bi_context() -> str:
    global _BI_CONTEXT_CACHE
    if _BI_CONTEXT_CACHE['data'] and (time.time() - _BI_CONTEXT_CACHE['ts'] < _BI_CONTEXT_TTL):
        return _BI_CONTEXT_CACHE['data']

    lines = [f"=== SYSCARA OMNISCIENT DATA HUB ({_dt.date.today().strftime('%d.%m.%Y')}) ==="]
    
    try:
        items = _get_orders()
        year_counts = Counter()
        for o in items:
            dt = extract_order_datetime(o)
            if dt: year_counts[dt.year] += 1
        lines.append(f"\nAUFTRÄGE GESAMT: {len(items)}")
        lines.append(f"Verteilung: " + ", ".join([f"{yr}: {cnt}" for yr, cnt in sorted(year_counts.items(), reverse=True)]))
    except: pass

    try:
        raw_veh = _MEM_CACHE.get('sale/vehicles') or get_cached_or_fetch('sale/vehicles', f"{SYSCARA_BASE}/sale/vehicles/")
        if raw_veh:
            vs = build_vehicle_stats(raw_veh)
            lines.append(f"\nFAHRZEUGBESTAND:")
            lines.append(f"  Bestand Gesamt: {vs.get('unique_total', '?')} (Verkaufsbereit: {vs.get('verkaufbar', '?')})")
            lines.append(f"  Durchschnittlicher VK: {vs.get('avg_preis', 0):,.0f} €".replace(',', '.'))
            
            # Neu: Einkaufspreis Übersicht
            raw_items = iter_items(raw_veh)
            eks = [float(v.get('prices',{}).get('purchase') or 0) for v in raw_items if float(v.get('prices',{}).get('purchase') or 0) > 0]
            if eks:
                avg_ek = sum(eks) / len(eks)
                lines.append(f"  Durchschnittlicher EK: {avg_ek:,.0f} €".replace(',', '.'))
            
            lines.append(f"  Längen: " + ", ".join([f"{k}: {v}" for k, v in vs.get('laenge_buckets', {}).items() if v > 0]))
            lines.append(f"  PS-Stärken: " + ", ".join([f"{p}: {c}" for p, c in sorted(vs.get('ps_counts', {}).items(), key=lambda x: int(x[0].split()[0]))[:5]]))
    except: pass
        
    res = "\n".join(lines)
    _BI_CONTEXT_CACHE = {'ts': time.time(), 'data': res}
    return res

def _detect_customer_query(question: str):
    q = question.lower()
    if 'kunde' in q or 'stadt' in q or 'plz' in q:
        # Hier könnte man die Regex-Suche aus dem vorherigen Stand wieder einbauen wenn gewünscht
        pass
    return False, {}

def _execute_local_customer_query(params: dict) -> tuple:
    return "Nicht implementiert.", None

def _detect_order_lookup_query(question: str):
    m = _re.search(r'(?:auftrags?|order)\s*#?\s*(\b[a-z0-9\-\/]{4,20}\b)', question.lower())
    if m: return True, {'type': 'order_nr', 'value': m.group(1).upper()}
    return False, {}

def _execute_local_order_lookup(params: dict):
    return "Nicht implementiert.", None, None

def _detect_employee_query(question: str):
    return False, {}

def _execute_local_employee_query(params: dict) -> tuple:
    return "Nicht implementiert.", None, None
