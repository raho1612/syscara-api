import os
import json
import datetime as _dt
import re as _re
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
        features = v.get('features', [])
        if not isinstance(features, list): features = []
        beds_list = beds_d.get('beds', [])
        if not isinstance(beds_list, list): beds_list = []
        bed_types = [str(b.get('type', '')).upper() for b in beds_list if isinstance(b, dict)]
        art_raw = str(v.get('typeof', '')).lower()
        art_label = 'wohnwagen' if v.get('type') == 'Caravan' else art_raw
        ps = engine.get('ps', 0) or 0
        laenge = dimensions.get('length', 0) or 0
        preis = prices.get('offer') or prices.get('list') or prices.get('basic') or 0
        modelljahr = model.get('modelyear', 0) or 0
        gewicht_kg = weights.get('allowed', 0) or weights.get('total', 0) or 0
        schlafplaetze = beds_d.get('sleeping', 0) or 0
        has_dusche = 'sep_dusche' in features or 'dusche' in features
        has_klima = bool(climate.get('aircondition', False))
        heating_type = str(climate.get('heating_type', '')).upper()
        has_festbett = 'FRENCH_BED' in bed_types or 'SINGLE_BEDS' in bed_types
        gear_raw = str(engine.get('gear', '') or engine.get('gearbox', '')).upper()
        has_auto = gear_raw == 'AUTOMATIC'
        condition = str(v.get('condition', '')).upper()

        if filters:
            if filters.get('art') and filters.get('art') != 'alle' and filters.get('art').lower() != art_label: continue
            if filters.get('zustand') and filters.get('zustand') != 'alle' and filters.get('zustand').upper() != condition: continue
            if filters.get('psMin') and ps < int(filters.get('psMin')): continue
            if filters.get('psMax') and ps > int(filters.get('psMax')): continue
            if filters.get('preisMin') and preis < int(filters.get('preisMin')): continue
            if filters.get('preisMax') and preis > int(filters.get('preisMax')): continue
            if filters.get('jahrMin') and modelljahr < int(filters.get('jahrMin')): continue
            if filters.get('jahrMax') and modelljahr > int(filters.get('jahrMax')): continue
            
            gw = filters.get('gewicht')
            if gw and gw != 'alle':
                tonnen = gewicht_kg / 1000.0
                if gw == 'bis35' and tonnen > 3.5: continue
                if gw == '35bis45' and (tonnen <= 3.5 or tonnen > 4.5): continue
                if gw == 'ueber45' and tonnen <= 4.5: continue

            l_min = filters.get('laengeMin')
            if l_min and laenge < float(l_min) * 100: continue
            l_max = filters.get('laengeMax')
            if l_max and laenge > float(l_max) * 100: continue
            
            if filters.get('schlafplaetzeMin') and schlafplaetze < int(filters.get('schlafplaetzeMin')): continue
            
            if filters.get('festbett') == 'ja' and not has_festbett: continue
            if filters.get('festbett') == 'nein' and has_festbett: continue
            if filters.get('dusche') == 'ja' and not has_dusche: continue
            if filters.get('dusche') == 'nein' and has_dusche: continue
            if filters.get('klima') == 'ja' and not has_klima: continue
            if filters.get('klima') == 'nein' and has_klima: continue

            ht = filters.get('heizung')
            if ht and ht != 'alle':
                if ht == 'gas' and 'GAS' not in heating_type: continue
                if ht == 'diesel' and 'DIESEL' not in heating_type: continue

            gt = filters.get('getriebe')
            if gt and gt != 'alle':
                if gt == 'automatik' and not has_auto: continue
                if gt == 'schaltung' and has_auto: continue

        obj = {
            "id": v.get('id'),
            "hersteller": model.get('producer', '-'),
            "modell": model.get('model', '-'),
            "serie": model.get('series', '-'),
            "preis": preis,
            "preis_format": fmt_preis(preis),
            "zustand": condition,
            "art": art_label,
            "ps": ps,
            "kw": engine.get('kw' , 0) or 0,
            "laenge_m": f"{laenge/100:.2f}" if laenge else "-",
            "laenge_cm": laenge,
            "modelljahr": modelljahr,
            "gewicht_kg": gewicht_kg,
            "schlafplaetze": schlafplaetze,
            "dusche": has_dusche,
            "festbett": has_festbett,
            "dinette": 'dinette' in features,
            "klima": has_klima,
            "getriebe": gear_raw,
            "vin": v.get('identifier', {}).get('vin', '-'),
            "thumb": None
        }
        vehicles.append(obj)
    return vehicles

def _build_bi_context() -> str:
    lines = [f"=== UNTERNEHMENSDATEN (Stand: {_dt.date.today().strftime('%d.%m.%Y')}) ==="]
    try:
        items = _get_orders()
    except: items = []

    status_counts = Counter()
    year_counts = Counter()
    month_2026 = Counter()
    employee_counts = Counter()

    for o in items:
        s = o.get('status', {})
        status = (s.get('key') or s.get('label')) if isinstance(s, dict) else str(s or '')
        if status: status_counts[status] += 1
        dt = extract_order_datetime(o)
        if dt:
            year_counts[dt.year] += 1
            if dt.year == 2026: month_2026[str(dt.month).zfill(2)] += 1
        user = o.get('user', {})
        if isinstance(user, dict):
            emp_id = user.get('order') or user.get('update')
            if emp_id: employee_counts[str(emp_id)] += 1

    lines.append(f"Aufträge gesamt: {len(items)}")
    for yr in sorted([y for y in year_counts if y >= 2023], reverse=True):
        lines.append(f"Aufträge {yr}: {year_counts[yr]}")
    
    if month_2026:
        lines.append("\nAufträge 2026 nach Monat:")
        for mon in sorted(month_2026.keys()):
            lines.append(f"  {mon}/2026: {month_2026[mon]}")
            
    if employee_counts:
        emp_names_map = _load_employee_names()
        lines.append("\nTop 5 Mitarbeiter (Auftragsanzahl):")
        for emp_id, cnt in employee_counts.most_common(5):
            label = emp_names_map.get(str(emp_id), f'ID {emp_id}')
            lines.append(f"  {label}: {cnt} Aufträge")

    # Fahrzeugbestand Details für "Gesamtintelligenz"
    try:
        raw_veh = _MEM_CACHE.get('sale/vehicles') or get_cached_or_fetch('sale/vehicles', f"{SYSCARA_BASE}/sale/vehicles/")
        if raw_veh:
            vs = build_vehicle_stats(raw_veh)
            lines.append(f"\nFAHRZEUGBESTAND:")
            lines.append(f"  Gesamtbestand: {vs.get('unique_total', '?')} Fahrzeuge")
            lines.append(f"  Verfügbar: {vs.get('verkaufbar', '?')} | Verkauft: {vs.get('verkauft', '?')}")
            
            # Typen
            types = vs.get('nach_typ', {})
            if types:
                lines.append("  Nach Typ: " + ", ".join([f"{k}: {v}" for k, v in types.items()]))
            
            # Preisklassen
            pb = vs.get('preis_buckets', {})
            pb_list = [f"{k}: {v}" for k, v in pb.items() if v > 0]
            if pb_list: lines.append("  Preisklassen: " + ", ".join(pb_list))
            
            # Längenklassen (Wichtig für die Benutzerfrage!)
            lb = vs.get('laenge_buckets', {})
            lb_list = [f"{k}: {v}" for k, v in lb.items() if v > 0]
            if lb_list: lines.append("  Längenklassen: " + ", ".join(lb_list))
            
            # Top Marken
            raw_items = iter_items(raw_veh)
            makes = Counter([str(v.get('model', {}).get('producer', 'Unbekannt')) for v in raw_items]).most_common(8)
            lines.append("  Top Marken: " + ", ".join([f"{m}: {c}" for m, c in makes]))
            
    except Exception as e:
        lines.append(f"\n(Statistik-Fehler: {str(e)})")
        
    return "\n".join(lines)

def _detect_customer_query(question: str):
    q = question.lower()
    city_patterns = [r'kunden?\s+(?:in|aus|von)\s+([a-zäöüß][a-zäöüß\s\-]{2,30})', r'(?:wohnt|wohnen|wohnhaft)\s+in\s+([a-zäöüß][a-zäöüß\s\-]{2,30})', r'(?:aus|von)\s+([a-zäöüß][a-zäöüß\s\-]{2,30})\s+(?:haben|mit|kaufen|bestell|auftrag)', r'(?:stadt|ort)[:\s]+([a-zäöüß][a-zäöüß\s\-]{2,30})']
    for pat in city_patterns:
        m = _re.search(pat, q)
        if m:
            city = m.group(1).strip().rstrip('?.,! ').strip()
            if len(city) >= 3: return True, {'type': 'city', 'value': city}
    zip_match = _re.search(r'\b(\d{5})\b', question)
    if zip_match and any(kw in q for kw in ['plz', 'postleitzahl', 'kunden', 'bestell', 'auftrag']):
        return True, {'type': 'zip', 'value': zip_match.group(1)}
    name_patterns = [r'(?:kunde|kundin|herr|frau)\s+([a-zäöüß]{2,30}(?:\s+[a-zäöüß]{2,30})?)', r'(?:name(?:ns)?|nachname|vorname)\s*[:\s]+([a-zäöüß]{2,30})']
    for pat in name_patterns:
        m = _re.search(pat, q)
        if m: return True, {'type': 'name', 'value': m.group(1).strip()}
    return False, {}

def _execute_local_customer_query(params: dict) -> tuple:
    try: items = _get_orders()
    except: return "Fehler: Auftragsdaten konnten nicht geladen werden.", None
    results = []
    q_t = params.get('type'); val = params.get('value', '').lower().strip()
    for o in items:
        c = o.get('customer', {}) or {}
        if not isinstance(c, dict): continue
        match = False
        if q_t == 'city':
            city = (c.get('city') or '').lower()
            match = val in city or city.startswith(val[:min(len(val), 6)])
        elif q_t == 'zip': match = str(c.get('zipcode') or '').strip() == val
        elif q_t == 'name':
            fn = (c.get('first_name') or '').lower(); ln = (c.get('last_name') or '').lower()
            match = val in fn or val in ln or val in f"{fn} {ln}"
        if match:
            nr = _extract_order_nr(o)
            s = o.get('status', {})
            status = (s.get('key') or s.get('label') or '?') if isinstance(s, dict) else str(s or '?')
            results.append({'nr': nr, 'name': f"{c.get('first_name', '')} {c.get('last_name', '')}".strip(), 'stadt': c.get('city', ''), 'plz': str(c.get('zipcode', '')), 'status': status})
    if not results: return f"Keine Aufträge mit Suchkriterium '{params.get('value')}' gefunden.", None
    total = len(results); capped = results[:50]
    answer = f"{total} Aufträge gefunden (lokal ermittelt, keine Daten an KI gesendet)"
    if total > 50: answer += f" — Tabelle zeigt die ersten 50 von {total}"
    table = {'columns': ['Auftrags-Nr.', 'Name', 'PLZ', 'Stadt', 'Status'], 'rows': [[r['nr'], r['name'], r['plz'], r['stadt'], r['status']] for r in capped]}
    return answer, table

def _detect_order_lookup_query(question: str):
    q = question.lower()
    nr_match = _re.search(r'(?:auftrags?|bestell|order)\s*(?:nummer|nr|#)?\s*[:\s]?\s*(\b[a-z0-9\-\/]{4,20}\b)', q)
    if nr_match:
        nr = nr_match.group(1).strip().upper().rstrip('?.! ')
        if len(nr) >= 3: return True, {'type': 'order_nr', 'value': nr}
    return False, {}

def _execute_local_order_lookup(params: dict):
    order_nr = params.get('value', '').upper()
    try: orders = _get_orders()
    except: return "Fehler: Daten nicht ladbar.", None, None
    found = None
    for o in orders:
        if _extract_order_nr(o).upper() == order_nr: found = o; break
    if not found: return f"Auftrag {order_nr} nicht gefunden.", None, None
    c = found.get('customer', {}) or {}
    cname = f"{c.get('first_name','')} {c.get('last_name','')}".strip()
    city = c.get('city', '-'); s_obj = found.get('status', {})
    status = (s_obj.get('label') or s_obj.get('key') or 'Unbekannt') if isinstance(s_obj, dict) else str(s_obj or 'Unbekannt')
    user = found.get('user', {}) or {}; uid = str(user.get('order') or user.get('update') or '')
    emp_names = _load_employee_names()
    seller_name = emp_names.get(uid, f'ID {uid}' if uid else 'Unbekannt')
    answer = f"Details zu Auftrag {order_nr}: {cname} aus {city}. Verkäufer: {seller_name}. Status: {status}."
    table = {'columns': ['Feld', 'Wert'], 'rows': [['Auftrags-Nr.', order_nr], ['Verkäufer', seller_name], ['Status', status], ['Kunde', cname], ['Stadt', city]]}
    return answer, table, None

def _detect_employee_query(question: str):
    q = question.lower(); id_match = _re.search(r'(?:mitarbeiter|user|verkäufer|berater)\s*[:\s#]?\s*(\d{3,6})', q)
    if id_match:
        eid = id_match.group(1); emp_names = _load_employee_names()
        return True, {'type': 'employee_id', 'value': eid, 'name': emp_names.get(eid, f'#{eid}')}
    emp_names = _load_employee_names()
    if emp_names:
        patterns = [r'auftr[äa]ge?\s+(?:von|durch|von\s+mitarbeiter)\s+([a-zäöüß]+(?:\s+[a-zäöüß]+)?)', r'(?:von|durch)\s+([a-zäöüß]+(?:\s+[a-zäöüß]+)?)\s+(?:auftr[äa]ge?|bearbeitet|erstellt)']
        for pat in patterns:
            m = _re.search(pat, q)
            if m:
                name_q = m.group(1).strip()
                for eid, ename in emp_names.items():
                    if name_q in ename.lower(): return True, {'type': 'employee_id', 'value': eid, 'name': ename}
    return False, {}

def _execute_local_employee_query(params: dict) -> tuple:
    emp_id_str = str(params.get('value', '')); emp_name = params.get('name', f'#{emp_id_str}')
    try: orders = _get_orders()
    except: return "Fehler: Daten nicht geladen.", None, None
    results = []; status_counts = {}
    for o in orders:
        user = o.get('user', {}) or {}
        if str(user.get('order') or user.get('update') or '') != emp_id_str: continue
        s = o.get('status', {}); status = (s.get('key') or s.get('label') or '?') if isinstance(s, dict) else str(s or '?')
        status_counts[status] = status_counts.get(status, 0) + 1
        results.append({'nr': _extract_order_nr(o), 'status': status, 'city': (o.get('customer',{}) or {}).get('city','')})
    if not results: return f"Keine Aufträge für {emp_name} gefunden.", None, None
    answer = f"Der Mitarbeiter {emp_name} hat {len(results)} Aufträge."
    table = {'columns': ['Auftrags-Nr.', 'Status', 'Stadt'], 'rows': [[r['nr'], r['status'], r['city']] for r in results[:50]]}
    return answer, table, None
