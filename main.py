import os
import json
import time
import requests
import threading
import datetime
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

load_dotenv()

app = Flask(__name__)
CORS(app, origins=[
    "http://localhost:3000",
    "http://localhost:5000",
    "https://dashboard.sellfriends24.de",
    "http://dashboard.sellfriends24.de",
])

SYSCARA_BASE  = "https://api.syscara.com"
USER          = os.getenv("SYSCARA_API_USER")
PASS          = os.getenv("SYSCARA_API_PASS")

# Supabase Konfig
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = None

if SUPABASE_URL and SUPABASE_KEY:
    try:
        from supabase import create_client, Client
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("[INIT] Supabase-Client erfolgreich erstellt.")
    except ImportError:
        print("[ERROR] Die 'supabase' Bibliothek ist nicht installiert! Bitte 'pip install supabase' ausführen.")
    except Exception as e:
        print(f"[ERROR] Supabase Init Fehler: {e}")
else:
    print("[INIT] Supabase nicht konfiguriert (URL/KEY fehlt in ENV).")

# ─── Cache-Helfer ─────────────────────────────────────────────────────────────

def get_cached_or_fetch(endpoint_name, url):
    """Generischer Cache-Loader mit Supabase-Ausfallschutz."""
    print(f"API-Call: {url} (Key: {endpoint_name})")
    try:
        # Timeout erhöht auf 30s für große Listen (Ads)
        response = requests.get(url, auth=HTTPBasicAuth(USER, PASS), timeout=30)
        response.raise_for_status()
        if not response.text.strip():
            print(f"Leere Antwort von {url}")
            return {}
        data = response.json()
        
        # In Supabase abspeichern (Erfolgsfall)
        if supabase:
            try:
                supabase.table("api_cache").upsert({
                    "key": endpoint_name,
                    "data": data,
                    "updated_at": int(time.time())
                }).execute()
            except Exception as dbe:
                print(f"Supabase Schreib-Fehler [{endpoint_name}]: {dbe}")
                
        return data

    except Exception as e:
        print(f"Fehler bei Syscara API [{endpoint_name}] - {type(e).__name__}: {e}")
        print("Versuche Fallback auf Supabase-Cache...")
        if supabase:
            try:
                res = supabase.table("api_cache").select("data, updated_at").eq("key", endpoint_name).execute()
                if res.data and len(res.data) > 0:
                    last_up = res.data[0].get("updated_at", 0)
                    nice_time = datetime.datetime.fromtimestamp(last_up).strftime('%Y-%m-%d %H:%M:%S')
                    print(f"+++ ERFOLG: Daten aus Supabase geladen (Stand: {nice_time}) +++")
                    return res.data[0]["data"]
            except Exception as dbe:
                print(f"Supabase Lese-Fehler [{endpoint_name}]: {dbe}")
                
        print("Kein Cache verfügbar. Gebe leeres Objekt zurück.")
        return {}

# ─── Hilfsfunktionen ──────────────────────────────────────────────────────────

def iter_items(raw):
    """Iteriert zuverlässig über API-Response – egal ob dict oder list."""
    if isinstance(raw, dict):
        return raw.values()
    if isinstance(raw, list):
        return raw
    return []

def fmt_preis(preis):
    if not preis:
        return '-'
    return f"{preis:,.2f} €".replace(',', 'X').replace('.', ',').replace('X', '.')

# ─── Filter-Logik für Ads ─────────────────────────────────────────────────────

def map_and_filter(raw, filters, with_photos=False):
    """Wandelt Syscara-Rohdaten in gemappte Fahrzeug-Objekte um und filtert."""
    vehicles = []

    for v in iter_items(raw):
        if not v or not isinstance(v, dict) or not v.get('id'):
            continue

        def _d(key): r = v.get(key); return r if isinstance(r, dict) else {}

        model      = _d('model')
        engine     = _d('engine')
        dimensions = _d('dimensions')
        prices     = _d('prices')
        weights    = _d('weights')
        beds_d     = _d('beds')
        climate    = _d('climate')

        features = v.get('features', [])
        if not isinstance(features, list):
            features = []

        beds_list = beds_d.get('beds', [])
        if not isinstance(beds_list, list):
            beds_list = []
        bed_types = [str(b.get('type', '')).upper() for b in beds_list if isinstance(b, dict)]

        art_raw    = str(v.get('typeof', '')).lower()
        art_label  = 'wohnwagen' if v.get('type') == 'Caravan' else art_raw
        ps         = engine.get('ps', 0) or 0
        laenge     = dimensions.get('length', 0) or 0
        preis      = prices.get('offer') or prices.get('list') or prices.get('basic') or 0
        modelljahr = model.get('modelyear', 0) or 0
        gewicht_kg = weights.get('allowed', 0) or weights.get('total', 0) or 0
        schlafplaetze = beds_d.get('sleeping', 0) or 0

        has_dusche   = 'sep_dusche' in features or 'dusche' in features
        has_navi     = 'navigationssystem' in features
        has_tv       = 'tv' in features or 'sat' in features
        has_solar    = 'solar' in features
        has_markise  = 'markise' in features
        has_garage   = 'heckgarage' in features or 'garage' in features
        has_fahrrad  = 'fahrradtraeger' in features or 'fahrradtraeger_e' in features
        has_backofen = 'backofen' in features or 'mikrowelle' in features
        has_klima = bool(climate.get('aircondition', False))
        heating_type = str(climate.get('heating_type', '')).upper()
        has_festbett = 'FRENCH_BED' in bed_types or 'SINGLE_BEDS' in bed_types
        gear_raw = str(engine.get('gear', '') or engine.get('gearbox', '')).upper()
        has_auto = gear_raw == 'AUTOMATIC'
        condition = str(v.get('condition', '')).upper()

        fa = str(filters.get('art', 'alle')).lower()
        if fa != 'alle':
            if fa == 'wohnwagen'      and v.get('type') != 'Caravan':   continue
            if fa == 'integriert'     and art_label != 'integriert':     continue
            if fa == 'teilintegriert' and art_label != 'teilintegriert': continue
            if fa == 'kastenwagen'    and art_label != 'kastenwagen':    continue

        zustand_filter = str(filters.get('zustand', 'alle')).lower()
        if zustand_filter == 'neu'       and condition != 'NEW':  continue
        if zustand_filter == 'gebraucht' and condition != 'USED': continue

        try:
            if ps         < int(filters.get('psMin')     or 0):       continue
            if ps         > int(filters.get('psMax')     or 99999):   continue
            if laenge     < int(filters.get('laengeMin') or 0):       continue
            if laenge     > int(filters.get('laengeMax') or 99999):   continue
            if preis      < int(filters.get('preisMin')  or 0):       continue
            if preis      > int(filters.get('preisMax')  or 9999999): continue
            if modelljahr < int(filters.get('jahrMin')   or 0):       continue
            if modelljahr > int(filters.get('jahrMax')   or 9999):    continue
            if schlafplaetze < int(filters.get('schlafplaetzeMin') or 0): continue
            gf = str(filters.get('gewicht', 'alle')).lower()
            if gf == 'bis35'   and gewicht_kg > 3500:                 continue
            if gf == '35bis45' and (gewicht_kg <= 3500 or gewicht_kg > 4500): continue
            if gf == 'ueber45' and gewicht_kg <= 4500:                continue
        except (ValueError, TypeError):
            pass

        heizung_filter = str(filters.get('heizung', 'alle')).lower()
        if heizung_filter == 'gas'    and heating_type not in ('AIR_GAS',):                          continue
        if heizung_filter == 'diesel' and heating_type not in ('AIR_DIESEL', 'AIR_DIESEL_ELECTRIC'): continue

        getriebe_filter = str(filters.get('getriebe', 'alle')).lower()
        if getriebe_filter == 'automatik' and not has_auto: continue
        if getriebe_filter == 'schaltung' and has_auto:     continue

        def yn(key, value):
            f_val = str(filters.get(key, 'egal')).lower()
            if f_val == 'ja'   and not value: return True
            if f_val == 'nein' and value:     return True
            return False

        if yn('dusche',        has_dusche):   continue
        if yn('badezimmer',    has_dusche):   continue
        if yn('festbett',      has_festbett): continue
        if yn('klima',         has_klima):    continue
        if yn('navi',          has_navi):     continue
        if yn('tv',            has_tv):       continue
        if yn('solar',         has_solar):    continue
        if yn('markise',       has_markise):  continue
        if yn('garage',        has_garage):   continue
        if yn('fahrradtraeger',has_fahrrad):  continue
        if yn('backofen',      has_backofen): continue

        bett_filter = str(filters.get('betten', 'alle')).lower()
        if bett_filter != 'alle':
            if bett_filter == 'einzelbetten' and 'SINGLE_BEDS' not in bed_types: continue
            if bett_filter == 'doppelbett'   and 'FRENCH_BED'  not in bed_types: continue
            if bett_filter == 'hubbett'      and 'PULL_BED'    not in bed_types and 'ROOF_BED'  not in bed_types: continue
            if bett_filter == 'stockbett'    and 'BUNK_BEDS'   not in bed_types: continue
            if bett_filter == 'alkoven'      and 'ALCOVE_BED'  not in bed_types: continue

        obj = {
            "id":           v.get('id'),
            "hersteller":   model.get('producer', '-'),
            "modell":       model.get('model', '-'),
            "serie":        model.get('series', '-'),
            "preis":        preis,
            "preis_format": fmt_preis(preis),
            "art":          'Wohnwagen' if v.get('type') == 'Caravan' else v.get('typeof', '-'),
            "ps":           ps,
            "kw":           engine.get('kw', 0) or 0,
            "laenge_m":     f"{(laenge / 100):.2f}" if laenge else '-',
            "laenge_cm":    laenge,
            "modelljahr":   modelljahr,
            "zustand":      'Neu' if condition == 'NEW' else ('Gebraucht' if condition == 'USED' else condition),
            "gewicht_kg":   gewicht_kg,
            "schlafplaetze":schlafplaetze,
            "dusche":       has_dusche,
            "festbett":     has_festbett,
            "dinette":      False,
            "klima":        has_klima,
            "navi":         has_navi,
            "tv":           has_tv,
            "solar":        has_solar,
            "markise":      has_markise,
            "heizung_typ":  heating_type,
            "getriebe":     'Automatik' if has_auto else ('Schaltung' if gear_raw == 'MANUAL' else '-'),
            "thumb":        None,
            "media_ids":    [],
        }

        if with_photos:
            media  = v.get('media', []) or []
            images = [m.get('url') for m in media if isinstance(m, dict) and m.get('group') == 'image' and m.get('url')]
            obj["thumb"]     = images[0] if images else None
            obj["media_ids"] = [m.get('id') for m in media if isinstance(m, dict) and m.get('group') == 'image']

        vehicles.append(obj)

    vehicles.sort(key=lambda x: x['preis'] or 0)
    return vehicles

# ─── Routen ───────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return send_file('fahrzeugsuche_local.html')

@app.route('/api/ads', methods=['POST'])
def api_ads():
    try:
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict): body = {}
        with_photos = bool(body.pop('withPhotos', False))
        raw      = get_cached_or_fetch('sale/ads', f"{SYSCARA_BASE}/sale/ads/")
        vehicles = map_and_filter(raw, body, with_photos=with_photos)
        return jsonify({"success": True, "count": len(vehicles), "vehicles": vehicles})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/vehicles', methods=['GET', 'POST'])
def api_vehicles():
    try:
        raw   = get_cached_or_fetch('sale/vehicles', f"{SYSCARA_BASE}/sale/vehicles/")
        items = []
        for v in iter_items(raw):
            if not v or not isinstance(v, dict): continue
            model  = v.get('model', {}) or {}
            prices = v.get('prices', {}) or {}
            preis  = prices.get('offer') or prices.get('list') or prices.get('basic') or 0
            items.append({
                "id":          v.get('id'),
                "hersteller":  model.get('producer', '-'),
                "modell":      model.get('model', '-'),
                "serie":       model.get('series', '-'),
                "typ":         v.get('typeof', '-'),
                "modelljahr":  model.get('modelyear', '-'),
                "zustand":     'Neu' if v.get('condition') == 'NEW' else ('Gebraucht' if v.get('condition') == 'USED' else '-'),
                "preis":       preis,
                "preis_format":fmt_preis(preis),
                "status":      v.get('status', '-'),
            })
        items.sort(key=lambda x: x['preis'] or 0)
        return jsonify({"success": True, "count": len(items), "vehicles": items})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/orders', methods=['GET', 'POST'])
def api_orders():
    try:
        raw   = get_cached_or_fetch('sale/orders', f"{SYSCARA_BASE}/sale/orders/")
        items = []
        for v in iter_items(raw):
            if not v or not isinstance(v, dict): continue
            items.append({
                "id":      v.get('id'),
                "nr":      v.get('number') or v.get('order_number', '-'),
                "fahrzeug":v.get('vehicle_id') or v.get('vehicle', '-'),
                "datum":   v.get('date') or v.get('created_at', '-'),
                "status":  v.get('status', '-'),
                "preis":   v.get('price') or v.get('total', 0),
                "kunde":   v.get('customer') or v.get('buyer', '-'),
            })
        return jsonify({"success": True, "count": len(items), "orders": items,
                        "info": "Keine Aufträge gefunden" if not items else None})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/equipment', methods=['GET', 'POST'])
def api_equipment():
    try:
        raw   = get_cached_or_fetch('sale/equipment', f"{SYSCARA_BASE}/sale/equipment/")
        items = []
        for v in iter_items(raw):
            if not v or not isinstance(v, dict): continue
            items.append({
                "id":        v.get('id'),
                "name":      v.get('name') or v.get('title', '-'),
                "preis_vk":  v.get('price') or v.get('selling_price') or v.get('retail_price', 0),
                "preis_ek":  v.get('purchase_price') or v.get('buying_price', 0),
                "gewicht":   v.get('weight', '-'),
                "zustaendig":v.get('responsible') or v.get('person', '-'),
                "kategorie": v.get('category') or v.get('group', '-'),
            })
        return jsonify({"success": True, "count": len(items), "equipment": items})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/stats', methods=['GET'])
def api_stats():
    try:
        raw = get_cached_or_fetch('sale/ads', f"{SYSCARA_BASE}/sale/ads/")
        stats = {
            "nach_typ":     {},
            "preis_buckets":{},
            "laenge_buckets":{},
            "heizung":      {"Diesel": 0, "Gas": 0, "Unbekannt": 0},
            "getriebe":     {"Automatik": 0, "Schaltung": 0, "Unbekannt": 0},
            "hubbett":      {"Ja": 0, "Nein": 0},
            "dinette":      {"Ja": 0, "Nein": 0},
            "dusche":       {"Ja": 0, "Nein": 0},
            "gesamt":       0,
            "avg_preis":    0,
        }
        preise = []
        for v in iter_items(raw):
            if not v or not isinstance(v, dict) or not v.get('id'): continue
            typ = v.get('typeof', '') or ('Wohnwagen' if v.get('type') == 'Caravan' else 'Sonstige')
            if not typ: typ = 'Sonstige'
            stats["nach_typ"][typ] = stats["nach_typ"].get(typ, 0) + 1
            prices = v.get('prices', {}) or {}
            preis  = prices.get('offer') or prices.get('list') or prices.get('basic') or 0
            if preis:
                preise.append(preis)
                if   preis < 30000:  bucket = "< 30T"
                elif preis < 50000:  bucket = "30–50T"
                elif preis < 70000:  bucket = "50–70T"
                elif preis < 100000: bucket = "70–100T"
                else:                bucket = "> 100T"
                stats["preis_buckets"][bucket] = stats["preis_buckets"].get(bucket, 0) + 1
            dims   = v.get('dimensions', {}) or {}
            laenge = dims.get('length', 0) or 0
            if laenge:
                if   laenge < 600:   lbucket = "< 6m"
                elif laenge < 700:   lbucket = "6–7m"
                elif laenge < 750:   lbucket = "7–7,5m"
                elif laenge < 800:   lbucket = "7,5–8m"
                else:                lbucket = "> 8m"
                stats["laenge_buckets"][lbucket] = stats["laenge_buckets"].get(lbucket, 0) + 1
            features = v.get('features', [])
            if not isinstance(features, list): features = []
            climate = v.get('climate', {}) or {}
            engine  = v.get('engine',  {}) or {}
            beds_d  = v.get('beds',    {}) or {}
            beds_list = beds_d.get('beds', []) if isinstance(beds_d.get('beds'), list) else []
            bed_types = [str(b.get('type', '')).upper() for b in beds_list if isinstance(b, dict)]
            heating_type = str(climate.get('heating_type', '')).upper()
            if 'DIESEL' in heating_type: stats["heizung"]["Diesel"] += 1
            elif 'GAS' in heating_type: stats["heizung"]["Gas"] += 1
            else: stats["heizung"]["Unbekannt"] += 1
            gear = str(engine.get('gear', '') or engine.get('gearbox', '')).upper()
            if gear == 'AUTOMATIC': stats["getriebe"]["Automatik"] += 1
            elif gear == 'MANUAL': stats["getriebe"]["Schaltung"] += 1
            else: stats["getriebe"]["Unbekannt"] += 1
            has_hub = 'PULL_BED' in bed_types or 'ROOF_BED' in bed_types
            stats["hubbett"]["Ja" if has_hub else "Nein"] += 1
            stats["dinette"]["Nein"] += 1
            has_du = 'sep_dusche' in features or 'dusche' in features
            stats["dusche"]["Ja" if has_du else "Nein"] += 1
        stats["gesamt"]    = len(preise)
        stats["avg_preis"] = int(sum(preise) / len(preise)) if preise else 0
        return jsonify({"success": True, "stats": stats})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/fahrzeugsuche', methods=['POST'])
def handle_search():
    try:
        filters  = request.get_json(silent=True) or {}
        if not isinstance(filters, dict): filters = {}
        raw      = get_cached_or_fetch('sale/ads', f"{SYSCARA_BASE}/sale/ads/")
        vehicles = map_and_filter(raw, filters, with_photos=True)
        return jsonify({"success": True, "count": len(vehicles), "filters": filters, "vehicles": vehicles})
    except Exception as e:
        return jsonify({"success": False, "error": "Interner Fehler", "details": str(e)}), 500

# ─── Proaktiver Background Sync ───────────────────────────────────────────────

def sync_all_now():
    """Holt alle wichtigen Listen von Syscara und spiegelt sie nach Supabase."""
    print("--- [BACKGROUND SYNC] Starte Abgleich mit Supabase ---")
    
    # Cleanup alter Kurz-Keys beim ersten Lauf
    if supabase:
        try:
            old_keys = ["ads", "vehicles", "orders", "equipment", "test_equipment"]
            supabase.table("api_cache").delete().in_("key", old_keys).execute()
            print("[SYNC] Alte Kurz-Keys bereinigt.")
        except:
            pass

    endpoints = {
        "sale/ads":       f"{SYSCARA_BASE}/sale/ads/",
        "sale/vehicles":  f"{SYSCARA_BASE}/sale/vehicles/",
        "sale/orders":    f"{SYSCARA_BASE}/sale/orders/",
        "sale/equipment": f"{SYSCARA_BASE}/sale/equipment/",
        "sale/lists":     f"{SYSCARA_BASE}/sale/lists/?list=pictures"
    }
    for name, url in endpoints.items():
        try:
            print(f"[SYNC] Lade {name}...")
            get_cached_or_fetch(name, url)
        except Exception as e:
            print(f"[SYNC ERROR] Fehler bei {name}: {e}")
    print("--- [BACKGROUND SYNC] Abgeschlossen ---")

def background_sync_loop():
    """Endlosschleife für den stündlichen Abgleich."""
    time.sleep(10)
    while True:
        sync_all_now()
        time.sleep(3600)

def start_sync_thread():
    """Startet den Sync-Prozess in einem eigenen Thread."""
    if not supabase:
        print("[SYNC] Übersprungen - Supabase nicht konfiguriert.")
        return
    thread = threading.Thread(target=background_sync_loop, daemon=True)
    thread.start()
    print("[SYNC] Hintergrund-Thread gestartet (Intervall: 1 Std).")

if __name__ == '__main__':
    port = int(os.getenv("PORT", 5000))
    print(f"Syscara Python API läuft auf Port {port}...")
    start_sync_thread()
    app.run(host='0.0.0.0', port=port, debug=False)
