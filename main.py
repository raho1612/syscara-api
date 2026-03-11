import os
import json
import time
import requests
import threading
import datetime
import math
from datetime import datetime
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
        from supabase import create_client, Client, ClientOptions
        import httpx
        options = ClientOptions(
            postgrest_client_timeout=120,
            storage_client_timeout=120,
        )
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY, options=options)
        print("[INIT] Supabase-Client erfolgreich erstellt.", flush=True)
    except ImportError:
        print("[ERROR] Die 'supabase' Bibliothek ist nicht installiert!", flush=True)
    except Exception as e:
        print(f"[ERROR] Supabase Init Fehler: {e}", flush=True)
else:
    print("[INIT] Supabase nicht konfiguriert (URL/KEY fehlt).", flush=True)

# ÔöÇÔöÇÔöÇ Hilfsfunktionen ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

def iter_items(raw):
    if isinstance(raw, dict): return list(raw.values())
    if isinstance(raw, list): return raw
    return []


def normalize_collection_items(raw, primary_key=None):
    if isinstance(raw, dict) and primary_key:
        primary = raw.get(primary_key)
        if isinstance(primary, list):
            return primary

    items = list(iter_items(raw))
    if len(items) == 1 and isinstance(items[0], list):
        return items[0]
    return items


def extract_order_datetime(order_item):
    candidates = []

    date_obj = order_item.get('date')
    if isinstance(date_obj, str):
        candidates.append(date_obj)
    elif isinstance(date_obj, dict):
        for key in ('order', 'create', 'created', 'create_date', 'created_at', 'createAt', 'update', 'updated_at'):
            value = date_obj.get(key)
            if isinstance(value, str) and value:
                candidates.append(value)
        for value in date_obj.values():
            if isinstance(value, str) and value:
                candidates.append(value)

    for key in ('created_at', 'created', 'create', 'date', 'createdAt', 'order_date'):
        value = order_item.get(key)
        if isinstance(value, str) and value:
            candidates.append(value)

    for value in candidates:
        try {
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        } catch {
            try {
                return datetime.strptime(value.split('T')[0], '%Y-%m-%d')
            } catch {
                continue
            }
        }
    }

    return None
def fmt_preis(preis):
    if not preis: return '-'
    return f"{preis:,.2f} Ôé¼".replace(',', 'X').replace('.', ',').replace('X', '.')

# ÔöÇÔöÇÔöÇ Chunking Logik f├╝r Supabase ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

CHUNK_SIZE = 500 # Maximale Anzahl an Elementen pro Chunk

def save_to_supabase_chunked(endpoint_name, data):
    """Speichert gro├ƒe Listen in kleineren Chunks, um Timeouts zu vermeiden."""
    if not supabase: return False
    
    items = iter_items(data)
    total_items = len(items)
    is_dict = isinstance(data, dict)
    keys_list = list(data.keys()) if is_dict else []
    
    if total_items == 0:
        return True

    # Kein DELETE mehr ÔÇô upsert ├╝berschreibt vorhandene Eintr├ñge direkt.
    num_chunks = math.ceil(total_items / CHUNK_SIZE)
    timestamp = int(time.time())
    
    for i in range(num_chunks):
        start_idx = i * CHUNK_SIZE
        end_idx = min((i + 1) * CHUNK_SIZE, total_items)
        
        chunk_key = f"{endpoint_name}#chunk{i}"
        
        # Rekonstruiere die Datenstruktur f├╝r den Chunk
        if is_dict:
            chunk_data = {k: data[k] for k in keys_list[start_idx:end_idx]}
        else:
            chunk_data = items[start_idx:end_idx]
            
        try:
            supabase.table("api_cache").upsert({
                "key": chunk_key,
                "data": chunk_data,
                "updated_at": timestamp
            }).execute()
            print(f"  [CHUNK] {chunk_key} gespeichert ({end_idx-start_idx} Items).", flush=True)
        except Exception as dbe:
            print(f"  [ERROR] Chunk {chunk_key} fehlgeschlagen: {dbe}", flush=True)
            return False
            
    # Speichere Meta-Info, wie viele Chunks es gibt
    try:
        supabase.table("api_cache").upsert({
            "key": f"{endpoint_name}#meta",
            "data": {"chunks": num_chunks, "is_dict": is_dict},
            "updated_at": timestamp
        }).execute()
    except: pass
    
    return True

def load_from_supabase_chunked(endpoint_name):
    """L├ñdt und kombiniert Chunks aus Supabase."""
    if not supabase: return {}
    
    try:
        # Lade zuerst die Metadaten
        meta_res = supabase.table("api_cache").select("data").eq("key", f"{endpoint_name}#meta").execute()
        if not meta_res.data:
            # Fallback: Versuche es auf die alte Art (ohne Chunks)
            res = supabase.table("api_cache").select("data, updated_at").eq("key", endpoint_name).execute()
            if res.data:
                return res.data[0]["data"]
            return {}
            
        meta = meta_res.data[0]["data"]
        num_chunks = meta.get("chunks", 0)
        is_dict = meta.get("is_dict", False)
        
        combined_list = []
        combined_dict = {}
        
        for i in range(num_chunks):
            chunk_key = f"{endpoint_name}#chunk{i}"
            res = supabase.table("api_cache").select("data").eq("key", chunk_key).execute()
            if res.data:
                chunk_data = res.data[0]["data"]
                if is_dict and isinstance(chunk_data, dict):
                    combined_dict.update(chunk_data)
                elif isinstance(chunk_data, list):
                    combined_list.extend(chunk_data)
                    
        print(f"+++ ERFOLG: {endpoint_name} ({num_chunks} Chunks) aus Supabase geladen +++", flush=True)
        return combined_dict if is_dict else combined_list
        
    except Exception as e:
        print(f"[CACHE] Ladefehler aus Supabase: {e}", flush=True)
        return {}

# ÔöÇÔöÇÔöÇ Cache-Helfer ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

def get_cached_or_fetch(endpoint_name, url):
    """Generischer Cache-Loader mit Supabase-Ausfallschutz und Chunking."""
    print(f"API-Call: {url}", flush=True)
    try:
        response = requests.get(url, auth=HTTPBasicAuth(USER, PASS), timeout=60)
        response.raise_for_status()
        data = response.json()
        
        # In Supabase abspeichern (mit Chunking)
        success = save_to_supabase_chunked(endpoint_name, data)
        if success:
            print(f"[CACHE] {endpoint_name} erfolgreich komplett in Supabase gesichert.", flush=True)
            
        return data

    except Exception as e:
        print(f"[ERROR] Syscara API [{endpoint_name}]: {e}", flush=True)
        print("Versuche Fallback auf Supabase-Cache...", flush=True)
        return load_from_supabase_chunked(endpoint_name)

# ÔöÇÔöÇÔöÇ Filter-Logik (unver├ñndert) ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

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

# ÔöÇÔöÇÔöÇ Routen ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

@app.route('/')
def index(): return send_file('fahrzeugsuche_local.html')

@app.route('/api/ads', methods=['POST'])
def api_ads():
    raw = get_cached_or_fetch('sale/ads', f"{SYSCARA_BASE}/sale/ads/")
    return jsonify({"success": True, "count": len(iter_items(raw)), "vehicles": map_and_filter(raw, {})})

@app.route('/api/vehicles', methods=['GET', 'POST'])
def api_vehicles():
    raw = get_cached_or_fetch('sale/vehicles', f"{SYSCARA_BASE}/sale/vehicles/")
    return jsonify({"success": True, "vehicles": iter_items(raw)})

@app.route('/api/orders', methods=['GET', 'POST'])
def api_orders():
    raw = get_cached_or_fetch('sale/orders', f"{SYSCARA_BASE}/sale/orders/")
    items = normalize_collection_items(raw, 'orders')

    year = request.args.get('year')
    if year and year != 'alle':
        try:
            year_num = int(year)
        except (TypeError, ValueError):
            year_num = None

        if year_num is not None:
            filtered_items = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                order_dt = extract_order_datetime(item)
                if order_dt and order_dt.year == year_num:
                    filtered_items.append(item)
            items = filtered_items

    try:
        print(f"[DEBUG] /api/orders -> items: {len(items)}", flush=True)
        if items:
            print(f"[DEBUG] first order sample: {str(items[0])[:200]}", flush=True)
    except Exception:
        pass
    return jsonify({"success": True, "count": len(items), "orders": items})
@app.route('/api/equipment', methods=['GET', 'POST'])
def api_equipment():
    year = request.args.get('year')
    if year and year != 'alle':
        url = f"{SYSCARA_BASE}/sale/equipment/?modelyear={year}"
        print(f"Direct Year Fetch for Equipment: {url}", flush=True)
        try:
            r = requests.get(url, auth=HTTPBasicAuth(USER, PASS), timeout=60)
            r.raise_for_status()
            data = r.json()
            return jsonify({"success": True, "equipment": iter_items(data)})
        except Exception as e:
            print(f"[ERROR] Direct Year Fetch failed: {e}", flush=True)
            raw = load_from_supabase_chunked('sale/equipment')
            return jsonify({"success": True, "equipment": iter_items(raw)})

    raw = get_cached_or_fetch('sale/equipment', f"{SYSCARA_BASE}/sale/equipment/")
    return jsonify({"success": True, "equipment": iter_items(raw)})

@app.route('/api/stats', methods=['GET'])
def api_stats():
    raw = get_cached_or_fetch('sale/ads', f"{SYSCARA_BASE}/sale/ads/")
    return jsonify({"success": True, "stats": {}})

# ÔöÇÔöÇÔöÇ Proaktiver Background Sync ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

def sync_all_now():
    """Holt alle Listen. Kleine Endpoints zuerst."""
    print("\n--- [BACKGROUND SYNC] Start ---", flush=True)

    # REIHENFOLGE: Kleine Pakete zuerst
    endpoints = {
        "sale/equipment": f"{SYSCARA_BASE}/sale/equipment/",
        "sale/orders":    f"{SYSCARA_BASE}/sale/orders/",
        "sale/lists":     f"{SYSCARA_BASE}/sale/lists/?list=pictures",
        "sale/vehicles":  f"{SYSCARA_BASE}/sale/vehicles/",
        "sale/ads":       f"{SYSCARA_BASE}/sale/ads/"
    }
    
    for name, url in endpoints.items():
        try:
            print(f"-----", flush=True)
            print(f"[SYNC] Verarbeite: {name}...", flush=True)
            get_cached_or_fetch(name, url)
        except Exception as e:
            print(f"[SYNC ERROR] {name}: {e}", flush=True)
            
    print("--- [BACKGROUND SYNC] Fertig ---\n", flush=True)

def background_sync_loop():
    time.sleep(5)
    while True:
        sync_all_now()
        time.sleep(3600)

def start_sync_thread():
    if not supabase: return
    t = threading.Thread(target=background_sync_loop, daemon=True)
    t.start()
    print("[SYNC] Hintergrund-Thread l├ñuft.", flush=True)

if __name__ == '__main__':
    port = int(os.getenv("PORT", 5000))
    print(f"Syscara Python API auf Port {port}...", flush=True)
    start_sync_thread()
    app.run(host='0.0.0.0', port=port, debug=False)
