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

# Debugging-Hilfe für die Zugangsdaten
print(f"[INIT] Syscara API User gefunden: {'JA' if USER else 'NEIN'}", flush=True)
print(f"[INIT] Syscara API Pass gefunden: {'JA' if PASS else 'NEIN'}", flush=True)

# Supabase Konfig
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = None

if SUPABASE_URL and SUPABASE_KEY:
    try:
        from supabase import create_client, Client
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("[INIT] Supabase-Client erfolgreich erstellt.", flush=True)
    except ImportError:
        print("[ERROR] Die 'supabase' Bibliothek ist nicht installiert!", flush=True)
    except Exception as e:
        print(f"[ERROR] Supabase Init Fehler: {e}", flush=True)
else:
    print("[INIT] Supabase nicht konfiguriert (URL/KEY fehlt).", flush=True)

# ─── Cache-Helfer ─────────────────────────────────────────────────────────────

def get_cached_or_fetch(endpoint_name, url):
    """Generischer Cache-Loader mit Supabase-Ausfallschutz."""
    print(f"API-Call: {url} (Key: {endpoint_name})", flush=True)
    try:
        response = requests.get(url, auth=HTTPBasicAuth(USER, PASS), timeout=45)
        response.raise_for_status()
        data = response.json()
        
        if supabase:
            try:
                supabase.table("api_cache").upsert({
                    "key": endpoint_name,
                    "data": data,
                    "updated_at": int(time.time())
                }).execute()
                print(f"[CACHE] {endpoint_name} erfolgreich in Supabase gespeichert.", flush=True)
            except Exception as dbe:
                print(f"[ERROR] Supabase Write [{endpoint_name}]: {dbe}", flush=True)
        return data

    except Exception as e:
        print(f"[ERROR] Syscara API [{endpoint_name}]: {e}", flush=True)
        if supabase:
            try:
                res = supabase.table("api_cache").select("data, updated_at").eq("key", endpoint_name).execute()
                if res.data and len(res.data) > 0:
                    return res.data[0]["data"]
            except: pass
        return {}

# ─── Hilfsfunktionen ──────────────────────────────────────────────────────────

def iter_items(raw):
    if isinstance(raw, dict): return raw.values()
    if isinstance(raw, list): return raw
    return []

def fmt_preis(preis):
    if not preis: return '-'
    return f"{preis:,.2f} €".replace(',', 'X').replace('.', ',').replace('X', '.')

# ─── Filter-Logik (unverändert) ────────────────────────────────────────────────

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
            "id": v.get('id'), "hersteller": model.get('producer', '-'), "modell": model.get('model', '-'),
            "preis": preis, "preis_format": fmt_preis(preis), "zustand": condition, "thumb": None
        }
        vehicles.append(obj)
    return vehicles

# ─── Routen ───────────────────────────────────────────────────────────────────

@app.route('/')
def index(): return send_file('fahrzeugsuche_local.html')

@app.route('/api/ads', methods=['POST'])
def api_ads():
    raw = get_cached_or_fetch('sale/ads', f"{SYSCARA_BASE}/sale/ads/")
    return jsonify({"success": True, "count": len(raw), "vehicles": map_and_filter(raw, {})})

@app.route('/api/vehicles', methods=['GET', 'POST'])
def api_vehicles():
    raw = get_cached_or_fetch('sale/vehicles', f"{SYSCARA_BASE}/sale/vehicles/")
    return jsonify({"success": True, "vehicles": raw})

@app.route('/api/orders', methods=['GET', 'POST'])
def api_orders():
    raw = get_cached_or_fetch('sale/orders', f"{SYSCARA_BASE}/sale/orders/")
    return jsonify({"success": True, "orders": raw})

@app.route('/api/equipment', methods=['GET', 'POST'])
def api_equipment():
    raw = get_cached_or_fetch('sale/equipment', f"{SYSCARA_BASE}/sale/equipment/")
    return jsonify({"success": True, "equipment": raw})

@app.route('/api/stats', methods=['GET'])
def api_stats():
    raw = get_cached_or_fetch('sale/ads', f"{SYSCARA_BASE}/sale/ads/")
    return jsonify({"success": True, "stats": {}})

# ─── Proaktiver Background Sync ───────────────────────────────────────────────

def sync_all_now():
    """Holt alle Listen. Kleine Endpoints zuerst."""
    print("\n--- [BACKGROUND SYNC] Start ---", flush=True)
    
    if supabase:
        try:
            old_keys = ["ads", "vehicles", "orders", "equipment", "test_equipment"]
            supabase.table("api_cache").delete().in_("key", old_keys).execute()
            print("[SYNC] Alte Einträge bereinigt.", flush=True)
        except Exception as e:
            print(f"[SYNC] Cleanup Info: {e}", flush=True)

    # REIHENFOLGE: Kleine Pakete zuerst, damit man sofort was sieht
    endpoints = {
        "sale/equipment": f"{SYSCARA_BASE}/sale/equipment/",
        "sale/orders":    f"{SYSCARA_BASE}/sale/orders/",
        "sale/lists":     f"{SYSCARA_BASE}/sale/lists/?list=pictures",
        "sale/vehicles":  f"{SYSCARA_BASE}/sale/vehicles/",
        "sale/ads":       f"{SYSCARA_BASE}/sale/ads/"
    }
    
    for name, url in endpoints.items():
        try:
            print(f"[SYNC] Verarbeite: {name}...", flush=True)
            get_cached_or_fetch(name, url)
        except Exception as e:
            print(f"[SYNC ERROR] {name}: {e}", flush=True)
            
    print("--- [BACKGROUND SYNC] Fertig ---\n", flush=True)

def background_sync_loop():
    time.sleep(10)
    while True:
        sync_all_now()
        time.sleep(3600)

def start_sync_thread():
    if not supabase: return
    t = threading.Thread(target=background_sync_loop, daemon=True)
    t.start()
    print("[SYNC] Hintergrund-Thread läuft.", flush=True)

if __name__ == '__main__':
    port = int(os.getenv("PORT", 5000))
    print(f"Syscara Python API auf Port {port}...", flush=True)
    start_sync_thread()
    app.run(host='0.0.0.0', port=port, debug=False)
