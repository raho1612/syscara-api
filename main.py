import json
import math
import os
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
from requests.auth import HTTPBasicAuth

try:
    import openai as _openai
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

try:
    import google.generativeai as _genai
    HAS_GEMINI = True
except ImportError:
    HAS_GEMINI = False

try:
    import anthropic as _anthropic
    HAS_CLAUDE = True
except ImportError:
    HAS_CLAUDE = False

CLAUDE_MODEL_CANDIDATES = {
    "sonnet": [
        "claude-sonnet-4-6",
        "claude-sonnet-4-5-20250929",
        "claude-sonnet-4-20250514",
        "claude-3-7-sonnet-20250219",
        "claude-3-5-sonnet-20241022",
        "claude-3-sonnet-20240229",
    ],
    "haiku": [
        "claude-3-5-haiku-20241022",
        "claude-3-haiku-20240307",
    ],
}

def _get_claude_candidates(model_key):
    env_override = os.getenv(f"CLAUDE_{model_key.upper()}_MODEL")
    candidates = []
    if env_override:
        candidates.append(env_override.strip())

    for candidate in CLAUDE_MODEL_CANDIDATES[model_key]:
        if candidate not in candidates:
            candidates.append(candidate)

    return candidates

def _is_claude_model_not_found(error_text):
    normalized = error_text.lower()
    return (
        "not_found_error" in normalized
        or "model:" in normalized
        or "model not found" in normalized
        or "404" in normalized
    )

CURRENT_DIR = Path(__file__).resolve().parent

def _discover_workspace_root() -> Path:
    for candidate in [CURRENT_DIR.parent, *CURRENT_DIR.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate
    return CURRENT_DIR.parent

WORKSPACE_ROOT = _discover_workspace_root()
ROOT_DIR = Path(os.getenv("APP_DATA_ROOT") or str(WORKSPACE_ROOT))

# Lade zuerst die lokale .env, dann bekannte Root-.env Kandidaten (enthält OPENAI_API_KEY etc.)
load_dotenv()
for env_path in (CURRENT_DIR / ".env", CURRENT_DIR.parent / ".env", WORKSPACE_ROOT / ".env"):
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)

SHARED_IMPORT_ROOTS = [
    CURRENT_DIR,
    CURRENT_DIR.parent,
    CURRENT_DIR.parent / "syscara-dashboard",
]

for shared_root in SHARED_IMPORT_ROOTS:
    shared_package = shared_root / "shared"
    if shared_package.exists() and str(shared_root) not in sys.path:
        sys.path.append(str(shared_root))

from shared.vehicle_stats import build_vehicle_stats

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
API_VERSION   = os.getenv("SYSCARA_API_VERSION", "v1.0.0")

# Supabase Konfig
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = None

if SUPABASE_URL and SUPABASE_KEY:
    try:
        import httpx
        from supabase import Client, ClientOptions, create_client
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

# ─── Hilfsfunktionen ──────────────────────────────────────────────────────────

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
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except Exception:
            try:
                return datetime.strptime(value.split('T')[0], '%Y-%m-%d')
            except Exception:
                continue

    return None

def fmt_preis(preis):
    if not preis: return '-'
    return f"{preis:,.2f} €".replace(',', 'X').replace('.', ',').replace('X', '.')

def _candidate_file_paths(filename, env_var=None):
    """Gibt Kandidaten-Pfade zurück, wo eine Datendatei liegen könnte."""
    candidates = []
    if env_var:
        override = os.getenv(env_var)
        if override:
            candidates.append(Path(override))
    candidates.extend([
        ROOT_DIR / filename,
        CURRENT_DIR / filename,
        Path('/data') / filename,
    ])
    seen = set()
    result = []
    for p in candidates:
        key = str(p.resolve())
        if key not in seen:
            seen.add(key)
            result.append(p)
    return result

# ─── Chunking Logik für Supabase & Lokaler Cache ──────────────────────────────
CACHE_DIR = CURRENT_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True)

CHUNK_SIZE = 500 # Maximale Anzahl an Elementen pro Chunk

def save_to_supabase_chunked(endpoint_name, data):
    """Speichert große Listen in kleineren Chunks, um Timeouts zu vermeiden."""
    if not supabase: return False

    items = iter_items(data)
    total_items = len(items)
    is_dict = isinstance(data, dict)
    keys_list = list(data.keys()) if is_dict else []

    if total_items == 0:
        return True

    # Kein DELETE mehr – upsert überschreibt vorhandene Einträge direkt.
    num_chunks = math.ceil(total_items / CHUNK_SIZE)
    timestamp = int(time.time())

    for i in range(num_chunks):
        start_idx = i * CHUNK_SIZE
        end_idx = min((i + 1) * CHUNK_SIZE, total_items)

        chunk_key = f"{endpoint_name}#chunk{i}"

        # Rekonstruiere die Datenstruktur für den Chunk
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
            err_msg = str(dbe).lower()
            if "quota" in err_msg or "limit" in err_msg or "429" in err_msg:
                print(f"  [CRITICAL] Supabase Limit erreicht! Speichern von {chunk_key} abgebrochen.", flush=True)
            else:
                print(f"  [ERROR] Chunk {chunk_key} fehlgeschlagen: {dbe}", flush=True)
            return False

    # Lokaler Fallback: Speichere immer auch eine Kopie lokal auf Disk
    try:
        local_path = CACHE_DIR / f"{endpoint_name.replace('/', '_')}.json"
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        print(f"  [CACHE] Lokal gesichert: {local_path}", flush=True)
    except: pass

    return True

def load_from_supabase_chunked(endpoint_name):
    """Lädt und kombiniert Chunks aus Supabase."""
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
        err_msg = str(e).lower()
        if "quota" in err_msg or "limit" in err_msg:
            print(f"[CACHE] Supabase Limit erreicht – Suche lokale Kopie...", flush=True)
        else:
            print(f"[CACHE] Ladefehler aus Supabase: {e}", flush=True)
        
        # Lokaler Fallback
        local_path = CACHE_DIR / f"{endpoint_name.replace('/', '_')}.json"
        if local_path.exists():
            try:
                with open(local_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except: pass
        return {}

# ─── In-Memory Cache ──────────────────────────────────────────────────────────
# Hält geladene Endpoint-Daten für die gesamte Server-Laufzeit im RAM,
# um wiederholte Supabase-/API-Fetches zu vermeiden.
_MEM_CACHE: dict = {}

# ─── Fragen-Cache ─────────────────────────────────────────────────────────────
# Cached Antworten auf gestellte /api/ask-Fragen (lokal + BI-Fragen).
# Verhindert wiederholte OpenAI-Calls und beschleunigt Wiederholungsfragen.
_QUESTION_CACHE: dict = {}  # key → {ts: float, response: dict, source: str}
_QUESTION_CACHE_TTL_LOCAL = 3600    # 1 Stunde für lokale Abfragen
_QUESTION_CACHE_TTL_BI    = 600     # 10 Minuten für BI/OpenAI-Abfragen

def _qcache_key(q: str) -> str:
    return q.strip().lower()

def _qcache_get(q: str) -> dict | None:
    key = _qcache_key(q)
    entry = _QUESTION_CACHE.get(key)
    if not entry:
        return None
    ttl = _QUESTION_CACHE_TTL_LOCAL if entry.get('source') == 'local' else _QUESTION_CACHE_TTL_BI
    if time.time() - entry['ts'] < ttl:
        return entry['response']
    del _QUESTION_CACHE[key]
    return None

def _qcache_put(q: str, response: dict):
    key = _qcache_key(q)
    _QUESTION_CACHE[key] = {'ts': time.time(), 'response': response, 'source': response.get('source', 'openai')}
    # Größe begrenzen: älteste 20 % entfernen sobald > 200 Einträge
    if len(_QUESTION_CACHE) > 200:
        oldest = sorted(_QUESTION_CACHE.items(), key=lambda x: x[1]['ts'])[:40]
        for k, _ in oldest:
            _QUESTION_CACHE.pop(k, None)

# Lokale JSON-Fallback-Dateien (Pfade relativ zum Workspace-Root)
_LOCAL_FALLBACKS = {
    "sale/orders": ROOT_DIR / "orders.json",
}

def _load_local_fallback(endpoint_name):
    fp = _LOCAL_FALLBACKS.get(endpoint_name)
    if fp and fp.exists():
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = __import__("json").load(f)
            print(f"[CACHE] Lokaler JSON-Fallback geladen: {fp} ({len(iter_items(data))} Items)", flush=True)
            return data
        except Exception as e:
            print(f"[CACHE] Lokaler Fallback fehlgeschlagen: {e}", flush=True)
    return None

# ─── Cache-Helfer ─────────────────────────────────────────────────────────────

def get_cached_or_fetch(endpoint_name, url):
    """Generischer Cache-Loader: In-Memory → lokaler JSON → Syscara-API → Supabase."""
    # 1. In-Memory Cache (schnellste Option)
    if endpoint_name in _MEM_CACHE:
        print(f"[CACHE] {endpoint_name} aus RAM-Cache geliefert ({len(iter_items(_MEM_CACHE[endpoint_name]))} Items)", flush=True)
        return _MEM_CACHE[endpoint_name]

    print(f"API-Call: {url}", flush=True)
    try:
        response = requests.get(url, auth=HTTPBasicAuth(USER, PASS), timeout=60)
        response.raise_for_status()
        data = response.json()

        # In Supabase abspeichern (mit Chunking)
        success = save_to_supabase_chunked(endpoint_name, data)
        if success:
            print(f"[CACHE] {endpoint_name} erfolgreich komplett in Supabase gesichert.", flush=True)

        _MEM_CACHE[endpoint_name] = data
        return data

    except Exception as e:
        print(f"[ERROR] Syscara API [{endpoint_name}]: {e}", flush=True)

        # 2. Lokaler JSON-Fallback (schnell, kein Netzwerk)
        local = _load_local_fallback(endpoint_name)
        if local is not None:
            _MEM_CACHE[endpoint_name] = local
            return local

        # 3. Supabase-Cache (langsam, aber vollständig)
        print("Versuche Fallback auf Supabase-Cache...", flush=True)
        data = load_from_supabase_chunked(endpoint_name)
        if data:
            _MEM_CACHE[endpoint_name] = data
        return data


def fetch_live_then_cache(endpoint_name, url, *, allow_stale_fallback=False):
    """Holt nach Möglichkeit frische Syscara-Daten und fällt nur optional auf Cache zurück."""
    print(f"LIVE-API-Call: {url}", flush=True)
    try:
        response = requests.get(url, auth=HTTPBasicAuth(USER, PASS), timeout=60)
        response.raise_for_status()
        data = response.json()

        success = save_to_supabase_chunked(endpoint_name, data)
        if success:
            print(f"[CACHE] {endpoint_name} nach Live-Refresh gespeichert.", flush=True)

        return data
    except Exception as e:
        print(f"[ERROR] Live Syscara API [{endpoint_name}]: {e}", flush=True)
        if allow_stale_fallback:
            print("Falle auf Supabase-Cache zurück...", flush=True)
            return load_from_supabase_chunked(endpoint_name)
        raise

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
        # Flexiblere Erkennung für Regression-Sicherheit
        has_auto = any(x in gear_raw for x in ["AUTOMATIC", "AUT", "AUTOMATIK"])
        has_manual = any(x in gear_raw for x in ["MANUAL", "MAN", "SCHALTUNG", "SCHALTER"])
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

            if filters.get('laengeMin') and laenge < float(filters.get('laengeMin')) * 100: continue
            if filters.get('laengeMax') and laenge > float(filters.get('laengeMax')) * 100: continue
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
                if gt == 'schaltung' and not has_manual: continue

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

# ─── Routen ───────────────────────────────────────────────────────────────────

@app.route('/')
def index(): return send_file('fahrzeugsuche_local.html')

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok"})

@app.route('/api/diag', methods=['GET'])
def api_diag():
    orders_fallback_candidates = _candidate_file_paths('orders.json', 'ORDERS_JSON_PATH')
    orders_fallback_found = [str(p) for p in orders_fallback_candidates if p.exists()]
    employee_names_path = Path(os.getenv('EMPLOYEE_NAMES_PATH', str(ROOT_DIR / 'employee_names.json')))

    return jsonify({
        "success": True,
        "service": "syscara-python-backend",
        "api_version": API_VERSION,
        "python": sys.version,
        "cwd": str(Path.cwd()),
        "root_dir": str(ROOT_DIR),
        "has_openai_lib": HAS_OPENAI,
        "has_claude_lib": HAS_CLAUDE,
        "has_gemini_lib": HAS_GEMINI,
        "has_openai_key": bool(os.getenv('OPENAI_API_KEY')),
        "has_anthropic_key": bool(os.getenv('ANTHROPIC_API_KEY')),
        "has_gemini_key": bool(os.getenv('GEMINI_API_KEY') or os.getenv('GOOGLE_API_KEY')),
        "has_supabase_url": bool(SUPABASE_URL),
        "has_supabase_key": bool(SUPABASE_KEY),
        "employee_names_path": str(employee_names_path),
        "employee_names_exists": employee_names_path.exists(),
        "orders_fallback_candidates": [str(p) for p in orders_fallback_candidates],
        "orders_fallback_found": orders_fallback_found,
        "routes": sorted(str(r.rule) for r in app.url_map.iter_rules()),
    })

@app.route('/api/ads', methods=['POST'])
def api_ads():
    raw = get_cached_or_fetch('sale/ads', f"{SYSCARA_BASE}/sale/ads/")
    filters = request.get_json(silent=True) or {}
    vehicles = map_and_filter(raw, filters)
    return jsonify({"success": True, "count": len(vehicles), "vehicles": vehicles})

@app.route('/api/vehicles', methods=['GET', 'POST'])
def api_vehicles():
    year = request.args.get('year')
    if year and year != 'alle':
        url = f"{SYSCARA_BASE}/sale/vehicles/?modelyear={year}"
        print(f"Direct Year Fetch for Vehicles: {url}", flush=True)
        try:
            r = requests.get(url, auth=HTTPBasicAuth(USER, PASS), timeout=60)
            r.raise_for_status()
            data = r.json()
            items = iter_items(data)
            return jsonify({"success": True, "count": len(items), "vehicles": items})
        except Exception as e:
            print(f"[ERROR] Direct Year Fetch failed: {e}", flush=True)

    raw = get_cached_or_fetch('sale/vehicles', f"{SYSCARA_BASE}/sale/vehicles/")
    items = iter_items(raw)

    if year and year != 'alle':
        try:
            filtered = []
            for v in items:
                y = v.get('modelljahr') or (v.get('model') or {}).get('modelyear')
                if str(y) == str(year):
                    filtered.append(v)
            items = filtered
        except:
            pass

    return jsonify({"success": True, "count": len(items), "vehicles": items})

@app.route('/api/orders', methods=['GET', 'POST'])
def api_orders():
    year = request.args.get('year')
    if year and year != 'alle':
        url = f"{SYSCARA_BASE}/sale/orders/?update={year}-01-01"
        print(f"Direct Year Fetch for Orders: {url}", flush=True)
        try:
            r = requests.get(url, auth=HTTPBasicAuth(USER, PASS), timeout=60)
            r.raise_for_status()
            data = r.json()
            items = normalize_collection_items(data, 'orders')
            
            try:
                year_num = int(year)
                filtered_items = []
                for item in items:
                    if not isinstance(item, dict): continue
                    order_dt = extract_order_datetime(item)
                    if order_dt and order_dt.year == year_num:
                        filtered_items.append(item)
                items = filtered_items
            except Exception:
                pass

            return jsonify({"success": True, "count": len(items), "orders": items})
        except Exception as e:
            print(f"[ERROR] Direct Year Fetch failed: {e}", flush=True)

    raw = get_cached_or_fetch('sale/orders', f"{SYSCARA_BASE}/sale/orders/?update=2024-01-01")
    items = normalize_collection_items(raw, 'orders')

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
    """Liefert die Fahrzeug-Statistiken für das Dashboard."""
    use_stale_fallback = str(request.args.get('allow_stale', '0')).lower() in ('1', 'true', 'yes')

    try:
        # Versuch, Live-Daten zu laden
        raw = fetch_live_then_cache(
            'sale/vehicles',
            f"{SYSCARA_BASE}/sale/vehicles/",
            allow_stale_fallback=use_stale_fallback,
        )
        if not raw:
            # Wenn gar nichts kommt, versuchen wir den Cache direkt
            raw = load_from_supabase_chunked('sale/vehicles')
            
        if not raw:
            return jsonify({"success": False, "error": "Keine Fahrzeugdaten verfügbar (Syscara & Cache leer)."}), 503

        stats = build_vehicle_stats(
            raw,
            enable_offset=os.getenv("SYSCARA_KPI_OFFSET_ENABLE", "0") == "1",
            offset_trigger=int(os.getenv("SYSCARA_KPI_OFFSET_TRIGGER", "483")),
            offset_value=int(os.getenv("SYSCARA_KPI_OFFSET_VALUE", "2")),
        )
        response = jsonify({"success": True, "stats": stats})
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        # CORS und Cache-Header sicherstellen
        return response

    except Exception as e:
        print(f"[CRITICAL] Fehler in /api/stats: {e}", flush=True)
        import traceback; traceback.print_exc()
        return jsonify({
            "success": False, 
            "error": "Interner Fehler bei der Statistik-Berechnung.",
            "details": str(e)
        }), 500


@app.route('/api/performance', methods=['GET'])
def api_performance():
    """Aggregierte Performance pro Mitarbeiter (MONTHS & QUARTERS).
    Liefert für jedes Jahr und jede Metrik (ORDER,OFFER,CONTRACT,CANCELLATION)
    eine Struktur, die vom Frontend erwartet wird.
    """
    year = int(request.args.get('year') or datetime.now().year)
    metric = (request.args.get('metric') or 'ORDER').upper()

    raw = get_cached_or_fetch('sale/orders', f"{SYSCARA_BASE}/sale/orders/?update=2024-01-01")
    # Normalize returned structure: Syscara may return a list or a dict {"orders": [...]}
    if isinstance(raw, dict) and isinstance(raw.get('orders'), list):
        items = raw.get('orders')
    else:
        items = list(iter_items(raw))
        # sometimes iter_items returns a single-item list containing the real list
        if len(items) == 1 and isinstance(items[0], list):
            items = items[0]
    
    print(f"[DEBUG] /api/performance: fetched {len(items)} order items for year {year}", flush=True)

    # Hilfsstruktur: name -> months(1..12) -> metrics
    employees: dict = {}

    def extract_date(o_item):
        # Try multiple date locations and formats
        candidates = []
        d = o_item.get('date')
        if isinstance(d, str):
            candidates.append(d)
        elif isinstance(d, dict):
            for k in ('create', 'created', 'create_date', 'created_at', 'createAt', 'update', 'updated_at'):
                v = d.get(k)
                if v: candidates.append(v)
            for v in d.values():
                if isinstance(v, str): candidates.append(v)
        for k in ('created_at', 'created', 'create', 'date', 'createdAt'):
            v = o_item.get(k)
            if isinstance(v, str): candidates.append(v)

        for s in candidates:
            if not s or not isinstance(s, str): continue
            try:
                return datetime.fromisoformat(s.replace('Z', '+00:00'))
            except Exception:
                try:
                    return datetime.strptime(s.split('T')[0], '%Y-%m-%d')
                except Exception:
                    continue
        return None

    # Klarname-Mapping laden (Mapping aus employee_names.json hat Vorrang)
    _emp_names = _load_employee_names()

    def extract_employee_name(o_item):
        """Gibt den Klarnamen des Mitarbeiters zurück. Löst IDs via employee_names.json auf.
        Falls kein Mapping existiert, werden Namen direkt aus dem User-Objekt bevorzugt.
        """
        u = o_item.get('user') or {}
        ids = []
        names = []

        # 1. IDs sammeln (für Mapping)
        for key in ('order', 'update', 'id'):
            v = u.get(key)
            if v and str(v).isdigit(): ids.append(str(v))

        # 2. Namen sammeln (als Fallback)
        for key in ('full_name', 'name', 'display_name', 'username'):
            v = u.get(key)
            if v and isinstance(v, str) and v.strip() and not v.strip().isdigit():
                names.append(v.strip())

        # 3. Zusätzliche Syscara-Felder prüfen
        for key in ('responsible', 'seller', 'sales_person'):
            v = o_item.get(key)
            if isinstance(v, str) and v.strip() and not v.strip().isdigit():
                names.append(v.strip())
            elif isinstance(v, dict):
                vv = v.get('name') or v.get('username')
                if vv: names.append(str(vv))

        # --- Auflösung ---
        for uid in ids:
            if uid in _emp_names: return _emp_names[uid]
        if names: return names[0]
        if ids: return f"ID {ids[0]}"
        return 'Unbekannt'

    for o in items:
        if not o or not isinstance(o, dict): continue
        dt = extract_date(o)
        if not dt or dt.year != year: continue

        month = dt.month
        quarter = (month - 1) // 3 + 1
        name = extract_employee_name(o)

        if name not in employees:
            m_template = {str(i): {k: {"count": 0, "revenue": 0, "cumulative_count": 0} for k in ['OFFER', 'ORDER', 'CONTRACT', 'CANCELLATION']} for i in range(1, 13)}
            q_template = {f'Q{i}': {k: {"count": 0, "revenue": 0, "cumulative_count": 0} for k in ['OFFER', 'ORDER', 'CONTRACT', 'CANCELLATION']} for i in range(1, 5)}
            employees[name] = {"id": name.replace(' ', '_'), "name": name, "months": m_template, "quarters": q_template}

        price = 0
        try:
            p_val = o.get('price') or o.get('total') or o.get('amount') or 0
            price = float(p_val)
        except: pass

        st_obj = o.get('status')
        status = ''
        if isinstance(st_obj, dict):
            status = str(st_obj.get('key') or st_obj.get('label') or '').upper()
        else:
            status = str(st_obj or '').upper()
        
        VALID_METRICS = {'OFFER', 'ORDER', 'CONTRACT', 'CANCELLATION'}
        m_type = status if status in VALID_METRICS else 'ORDER'

        emp = employees[name]
        emp['months'][str(month)][m_type]['count'] += 1
        emp['months'][str(month)][m_type]['revenue'] += price
        emp['quarters'][f'Q{quarter}'][m_type]['count'] += 1
        emp['quarters'][f'Q{quarter}'][m_type]['revenue'] += price

    # Cumulative counts
    for name, emp in employees.items():
        running = {k: 0 for k in ['OFFER', 'ORDER', 'CONTRACT', 'CANCELLATION']}
        for i in range(1, 13):
            month_key = str(i)
            for k in running.keys():
                running[k] += emp['months'][month_key][k]['count']
                emp['months'][month_key][k]['cumulative_count'] = running[k]

    # Formatiere Antwort
    emp_list = list(employees.values())

    return jsonify({"success": True, "year": year, "employees": emp_list})


# ─── Proaktiver Background Sync ───────────────────────────────────────────────

def sync_all_now():
    """Holt alle Listen. Kleine Endpoints zuerst."""
    print("\n--- [BACKGROUND SYNC] Start ---", flush=True)

    # REIHENFOLGE: Kleine Pakete zuerst
    endpoints = {
        "sale/equipment": f"{SYSCARA_BASE}/sale/equipment/",
        "sale/orders":    f"{SYSCARA_BASE}/sale/orders/?update=2024-01-01",
        "sale/lists":     f"{SYSCARA_BASE}/sale/lists/?list=pictures",
        "sale/vehicles":  f"{SYSCARA_BASE}/sale/vehicles/",
        "sale/ads":       f"{SYSCARA_BASE}/sale/ads/"
    }

    for name, url in endpoints.items():
        try:
            print("-----", flush=True)
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


@app.route('/api/sync', methods=['GET', 'POST'])
def api_sync():
    """Trigger a background sync. Accepts GET for safe local triggering (no data POSTed).
    Using GET avoids sending data that might change remote state; this only performs GET requests
    to the Syscara API and saves cached data locally (Supabase)."""
    try:
        _MEM_CACHE.clear()
        _QUESTION_CACHE.clear()
        t = threading.Thread(target=sync_all_now, daemon=True)
        t.start()
        return jsonify({"success": True, "message": "Background sync started"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

def start_sync_thread():
    if not supabase: return
    t = threading.Thread(target=background_sync_loop, daemon=True)
    t.start()
    print("[SYNC] Hintergrund-Thread läuft.", flush=True)

@app.route('/api/evaluate', methods=['POST'])
def api_evaluate():
    """Fahrzeugbewertung via ChatGPT.

    Sicherheitsdesign (Prompt-Injection-Schutz):
    - 'instruction' landet ausschliesslich im system-Parameter (vertrauenswürdig, kontrolliert).
    - 'data' (Nutzereingabe) landet im user-Message-Teil – strikt getrennt.
    - Kein Inhalt aus 'data' kann den System-Prompt überschreiben.
    """
    if not HAS_OPENAI:
        return jsonify({"success": False, "error": "OpenAI-Bibliothek nicht installiert."}), 503

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        return jsonify({"success": False, "error": "Ungültige Anfrage."}), 400

    vehicle_data = str(body.get('data', '')).strip()
    instruction = str(body.get('instruction', '')).strip()

    if not vehicle_data:
        return jsonify({"success": False, "error": "Keine Fahrzeugdaten übergeben."}), 400

    if not instruction:
        instruction = (
            "Handle als Senior-Fahrzeugexperte für Reisemobile. Analysiere die freien Daten des Nutzers. "
            "Erstelle eine Marktwert-Tabelle (Händler, Privat, Ankauf) Stand 2026. "
            "Strukturiere die Antwort zwingend in: 1. Hygiene (Marktlage), "
            "2. Konfiguration (Wert der Extras), 3. Validierung (Vergleich zum aktuellen Markt). "
            "Antworte präzise, direkt und ehrlich. Gib am Ende Vergleichslinks zu "
            "mobile.de-Suchanfragen und caravans.de an (Suchseiten, keine Einzelinserate)."
        )

    vehicle_data = vehicle_data[:5000]
    instruction = instruction[:3000]

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return jsonify({"success": False, "error": "OPENAI_API_KEY nicht konfiguriert."}), 503

    try:
        client = _openai.OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o",
            max_tokens=2048,
            messages=[
                {"role": "system", "content": instruction},
                {"role": "user", "content": f"Fahrzeugdaten zur Bewertung:\n\n{vehicle_data}"},
            ],
        )
        text = response.choices[0].message.content or "Keine Antwort erhalten."
        return jsonify({"success": True, "text": text})
    except Exception as e:
        import traceback; traceback.print_exc()
        err_str = str(e)
        if "insufficient_quota" in err_str or "billing" in err_str.lower() or "credit" in err_str.lower():
            return jsonify({"success": False, "error": "⚠️ OpenAI-Guthaben aufgebraucht. Bitte unter platform.openai.com → Billing neue Credits kaufen."}), 402
        if "authentication" in err_str.lower() or "api_key" in err_str.lower() or "401" in err_str:
            return jsonify({"success": False, "error": "⚠️ Ungültiger OpenAI API-Key. Bitte in der .env-Datei prüfen."}), 401
        if "rate_limit" in err_str.lower() or "429" in err_str:
            return jsonify({"success": False, "error": "⚠️ Zu viele Anfragen – bitte kurz warten und erneut versuchen."}), 429
        return jsonify({"success": False, "error": f"KI-Fehler: {err_str}"}), 500


@app.route('/api/evaluate-claude', methods=['POST'])
def api_evaluate_claude():
    """Fahrzeugbewertung via Anthropic Claude.

    Sicherheitsdesign (Prompt-Injection-Schutz):
    - 'instruction' landet ausschliesslich im system-Parameter.
    - 'data' (Nutzereingabe) landet im user-Message-Teil – strikt getrennt.
    - Unterstützte Modelle: 'sonnet' (claude-3-7-sonnet-20250219) oder 'haiku' (claude-3-5-haiku-20241022).
    """
    if not HAS_CLAUDE:
        return jsonify({"success": False, "error": "anthropic Bibliothek nicht installiert. Bitte 'pip install anthropic' ausführen."}), 503

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        return jsonify({"success": False, "error": "Ungültige Anfrage."}), 400

    vehicle_data = str(body.get('data', '')).strip()
    instruction = str(body.get('instruction', '')).strip()
    model_key = str(body.get('model', 'sonnet')).strip().lower()

    if not vehicle_data:
        return jsonify({"success": False, "error": "Keine Fahrzeugdaten übergeben."}), 400

    if not instruction:
        instruction = (
            "Handle als Senior-Fahrzeugexperte für Reisemobile. Analysiere die freien Daten des Nutzers. "
            "Erstelle eine Marktwert-Tabelle (Händler, Privat, Ankauf) Stand 2026. "
            "Strukturiere die Antwort zwingend in: 1. Konfiguration (Wert der Extras), "
            "2. Validierung (Vergleich zum aktuellen Markt). 3. Marktlage. "
            "Antworte präzise, direkt und ehrlich. "
            "Ohne die Informationen von Reisemobile-MKK mit einzubeziehen."
        )

    vehicle_data = vehicle_data[:5000]
    instruction = instruction[:3000]

    if model_key not in CLAUDE_MODEL_CANDIDATES:
        return jsonify({
            "success": False,
            "error": "Ungültiges Claude-Modell. Erlaubt sind nur 'sonnet' oder 'haiku'.",
        }), 400

    model_candidates = _get_claude_candidates(model_key)

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"success": False, "error": "ANTHROPIC_API_KEY nicht konfiguriert."}), 503

    try:
        client = _anthropic.Anthropic(api_key=api_key)
        max_tokens = 2000 if model_key == "haiku" else 4000
        last_error = None

        for model_id in model_candidates:
            try:
                response = client.messages.create(
                    model=model_id,
                    max_tokens=max_tokens,
                    system=instruction,
                    messages=[
                        {"role": "user", "content": f"Fahrzeugdaten zur Bewertung:\n\n{vehicle_data}"}
                    ]
                )
                text_blocks = []
                for block in response.content or []:
                    if getattr(block, "type", None) == "text":
                        block_text = getattr(block, "text", "")
                        if block_text:
                            text_blocks.append(block_text)

                text = "\n".join(text_blocks).strip() or "Keine Antwort erhalten."
                return jsonify({"success": True, "text": text, "model": model_id})
            except Exception as model_error:
                last_error = str(model_error)
                if _is_claude_model_not_found(last_error):
                    continue
                raise

        return jsonify({
            "success": False,
            "error": f"Claude-Fehler: Kein kompatibles Modell gefunden. Letzter Fehler: {last_error}",
        }), 500
    except Exception as e:
        import traceback; traceback.print_exc()
        err_str = str(e)
        if "credit" in err_str.lower() or "quota" in err_str.lower() or "billing" in err_str.lower():
            return jsonify({"success": False, "error": "⚠️ Claude-Kontingent erschöpft. Bitte Guthaben prüfen."}), 402
        if "authentication" in err_str.lower() or "api_key" in err_str.lower() or "401" in err_str:
            return jsonify({"success": False, "error": "⚠️ Ungültiger Anthropic API-Key. Bitte in der .env-Datei prüfen."}), 401
        if "rate" in err_str.lower() or "429" in err_str:
            return jsonify({"success": False, "error": "⚠️ Zu viele Anfragen – bitte kurz warten und erneut versuchen."}), 429
        return jsonify({"success": False, "error": f"Claude-Fehler: {err_str}"}), 500


@app.route('/api/evaluate-gemini', methods=['POST'])
def api_evaluate_gemini():
    """Fahrzeugbewertung via Google Gemini.

    Sicherheitsdesign (Prompt-Injection-Schutz):
    - 'instruction' landet ausschliesslich im system_instruction-Parameter.
    - 'data' (Nutzereingabe) landet im user-Message-Teil – strikt getrennt.
    """
    if not HAS_GEMINI:
        return jsonify({"success": False, "error": "google-generativeai Bibliothek nicht installiert. Bitte 'pip install google-generativeai' ausführen."}), 503

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        return jsonify({"success": False, "error": "Ungültige Anfrage."}), 400

    vehicle_data = str(body.get('data', '')).strip()
    instruction = str(body.get('instruction', '')).strip()

    if not vehicle_data:
        return jsonify({"success": False, "error": "Keine Fahrzeugdaten übergeben."}), 400

    if not instruction:
        instruction = (
            "Handle als Senior-Fahrzeugexperte für Reisemobile. Analysiere die freien Daten des Nutzers. "
            "Erstelle eine Marktwert-Tabelle (Händler, Privat, Ankauf) Stand 2026. "
            "Strukturiere die Antwort zwingend in: 1. Konfiguration (Wert der Extras), "
            "2. Validierung (Vergleich zum aktuellen Markt). 3. Marktlage. "
            "Antworte präzise, direkt und ehrlich. "
            "Ohne die Informationen von Reisemobile-MKK mit einzubeziehen."
        )

    vehicle_data = vehicle_data[:5000]
    instruction = instruction[:3000]

    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not api_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY (oder GOOGLE_API_KEY) nicht konfiguriert."}), 503

    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(
            model_name="gemini-2.0-flash",
            system_instruction=instruction,
        )
        response = model.generate_content(f"Fahrzeugdaten zur Bewertung:\n\n{vehicle_data}")
        text = response.text or "Keine Antwort erhalten."
        return jsonify({"success": True, "text": text})
    except Exception as e:
        import traceback; traceback.print_exc()
        err_str = str(e)
        if "quota" in err_str.lower() or "429" in err_str:
            return jsonify({"success": False, "error": "⚠️ Gemini-Kontingent erschöpft oder zu viele Anfragen."}), 429
        if "api_key" in err_str.lower() or "permission" in err_str.lower() or "401" in err_str or "403" in err_str:
            return jsonify({"success": False, "error": "⚠️ Ungültiger Gemini API-Key. Bitte in der .env-Datei prüfen."}), 401
        return jsonify({"success": False, "error": f"Gemini-Fehler: {err_str}"}), 500


# ─── KI-Analyst Hilfsfunktionen ───────────────────────────────────────────────

def _get_orders() -> list:
    """Liefert die Auftrags-Liste korrekt normalisiert (same logic as api_performance)."""
    raw = get_cached_or_fetch('sale/orders', f"{SYSCARA_BASE}/sale/orders/?update=2024-01-01")
    # Struktur 1: {"orders": [...], "success": True}
    if isinstance(raw, dict) and isinstance(raw.get('orders'), list):
        return raw['orders']
    # Struktur 2: reine Liste oder anderes Dict
    items = list(iter_items(raw))
    if len(items) == 1 and isinstance(items[0], list):
        return items[0]
    return [o for o in items if isinstance(o, dict)]


def _build_bi_context() -> str:
    """Aggregierte anonyme Unternehmenskennzahlen für OpenAI.
    KEINE personenbezogenen Daten (kein Name, E-Mail, Adresse, Ausweis-Nr.).
    DSGVO-konform: Es werden ausschließlich Zahlen/Statistiken übertragen."""
    import datetime as _dt
    from collections import Counter

    lines = [f"=== UNTERNEHMENSDATEN (Stand: {_dt.date.today().strftime('%d.%m.%Y')}) ==="]

    # Auftrags-Statistiken
    try:
        items = _get_orders()
    except Exception:
        items = []

    status_counts: Counter = Counter()
    year_counts: Counter = Counter()
    month_2026: Counter = Counter()
    employee_counts: Counter = Counter()

    for o in items:
        # Status
        s = o.get('status', {})
        status = (s.get('key') or s.get('label')) if isinstance(s, dict) else str(s or '')
        if status:
            status_counts[status] += 1

        # Datum
        date_obj = o.get('date', {})
        created = (date_obj.get('created') or date_obj.get('create', '')) if isinstance(date_obj, dict) else ''
        try:
            year = int(str(created)[:4])
            year_counts[year] += 1
            if year == 2026:
                month_2026[str(created)[5:7]] += 1
        except (ValueError, TypeError):
            pass

        # Mitarbeiter (nur IDs, keine Namen)
        user = o.get('user', {})
        if isinstance(user, dict):
            emp_id = user.get('order') or user.get('update')
            if emp_id:
                employee_counts[str(emp_id)] += 1

    lines.append(f"Aufträge gesamt: {len(items)}")
    for yr in sorted([y for y in year_counts if y >= 2023], reverse=True):
        lines.append(f"Aufträge {yr}: {year_counts[yr]}")

    if status_counts:
        lines.append("\nNach Status:")
        for st, cnt in sorted(status_counts.items(), key=lambda x: -x[1]):
            lines.append(f"  {st}: {cnt}")

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

    # Fahrzeug-Statistiken
    try:
        raw_veh = _MEM_CACHE.get('sale/vehicles')
        if raw_veh:
            vs = build_vehicle_stats(raw_veh)
            lines.append(f"\nFahrzeugbestand Gesamt: {vs.get('unique_total', '?')} (davon Verkaufbar: {vs.get('verkaufbar', '?')}, Verkauft: {vs.get('verkauft', '?')})")
            
            # BI Context Data Injection
            if vs.get('nach_typ'):
                lines.append(f"  Typen: " + ", ".join(f"{k} ({v})" for k, v in vs['nach_typ'].items() if v > 0))
            if vs.get('getriebe'):
                lines.append(f"  Getriebe: " + ", ".join(f"{k} ({v})" for k, v in vs['getriebe'].items() if v > 0))
            if vs.get('heizung'):
                lines.append(f"  Heizung: " + ", ".join(f"{k} ({v})" for k, v in vs['heizung'].items() if v > 0))
            if vs.get('make_counts'):
                lines.append(f"  Marken: " + ", ".join(f"{k} ({v})" for k, v in sorted(vs['make_counts'].items(), key=lambda x: -x[1])[:15] if v > 0))
            if vs.get('condition_counts'):
                lines.append(f"  Zustand: " + ", ".join(f"{k} ({v})" for k, v in vs['condition_counts'].items() if v > 0))

            # Zu große Mappings auf Top-Werte begrenzen, damit Kontext nicht explodiert
            if vs.get('ps_counts'):
                top_ps = sorted(vs['ps_counts'].items(), key=lambda x: -x[1])[:12]
                lines.append(f"  Top PS-Klassen: " + ", ".join(f"{k} ({v})" for k, v in top_ps if v > 0))
            if vs.get('exact_lengths'):
                top_lengths = sorted(vs['exact_lengths'].items(), key=lambda x: -x[1])[:12]
                lines.append(f"  Top Fahrzeuglängen: " + ", ".join(f"{k} ({v})" for k, v in top_lengths if v > 0))
            if vs.get('year_counts'):
                top_years = sorted(vs['year_counts'].items(), key=lambda x: x[0], reverse=True)[:8]
                lines.append(f"  Modelljahre: " + ", ".join(f"{k} ({v})" for k, v in top_years if v > 0))
            if vs.get('preis_buckets'):
                lines.append(f"  Preissegmente: " + ", ".join(f"{k} ({v})" for k, v in vs['preis_buckets'].items() if v > 0))
            if vs.get('bed_types'):
                top_beds = sorted(vs['bed_types'].items(), key=lambda x: -x[1])[:8]
                lines.append(f"  Betten-Typen: " + ", ".join(f"{k} ({v})" for k, v in top_beds if v > 0))

    except Exception:
        pass

    return "\n".join(lines)


def _detect_customer_query(question: str):
    """Erkennt ob eine Frage eine Kunden-Datenabfrage ist (lokal zu beantworten).
    Gibt (True, params) oder (False, {}) zurück.
    Kunden-Abfragen werden LOKAL beantwortet – kein Datentransfer zu OpenAI."""
    import re as _re
    q = question.lower()

    # Stadtabfragen
    city_patterns = [
        r'kunden?\s+(?:in|aus|von)\s+([a-zäöüß][a-zäöüß\s\-]{2,30})',
        r'(?:wohnt|wohnen|wohnhaft)\s+in\s+([a-zäöüß][a-zäöüß\s\-]{2,30})',
        r'(?:aus|von)\s+([a-zäöüß][a-zäöüß\s\-]{2,30})\s+(?:haben|mit|kaufen|bestell|auftrag)',
        r'(?:stadt|ort)[:\s]+([a-zäöüß][a-zäöüß\s\-]{2,30})',
    ]
    for pat in city_patterns:
        m = _re.search(pat, q)
        if m:
            city = m.group(1).strip().rstrip('?.,! ').strip()
            if len(city) >= 3:
                return True, {'type': 'city', 'value': city}

    # PLZ-Abfragen
    zip_match = _re.search(r'\b(\d{5})\b', question)
    if zip_match and any(kw in q for kw in ['plz', 'postleitzahl', 'kunden', 'bestell', 'auftrag']):
        return True, {'type': 'zip', 'value': zip_match.group(1)}

    # Namensabfragen
    name_patterns = [
        r'(?:kunde|kundin|herr|frau)\s+([a-zäöüß]{2,30}(?:\s+[a-zäöüß]{2,30})?)',
        r'(?:name(?:ns)?|nachname|vorname)\s*[:\s]+([a-zäöüß]{2,30})',
    ]
    for pat in name_patterns:
        m = _re.search(pat, q)
        if m:
            return True, {'type': 'name', 'value': m.group(1).strip()}

    return False, {}


def _execute_local_customer_query(params: dict) -> tuple:
    """Führt Kunden-Suche LOKAL aus. Keine Daten werden an externe APIs gesendet.
    Gibt (answer: str, table: dict | None) zurück."""
    try:
        items = _get_orders()
    except Exception:
        return "Fehler: Auftragsdaten konnten nicht geladen werden.", None

    query_type = params.get('type')
    value = params.get('value', '').lower().strip()
    results = []

    for o in items:
        c = o.get('customer', {}) or {}
        if not isinstance(c, dict):
            continue

        match = False
        if query_type == 'city':
            city = (c.get('city') or '').lower()
            match = value in city or city.startswith(value[:min(len(value), 6)])
        elif query_type == 'zip':
            match = str(c.get('zipcode') or '').strip() == value
        elif query_type == 'name':
            fn = (c.get('first_name') or '').lower()
            ln = (c.get('last_name') or '').lower()
            match = value in fn or value in ln or value in f"{fn} {ln}"

        if match:
            nr = _extract_order_nr(o)
            s = o.get('status', {})
            status = (s.get('key') or s.get('label') or '?') if isinstance(s, dict) else str(s or '?')
            results.append({
                'nr': nr,
                'name': f"{c.get('first_name', '')} {c.get('last_name', '')}".strip(),
                'stadt': c.get('city', ''),
                'plz': str(c.get('zipcode', '')),
                'status': status,
            })

    if not results:
        return f"Keine Aufträge mit Suchkriterium '{params.get('value')}' gefunden.", None

    total = len(results)
    capped = results[:50]
    answer = f"{total} Aufträge gefunden (lokal ermittelt, keine Daten an KI gesendet)"
    if total > 50:
        answer += f" — Tabelle zeigt die ersten 50 von {total}"

    table = {
        'columns': ['Auftrags-Nr.', 'Name', 'PLZ', 'Stadt', 'Status'],
        'rows': [[r['nr'], r['name'], r['plz'], r['stadt'], r['status']] for r in capped],
    }
    if total > 50:
        table['footer'] = f"… und {total - 50} weitere Einträge"

    return answer, table


# ─── Mitarbeiter-Abfragen ─────────────────────────────────────────────────────

def _load_employee_names() -> dict:
    """Lädt employee_names.json aus verschiedenen Quellen. Sicherer Fallback für Docker."""
    # 1. ENV-Variable hat höchste Priorität
    env_path = os.getenv("EMPLOYEE_NAMES_PATH")
    if env_path and os.path.exists(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except: pass

    # 2. Direkt im Verzeichnis der main.py (wichtig für Docker CMD)
    local_path = Path(__file__).resolve().parent / "employee_names.json"
    if local_path.exists():
        try:
            with open(local_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except: pass

    # 3. Klassische Kandidaten-Suche
    for emp_file in _candidate_file_paths("employee_names.json"):
        if emp_file.exists():
            try:
                with open(emp_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except: pass

    return {}


def _detect_employee_query(question: str):
    """Erkennt Mitarbeiter-bezogene Auftrags-Abfragen (lokal zu beantworten).
    Gibt (True, params) oder (False, {}) zurück."""
    import re as _re
    q = question.lower()

    # Explizit nach Mitarbeiter-ID
    id_match = _re.search(r'(?:mitarbeiter|user|verkäufer|berater)\s*[:\s#]?\s*(\d{3,6})', q)
    if id_match:
        eid = id_match.group(1)
        emp_names = _load_employee_names()
        return True, {'type': 'employee_id', 'value': eid, 'name': emp_names.get(eid, f'#{eid}')}

    # Nach Name aus employee_names.json
    emp_names = _load_employee_names()
    if emp_names:
        name_patterns = [
            r'auftr[äa]ge?\s+(?:von|durch|von\s+mitarbeiter)\s+([a-zäöüß]+(?:\s+[a-zäöüß]+)?)',
            r'(?:von|durch)\s+([a-zäöüß]+(?:\s+[a-zäöüß]+)?)\s+(?:auftr[äa]ge?|bearbeitet|erstellt)',
            r'(?:mitarbeiter|verkäufer|berater)\s+([a-zäöüß]{2,}(?:\s+[a-zäöüß]{2,})?)',
            r'was\s+hat\s+([a-zäöüß]{2,}(?:\s+[a-zäöüß]{2,})?)\s+(?:gemacht|verkauft|erstellt)',
        ]
        for pat in name_patterns:
            m = _re.search(pat, q)
            if m:
                name_q = m.group(1).strip()
                for eid, ename in emp_names.items():
                    if name_q in ename.lower() or ename.lower().startswith(name_q[:4]):
                        return True, {'type': 'employee_id', 'value': eid, 'name': ename}

    # 4-6-stellige Zahl mit Kontext-Keyword
    if any(kw in q for kw in ['auftr', 'order', 'bearbeitet', 'erstellt', 'verkäufer', 'berater', 'mitarbeiter']):
        id_match2 = _re.search(r'\b(\d{4,6})\b', question)
        if id_match2:
            eid = id_match2.group(1)
            emp_names = _load_employee_names()
            return True, {'type': 'employee_id', 'value': eid, 'name': emp_names.get(eid, f'#{eid}')}

    return False, {}


def _execute_local_employee_query(params: dict) -> tuple:
    """Filtert Aufträge eines Mitarbeiters LOKAL.
    Gibt (answer: str, table: dict | None, chart: dict | None) zurück."""
    emp_id_str = str(params.get('value', ''))
    emp_name = params.get('name', f'#{emp_id_str}')

    try:
        orders = _get_orders()
    except Exception:
        return "Fehler: Auftragsdaten konnten nicht geladen werden.", None, None

    results = []
    status_counts: dict = {}

    for o in orders:
        user = o.get('user', {}) or {}
        uid = str(user.get('order') or user.get('update') or '')
        if uid != emp_id_str:
            continue
        s = o.get('status', {})
        status = (s.get('key') or s.get('label') or '?') if isinstance(s, dict) else str(s or '?')
        status_counts[status] = status_counts.get(status, 0) + 1
        ident = o.get('identifier', {}) or {}
        nr = _extract_order_nr(o)
        c = o.get('customer', {}) or {}
        city = c.get('city', '') if isinstance(c, dict) else ''
        results.append({'nr': nr, 'status': status, 'city': city})

    if not results:
        return f"Keine Aufträge für Mitarbeiter {emp_name} (ID: {emp_id_str}) gefunden.", None, None

    total = len(results)
    capped = results[:50]
    answer = f"Mitarbeiter {emp_name}: {total} Aufträge gesamt (lokal ermittelt)"
    if total > 50:
        answer += f" — Tabelle zeigt die ersten 50"

    table = {
        'columns': ['Auftrags-Nr.', 'Status', 'Stadt'],
        'rows': [[r['nr'], r['status'], r['city']] for r in capped],
    }
    if total > 50:
        table['footer'] = f"… und {total - 50} weitere Einträge"

    chart = {
        'type': 'bar',
        'title': f'Aufträge von {emp_name} nach Status',
        'data': [{'name': k, 'value': v} for k, v in sorted(status_counts.items(), key=lambda x: -x[1])],
    }

    return answer, table, chart


# ─── Auftrags-Detail-Abfragen ──────────────────────────────────────────────────

def _extract_order_nr(o: dict) -> str:
    """Gibt die beste verfügbare Auftrags-Nummer zurück.
    Prüft: identifier.uid, identifier.number, identifier.internal, nr, id."""
    ident = o.get('identifier', {}) or {}
    return str(
        ident.get('uid') or ident.get('number') or ident.get('internal')
        or o.get('nr') or o.get('id', '?')
    )


def _detect_order_lookup_query(question: str):
    """Erkennt Fragen nach einem konkreten Auftrag (z.B. Verkäufer, Kunde, Status).
    Gibt (True, params) oder (False, {}) zurück."""
    import re as _re
    q = question.lower()

    # Muster: Zahlenfolge mit ≥ 5 Stellen + Kontext-Keywords
    order_triggers = [
        'auftrag', 'order', 'verkäufer', 'verkaeufer', 'wer hat', 'wer war',
        'welcher', 'bearbeitet', 'erstellt', 'verkauft', 'uid',
    ]
    if not any(t in q for t in order_triggers):
        return False, {}

    m = _re.search(r'\b(\d{5,8})\b', question)
    if m:
        return True, {'order_nr': m.group(1)}

    return False, {}


def _execute_local_order_lookup(params: dict) -> tuple:
    """Sucht einen Auftrag nach Nummer und gibt Verkäufer + Details zurück.
    Gibt (answer: str, table: dict | None, chart: None) zurück."""
    order_nr = str(params.get('order_nr', '')).strip()
    emp_names = _load_employee_names()

    try:
        orders = _get_orders()
    except Exception:
        return 'Fehler: Auftragsdaten konnten nicht geladen werden.', None, None

    found = None
    for o in orders:
        if _extract_order_nr(o) == order_nr:
            found = o
            break

    if not found:
        return (
            f'Auftrag {order_nr} wurde in den lokalen Daten nicht gefunden. '
            f'Möglicherweise ist er zu neu – bitte "Daten-Sync" ausführen.',
            None, None
        )

    user = found.get('user', {}) or {}
    uid = str(user.get('order') or user.get('update') or '')
    seller_name = emp_names.get(uid, f'Mitarbeiter-ID {uid}' if uid else 'unbekannt')

    c = found.get('customer', {}) or {}
    cname = f"{c.get('first_name', '')} {c.get('last_name', '')}".strip() if isinstance(c, dict) else ''
    city  = c.get('city', '') if isinstance(c, dict) else ''

    s = found.get('status', {})
    status = (s.get('key') or s.get('label') or str(s)) if isinstance(s, dict) else str(s or '?')

    typ = found.get('type', '')
    ident = found.get('identifier', {}) or {}
    vin   = ident.get('vin', '')

    answer = f'Auftrag {order_nr}: Verkäufer ist **{seller_name}**'
    if uid and uid not in seller_name:
        answer += f' (ID: {uid})'

    table = {
        'columns': ['Feld', 'Wert'],
        'rows': [
            ['Auftrags-Nr.',  order_nr],
            ['Verkäufer',     f'{seller_name}' + (f' (ID: {uid})' if uid and uid not in seller_name else '')],
            ['Status',        status],
            ['Typ',           typ],
            ['Kunde',         cname],
            ['Stadt',         city],
            ['FIN/VIN',       vin or '–'],
        ],
    }

    return answer, table, None


def _tool_query_vehicle_inventory(args: dict) -> str:
    """Tool-Funktion für die KI: Ermöglicht punktgenaue Bestandsabfragen direkt in den Daten.
    Diese Funktion ist strikt isoliert und dient nur dem KI-Analysten als Daten-Quelle.
    """
    from collections import Counter
    try:
        # Nutzt bestehende, validierte Cache-Logik
        raw = get_cached_or_fetch('sale/vehicles', f"{SYSCARA_BASE}/sale/vehicles/")
        if not raw:
            return "Fehler: Fahrzeugdaten konnten nicht geladen werden (Supabase Limit oder Syscara offline)."
            
        # Nutzt bewährte Filter-Logik (map_and_filter)
        # Die Längen-Parameter müssen von Metern in cm umgerechnet werden, da map_and_filter cm erwartet
        if 'laengeMin' in args:
            args['laengeMin'] = args['laengeMin'] * 100
        if 'laengeMax' in args:
            args['laengeMax'] = args['laengeMax'] * 100

        vehicles = map_and_filter(raw, args)
        
        # Zusätzlicher Marken-Filter, da nicht nativ in map_and_filter
        make_q = str(args.get('make') or '').strip().lower()
        if make_q:
            vehicles = [v for v in vehicles if make_q in v.get('hersteller', '').lower()]
            
        count = len(vehicles)
        if count == 0:
            return "Ergebnis: 0 Fahrzeuge gefunden. (Prüfe Filter: Getriebe, Preis, Typ)"
            
        prices = [v['preis'] for v in vehicles if v['preis'] > 0]
        avg_preis = sum(prices) / len(prices) if prices else 0
        
        res = {
            "treffer_anzahl": count,
            "preis_durchschnitt": int(avg_preis),
            "status": "Erfolg",
            "hinweis": "Alle Daten basieren auf dem aktuellen Echtzeit-Bestand."
        }
        
        # Detail-Liste für kleine Treffer-Mengen
        if 0 < count <= 15:
            res["beispiele"] = [{"marke": v['hersteller'], "modell": v['modell'], "preis": v['preis_format'], "getriebe": v['getriebe']} for v in vehicles]
        else:
            top_makes = Counter(v['hersteller'] for v in vehicles).most_common(5)
            res["top_marken"] = dict(top_makes)
            
        return json.dumps(res, ensure_ascii=False)
    except Exception as e:
        return f"Technischer Fehler im Tool: {str(e)}"

@app.route('/api/ask', methods=['POST'])
def api_ask():
    """KI-Analyst: Beantwortet BI-Fragen via ChatGPT mit anonymisierten Unternehmensdaten.
    Kunden-Datenabfragen werden LOKAL bearbeitet – kein Datentransfer zu OpenAI (DSGVO)."""
    body = request.get_json(silent=True) or {}
    question = str(body.get('question', '')).strip()[:2000]
    if not question:
        return jsonify({"success": False, "error": "Keine Frage übergeben."}), 400

    # ── Fragen-Cache: Wiederholungsfragen sofort beantworten ──
    cached = _qcache_get(question)
    if cached:
        cached['cached'] = True
        return jsonify(cached)

    # ── Schritt 1a: Kunden-Abfrage lokal beantworten (DSGVO – kein Transfer persönl. Daten) ──
    is_customer_query, cq_params = _detect_customer_query(question)
    if is_customer_query:
        answer, table = _execute_local_customer_query(cq_params)
        resp = {"success": True, "answer": answer, "chart": None, "table": table, "source": "local"}
        _qcache_put(question, resp)
        return jsonify(resp)

    # ── Schritt 1b: Einzelner Auftrag-Lookup (Verkäufer, Status, Details) ──
    is_order_lookup, ol_params = _detect_order_lookup_query(question)
    if is_order_lookup:
        answer, table, chart = _execute_local_order_lookup(ol_params)
        resp = {"success": True, "answer": answer, "chart": chart, "table": table, "source": "local"}
        _qcache_put(question, resp)
        return jsonify(resp)

    # ── Schritt 1c: Mitarbeiter-Abfrage lokal beantworten (DSGVO) ──
    is_employee_query, eq_params = _detect_employee_query(question)
    if is_employee_query:
        answer, table, chart = _execute_local_employee_query(eq_params)
        resp = {"success": True, "answer": answer, "chart": chart, "table": table, "source": "local"}
        _qcache_put(question, resp)
        return jsonify(resp)

    # ── Schritt 2: BI-Frage → OpenAI mit anonymem Kontext ──
    if not HAS_OPENAI:
        return jsonify({"success": False, "error": "OpenAI-Bibliothek nicht installiert."}), 503

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return jsonify({"success": False, "error": "OPENAI_API_KEY nicht konfiguriert."}), 503

    # Aggregierte Statistiken als Kontext – KEINE personenbezogenen Daten
    bi_context = _build_bi_context()

    # Definition der Tools für Function Calling ( OpenAI Beta Standards)
    tools = [{
        "type": "function",
        "function": {
            "name": "query_inventory",
            "description": "Führt EXAKTE Abfragen im Fahrzeugbestand durch. Nutze dies für alle Fragen nach Bestandszahlen, Getriebearten, Preisen oder Marken.",
            "parameters": {
                "type": "object",
                "properties": {
                    "art": {"type": "string", "enum": ["kastenwagen", "integriert", "teilintegriert", "alkoven", "wohnwagen", "alle"]},
                    "getriebe": {"type": "string", "enum": ["automatik", "schaltung", "alle"]},
                    "zustand": {"type": "string", "enum": ["NEW", "USED", "alle"]},
                    "preisMax": {"type": "number"},
                    "preisMin": {"type": "number"},
                    "psMin": {"type": "number"},
                    "laengeMin": {"type": "number", "description": "Länge in METERN (z.B. 5.4 oder 6.0)"},
                    "laengeMax": {"type": "number", "description": "Länge in METERN (z.B. 7.5)"},
                    "make": {"type": "string", "description": "Markenname (z.B. Adria, Hymer)"}
                }
            }
        }
    }]

    system_prompt = (
        "Du bist ein intelligenter Business-Analyst für ein Reisemobil-Handelsunternehmen. "
        "Du beantwortest Fragen zu Fahrzeugbestand, Aufträgen, KPIs und Mitarbeiter-Performance. "
        "Wenn eine Frage nach Bestandszahlen gestellt wird (z.B. 'Wie viele...?'), nutze IMMER das Tool 'query_inventory'."
        "Antworte präzise auf Deutsch. Erfinde keine Zahlen.\n\n"
        "Wenn eine Antwort von einem Balken- oder Kreisdiagramm profitieren würde, füge am Ende "
        "einen JSON-Block in exakt diesem Format ein (kein Markdown-Fence, nur der Block selbst):\n"
        "[CHART]{\"type\": \"bar\", \"title\": \"Titel\", \"data\": [{\"name\": \"Label\", \"value\": 42}]}[/CHART]\n"
        f"Grobstatistik:\n{bi_context}"
    )

    try:
        import re as _re
        client = _openai.OpenAI(api_key=api_key)
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ]
        
        # 1. Anfrage an OpenAI mit Tools
        completion = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            tools=tools,
            tool_choice="auto"
        )
        
        choice = completion.choices[0].message
        
        # 2. Tool-Calls abarbeiten (Function Calling)
        if choice.tool_calls:
            messages.append(choice)
            for tool_call in choice.tool_calls:
                if tool_call.function.name == "query_inventory":
                    fn_args = json.loads(tool_call.function.arguments)
                    tool_res = _tool_query_vehicle_inventory(fn_args)
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "name": "query_inventory",
                        "content": tool_res
                    })
            
            # Zweite Antwort generieren (jetzt mit den Daten aus dem Tool)
            final_comp = client.chat.completions.create(
                model="gpt-4o",
                messages=messages
            )
            raw = final_comp.choices[0].message.content or ""
        else:
            raw = choice.content or ""

        # Chart-Extraktion
        chart = None
        chart_match = _re.search(r'\[CHART\](.*?)\[/CHART\]', raw, _re.DOTALL)
        if chart_match:
            try:
                chart = json.loads(chart_match.group(1).strip())
                raw = raw[:chart_match.start()].rstrip() + raw[chart_match.end():]
            except (json.JSONDecodeError, ValueError):
                chart = None

        resp = {"success": True, "answer": raw.strip(), "chart": chart, "table": None, "source": "openai"}
        _qcache_put(question, resp)
        return jsonify(resp)
    except Exception as e:
        import traceback; traceback.print_exc()
        err_str = str(e)
        if "insufficient_quota" in err_str or "billing" in err_str.lower() or "credit" in err_str.lower():
            return jsonify({"success": False, "error": "⚠️ OpenAI-Guthaben aufgebraucht."}), 402
        if "authentication" in err_str.lower() or "401" in err_str:
            return jsonify({"success": False, "error": "⚠️ Ungültiger OpenAI API-Key."}), 401
        if "rate_limit" in err_str.lower() or "429" in err_str:
            return jsonify({"success": False, "error": "⚠️ Zu viele Anfragen – bitte kurz warten."}), 429
        return jsonify({"success": False, "error": f"KI-Fehler: {err_str}"}), 500


if __name__ == '__main__':
    port = int(os.getenv("PORT", 5000))
    print(f"Syscara Python API auf Port {port}...", flush=True)
    start_sync_thread()
    app.run(host='0.0.0.0', port=port, debug=False)
