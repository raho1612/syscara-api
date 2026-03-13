import os
import math
import time
import json
import requests
from pathlib import Path
from requests.auth import HTTPBasicAuth
from core.config import SYSCARA_BASE, SYSCARA_USER, SYSCARA_PASS, ROOT_DIR, CURRENT_DIR

# Supabase Initialisierung
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = None

if SUPABASE_URL and SUPABASE_KEY:
    try:
        from supabase import ClientOptions, create_client
        options = ClientOptions(postgrest_client_timeout=120, storage_client_timeout=120)
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY, options=options)
    except Exception as e:
        print(f"[ERROR] Supabase Init Fehler: {e}", flush=True)

# Cache Verzeichnis
CACHE_DIR = CURRENT_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True)
CHUNK_SIZE = 500

# In-Memory Caches
_MEM_CACHE = {}
_QUESTION_CACHE = {}
_QUESTION_CACHE_TTL_LOCAL = 3600
_QUESTION_CACHE_TTL_BI    = 600

def _qcache_key(q: str) -> str:
    return q.strip().lower()

def _qcache_get(q: str) -> dict | None:
    key = _qcache_key(q)
    entry = _QUESTION_CACHE.get(key)
    if not entry: return None
    ttl = _QUESTION_CACHE_TTL_LOCAL if entry.get('source') == 'local' else _QUESTION_CACHE_TTL_BI
    if time.time() - entry['ts'] < ttl: return entry['response']
    del _QUESTION_CACHE[key]; return None

def _qcache_put(q: str, response: dict):
    key = _qcache_key(q)
    _QUESTION_CACHE[key] = {'ts': time.time(), 'response': response, 'source': response.get('source', 'openai')}
    if len(_QUESTION_CACHE) > 200:
        for k in sorted(_QUESTION_CACHE.items(), key=lambda x: x[1]['ts'])[:40]: _QUESTION_CACHE.pop(k[0], None)

def iter_items(raw):
    if isinstance(raw, dict): return list(raw.values())
    if isinstance(raw, list): return raw
    return []

def save_to_supabase_chunked(endpoint_name, data):
    if not supabase: return False
    items = iter_items(data)
    total_items = len(items)
    is_dict = isinstance(data, dict)
    keys_list = list(data.keys()) if is_dict else []
    
    if total_items == 0: return True
    
    num_chunks = math.ceil(total_items / CHUNK_SIZE)
    timestamp = int(time.time())

    for i in range(num_chunks):
        start_idx = i * CHUNK_SIZE
        end_idx = min((i + 1) * CHUNK_SIZE, total_items)
        chunk_key = f"{endpoint_name}#chunk{i}"
        chunk_data = {k: data[k] for k in keys_list[start_idx:end_idx]} if is_dict else items[start_idx:end_idx]

        try:
            supabase.table("api_cache").upsert({"key": chunk_key, "data": chunk_data, "updated_at": timestamp}).execute()
        except Exception:
            return False

    try:
        local_path = CACHE_DIR / f"{endpoint_name.replace('/', '_')}.json"
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except: pass
    return True

def load_from_supabase_chunked(endpoint_name):
    if not supabase: return {}
    try:
        meta_res = supabase.table("api_cache").select("data").eq("key", f"{endpoint_name}#meta").execute()
        if not meta_res.data:
            res = supabase.table("api_cache").select("data").eq("key", endpoint_name).execute()
            return res.data[0]["data"] if res.data else {}

        meta = meta_res.data[0]["data"]
        num_chunks = meta.get("chunks", 0)
        is_dict = meta.get("is_dict", False)
        combined = {} if is_dict else []

        for i in range(num_chunks):
            res = supabase.table("api_cache").select("data").eq("key", f"{endpoint_name}#chunk{i}").execute()
            if res.data:
                if is_dict: combined.update(res.data[0]["data"])
                else: combined.extend(res.data[0]["data"])
        return combined
    except:
        local_path = CACHE_DIR / f"{endpoint_name.replace('/', '_')}.json"
        if local_path.exists():
            with open(local_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

def get_cached_or_fetch(endpoint_name, url):
    if endpoint_name in _MEM_CACHE: return _MEM_CACHE[endpoint_name]
    try:
        response = requests.get(url, auth=HTTPBasicAuth(SYSCARA_USER, SYSCARA_PASS), timeout=60)
        response.raise_for_status()
        data = response.json()
        save_to_supabase_chunked(endpoint_name, data)
        _MEM_CACHE[endpoint_name] = data
        return data
    except Exception:
        data = load_from_supabase_chunked(endpoint_name)
        if data: _MEM_CACHE[endpoint_name] = data
        return data

def fetch_live_then_cache(endpoint_name, url, *, allow_stale_fallback=False):
    try:
        response = requests.get(url, auth=HTTPBasicAuth(SYSCARA_USER, SYSCARA_PASS), timeout=60)
        response.raise_for_status()
        data = response.json()
        save_to_supabase_chunked(endpoint_name, data)
        return data
    except Exception:
        if allow_stale_fallback: return load_from_supabase_chunked(endpoint_name)
        raise
