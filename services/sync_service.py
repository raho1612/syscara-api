import threading
import time

from core.config import SYSCARA_BASE
from core.database import _MEM_CACHE, _QUESTION_CACHE, get_cached_or_fetch


def sync_all_now():
    print("\n--- [BACKGROUND SYNC] Start ---", flush=True)
    endpoints = {
        "sale/equipment": f"{SYSCARA_BASE}/sale/equipment/",
        "sale/orders":    f"{SYSCARA_BASE}/sale/orders/?update=2024-01-01",
        "sale/lists":     f"{SYSCARA_BASE}/sale/lists/?list=pictures",
        "sale/vehicles":  f"{SYSCARA_BASE}/sale/vehicles/",
        "sale/ads":       f"{SYSCARA_BASE}/sale/ads/"
    }
    for name, url in endpoints.items():
        try:
            get_cached_or_fetch(name, url)
        except Exception as e:
            print(f"[SYNC ERROR] {name}: {e}", flush=True)
    print("--- [BACKGROUND SYNC] Fertig ---\n", flush=True)

def background_sync_loop():
    time.sleep(5)
    while True:
        sync_all_now()
        time.sleep(3600)

def start_sync_thread(supabase):
    if not supabase: return
    t = threading.Thread(target=background_sync_loop, daemon=True)
    t.start()
    print("[SYNC] Hintergrund-Thread läuft.", flush=True)

def register_sync_routes(app):
    @app.route('/api/sync', methods=['GET', 'POST'])
    def api_sync():
        _MEM_CACHE.clear()
        _QUESTION_CACHE.clear()
        t = threading.Thread(target=sync_all_now, daemon=True)
        t.start()
        return {"success": True, "message": "Background sync started"}
