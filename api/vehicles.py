import os
import requests
from flask import jsonify, request
from core.config import SYSCARA_BASE, SYSCARA_USER, SYSCARA_PASS
from core.database import (
    get_cached_or_fetch,
    iter_items,
    load_from_supabase_chunked
)
from core.utils import normalize_collection_items, extract_order_datetime
from services.bi_service import map_and_filter
from shared.vehicle_stats import build_vehicle_stats
from requests.auth import HTTPBasicAuth

def register_vehicle_routes(app):

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
            try:
                auth = HTTPBasicAuth(SYSCARA_USER, SYSCARA_PASS)
                r = requests.get(url, auth=auth, timeout=60)
                r.raise_for_status()
                items = iter_items(r.json())
                return jsonify({"success": True, "count": len(items), "vehicles": items})
            except Exception:
                pass
        
        raw = get_cached_or_fetch('sale/vehicles', f"{SYSCARA_BASE}/sale/vehicles/")
        items = iter_items(raw)
        return jsonify({"success": True, "count": len(items), "vehicles": items})

    @app.route('/api/orders', methods=['GET', 'POST'])
    def api_orders():
        year = request.args.get('year')
        if year and year != 'alle':
            url = f"{SYSCARA_BASE}/sale/orders/?update={year}-01-01"
            try:
                auth = HTTPBasicAuth(SYSCARA_USER, SYSCARA_PASS)
                r = requests.get(url, auth=auth, timeout=60)
                r.raise_for_status()
                items = normalize_collection_items(r.json(), 'orders')
                try:
                    y_num = int(year)
                    items = [i for i in items if extract_order_datetime(i) and extract_order_datetime(i).year == y_num]
                except (ValueError, TypeError):
                    pass
                return jsonify({"success": True, "count": len(items), "orders": items})
            except Exception:
                pass

        raw = get_cached_or_fetch('sale/orders', f"{SYSCARA_BASE}/sale/orders/?update=2024-01-01")
        items = normalize_collection_items(raw, 'orders')
        if year and year != 'alle':
            try:
                y_num = int(year)
                items = [i for i in items if (extract_order_datetime(i) and
                                              extract_order_datetime(i).year == y_num)]
            except (ValueError, TypeError):
                pass
        return jsonify({"success": True, "count": len(items), "orders": items})

    @app.route('/api/equipment', methods=['GET', 'POST'])
    def api_equipment():
        year = request.args.get('year')
        if year and year != 'alle':
            url = f"{SYSCARA_BASE}/sale/equipment/?modelyear={year}"
            auth = HTTPBasicAuth(SYSCARA_USER, SYSCARA_PASS)
            try:
                r = requests.get(url, auth=auth, timeout=60)
                r.raise_for_status()
                return jsonify({"success": True, "equipment": iter_items(r.json())})
            except Exception:
                raw = load_from_supabase_chunked('sale/equipment')
                return jsonify({"success": True, "equipment": iter_items(raw)})

        raw = get_cached_or_fetch('sale/equipment', f"{SYSCARA_BASE}/sale/equipment/")
        return jsonify({"success": True, "equipment": iter_items(raw)})

    @app.route('/api/stats', methods=['GET'])
    def api_stats():
        # Wir nutzen primär den schnellen Cache, außer allow_stale ist explizit False
        # (Standard ist jetzt get_cached_or_fetch für Speed)
        try:
            raw = get_cached_or_fetch('sale/vehicles', f"{SYSCARA_BASE}/sale/vehicles/")
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 503
        
        stats = build_vehicle_stats(
            raw,
            enable_offset=os.getenv("SYSCARA_KPI_OFFSET_ENABLE", "0") == "1",
            offset_trigger=int(os.getenv("SYSCARA_KPI_OFFSET_TRIGGER", "483")),
            offset_value=int(os.getenv("SYSCARA_KPI_OFFSET_VALUE", "2")),
        )
        return jsonify({"success": True, "stats": stats})
