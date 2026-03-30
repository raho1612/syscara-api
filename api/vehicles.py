import os

import requests
from core.config import SYSCARA_BASE, SYSCARA_PASS, SYSCARA_USER
from core.database import (
    get_cached_or_fetch,
    iter_items,
    load_from_supabase_chunked,
)
from core.utils import extract_order_datetime, normalize_collection_items
from flask import jsonify, request
from requests.auth import HTTPBasicAuth
from services.bi_service import map_and_filter
from shared.vehicle_stats import build_vehicle_stats


def register_vehicle_routes(app):

    @app.route("/api/ads", methods=["POST"])
    def api_ads():
        raw = get_cached_or_fetch("sale/ads", f"{SYSCARA_BASE}/sale/ads/")
        filters = request.get_json(silent=True) or {}
        vehicles = map_and_filter(raw, filters)
        return jsonify({"success": True, "count": len(vehicles), "vehicles": vehicles})

    @app.route("/api/vehicles", methods=["GET", "POST"])
    def api_vehicles():
        year = request.args.get("year")
        if year and year != "alle":
            url = f"{SYSCARA_BASE}/sale/vehicles/?modelyear={year}"
            try:
                r = requests.get(
                    url, auth=HTTPBasicAuth(SYSCARA_USER, SYSCARA_PASS), timeout=60
                )
                r.raise_for_status()
                items = iter_items(r.json())
                return jsonify(
                    {"success": True, "count": len(items), "vehicles": items}
                )
            except Exception:
                pass

        raw = get_cached_or_fetch("sale/vehicles", f"{SYSCARA_BASE}/sale/vehicles/")
        items = iter_items(raw)
        return jsonify({"success": True, "count": len(items), "vehicles": items})

    @app.route("/api/orders", methods=["GET", "POST"])
    def api_orders():
        year = request.args.get("year")
        if year and year != "alle":
            url = f"{SYSCARA_BASE}/sale/orders/?update={year}-01-01"
            try:
                r = requests.get(
                    url, auth=HTTPBasicAuth(SYSCARA_USER, SYSCARA_PASS), timeout=60
                )
                r.raise_for_status()
                items = normalize_collection_items(r.json(), "orders")
                try:
                    y_num = int(year)
                    items = [
                        i
                        for i in items
                        if extract_order_datetime(i)
                        and extract_order_datetime(i).year == y_num
                    ]
                except:
                    pass
                return jsonify({"success": True, "count": len(items), "orders": items})
            except Exception:
                pass

        raw = get_cached_or_fetch(
            "sale/orders", f"{SYSCARA_BASE}/sale/orders/?update=2024-01-01"
        )
        items = normalize_collection_items(raw, "orders")
        if year and year != "alle":
            try:
                y_num = int(year)
                items = [
                    i
                    for i in items
                    if extract_order_datetime(i)
                    and extract_order_datetime(i).year == y_num
                ]
            except:
                pass
        return jsonify({"success": True, "count": len(items), "orders": items})

    @app.route("/api/equipment", methods=["GET", "POST"])
    def api_equipment():
        year = request.args.get("year")
        if year and year != "alle":
            url = f"{SYSCARA_BASE}/sale/equipment/?modelyear={year}"
            try:
                r = requests.get(
                    url, auth=HTTPBasicAuth(SYSCARA_USER, SYSCARA_PASS), timeout=60
                )
                r.raise_for_status()
                items = iter_items(r.json())
                return jsonify(
                    {"success": True, "count": len(items), "equipment": items}
                )
            except:
                raw = load_from_supabase_chunked("sale/equipment")
                items = iter_items(raw)
                return jsonify(
                    {"success": True, "count": len(items), "equipment": items}
                )
        raw = get_cached_or_fetch("sale/equipment", f"{SYSCARA_BASE}/sale/equipment/")
        items = iter_items(raw)
        return jsonify({"success": True, "count": len(items), "equipment": items})

    @app.route("/api/stats", methods=["GET"])
    def api_stats():
        try:
            raw = get_cached_or_fetch("sale/vehicles", f"{SYSCARA_BASE}/sale/vehicles/")
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 503
        stats = build_vehicle_stats(
            raw,
            enable_offset=os.getenv("SYSCARA_KPI_OFFSET_ENABLE", "0") == "1",
            offset_trigger=int(os.getenv("SYSCARA_KPI_OFFSET_TRIGGER", "483")),
            offset_value=int(os.getenv("SYSCARA_KPI_OFFSET_VALUE", "2")),
        )
        return jsonify({"success": True, "stats": stats})
