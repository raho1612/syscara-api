import os
import requests
from flask import jsonify, request
from core.config import SYSCARA_BASE, SYSCARA_USER, SYSCARA_PASS, HAS_OPENAI, HAS_CLAUDE
from core.database import get_cached_or_fetch, iter_items
from core.utils import map_and_filter, normalize_collection_items, extract_order_datetime
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
                r = requests.get(url, auth=HTTPBasicAuth(SYSCARA_USER, SYSCARA_PASS), timeout=60)
                r.raise_for_status()
                items = iter_items(r.json())
                return jsonify({"success": True, "count": len(items), "vehicles": items})
            except Exception: pass
        
        raw = get_cached_or_fetch('sale/vehicles', f"{SYSCARA_BASE}/sale/vehicles/")
        items = iter_items(raw)
        return jsonify({"success": True, "count": len(items), "vehicles": items})

    @app.route('/api/evaluate', methods=['POST'])
    def api_evaluate():
        if not HAS_OPENAI: return jsonify({"success": False, "error": "OpenAI nicht installiert."}), 503
        body = request.get_json(silent=True) or {}
        data = str(body.get('data', ''))[:5000]
        instruction = str(body.get('instruction', ''))[:3000] or "Agiere als Fahrzeugexperte."
        
        import openai
        client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        try:
            res = client.chat.completions.create(model="gpt-4o", messages=[{"role": "system", "content": instruction}, {"role": "user", "content": data}])
            return jsonify({"success": True, "text": res.choices[0].message.content})
        except Exception as e: return jsonify({"success": False, "error": str(e)}), 500
