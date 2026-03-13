import os
import json
import time
from flask import jsonify, request
from core.config import HAS_OPENAI, SYSCARA_BASE
from core.database import get_cached_or_fetch, _qcache_get, _qcache_put
from services.bi_service import (
    _detect_customer_query, _execute_local_customer_query,
    _detect_order_lookup_query, _execute_local_order_lookup,
    _detect_employee_query, _execute_local_employee_query,
    _build_bi_context, map_and_filter
)

def register_ai_analyst_routes(app):
    
    def _tool_query_vehicle_inventory(args: dict) -> str:
        from collections import Counter
        try:
            raw = get_cached_or_fetch('sale/vehicles', f"{SYSCARA_BASE}/sale/vehicles/")
            if not raw: return "Fehler: Fahrzeugdaten konnten nicht geladen werden."
            
            # Korrektur für Längen-Parameter
            filter_args = args.copy()
            if 'laengeMin' in filter_args: filter_args['laengeMin'] = float(filter_args['laengeMin']) * 100
            if 'laengeMax' in filter_args: filter_args['laengeMax'] = float(filter_args['laengeMax']) * 100

            vehicles = map_and_filter(raw, filter_args)
            make_q = str(filter_args.get('make') or '').strip().lower()
            if make_q: vehicles = [v for v in vehicles if make_q in v.get('hersteller', '').lower()]
            
            count = len(vehicles)
            if count == 0: return "Ergebnis: 0 Fahrzeuge gefunden."
            
            prices = [v['preis'] for v in vehicles if v['preis'] > 0]
            avg_preis = sum(prices) / len(prices) if prices else 0
            
            res = {"treffer_anzahl": count, "preis_durchschnitt": int(avg_preis), "status": "Erfolg"}
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
        body = request.get_json(silent=True) or {}
        question = str(body.get('question', '')).strip()[:2000]
        if not question: return jsonify({"success": False, "error": "Keine Frage übergeben."}), 400

        cached = _qcache_get(question)
        if cached: return jsonify({**cached, "cached": True})

        # Lokale Detektionen (DSGVO)
        is_cust, cp = _detect_customer_query(question)
        if is_cust:
            a, t = _execute_local_customer_query(cp)
            r = {"success": True, "answer": a, "chart": None, "table": t, "source": "local"}
            _qcache_put(question, r); return jsonify(r)

        is_ord, op = _detect_order_lookup_query(question)
        if is_ord:
            a, t, c = _execute_local_order_lookup(op)
            r = {"success": True, "answer": a, "chart": c, "table": t, "source": "local"}
            _qcache_put(question, r); return jsonify(r)

        is_emp, ep = _detect_employee_query(question)
        if is_emp:
            a, t, c = _execute_local_employee_query(ep)
            r = {"success": True, "answer": a, "chart": c, "table": t, "source": "local"}
            _qcache_put(question, r); return jsonify(r)

        if not HAS_OPENAI: return jsonify({"success": False, "error": "OpenAI nicht installiert."}), 503
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key: return jsonify({"success": False, "error": "Key fehlt."}), 503

        bi_context = _build_bi_context()
        
        # Function Calling Definition & OpenAI Call
        import openai
        client = openai.OpenAI(api_key=api_key)
        tools = [{"type": "function", "function": {"name": "query_inventory", "description": "Abfrage im Fahrzeugbestand.", "parameters": {"type": "object", "properties": {"art": {"type": "string"}, "getriebe": {"type": "string"}, "laengeMin": {"type": "number"}, "laengeMax": {"type": "number"}, "make": {"type": "string"}}}}}]
        
        messages = [{"role": "system", "content": f"Du bist ein intelligenter Business-Analyst für ein Reisemobil-Handelsunternehmen. Statistik:\n{bi_context}"}, {"role": "user", "content": question}]
        
        try:
            comp = client.chat.completions.create(model="gpt-4o", messages=messages, tools=tools, tool_choice="auto")
            msg = comp.choices[0].message

            if msg.tool_calls:
                messages.append(msg)
                for tc in msg.tool_calls:
                    if tc.function.name == "query_inventory":
                        res = _tool_query_vehicle_inventory(json.loads(tc.function.arguments))
                        messages.append({"role": "tool", "tool_call_id": tc.id, "name": "query_inventory", "content": res})
                final = client.chat.completions.create(model="gpt-4o", messages=messages)
                raw = final.choices[0].message.content or ""
            else:
                raw = msg.content or ""

            import re
            chart = None
            match = re.search(r'\[CHART\](.*?)\[/CHART\]', raw, re.DOTALL)
            if match:
                try:
                    chart = json.loads(match.group(1).strip())
                    raw = raw[:match.start()].rstrip() + raw[match.end():]
                except: pass

            resp = {"success": True, "answer": raw.strip(), "chart": chart, "table": None, "source": "openai"}
            _qcache_put(question, resp)
            return jsonify(resp)
        except Exception as e:
            import traceback; traceback.print_exc()
            return jsonify({"success": False, "error": str(e)}), 500
