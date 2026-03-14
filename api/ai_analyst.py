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
    _build_bi_context, map_and_filter, _get_orders
)

def register_ai_analyst_routes(app):
    
    def _tool_query_inventory(args: dict) -> str:
        from collections import Counter
        try:
            year = args.get('jahrMin') or args.get('jahrMax')
            is_sales_query = args.get('isSalesQuery') or (year and year >= 2024 and 'sale' in str(args).lower())
            
            if is_sales_query:
                raw_orders = _get_orders()
                results = []
                for o in raw_orders:
                    d_str = (o.get('date') or {}).get('create') or ''
                    if year and not d_str.startswith(str(year)): continue
                    
                    veh = o.get('vehicle') or o
                    typeof = str(o.get('typeof') or veh.get('typeof') or '').lower()
                    if args.get('art') and args.get('art').lower() not in typeof: continue
                    
                    dim = o.get('dimensions') or veh.get('dimensions') or {}
                    laenge = dim.get('length', 0)
                    if args.get('laengeMin') and laenge < float(args.get('laengeMin')) * 100: continue
                    if args.get('laengeMax') and laenge > float(args.get('laengeMax')) * 100: continue
                    
                    # Keyword search in orders too
                    if args.get('q'):
                        q_str = str(args.get('q')).lower()
                        full_txt = str(o).lower()
                        if q_str not in full_txt: continue

                    results.append(o)
                
                count = len(results)
                return json.dumps({"treffer_anzahl": count, "kontext": "Historische Verkäufe (Aufträge)", "jahr": year, "status": "Erfolg"}, ensure_ascii=False)

            # Standard Inventory Search
            raw = get_cached_or_fetch('sale/vehicles', f"{SYSCARA_BASE}/sale/vehicles/")
            if not raw: return "Fehler: Fahrzeugdaten konnten nicht geladen werden."
            
            vehicles = map_and_filter(raw, args)
            make_q = str(args.get('make') or '').strip().lower()
            if make_q: vehicles = [v for v in vehicles if make_q in v.get('hersteller', '').lower()]
            
            count = len(vehicles)
            if count == 0: return "Ergebnis: 0 Fahrzeuge im aktuellen Bestand gefunden."
            
            prices = [v['preis'] for v in vehicles if v['preis'] > 0]
            avg_preis = sum(prices) / len(prices) if prices else 0
            ek_prices = [float(v.get('ek_preis') or 0) for v in vehicles if float(v.get('ek_preis') or 0) > 0]
            avg_ek = sum(ek_prices) / len(ek_prices) if ek_prices else 0
            
            res = {
                "treffer_anzahl": count, 
                "basis_preis_durchschnitt": int(avg_preis),
                "einkaufspreis_durchschnitt": int(avg_ek),
                "status": "Erfolg"
            }
            if count <= 15:
                res["beispiele"] = [
                    {
                        "marke": v['hersteller'], 
                        "modell": v['modell'], 
                        "preis": v['preis_format'], 
                        "laenge": v['laenge_m'],
                        "ausstattung": v.get('ausstattung', '')[:200]
                    } for v in vehicles
                ]
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

        # DSGVO-Bereich
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
        
        import openai
        client = openai.OpenAI(api_key=api_key)
        
        tools = [{
            "type": "function", 
            "function": {
                "name": "query_inventory", 
                "description": "UNIVERSAL-SUCHE für Bestand (VK/EK/Ausstattung) und historische Aufträge.", 
                "parameters": {
                    "type": "object", 
                    "properties": {
                        "q": {"type": "string", "description": "Suchbegriff für Ausstattung (z.B. 'hubbett', 'markise', 'solar')"},
                        "art": {"type": "string", "description": "Fahrzeugtyp (z.B. Kastenwagen)"}, 
                        "laengeMin": {"type": "number", "description": "Mindestlänge (m)"}, 
                        "laengeMax": {"type": "number", "description": "Maximallänge (m)"},
                        "hubbett": {"type": "boolean", "description": "Expliziter Filter für Hubbett"},
                        "dusche": {"type": "boolean", "description": "Expliziter Filter für Sep. Dusche"},
                        "jahrMin": {"type": "integer", "description": "Jahr für Verkäufe"},
                        "isSalesQuery": {"type": "boolean", "description": "Soll im Auftragsarchiv (Historie) gesucht werden?"}
                    }
                }
            }
        }]
        
        messages = [
            {"role": "system", "content": (
                "Du bist der allwissende Syscara-Analyst. Du hast VOLLZUGRIFF auf alle Daten via Tool.\n"
                "Nutze den Parameter 'q' im Tool 'query_inventory', um nach JEDEM beliebigen Ausstattungsmerkmal zu suchen (z.B. Klima, Solar, TV).\n"
                "Wenn der User nach Hubbetten fragt, nutze hubbett=True UND q='hubbett'.\n"
                "5,40m Kastenwagen werden im System oft mit Länge 540 oder 541 cm geführt.\n\n"
                f"{bi_context}"
            )},
            {"role": "user", "content": question}
        ]
        
        try:
            comp = client.chat.completions.create(model="gpt-4o", messages=messages, tools=tools, tool_choice="auto")
            msg = comp.choices[0].message

            if msg.tool_calls:
                messages.append(msg)
                for tc in msg.tool_calls:
                    if tc.function.name == "query_inventory":
                        res = _tool_query_inventory(json.loads(tc.function.arguments))
                        messages.append({"role": "tool", "tool_call_id": tc.id, "name": "query_inventory", "content": res})
                final = client.chat.completions.create(model="gpt-4o", messages=messages)
                raw = final.choices[0].message.content or ""
            else:
                raw = msg.content or ""

            resp = {"success": True, "answer": raw.strip(), "chart": None, "table": None, "source": "openai"}
            _qcache_put(question, resp)
            return jsonify(resp)
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500
