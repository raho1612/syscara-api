import json
import os
import pathlib
import re
from collections import defaultdict
from datetime import datetime

import openai
from core.config import HAS_OPENAI, SYSCARA_BASE
from core.database import _qcache_get, _qcache_put, get_cached_or_fetch
from flask import jsonify, request, session
from services.bi_service import (
    _build_bi_context,
    _detect_customer_query,
    _detect_employee_query,
    _detect_order_lookup_query,
    _execute_local_customer_query,
    _execute_local_employee_query,
    _execute_local_order_lookup,
    _get_orders,
    map_and_filter,
)
from shared.vehicle_stats import build_vehicle_identity_key, classify_sale_kpi_bucket


def _normalize_vehicle_type(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw or raw == "alle":
        return "alle"
    mapping = {
        "alkov": "alkofen",
        "alkoven": "alkofen",
        "alkofen": "alkofen",
        "integriert": "integriert",
        "teilintegriert": "teilintegriert",
        "ti": "teilintegriert",
        "kasten": "kastenwagen",
        "kastenwagen": "kastenwagen",
        "camper": "kastenwagen",
        "campervan": "kastenwagen",
        "wohnwagen": "wohnwagen",
    }
    return mapping.get(raw, raw)


def _vehicle_matches_sales_filters(vehicle: dict, filters: dict) -> bool:
    art_filter = _normalize_vehicle_type(filters.get("art"))
    typeof_raw = str(vehicle.get("typeof") or "").lower()
    vehicle_type = "default"
    if vehicle.get("type") == "Caravan":
        vehicle_type = "wohnwagen"
    elif "alkov" in typeof_raw:
        vehicle_type = "alkofen"
    elif "teilintegriert" in typeof_raw:
        vehicle_type = "teilintegriert"
    elif "integriert" in typeof_raw:
        vehicle_type = "integriert"
    elif "kastenwagen" in typeof_raw or "camper" in typeof_raw:
        vehicle_type = "kastenwagen"

    if art_filter != "alle" and art_filter != vehicle_type:
        return False

    dimensions = vehicle.get("dimensions") or {}
    if isinstance(dimensions, list):
        dimensions = dimensions[0] if dimensions else {}
    length_cm = float(dimensions.get("length") or 0)
    if filters.get("laengeMin") and length_cm < float(filters.get("laengeMin")) * 100:
        return False
    if filters.get("laengeMax") and length_cm > float(filters.get("laengeMax")) * 100:
        return False

    return True


def _build_sales_vehicle_index() -> dict[str, tuple[str, dict]]:
    raw = get_cached_or_fetch("sale/vehicles_full", f"{SYSCARA_BASE}/sale/vehicles/")
    index: dict[str, tuple[str, dict]] = {}

    iterable = raw.values() if isinstance(raw, dict) else raw or []
    for position, vehicle in enumerate(iterable):
        if not isinstance(vehicle, dict):
            continue
        if classify_sale_kpi_bucket(vehicle) != "sold":
            continue

        stable_key = build_vehicle_identity_key(vehicle, position)
        identifier = vehicle.get("identifier") or {}
        if isinstance(identifier, list):
            identifier = identifier[0] if identifier else {}
        join_keys = [
            str(vehicle.get("id") or "").strip(),
            str(vehicle.get("uid") or "").strip(),
            str(identifier.get("internal") or "").strip(),
            str(identifier.get("uid") or "").strip(),
            str(identifier.get("serial") or "").strip(),
            str(identifier.get("vin") or "").strip().upper(),
        ]
        for join_key in join_keys:
            if join_key:
                index[join_key] = (stable_key, vehicle)
    return index


def _extract_order_date_prefix(order: dict) -> str:
    date_obj = order.get("date")
    if isinstance(date_obj, dict):
        for sub_key in ("created", "delivery", "updated"):
            value = date_obj.get(sub_key)
            if isinstance(value, str) and len(value) >= 10:
                return value[:10]
    for key in ("created_at", "created", "create_date", "createdAt", "order_date"):
        value = order.get(key)
        if isinstance(value, str) and len(value) >= 10:
            return value[:10]
    return ""


def _extract_employee_from_order(order: dict) -> str:
    """Extrahiere Mitarbeiter aus Order.user.order"""
    user = order.get("user") or {}
    if isinstance(user, dict):
        emp_id = str(user.get("order") or "").strip()
        if emp_id:
            return emp_id
    return ""


def _extract_order_join_keys(order: dict) -> list[str]:
    identifier = order.get("identifier") or {}
    if isinstance(identifier, list):
        identifier = identifier[0] if identifier else {}
    keys = [
        str(identifier.get("internal") or "").strip(),
        str(identifier.get("vin") or "").strip().upper(),
        str(identifier.get("uid") or "").strip(),
    ]
    return [key for key in keys if key]


def _filter_orders_by_date(order: dict, filters: dict) -> bool:
    """Check if an order matches the date filters."""
    order_date = _extract_order_date_prefix(order)
    if not order_date:
        return False

    try:
        order_year = int(order_date[:4])
        order_month = int(order_date[5:7]) if len(order_date) >= 7 else 0
    except (ValueError, IndexError):
        return False

    year_min = filters.get("jahr_min")
    year_max = filters.get("jahr_max")
    month_min = filters.get("monat_min")
    month_max = filters.get("monat_max")

    if year_min and order_year < year_min:
        return False
    if year_max and order_year > year_max:
        return False
    if month_min and (order_year != year_min or order_month < month_min):
        return False
    if month_max and (order_year != year_max or order_month > month_max):
        return False
    return True


def _query_sales_history(args: dict) -> str:
    """Query historical sales with deduplication and vehicle matching."""
    try:
        f = {
            "jahr_min": int(args.get("jahrMin") or 0) or None,
            "jahr_max": int(args.get("jahrMax") or 0) or None,
            "monat_min": int(args.get("monatMin") or 0) or None,
            "monat_max": int(args.get("monatMax") or 0) or None,
        }
        vehicle_index = _build_sales_vehicle_index()
        raw_orders = _get_orders()

        matched: dict[str, dict] = {}
        unmatched_orders = 0

        for order in raw_orders:
            if not _filter_orders_by_date(order, f):
                continue

            match = None
            for join_key in _extract_order_join_keys(order):
                match = vehicle_index.get(join_key)
                if not match:
                    match = vehicle_index.get(join_key.upper())
                if match:
                    break

            if not match:
                unmatched_orders += 1
                continue

            stable_key, vehicle = match
            if not _vehicle_matches_sales_filters(vehicle, args):
                continue

            order_date = _extract_order_date_prefix(order)
            existing = matched.get(stable_key)
            if not existing or order_date > existing["verkauf_datum"]:
                mod = vehicle.get("model") or {}
                if isinstance(mod, list):
                    mod = mod[0] if mod else {}
                matched[stable_key] = {
                    "verkauf_datum": order_date,
                    "hersteller": str(mod.get("producer") or "-").strip() or "-",
                    "modell": str(mod.get("model") or "-").strip() or "-",
                    "status": str(vehicle.get("status") or ""),
                }

        results = sorted(
            matched.values(), key=lambda x: x["verkauf_datum"], reverse=True
        )
        response = {
            "treffer_anzahl": len(results),
            "kontext": "Historische Verkäufe (eindeutige Fahrzeuge)",
            "jahr_von": f["jahr_min"],
            "jahr_bis": f["jahr_max"],
            "monat_von": f["monat_min"],
            "monat_bis": f["monat_max"],
            "ungepairte_auftraege": unmatched_orders,
            "status": "Erfolg",
        }
        if len(results) <= 20:
            response["beispiele"] = results
        return json.dumps(response, ensure_ascii=False)
    except (TypeError, ValueError, KeyError) as exc:
        return f"Technischer Fehler im Sales-Tool: {str(exc)}"


def _get_emp_map() -> dict:
    """Load employee names from JSON if available."""
    e_path = pathlib.Path(__file__).parent.parent / "employee_names.json"
    if e_path.exists():
        try:
            return json.loads(e_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _query_employee_ranking(args: dict) -> str:
    """Calculates employee ranking based on sales or revenue."""
    try:
        f = {
            "jahr_min": int(args.get("jahrMin") or 0) or None,
            "jahr_max": int(args.get("jahrMax") or 0) or None,
            "monat_min": int(args.get("monatMin") or 0) or None,
            "monat_max": int(args.get("monatMax") or 0) or None,
        }
        metric = str(args.get("metrik") or "verkaeufe").lower()
        idx = _build_sales_vehicle_index()
        raw_rows = _get_orders()
        e_map = _get_emp_map()

        e_stats = defaultdict(lambda: {"total": 0, "items": set()})

        for order in raw_rows:
            if not _filter_orders_by_date(order, f):
                continue
            
            match = None
            for jk in _extract_order_join_keys(order):
                match = idx.get(jk) or idx.get(jk.upper())
                if match:
                    break
            if not match:
                continue

            s_key, vehicle = match
            eid = _extract_employee_from_order(order)
            if not eid:
                continue
                
            name = e_map.get(eid) or eid
            if s_key not in e_stats[name]["items"]:
                e_stats[name]["items"].add(s_key)
                if metric == "umsatz":
                    p = vehicle.get("prices") or {}
                    if isinstance(p, list):
                        p = p[0] if p else {}
                    e_stats[name]["total"] += float(p.get("offer") or 0)
                else:
                    e_stats[name]["total"] += 1

        top = sorted(e_stats.items(), key=lambda x: x[1]["total"], reverse=True)
        res = {
            "ranking": [
                {"name": n, "wert": round(v["total"], 2)}
                for n, v in top[:10]
            ],
            "metrik": metric,
            "status": "Erfolg",
        }
        return json.dumps(res, ensure_ascii=False)
    except (TypeError, ValueError, KeyError) as e:
        return f"Fehler im Ranking-Tool: {str(e)}"


def _detect_simple_sales_count_query(question: str) -> dict | None:
    q = (question or "").lower()
    if not any(word in q for word in ("verkauft", "verkäufe", "verkaeufe")):
        return None
    if not any(word in q for word in ("wie viele", "wieviele", "anzahl", "count")):
        return None

    years = [int(match) for match in re.findall(r"\b(20\d{2})\b", q)]
    payload: dict[str, object] = {"art": "alle"}
    if years:
        payload["jahrMin"] = min(years)
        payload["jahrMax"] = max(years)

    type_map = {
        "kastenwagen": "kastenwagen",
        "teilintegriert": "teilintegriert",
        "integriert": "integriert",
        "alkoven": "alkofen",
        "alkofen": "alkofen",
        "wohnwagen": "wohnwagen",
    }
    for token, normalized in type_map.items():
        if token in q:
            payload["art"] = normalized
            break

    return payload




def _tool_query_inventory(args: dict) -> str:
    """Tool for current inventory search with filters."""
    try:
        raw = get_cached_or_fetch("sale/vehicles", f"{SYSCARA_BASE}/sale/vehicles/")
        if not raw:
            return "Fehler: Fahrzeugdaten konnten nicht geladen werden."

        vehicles = map_and_filter(raw, args)
        make_q = str(args.get("make") or "").strip().lower()
        if make_q:
            vehicles = [
                v for v in vehicles
                if make_q in v.get("hersteller", "").lower()
            ]

        count = len(vehicles)
        if count == 0:
            return "Ergebnis: 0 Fahrzeuge im aktuellen Bestand gefunden."

        prices = [v["preis"] for v in vehicles if v["preis"] > 0]
        avg_preis = sum(prices) / len(prices) if prices else 0
        eks = [
            float(v.get("ek_preis") or 0)
            for v in vehicles
            if float(v.get("ek_preis") or 0) > 0
        ]
        avg_ek = sum(eks) / len(eks) if eks else 0

        res = {
            "treffer_anzahl": count,
            "basis_preis_durchschnitt": int(avg_preis),
            "einkaufspreis_durchschnitt": int(avg_ek),
            "status": "Erfolg",
        }
        if count <= 15:
            res["beispiele"] = [
                {
                    "marke": v["hersteller"],
                    "modell": v["modell"],
                    "preis": v["preis_format"],
                    "laenge": v["laenge_m"],
                    "hubbett": "Ja" if v.get("has_hubbett") else "Nein",
                    "dusche": "Ja" if v.get("has_dusche") else "Nein",
                }
                for v in vehicles
            ]
        return json.dumps(res, ensure_ascii=False)
    except (TypeError, ValueError, KeyError) as e:
        return f"Technischer Fehler im Tool: {str(e)}"


AI_TOOLS_SPEC = [
    {
        "type": "function",
        "function": {
            "name": "query_inventory",
            "description": (
                "Werkzeug für aktuellen Fahrzeugbestand und Merkmals-/Preisfilter. "
                "Nicht für historische Verkaufszahlen verwenden."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "art": {
                        "type": "string",
                        "description": "Fahrzeugtyp (z.B. Kastenwagen)",
                    },
                    "laengeMin": {
                        "type": "number",
                        "description": "Mindestlänge in METERN (z.B. 5.40)",
                    },
                    "laengeMax": {
                        "type": "number",
                        "description": "Maximallänge in METERN (z.B. 7.50)",
                    },
                    "hubbett": {
                        "type": "boolean",
                        "description": "Filter nach Hubbett (bed)",
                    },
                    "dusche": {
                        "type": "boolean",
                        "description": "Filter nach separater Dusche",
                    },
                    "jahrMin": {
                        "type": "integer",
                        "description": "Baujahr Minimum",
                    },
                    "jahrMax": {
                        "type": "integer",
                        "description": "Baujahr Maximum",
                    },
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_sales_history",
            "description": (
                "Werkzeug für historische Verkäufe. Zählt eindeutige Fahrzeuge, "
                "dedupliziert Aufträge und nutzt Order→Vehicle-Matching."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "art": {
                        "type": "string",
                        "description": "Fahrzeugtyp (z.B. Kastenwagen)",
                    },
                    "laengeMin": {
                        "type": "number",
                        "description": "Mindestlänge (m)",
                    },
                    "laengeMax": {
                        "type": "number",
                        "description": "Maximallänge (m)",
                    },
                    "jahrMin": {"type": "integer", "description": "Jahr von"},
                    "jahrMax": {"type": "integer", "description": "Jahr bis"},
                    "monatMin": {"type": "integer", "description": "Monat von"},
                    "monatMax": {"type": "integer", "description": "Monat bis"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_employee_ranking",
            "description": (
                "Mitarbeiter-Ranking nach Verkäufen oder Umsatz. "
                "Nutze dies für Top-Mitarbeiter oder Verkäufer-Leistung."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "jahrMin": {"type": "integer", "description": "Jahr von"},
                    "jahrMax": {"type": "integer", "description": "Jahr bis"},
                    "monatMin": {"type": "integer", "description": "Monat von"},
                    "monatMax": {"type": "integer", "description": "Monat bis"},
                    "metrik": {
                        "type": "string",
                        "enum": ["verkaeufe", "umsatz"],
                        "description": "Sort: verkaeufe (Anzahl) oder umsatz (€)",
                    },
                },
            },
        },
    },
]


def _execute_ai_tool(name: str, arguments: str) -> str:
    """Dispatches tool calls from the AI to the corresponding Python functions."""
    try:
        args = json.loads(arguments)
        if name == "query_inventory":
            return _tool_query_inventory(args)
        if name == "query_sales_history":
            return _query_sales_history(args)
        if name == "query_employee_ranking":
            return _query_employee_ranking(args)
        return f"Fehler: Unbekanntes Tool '{name}'"
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        return f"Fehler bei Tool-Ausführung ({name}): {str(e)}"


def _handle_local_detections(question: str):
    """Try to answer the question using deterministic local lookups."""
    # DSGVO-Bereich
    is_cust, cp = _detect_customer_query(question)
    if is_cust:
        a, t = _execute_local_customer_query(cp)
        return {"success": True, "answer": a, "table": t}

    is_ord, op = _detect_order_lookup_query(question)
    if is_ord:
        a, t, c = _execute_local_order_lookup(op)
        return {"success": True, "answer": a, "chart": c, "table": t}

    is_emp, ep = _detect_employee_query(question)
    if is_emp:
        a, t, c = _execute_local_employee_query(ep)
        return {"success": True, "answer": a, "chart": c, "table": t}

    sales_payload = _detect_simple_sales_count_query(question)
    if sales_payload:
        raw_sales = _query_sales_history(sales_payload)
        try:
            sales_data = json.loads(raw_sales)
        except json.JSONDecodeError:
            sales_data = None

        if sales_data and sales_data.get("status") == "Erfolg":
            return _format_sales_response(sales_data, sales_payload)
    return None


def _format_sales_response(data: dict, payload: dict):
    """Format the sales tool result for the UI."""
    y_from = data.get("jahr_von")
    y_to = data.get("jahr_bis")
    year_label = str(y_from) if y_from and y_from == y_to else f"{y_from} bis {y_to}"
    type_label = payload.get("art") if payload.get("art") != "alle" else None

    ans = f"Es wurden {data.get('treffer_anzahl', 0)} eindeutige Fahrzeuge"
    if type_label:
        ans += f" vom Typ {type_label}"
    if y_from or y_to:
        ans += f" im Zeitraum {year_label}"
    ans += " verkauft."

    table = None
    examples = data.get("beispiele") or []
    if examples:
        table = {
            "columns": ["Verkauf", "Hersteller", "Modell", "Status"],
            "rows": [
                [
                    i.get("verkauf_datum", "-"),
                    i.get("hersteller", "-"),
                    i.get("modell", "-"),
                    i.get("status", "-"),
                ]
                for i in examples[:10]
            ],
            "footer": "Deterministisch aus Aufträgen berechnet.",
        }
    return {"success": True, "answer": ans, "table": table}


def register_ai_analyst_routes(app):
    """Registers AI analyst routes on the Flask app."""

    @app.route("/api/ask", methods=["POST"])
    def api_ask():
        """Main endpoint for the AI analyst."""
        body = request.get_json(silent=True) or {}
        question = str(body.get("question", "")).strip()[:2000]
        if not question:
            return jsonify({"success": False, "error": "Keine Frage übergeben."}), 400

        cached = _qcache_get(question)
        if cached:
            return jsonify({**cached, "cached": True})

        local_res = _handle_local_detections(question)
        if local_res:
            res = {
                "chart": None,
                "table": None,
                "source": "local",
                **local_res,
            }
            _qcache_put(question, res)
            return jsonify(res)

        if not HAS_OPENAI:
            return jsonify({"success": False, "error": "OpenAI nicht installiert."}), 503
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return jsonify({"success": False, "error": "Key fehlt."}), 503

        bi_context = _build_bi_context()
        client = openai.OpenAI(api_key=api_key)

        # Conversation History aus Session laden
        conversation_id = body.get("conversation_id") or "default"
        session_key = f"ai_conversation_{conversation_id}"
        conversation_history = session.get(session_key, [])

        cur_dt = datetime.now().strftime("%Y-%m-%d")
        cur_mo = datetime.now().strftime("%B %Y")  # z.B. "March 2026"

        sys_msg = (
            f"Datum: {cur_dt} ({cur_mo}).\n"
            "Zugriff auf Hubbett, Dusche, Preise (VK/EK), Aufträge.\n\n"
            "REGELN:\n"
            "1. KONTEXT BEHALTEN: Anschlussfragen beziehen sich auf den Zeitraum.\n"
            "2. Monatsfilter: Mär 26 -> jahrMin=2026, monatMin=3 (usw.)\n"
            "3. Ranking -> query_employee_ranking\n"
            "4. Verkaufsanzahl -> query_sales_history\n"
            "5. NIEMALS raten!\n"
            f"{bi_context}"
        )

        # Baue Messages mit History
        messages = [{"role": "system", "content": sys_msg}] + conversation_history[-6:]
        messages.append({"role": "user", "content": question})

        try:
            comp = client.chat.completions.create(
                model="gpt-4o", messages=messages, tools=AI_TOOLS_SPEC, tool_choice="auto"
            )
            msg = comp.choices[0].message

            if msg.tool_calls:
                messages.append(msg)
                for tc in msg.tool_calls:
                    res = _execute_ai_tool(tc.function.name, tc.function.arguments)
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tc.id,
                            "name": tc.function.name,
                            "content": res,
                        }
                    )
                final = client.chat.completions.create(model="gpt-4o", messages=messages)
                raw = final.choices[0].message.content or ""
            else:
                raw = msg.content or ""

            # Speichere Conversation History (maximal 10 Turns)
            conversation_history.append({"role": "user", "content": question})
            conversation_history.append({"role": "assistant", "content": raw})
            session[session_key] = conversation_history[-10:]
            session.modified = True

            resp = {
                "success": True,
                "answer": raw.strip(),
                "chart": None,
                "table": None,
                "source": "openai",
            }
            _qcache_put(question, resp)
            return jsonify(resp)
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500
