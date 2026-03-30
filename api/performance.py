from datetime import datetime

from core.config import SYSCARA_BASE
from core.database import get_cached_or_fetch, iter_items
from core.utils import _load_employee_names, extract_order_datetime
from flask import jsonify, request
from shared.sales_engine import calculate_net_sales


def extract_employee_name(o_item, _emp_names):
    u = o_item.get('user') or {}
    ids = []
    names = []

    for key in ('order', 'update', 'id'):
        v = u.get(key)
        if v:
            s_v = str(v).split('.')[0]
            if s_v.isdigit():
                ids.append(s_v)

    for key in ('full_name', 'name', 'display_name', 'username'):
        v = u.get(key)
        if v and isinstance(v, str) and v.strip() and not v.strip().split('.')[0].isdigit():
            names.append(v.strip())

    for key in ('responsible', 'seller', 'sales_person'):
        v = o_item.get(key)
        if isinstance(v, str) and v.strip() and not v.strip().isdigit():
            names.append(v.strip())
        elif isinstance(v, dict):
            vv = v.get('name') or v.get('username')
            if vv:
                names.append(str(vv))
            vid = v.get('id')
            if vid:
                ids.append(str(vid).split('.')[0])

    for uid in ids:
        if uid in _emp_names:
            return _emp_names[uid]
    if names:
        return names[0]
    if ids:
        return f"ID {ids[0]}"
    return 'Unbekannt'


def register_performance_routes(app):
    @app.route('/api/performance', methods=['GET'])
    def api_performance():
        year = int(request.args.get('year') or datetime.now().year)
        raw = get_cached_or_fetch('sale/orders', f"{SYSCARA_BASE}/sale/orders/?update=2024-01-01")

        if isinstance(raw, dict) and isinstance(raw.get('orders'), list):
            items = raw.get('orders')
        else:
            items = list(iter_items(raw))
            if len(items) == 1 and isinstance(items[0], list):
                items = items[0]

        _emp_names = _load_employee_names()

        # 1. Netto-Verkäufe über die zentrale Engine berechnen
        # (Dies filtert Stornos, Doubletten und Tausche raus)
        net_result = calculate_net_sales(items, year_min=year, year_max=year)

        # 2. Mitarbeiter-Datenstruktur vorbereiten
        employees: dict = {}

        def get_emp_entry(name):
            if name not in employees:
                m_t = {str(i): {k: {"count": 0, "revenue": 0, "cumulative_count": 0} for k in ['OFFER', 'ORDER', 'CONTRACT', 'CANCELLATION']} for i in range(1, 13)}
                q_t = {f'Q{i}': {k: {"count": 0, "revenue": 0, "cumulative_count": 0} for k in ['OFFER', 'ORDER', 'CONTRACT', 'CANCELLATION']} for i in range(1, 5)}
                employees[name] = {"id": name.replace(' ', '_'), "name": name, "months": m_t, "quarters": q_t}
            return employees[name]

        # 3. Netto-Fahrzeuge (ORDER/Verbucht) eintragen
        for fzg in net_result["fahrzeuge"]:
            dt_str = fzg["datum_ab"] # YYYY-MM-DD
            dt = datetime.strptime(dt_str, "%Y-%m-%d")
            month = dt.month
            quarter = (month - 1) // 3 + 1

            # Eine AB kann mehreren IDs zugeordnet sein, falls Kooperation
            for emp_id in (fzg["employee_ids"] or ["UNBEKANNT"]):
                # Name auflösen
                emp_name = _emp_names.get(emp_id) or (f"ID {emp_id}" if emp_id != "UNBEKANNT" else "Unbekannt")
                emp = get_emp_entry(emp_name)

                # Wir zählen dieses Fahrzeug als 'ORDER' (Verkaufserfolg)
                emp['months'][str(month)]['ORDER']['count'] += 1
                emp['months'][str(month)]['ORDER']['revenue'] += fzg["revenue"]
                emp['quarters'][f'Q{quarter}']['ORDER']['count'] += 1
                emp['quarters'][f'Q{quarter}']['ORDER']['revenue'] += fzg["revenue"]

        # 4. Angebote (OFFER) und rohe Stornos (CANCELLATION) für die Übersicht ergänzen
        # (Angebote werden nicht dedupliziert, da sie nur Bemühung zeigen)
        for o in items:
            dt = extract_order_datetime(o)
            if not dt or dt.year != year:
                continue

            status_raw = str(o.get('status', {}).get('key') if isinstance(o.get('status'), dict) else o.get('status') or '').upper()
            if status_raw == 'OFFER':
                name = extract_employee_name(o, _emp_names)
                emp = get_emp_entry(name)
                month = dt.month
                quarter = (month - 1) // 3 + 1
                # Angebots-Umsatz extrahieren
                p = o.get('prices') or {}
                rev = float(p.get('offer') or p.get('basic') or p.get('brutto') or 0)

                emp['months'][str(month)]['OFFER']['count'] += 1
                emp['months'][str(month)]['OFFER']['revenue'] += rev
                emp['quarters'][f'Q{quarter}']['OFFER']['count'] += 1
                emp['quarters'][f'Q{quarter}']['OFFER']['revenue'] += rev

        # 5. Kumulative Summen berechnen
        for emp in employees.values():
            run_count = dict.fromkeys(['OFFER', 'ORDER', 'CONTRACT', 'CANCELLATION'], 0)
            run_rev = dict.fromkeys(['OFFER', 'ORDER', 'CONTRACT', 'CANCELLATION'], 0.0)
            for i in range(1, 13):
                m_k = str(i)
                for k in run_count.keys():
                    run_count[k] += emp['months'][m_k][k]['count']
                    run_rev[k] += emp['months'][m_k][k]['revenue']
                    emp['months'][m_k][k]['cumulative_count'] = run_count[k]
                    # Auch kumulativen Umsatz speichern falls das Frontend es anzeigt
                    emp['months'][m_k][k]['cumulative_revenue'] = round(run_rev[k], 2)

        return jsonify({
            "success": True,
            "year": year,
            "employees": list(employees.values()),
            "meta": {
                "engine": "net_sales_v1",
                "netto_total_count": net_result["netto_verkauft"],
                "netto_total_revenue": net_result["netto_umsatz"],
                "stornos_ignored": net_result["storno_count"]
            }
        })
