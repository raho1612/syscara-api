import json
from pathlib import Path
from flask import jsonify, request
from datetime import datetime
from core.config import SYSCARA_BASE
from core.database import get_cached_or_fetch, iter_items
from core.utils import extract_order_datetime, _load_employee_names

def extract_employee_name(o_item, _emp_names):
    u = o_item.get('user') or {}
    ids = []
    names = []
    
    # Check regular user object
    for key in ('order', 'update', 'id'):
        v = u.get(key)
        if v:
            s_v = str(v).split('.')[0]
            if s_v.isdigit(): ids.append(s_v)
            
    for key in ('full_name', 'name', 'display_name', 'username'):
        v = u.get(key)
        if v and isinstance(v, str) and v.strip() and not v.strip().split('.')[0].isdigit():
            names.append(v.strip())
            
    # Check top-level keys
    for key in ('responsible', 'seller', 'sales_person'):
        v = o_item.get(key)
        if isinstance(v, str) and v.strip() and not v.strip().isdigit():
            names.append(v.strip())
        elif isinstance(v, dict):
            vv = v.get('name') or v.get('username')
            if vv: names.append(str(vv))
            vid = v.get('id')
            if vid: ids.append(str(vid).split('.')[0])
            
    # Resolution priority: Mapping -> Name -> ID
    for uid in ids:
        if uid in _emp_names: return _emp_names[uid]
    if names: return names[0]
    if ids: return f"ID {ids[0]}"
    return 'Unbekannt'

def register_performance_routes(app):
    @app.route('/api/performance', methods=['GET'])
    def api_performance():
        year = int(request.args.get('year') or datetime.now().year)
        raw = get_cached_or_fetch('sale/orders', f"{SYSCARA_BASE}/sale/orders/?update=2024-01-01")
        
        if isinstance(raw, dict) and isinstance(raw.get('orders'), list): items = raw.get('orders')
        else:
            items = list(iter_items(raw))
            if len(items) == 1 and isinstance(items[0], list): items = items[0]
        
        employees: dict = {}
        _emp_names = _load_employee_names()

        for o in items:
            if not o or not isinstance(o, dict): continue
            dt = extract_order_datetime(o)
            if not dt or dt.year != year: continue

            month = dt.month
            quarter = (month - 1) // 3 + 1
            name = extract_employee_name(o, _emp_names)

            if name not in employees:
                m_t = {str(i): {k: {"count": 0, "revenue": 0, "cumulative_count": 0} for k in ['OFFER', 'ORDER', 'CONTRACT', 'CANCELLATION']} for i in range(1, 13)}
                q_t = {f'Q{i}': {k: {"count": 0, "revenue": 0, "cumulative_count": 0} for k in ['OFFER', 'ORDER', 'CONTRACT', 'CANCELLATION']} for i in range(1, 5)}
                employees[name] = {"id": name.replace(' ', '_'), "name": name, "months": m_t, "quarters": q_t}

            price = 0
            try:
                p_val = o.get('price') or o.get('total') or o.get('amount') or 0
                price = float(p_val)
            except: pass

            st_obj = o.get('status')
            status = str(st_obj.get('key') or st_obj.get('label') or '').upper() if isinstance(st_obj, dict) else str(st_obj or '').upper()
            m_type = status if status in {'OFFER', 'ORDER', 'CONTRACT', 'CANCELLATION'} else 'ORDER'

            emp = employees[name]
            emp['months'][str(month)][m_type]['count'] += 1
            emp['months'][str(month)][m_type]['revenue'] += price
            emp['quarters'][f'Q{quarter}'][m_type]['count'] += 1
            emp['quarters'][f'Q{quarter}'][m_type]['revenue'] += price

        for name, emp in employees.items():
            run = {k: 0 for k in ['OFFER', 'ORDER', 'CONTRACT', 'CANCELLATION']}
            for i in range(1, 13):
                m_k = str(i)
                for k in run.keys():
                    run[k] += emp['months'][m_k][k]['count']
                    emp['months'][m_k][k]['cumulative_count'] = run[k]

        return jsonify({"success": True, "year": year, "employees": list(employees.values())})
