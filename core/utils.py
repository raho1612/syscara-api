import json
import os
from datetime import datetime
from pathlib import Path


def iter_items(raw):
    if isinstance(raw, dict): return list(raw.values())
    if isinstance(raw, list): return raw
    return []

def normalize_collection_items(raw, primary_key=None):
    if isinstance(raw, dict) and primary_key:
        primary = raw.get(primary_key)
        if isinstance(primary, list):
            return primary
    items = list(iter_items(raw))
    if len(items) == 1 and isinstance(items[0], list):
        return items[0]
    return items

def extract_order_datetime(order_item):
    candidates = []
    date_obj = order_item.get('date')
    if isinstance(date_obj, str):
        candidates.append(date_obj)
    elif isinstance(date_obj, dict):
        for key in ('order', 'create', 'created', 'create_date', 'created_at', 'createAt', 'update', 'updated_at'):
            value = date_obj.get(key)
            if isinstance(value, str) and value:
                candidates.append(value)
        for value in date_obj.values():
            if isinstance(value, str) and value:
                candidates.append(value)
    for key in ('created_at', 'created', 'create', 'date', 'createdAt', 'order_date'):
        value = order_item.get(key)
        if isinstance(value, str) and value:
            candidates.append(value)
    for value in candidates:
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except Exception:
            try:
                return datetime.strptime(value.split('T')[0], '%Y-%m-%d')
            except Exception:
                continue
    return None

def _extract_order_nr(o_item):
    ident = o_item.get('identifier', {}) or {}
    if isinstance(ident, dict):
        return str(ident.get('order') or ident.get('internal') or o_item.get('id') or '?')
    return str(o_item.get('id') or '?')

def fmt_preis(preis):
    if not preis: return '-'
    try:
        f_preis = float(preis)
        return f"{f_preis:,.2f} €".replace(',', 'X').replace('.', ',').replace('X', '.')
    except:
        return str(preis)

def _candidate_file_paths(filename, env_var=None):
    from core.config import CURRENT_DIR, ROOT_DIR
    candidates = []
    if env_var:
        override = os.getenv(env_var)
        if override: candidates.append(Path(override))
    candidates.extend([ROOT_DIR / filename, CURRENT_DIR / filename, Path('/data') / filename])
    seen = set(); result = []
    for p in candidates:
        key = str(p.resolve())
        if key not in seen:
            seen.add(key); result.append(p)
    return result

def _load_employee_names() -> dict:
    import os
    env_path = os.getenv("EMPLOYEE_NAMES_PATH")
    if env_path:
        if os.path.exists(env_path):
            try:
                with open(env_path, "r", encoding="utf-8") as f: return json.load(f)
            except: pass

    local_path = Path(__file__).resolve().parent.parent / "employee_names.json"
    if local_path.exists():
        try:
            with open(local_path, "r", encoding="utf-8") as f: return json.load(f)
        except: pass

    for emp_file in _candidate_file_paths("employee_names.json"):
        if emp_file.exists():
            try:
                with open(emp_file, "r", encoding="utf-8") as f: return json.load(f)
            except: pass
    return {}
