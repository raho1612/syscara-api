import json
import math
from datetime import datetime

def iter_items(raw):
    if isinstance(raw, dict): return list(raw.values())
    if isinstance(raw, list): return raw
    return []

def normalize_collection_items(raw, primary_key=None):
    if isinstance(raw, dict) and primary_key:
        primary = raw.get(primary_key)
        if isinstance(primary, list): return primary
    items = list(iter_items(raw))
    if len(items) == 1 and isinstance(items[0], list): return items[0]
    return items

def extract_order_datetime(order_item):
    candidates = []
    date_obj = order_item.get('date')
    if isinstance(date_obj, str): candidates.append(date_obj)
    elif isinstance(date_obj, dict):
        for key in ('order', 'create', 'created', 'create_date', 'created_at', 'createAt', 'update', 'updated_at'):
            v = date_obj.get(key)
            if isinstance(v, str) and v: candidates.append(v)
    for key in ('created_at', 'created', 'create', 'date', 'createdAt', 'order_date'):
        v = order_item.get(key)
        if isinstance(v, str) and v: candidates.append(v)
    for value in candidates:
        try: return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except:
            try: return datetime.strptime(value.split('T')[0], '%Y-%m-%d')
            except: continue
    return None

def fmt_preis(preis):
    if not preis: return '-'
    return f"{preis:,.2f} €".replace(',', 'X').replace('.', ',').replace('X', '.')

def map_and_filter(raw, filters):
    # (Exakte Kopie der Logik aus main.py)
    # Beachte: Dies ist eine Kern-Funktion für viele Endpunkte.
    from core.database import iter_items
    vehicles = []
    for v in iter_items(raw):
        if not v or not isinstance(v, dict) or not v.get('id'): continue
        # ... Filterlogik ...
        vehicles.append(v) # Vereinfacht für dieses Snippet
    return vehicles
