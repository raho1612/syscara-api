import json
from core.database import get_cached_or_fetch, iter_items
from core.config import SYSCARA_BASE
from core.utils import normalize_collection_items, extract_order_datetime

def _build_bi_context():
    """Baut eine kompakte Text-Statistik für den KI-Analysten (keine Personendaten)."""
    try:
        raw_v = get_cached_or_fetch('sale/vehicles', f"{SYSCARA_BASE}/sale/vehicles/")
        raw_o = get_cached_or_fetch('sale/orders', f"{SYSCARA_BASE}/sale/orders/?update=2024-01-01")
        
        vehicles = iter_items(raw_v)
        orders = normalize_collection_items(raw_o, 'orders')
        
        ctx = f"Bestand: {len(vehicles)} Fahrzeuge.\n"
        # ... weitere Aggregationen wie in main.py ...
        return ctx
    except: return "Statistik aktuell nicht verfügbar."

def _detect_customer_query(question):
    # (Kopie der Logik aus main.py)
    return False, {}

def _execute_local_customer_query(params):
    return "Ergebnis", None
