import os
import sys

# Pfade patchen
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from api.kosten import _load_orders, _extract_order_date, _order_join_keys, _vehicle_join_keys
from core.database import get_cached_or_fetch
from core.config import SYSCARA_BASE

vehicles_raw = get_cached_or_fetch("sale/vehicles", f"{SYSCARA_BASE}/sale/vehicles/")
if isinstance(vehicles_raw, dict) and "vehicles" in vehicles_raw:
    vehicles = vehicles_raw["vehicles"]
elif isinstance(vehicles_raw, list):
    vehicles = vehicles_raw
elif hasattr(vehicles_raw, "values"):
    vehicles = [v for v in vehicles_raw.values() if isinstance(v, dict)]
else:
    vehicles = []

orders = _load_orders()

print(f"Loaded {len(vehicles)} vehicles and {len(orders)} orders")

vehicle_sale_date = {}
for o in orders:
    jkeys = _order_join_keys(o)
    if not jkeys: continue
    odate = _extract_order_date(o)
    if odate:
        for k in jkeys:
            if not vehicle_sale_date.get(k) or odate > vehicle_sale_date[k]:
                vehicle_sale_date[k] = odate

print(f"Populated {len(vehicle_sale_date)} order dates.")

v_in_range = 0
re_count = 0
for v in vehicles:
    status = str(v.get("status", "")).upper()
    if status != "RE": continue
    re_count += 1
    
    jkeys = _vehicle_join_keys(v)
    
    found = False
    for k in jkeys:
        d = vehicle_sale_date.get(k)
        if d:
            ym = d[:7]
            if "2025-01" <= ym <= "2026-03":
                found = True
                break
    
    if found:
        v_in_range += 1

print(f"Found {re_count} RE vehicles.")
print(f"Found {v_in_range} vehicles inside date range 2025-01 to 2026-03.")
