import sys
from pathlib import Path
import json

sys.path.append(str(Path(__file__).parent))

from core.database import get_cached_or_fetch
from core.config import SYSCARA_BASE

def test_orders():
    orders_raw = get_cached_or_fetch("sale/orders", f"{SYSCARA_BASE}/sale/orders/?update=2024-01-01")
    orders = []
    if isinstance(orders_raw, dict) and "orders" in orders_raw:
        orders = orders_raw["orders"]
    elif isinstance(orders_raw, list):
        orders = orders_raw
    elif hasattr(orders_raw, "values"):
        orders = [o for o in orders_raw.values() if isinstance(o, dict)]
    
    if orders:
        o = orders[0]
        print(json.dumps(o, indent=2))
        
if __name__ == "__main__":
    test_orders()
