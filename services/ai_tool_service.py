import re
import logging
from typing import Tuple, Optional, Any, Dict
from core.utils import _extract_order_nr, iter_items

# Setup logging
logger = logging.getLogger(__name__)

def detect_customer_query(question: str) -> Tuple[bool, dict]:
    """Detect if a query is about a customer (city, zip, or name)."""
    q = question.lower()
    city_pts = [
        r"kunden?\s+(?:in|aus|von)\s+([a-zäöüß][a-zäöüß\s\-]{2,20})",
        r"stadt\s*:\s*([a-zäöüß]{2,20})",
    ]
    for p in city_pts:
        m = re.search(p, q)
        if m:
            return True, {"type": "city", "value": m.group(1).strip()}
            
    zip_m = re.search(r"\b(\d{5})\b", q)
    if zip_m:
        return True, {"type": "zip", "value": zip_m.group(1)}
        
    name_pts = [r"(?:kunde|herr|frau)\s+([a-zäöüß]{2,20}(?:\s+[a-zäöüß]{2,20})?)"]
    for p in name_pts:
        m = re.search(p, q)
        if m:
            return True, {"type": "name", "value": m.group(1).strip()}
            
    return False, {}

def execute_local_customer_query(params: dict, orders: list) -> Tuple[str, Optional[dict]]:
    """Execute a customer query against the local order data."""
    results = []
    q_t = params.get("type")
    val = params.get("value", "").lower().strip()
    
    for o in orders:
        c = o.get("customer", {}) or {}
        if not isinstance(c, dict):
            continue
            
        match = False
        if q_t == "city":
            match = val in (c.get("city") or "").lower()
        elif q_t == "zip":
            match = str(c.get("zipcode", "")) == val
        elif q_t == "name":
            fn = (c.get("first_name") or "").lower()
            ln = (c.get("last_name") or "").lower()
            match = val in fn or val in ln
            
        if match:
            results.append(o)

    if not results:
        return "Keine Treffer gefunden.", None

    rows = []
    for r in results[:50]:
        cust = r.get("customer", {}) or {}
        stat = r.get("status", {}) or {}
        rows.append([
            _extract_order_nr(r),
            f"{cust.get('first_name','') or ''} {cust.get('last_name','') or ''}".strip(),
            cust.get("city", "") or "-",
            (stat.get("label") or "?"),
        ])

    table = {"columns": ["Nr", "Name", "Stadt", "Status"], "rows": rows}
    return f"{len(results)} Kunden/Aufträge gefunden.", table

def detect_order_lookup_query(question: str) -> Tuple[bool, dict]:
    """Detect if a query is looking for a specific order number."""
    pattern = r"(?:auftrags?|order)\s*#?\s*(\b[a-z0-9\-\/]{4,20}\b)"
    m = re.search(pattern, question.lower())
    if m:
        return True, {"type": "order_nr", "value": m.group(1).upper()}
    return False, {}

def execute_local_order_lookup(params: dict, orders: list) -> Tuple[str, Optional[dict]]:
    """Lookup a single order by number."""
    search_val = params["value"]
    for o in orders:
        if _extract_order_nr(o).upper() == search_val:
            cust = o.get("customer", {}) or {}
            stat = o.get("status", {}) or {}
            fn = cust.get("first_name", "")
            ln = cust.get("last_name", "")
            ans = (
                f"Auftrag {search_val}: {fn} {ln} aus {cust.get('city', '-')}. "
                f"Status: {stat.get('label', '?')}"
            )
            return ans, None
    return f"Auftrag {search_val} wurde nicht im System gefunden.", None

def detect_employee_query(question: str) -> Tuple[bool, dict]:
    """Detect if a query is looking for a specific employee ID."""
    m = re.search(r"(?:mitarbeiter|id)\s*#?\s*(\d{3,6})", question.lower())
    if m:
        return True, {"type": "employee_id", "value": m.group(1)}
    return False, {}

def execute_local_employee_query(params: dict, orders: list) -> Tuple[str, Optional[dict]]:
    """Lookup orders for a specific employee."""
    emp_id = params["value"]
    res = [
        o for o in orders
        if str(o.get("user", {}).get("order") or o.get("user", {}).get("update")) == emp_id
    ]
    if not res:
        return f"Keine Daten für Mitarbeiter-ID {emp_id} gefunden.", None
    return f"Mitarbeiter (ID {emp_id}) ist in {len(res)} Aufträgen als Bearbeiter hinterlegt.", None
