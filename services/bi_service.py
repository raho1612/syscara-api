import datetime
import re
import time
from collections import Counter

from core.config import SYSCARA_BASE
from core.database import _MEM_CACHE, get_cached_or_fetch, iter_items
from core.utils import _extract_order_nr, extract_order_datetime, fmt_preis
from shared.sales_engine import calculate_net_sales
from shared.vehicle_stats import build_vehicle_stats


def _get_orders() -> list:
    """Fetch orders from the cache or the Syscara API."""
    raw = get_cached_or_fetch(
        "sale/orders", f"{SYSCARA_BASE}/sale/orders/?update=2024-01-01"
    )
    if isinstance(raw, dict) and isinstance(raw.get("orders"), list):
        return raw["orders"]
    items = list(iter_items(raw))
    if len(items) == 1 and isinstance(items[0], list):
        return items[0]
    return [o for o in items if isinstance(o, dict)]

def _extract_vehicle_features(v: dict) -> dict:
    """Helper to extract vehicle features for mapping."""

    def _d(key):
        res = v.get(key)
        return res if isinstance(res, dict) else {}

    model = _d("model")
    engine = _d("engine")
    dimensions = _d("dimensions")
    prices = _d("prices")
    beds_d = _d("beds")

    art_raw = str(v.get("typeof", "")).lower()
    art_label = "Wohnwagen" if v.get("type") == "Caravan" else art_raw.capitalize()
    if "integriert" in art_raw:
        art_label = "Integriert"
    if "teilintegriert" in art_raw:
        art_label = "Teilintegriert"
    if "kastenwagen" in art_raw or "camper" in art_raw:
        art_label = "Kastenwagen"

    ps = engine.get("ps", 0) or engine.get("power", 0) or 0
    laenge = dimensions.get("length", 0) or 0
    preis = prices.get("offer") or prices.get("list") or prices.get("basic") or 0
    ek_preis = prices.get("purchase") or 0
    modelljahr = model.get("modelyear", 0) or 0
    schlafplaetze = beds_d.get("sleeping", 0) or 0
    features = v.get("features", [])
    if not isinstance(features, list):
        features = []

    beds_list = beds_d.get("beds", []) if isinstance(beds_d.get("beds"), list) else []
    bed_types = [str(bed.get("type", "")).upper() for bed in beds_list if isinstance(bed, dict)]
    has_hubbett = "PULL_BED" in bed_types or "ROOF_BED" in bed_types
    has_dusche = "sep_dusche" in features or "dusche" in features
    gear_raw = str(engine.get("gear", "") or engine.get("gearbox", "")).upper()
    has_auto = any(x in gear_raw for x in ["AUTOMATIC", "AUT", "AUTOMATIK"])
    condition = str(v.get("condition", "")).upper()

    return {
        "art_raw": art_raw,
        "art_label": art_label,
        "ps": ps,
        "laenge": laenge,
        "preis": preis,
        "ek_preis": ek_preis,
        "modelljahr": modelljahr,
        "schlafplaetze": schlafplaetze,
        "has_hubbett": has_hubbett,
        "has_dusche": has_dusche,
        "has_auto": has_auto,
        "condition": condition,
        "producer": model.get("producer", "-"),
        "model_name": model.get("model", "-"),
    }


def _apply_basic_filters(f: dict, v: dict, filters: dict) -> bool:
    """Helper to apply basic filters (type, status, condition, gearbox)."""
    f_art = str(filters.get("art", "alle")).lower()
    if f_art != "alle" and f_art not in f["art_raw"]:
        return False

    f_stat = filters.get("status")
    if f_stat and str(v.get("status")).upper() != f_stat.upper():
        return False

    f_cond = filters.get("zustand")
    if f_cond and f_cond.upper() != f["condition"]:
        return False

    getriebe = filters.get("getriebe")
    if getriebe:
        if getriebe == "automatik" and not f["has_auto"]:
            return False
        if getriebe == "schaltung" and f["has_auto"]:
            return False
    return True


def _apply_range_filters(f: dict, filters: dict) -> bool:
    """Helper to apply range filters (PS, price, year, length, etc)."""
    ps_min = filters.get("psMin")
    if ps_min and f["ps"] < int(ps_min):
        return False
    ps_max = filters.get("psMax")
    if ps_max and f["ps"] > int(ps_max):
        return False

    pr_min = filters.get("preisMin")
    if pr_min and f["preis"] < int(pr_min):
        return False
    pr_max = filters.get("preisMax")
    if pr_max and f["preis"] > int(pr_max):
        return False

    yr_min = filters.get("jahrMin")
    if yr_min and f["modelljahr"] < int(yr_min):
        return False
    yr_max = filters.get("jahrMax")
    if yr_max and f["modelljahr"] > int(yr_max):
        return False

    l_min = filters.get("laengeMin")
    if l_min and f["laenge"] < float(l_min) * 100:
        return False
    l_max = filters.get("laengeMax")
    if l_max and f["laenge"] > float(l_max) * 100:
        return False

    slp_min = filters.get("schlafplaetzeMin")
    if slp_min and f["schlafplaetze"] < int(slp_min):
        return False

    if filters.get("hubbett") is True and not f["has_hubbett"]:
        return False
    if filters.get("dusche") is True and not f["has_dusche"]:
        return False

    return True


def _apply_filters(f: dict, v: dict, filters: dict) -> bool:
    """Helper to apply all filters to a vehicle."""
    if not filters:
        return True
    if not _apply_basic_filters(f, v, filters):
        return False
    if not _apply_range_filters(f, filters):
        return False
    return True


def map_and_filter(raw, filters, with_photos=False):
    """Map raw vehicle data to UI format and apply filters."""
    vehicles = []
    for v in iter_items(raw):
        if not v or not isinstance(v, dict):
            continue
        v_id = v.get("id") or v.get("uid") or v.get("internal")
        if not v_id:
            continue

        f = _extract_vehicle_features(v)
        if not _apply_filters(f, v, filters):
            continue

        vehicles.append(
            {
                "id": v_id,
                "hersteller": f["producer"],
                "modell": f["model_name"],
                "preis": f["preis"],
                "ek_preis": f["ek_preis"],
                "preis_format": fmt_preis(f["preis"]),
                "ps": f["ps"],
                "laenge_m": f"{f['laenge']/100:.2f}",
                "laenge_cm": f["laenge"],
                "modelljahr": f["modelljahr"],
                "getriebe": "Automatik" if f["has_auto"] else "Schaltung",
                "zustand": f["condition"],
                "typ": f["art_label"],
                "schlafplaetze": f["schlafplaetze"],
                "has_hubbett": f["has_hubbett"],
                "has_dusche": f["has_dusche"],
            }
        )
    return vehicles

_BI_CONTEXT_CACHE = {'ts': 0, 'data': None}
_BI_CONTEXT_TTL = 300

def _build_bi_context() -> str:
    """Build a text context for the AI analyst with key KPIs and inventory stats."""
    global _BI_CONTEXT_CACHE
    cached_data = _BI_CONTEXT_CACHE["data"]
    if cached_data and (time.time() - _BI_CONTEXT_CACHE["ts"] < _BI_CONTEXT_TTL):
        return cached_data

    lines = [f"=== SYSCARA OMNISCIENT DATA HUB ({datetime.date.today().strftime('%d.%m.%Y')}) ==="]

    try:
        items = _get_orders()
        year_counts = Counter()
        status_counts = Counter()
        for o in items:
            dt = extract_order_datetime(o)
            if dt:
                year_counts[dt.year] += 1
            s = o.get("status", {})
            status = (s.get("key") or s.get("label")) if isinstance(s, dict) else str(s or "")
            if status:
                status_counts[status] += 1

        lines.append(f"\nAUFTRÄGE GESAMT: {len(items)}")
        lines.append(
            "  Verteilung: "
            + ", ".join(
                [
                    f"{yr}: {cnt}"
                    for yr, cnt in sorted(year_counts.items(), reverse=True)
                ]
            )
        )
        lines.append(
            "Top Status: "
            + ", ".join([f"{st}: {cnt}" for st, cnt in status_counts.most_common(5)])
        )

        # NEU: Netto-Erfolg 2026 für Baseline-Wissen der KI
        current_year = datetime.date.today().year
        net_now = calculate_net_sales(
            items, year_min=current_year, year_max=current_year
        )
        lines.append(f"\nVERKAUFSERFOLG {current_year} (NETTO):")
        lines.append(
            f"  Fahrzeuge: {net_now['netto_verkauft']} (Dedupliziert & Storno-geprüft)"
        )
        lines.append(f"  Umsatz: {net_now['netto_umsatz']:,.2f} €".replace(",", "."))
    except (TypeError, ValueError, KeyError):
        # Specific errors caught, but we continue with what we have
        pass

    try:
        raw_veh = _MEM_CACHE.get("sale/vehicles") or get_cached_or_fetch(
            "sale/vehicles", f"{SYSCARA_BASE}/sale/vehicles/"
        )
        if raw_veh:
            vs = build_vehicle_stats(raw_veh)
            lines.append("\nFAHRZEUGBESTAND:")
            lines.append(
                f"  Gesamt: {vs.get('unique_total', '?')} "
                f"(Verkaufsbereit: {vs.get('verkaufbar', '?')}, "
                f"Verkauft: {vs.get('verkauft', '?')})"
            )
            lines.append(
                f"  Durschn. VK: {vs.get('avg_preis', 0):,.0f} €".replace(",", ".")
            )

            raw_items = iter_items(raw_veh)
            eks = [
                float(v.get("prices", {}).get("purchase") or 0)
                for v in raw_items
                if float(v.get("prices", {}).get("purchase") or 0) > 0
            ]
            if eks:
                avg_ek = sum(eks) / len(eks)
                lines.append(f"  Durschn. EK: {avg_ek:,.0f} €".replace(",", "."))

            lines.append(
                "  Typen: "
                + ", ".join([f"{k}: {v}" for k, v in vs.get("nach_typ", {}).items()])
            )
            lines.append(
                "  Marken: "
                + ", ".join(
                    [
                        f"{m}: {c}"
                        for m, c in sorted(
                            vs.get("make_counts", {}).items(), key=lambda x: -x[1]
                        )[:8]
                    ]
                )
            )
            lines.append(
                "  PS: "
                + ", ".join(
                    [
                        f"{p}: {c}"
                        for p, c in sorted(
                            vs.get("ps_counts", {}).items(),
                            key=lambda x: int(x[0].split()[0]),
                        )[:5]
                    ]
                )
            )
            lines.append(
                "  Längen: "
                + ", ".join(
                    [
                        f"{k}: {v}"
                        for k, v in vs.get("laenge_buckets", {}).items()
                        if v > 0
                    ]
                )
            )
            lines.append(
                "  Getriebe: "
                + ", ".join([f"{k}: {v}" for k, v in vs.get("getriebe", {}).items()])
            )

            feats = []
            if vs.get("hubbett", {}).get("Ja", 0):
                feats.append(f"Hubbett: {vs['hubbett']['Ja']}")
            if vs.get("dusche", {}).get("Ja", 0):
                feats.append(f"Sep. Dusche: {vs['dusche']['Ja']}")
            if feats:
                lines.append("  Ausstattung: " + ", ".join(feats))
    except (TypeError, ValueError, KeyError):
        pass

    res = "\n".join(lines)
    _BI_CONTEXT_CACHE = {"ts": time.time(), "data": res}
    return res

    res = "\n".join(lines)
    _BI_CONTEXT_CACHE = {'ts': time.time(), 'data': res}
    return res

def _detect_customer_query(question: str):
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


def _execute_local_customer_query(params: dict) -> tuple:
    """Execute a customer query against the local order data."""
    try:
        items = _get_orders()
    except (TypeError, ValueError, KeyError):
        return "Fehler beim Laden der Aufträge.", None

    results = []
    q_t = params.get("type")
    val = params.get("value", "").lower().strip()
    for o in items:
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
        return "Keine Treffer.", None

    rows = []
    for r in results[:50]:
        cust = r.get("customer", {}) or {}
        stat = r.get("status", {}) or {}
        rows.append(
            [
                _extract_order_nr(r),
                f"{cust.get('first_name','')} {cust.get('last_name','')}",
                cust.get("city", ""),
                (stat.get("label") or "?"),
            ]
        )

    table = {"columns": ["Nr", "Name", "Stadt", "Status"], "rows": rows}
    return f"{len(results)} Kunden/Aufträge gefunden.", table


def _detect_order_lookup_query(question: str):
    """Detect if a query is looking for a specific order number."""
    pattern = r"(?:auftrags?|order)\s*#?\s*(\b[a-z0-9\-\/]{4,20}\b)"
    m = re.search(pattern, question.lower())
    if m:
        return True, {"type": "order_nr", "value": m.group(1).upper()}
    return False, {}


def _execute_local_order_lookup(params: dict):
    """Lookup a single order by number."""
    try:
        orders = _get_orders()
    except (TypeError, ValueError, KeyError):
        return "Fehler beim Laden der Aufträge.", None, None

    for o in orders:
        if _extract_order_nr(o).upper() == params["value"]:
            cust = o.get("customer", {}) or {}
            stat = o.get("status", {}) or {}
            fn = cust.get("first_name", "")
            ln = cust.get("last_name", "")
            ans = (
                f"Auftrag {params['value']}: {fn} {ln} aus {cust.get('city', '-')}. "
                f"Status: {stat.get('label', '?')}"
            )
            return ans, None, None
    return "Nicht gefunden.", None, None


def _detect_employee_query(question: str):
    """Detect if a query is looking for a specific employee ID."""
    m = re.search(r"(?:mitarbeiter|id)\s*#?\s*(\d{3,6})", question.lower())
    if m:
        return True, {"type": "employee_id", "value": m.group(1)}
    return False, {}


def _execute_local_employee_query(params: dict) -> tuple:
    """Lookup orders for a specific employee."""
    try:
        orders = _get_orders()
    except (TypeError, ValueError, KeyError):
        return "Fehler beim Laden der Aufträge.", None, None

    emp_id = params["value"]
    res = [
        o
        for o in orders
        if str(o.get("user", {}).get("order") or o.get("user", {}).get("update"))
        == emp_id
    ]
    if not res:
        return f"Keine Daten für ID {emp_id}.", None, None
    return f"Mitarbeiter (ID {emp_id}) hat {len(res)} Aufträge im System.", None, None
