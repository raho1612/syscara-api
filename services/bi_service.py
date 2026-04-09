import datetime
import re
import time
from collections import Counter

from core.config import SYSCARA_BASE
from core.database import _MEM_CACHE, get_cached_or_fetch, iter_items
from core.utils import _extract_order_nr, extract_order_datetime, fmt_preis
from shared.sales_engine import calculate_net_sales
from shared.vehicle_stats import build_vehicle_stats
from services.vehicle_service import map_and_filter


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


# Die lokalen Detections und Suchen wurden in services/ai_tool_service.py ausgelagert.
