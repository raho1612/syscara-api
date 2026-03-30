import json
import os
import sys

# Füge das Backend-Verzeichnis zum Python-Pfad hinzu, damit api.* importiert werden kann
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from api import ai_analyst


def test_query_sales_history_counts_unique_sold_vehicles_per_year(monkeypatch):
    vehicles = [
        {
            "id": 1,
            "status": "RE",
            "typeof": "Kastenwagen",
            "identifier": {"internal": "veh-1", "vin": "VIN-1"},
            "model": {"producer": "Sunlight", "model": "Cliff 600"},
            "dimensions": {"length": 599},
        },
        {
            "id": 2,
            "status": "RE",
            "typeof": "Kastenwagen",
            "identifier": {"internal": "veh-2", "vin": "VIN-2"},
            "model": {"producer": "Sunlight", "model": "Cliff 640"},
            "dimensions": {"length": 636},
        },
        {
            "id": 3,
            "status": "BE",
            "customer": {"id": 123},
            "typeof": "Teilintegriert",
            "identifier": {"internal": "veh-3", "vin": "VIN-3"},
            "model": {"producer": "Carado", "model": "T 447"},
            "dimensions": {"length": 741},
        },
    ]
    orders = [
        {"identifier": {"internal": "veh-1"}, "date": {"created": "2025-01-10T08:00:00"}},
        {"identifier": {"internal": "veh-1"}, "date": {"updated": "2025-02-14T08:00:00"}},
        {"identifier": {"vin": "VIN-2"}, "date": {"created": "2025-03-21T08:00:00"}},
        {"identifier": {"internal": "veh-3"}, "date": {"created": "2024-11-09T08:00:00"}},
        {"identifier": {"internal": "missing"}, "date": {"created": "2025-05-01T08:00:00"}},
    ]

    def fake_fetch(cache_key, _url):
        if cache_key == "sale/vehicles_full":
            return vehicles
        raise AssertionError(f"unexpected cache key: {cache_key}")

    monkeypatch.setattr(ai_analyst, "get_cached_or_fetch", fake_fetch)
    monkeypatch.setattr(ai_analyst, "_get_orders", lambda: orders)

    payload = json.loads(ai_analyst._query_sales_history({"jahrMin": 2025, "jahrMax": 2025, "art": "alle"}))

    assert payload["status"] == "Erfolg"
    assert payload["treffer_anzahl"] == 2
    assert payload["ungepairte_auftraege"] == 1
    assert [item["modell"] for item in payload["beispiele"]] == ["Cliff 640", "Cliff 600"]


def test_detect_simple_sales_count_query_extracts_year_and_type():
    payload = ai_analyst._detect_simple_sales_count_query("Wie viele Kastenwagen wurden 2025 verkauft?")

    assert payload == {"art": "kastenwagen", "jahrMin": 2025, "jahrMax": 2025}