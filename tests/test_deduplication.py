import importlib.util
import json
import os
import sys
from pathlib import Path

# Füge das Backend-Verzeichnis zum Python-Pfad hinzu, damit main.py importiert werden kann
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from shared.vehicle_stats import (
    build_vehicle_stats,
    classify_sale_kpi_bucket,
    dedupe_vehicles,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


def _load_module(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Konnte Modul nicht laden: {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _stats_fixture():
    # 3 sold (RE, ST, BE+Kunde), 4 verkaufbar (BE-ohne-Kunde, AB, BS, AB-dup deduped)
    return [
        {"id": 1, "status": "RE", "identifier": {"internal": "sold-re"}, "customer": {"id": 0}},
        {"id": 2, "status": "ST", "identifier": {"internal": "sold-st"}, "customer": {"id": 0}},
        {"id": 3, "status": "BE", "identifier": {"internal": "sold-be-with-customer"}, "customer": {"id": 9999}},
        {"id": 4, "status": "BE", "identifier": {"internal": "market-be-no-customer"}, "customer": {"id": 0}},
        {"id": 5, "status": "AB", "identifier": {"internal": "market-ab"}, "customer": {"id": 0}},
        {"id": 6, "status": "BS", "identifier": {"internal": "market-bs"}, "customer": {"id": 0}},
        {"id": 7, "status": "AB", "identifier": {"internal": "market-ab-dup"}, "customer": {"id": 0}},
        {"id": 8, "status": "AB", "identifier": {"internal": "market-ab-dup"}, "customer": {"id": 0}},
    ]


def _assert_stats_route(module, route_name: str):
    original_fetch = module.fetch_live_then_cache
    try:
        module.fetch_live_then_cache = lambda *args, **kwargs: _stats_fixture()
        client = module.app.test_client()
        response = client.get('/api/stats')
    finally:
        module.fetch_live_then_cache = original_fetch

    payload = response.get_json()
    assert response.status_code == 200, f"{route_name}: unerwarteter HTTP-Status {response.status_code}"
    assert response.headers.get('Cache-Control') == 'no-store, no-cache, must-revalidate, max-age=0'
    assert response.headers.get('Pragma') == 'no-cache'
    assert response.headers.get('Expires') == '0'
    assert payload["success"] is True
    assert payload["stats"]["verkaufbar"] == 4
    assert payload["stats"]["verfügbar"] == 4
    assert payload["stats"]["verkauft"] == 3

def test_deduplication_logic():
    with open(os.path.join(os.path.dirname(__file__), 'mock_vehicles.json'), 'r') as f:
        mock_data = json.load(f)

    unique_count = len(dedupe_vehicles(mock_data))
    expected = 2
    assert unique_count == expected, f"Fehler: Erwartet {expected}, aber bekam {unique_count}"

    stats = build_vehicle_stats(mock_data)
    assert stats["unique_total"] == expected, f"Fehler: Erwartet {expected} deduplizierte Fahrzeuge, aber bekam {stats['unique_total']}"
    print(f"Test erfolgreich: Logik arbeitet korrekt. Found {unique_count} unique vehicles.")


def test_sale_kpi_bucket_classification():
    # RE und ST sind immer verkauft (Status)
    assert classify_sale_kpi_bucket({"id": 1, "status": "RE"}) == "sold"
    assert classify_sale_kpi_bucket({"id": 2, "status": "ST"}) == "sold"
    # BE mit Kunde = verkauft; BE ohne Kunde = verfügbar
    assert classify_sale_kpi_bucket({"id": 3, "status": "BE", "customer": {"id": 9999}}) == "sold"
    assert classify_sale_kpi_bucket({"id": 4, "status": "BE", "customer": {"id": 0}}) == "marketable"
    assert classify_sale_kpi_bucket({"id": 5, "status": "BE"}) == "marketable"  # kein customer-Feld
    # AB/BS mit Kunde = verkauft; AB/BS ohne Kunde = verfügbar
    assert classify_sale_kpi_bucket({"id": 6, "status": "AB", "customer": {"id": 9999}}) == "sold"
    assert classify_sale_kpi_bucket({"id": 7, "status": "AB", "customer": {"id": 0}}) == "marketable"
    assert classify_sale_kpi_bucket({"id": 8, "status": "BS"}) == "marketable"  # kein customer-Feld


def test_vehicle_stats_sale_kpi_regression_fixture():
    fixture = _stats_fixture()

    stats = build_vehicle_stats(fixture)

    assert stats["raw_total"] == 8
    assert stats["unique_total"] == 7
    assert stats["gesamt"] == 7
    assert stats["verkaufbar"] == 4
    assert stats["verfügbar"] == 4
    assert stats["verkauft"] == 3


def test_root_stats_route_headers_and_counts():
    module = _load_module("root_syscara_api_main_test", REPO_ROOT / "syscara-api-python" / "main.py")
    _assert_stats_route(module, "root api")


def test_dashboard_api_stats_route_headers_and_counts():
    module = _load_module("dashboard_api_index_test", REPO_ROOT / "syscara-dashboard" / "api" / "index.py")
    _assert_stats_route(module, "dashboard api")


def test_dashboard_local_backend_stats_route_headers_and_counts():
    module = _load_module("dashboard_local_backend_test", REPO_ROOT / "syscara-dashboard" / "syscara-api-python" / "main.py")
    _assert_stats_route(module, "dashboard local backend")

if __name__ == "__main__":
    test_deduplication_logic()
    test_sale_kpi_bucket_classification()
    test_vehicle_stats_sale_kpi_regression_fixture()
    test_root_stats_route_headers_and_counts()
    test_dashboard_api_stats_route_headers_and_counts()
    test_dashboard_local_backend_stats_route_headers_and_counts()
