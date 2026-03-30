# HANDOVER – Syscara Sales Analytics
**Zuletzt aktualisiert:** 29.03.2026 | **Commit:** `b0e45c8`

---

## Status (1–3 Sätze)
Die zentrale Netto-Verkaufs-Engine (`shared/sales_engine.py`) ist vollständig implementiert und auf `origin/main` gepusht. Alle Tabs (KI-Analyst, Mitarbeiter-Performance, BI-Kontext) nutzen jetzt einheitlich die deduplizierte 412er-Basis statt 420 Brutto-Aufträge. **Coolify-Redeploy muss noch manuell angestoßen werden.**

---

## Letzte Änderungen (diese Session, Commit `b0e45c8`)
- **`shared/sales_engine.py`**: `_get_order_price()` extrahiert Umsatz aus `prices.offer` / `prices.basic`. Ergebnis enthält jetzt `netto_umsatz` als Float.
- **`api/performance.py`**: Revenue-Felder pro Mitarbeiter (monatlich, quartalsweise, kumuliert). Unused Imports `CANCEL_STATUSES` + `POSITIVE_STATUSES` entfernt. Meta-Daten enthalten `netto_total_count` + `netto_total_revenue`.
- **`api/ai_analyst.py`**: Schnell-Antwort-Pfad zeigt Umsatz neben Fahrzeuganzahl. Tool-Response liefert `netto_umsatz`.
- **`services/bi_service.py`**: KI-Baseline (`_build_bi_context`) enthält jetzt Netto-Fahrzeuge + Umsatz für laufendes Jahr.
- **`docs/WALKTHROUGH.md`**: Neu erstellt – beschreibt die Engine-Logik.

### Vorherige Session (Commit `90ab281`)
- `shared/sales_engine.py` NEU: Deduplizierung, VIN-Grouping, Storno-Gegenrechnung, Tausch-Erkennung.
- `api/ai_analyst.py`: Schnell-Antwort auf `netto_verkauft`/`datum_ab` korrigiert. Anthropic-Loop-Bug (doppelter API-Call) behoben.
- `api/performance.py`: Mitarbeiter-Tab auf Netto-Zählung umgestellt (war vorher Brutto-Aufträge).

---

## Offene Punkte
- **Coolify-Redeploy:** Muss manuell angestoßen werden → https://coolify.kimation.sellfriends24.de
- **Umsatz-Validierung nach Deploy:** KI fragen: *"Wie hoch war der Netto-Umsatz 2026?"* → Zahlen müssen konsistent mit 412 Fahrzeugen sein.
- **Cache-Problem:** `sale/orders` lädt nur ab `2024-01-01`. 2026er Live-Daten kommen direkt von der Syscara-API (kein Cache). Ein Cronjob zur automatischen Cache-Aktualisierung fehlt.
- **Fahrzeugtyp-Filter in Engine:** Aktuell String-Matching auf `fahrzeug_key`. Besser wäre ein direktes Lookup im Fahrzeugstamm (`sale/vehicles`).
- **Lint-Warnungen:** `ai_analyst.py` und `bi_service.py` haben zahlreiche Zeilenlängen- und Komplexitätswarnungen (Ruff). Keine Funktionsfehler, aber technische Schulden.

---

## Nächste Schritte (Priorität)
| Prio | Aufgabe |
|------|---------|
| **A** | Coolify-Redeploy anstoßen, Umsatzanzeige im Performance-Tab live validieren |
| **B** | Testen: KI-Analyst Fragen zu Umsatz 2026, Mitarbeiter-Performance-Tab öffnen |
| **C** | Cache-Aktualisierung für 2026er Daten automatisieren |
| **C** | Lint-Bereinigung in `ai_analyst.py` (Komplexität aufteilen) |

---

## Risiken / Abhängigkeiten
- **Preis-Feld `prices.offer` kann `null` sein** – `_get_order_price()` fällt auf `basic` / `brutto` zurück, dann auf `0`. Fahrzeuge ohne Preisangabe haben `revenue: 0.0` → Umsatzsumme ist Untergrenze.
- **Zubehör-AB-Preis-Logik:** Bei mehreren ABs für dasselbe Fahrzeug wird der Preis der *letzten* AB genommen. Das ist technisch korrekt (neuester Preis ≈ Gesamtpreis inkl. Zubehör), aber muss nach Deploy validiert werden.
- **Syscara API Timeout:** Bei `sale/orders` > 80s Ladezeit fällt der Stack auf den Supabase-Cache zurück. Cache hat nur Daten bis 2024 → 2026er Daten könnten bei Timeout fehlen. **Fail-Open ist implementiert** (kein Crash, aber ggf. veraltete Zahlen).

---

## Relevante Dateien & Struktur
```
syscara-api-python/
├── shared/
│   ├── sales_engine.py       # ← KERNLOGIK: Alle Netto-Berechnungen hier
│   └── vehicle_stats.py      # KPI-Stats für /api/stats (unabhängig)
├── api/
│   ├── ai_analyst.py         # KI-Analyst: Fragen beantworten
│   ├── performance.py        # Mitarbeiter-Performance-Tab
│   ├── kosten.py             # Deckungsbeitrag-Kalkulator
│   └── vehicles.py           # Fahrzeugliste & Aufträge (roh)
├── services/
│   └── bi_service.py         # KI-Baseline-Kontext (Omniscient Hub)
└── docs/
    ├── HANDOVER.md           # ← Diese Datei
    └── WALKTHROUGH.md        # Technische Engine-Beschreibung
```

## Relevante Kommandos
```powershell
# API lokal starten:
python main.py

# Performance-Tab testen:
curl http://localhost:5000/api/performance?year=2026

# KI-Analyst testen:
curl -X POST http://localhost:5000/api/ask -H "Content-Type: application/json" -d '{"question": "Wie viele Fahrzeuge wurden 2026 verkauft?"}'

# Coolify produtiv:
https://coolify.kimation.sellfriends24.de
```

## Wichtige technische Entscheidungen (ADR)
1. **Single Source of Truth:** `sales_engine.py` ist die EINZIGE Stelle für Verkaufslogik. Andere Module importieren und rufen auf – sie berechnen NICHT selbst.
2. **Fahrzeug-Identität:** Priorisierung VIN → interne ID → UID → Auftrags-ID. Fahrzeuge ohne stabile ID werden mit `NO_VIN_<order_id>` geführt.
3. **Zubehör-Deduplizierung:** Gleiche VIN + gleicher Kunde = 1 Netto-Verkauf, unabhängig von der Anzahl der ABs.
4. **Tausch-Szenario:** Storno A + neue AB B (gleicher Kunde, anderes Fahrzeug) = 1 Netto-Verkauf.
5. **Revenue:** Preis der *letzten* gültigen AB pro Fahrzeug-Timeline. Fallback: `prices.offer` → `prices.basic` → `0`.
