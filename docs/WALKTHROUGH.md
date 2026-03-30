# Syscara Sales Engine Walkthrough

## Ziel / Scope
Zentralisierung der Netto-Verkaufslogik für alle Syscara-Module (Dashboard-KPIs, KI-Analyst, Mitarbeiter-Performance). Das Ziel ist eine deduplizierte "Netto-Sicht" auf den Verkaufserfolg, die Stornierungen und Tausch-Szenarien berücksichtigt.

## Setup & Run
Die Engine befindet sich in `shared/sales_engine.py`. Sie benötigt Zugriff auf die Auftragsdaten (`sale/orders`).
- **Input:** Rohliste der Syscara-Aufträge (JSON/List).
- **Zentrale Funktion:** `calculate_net_sales(orders, year_min, year_max)`.

## Ablauf
1. **Identifikation:** Jedes Fahrzeug wird über eine UID (VIN, interne ID oder Syscara-ID) identifiziert.
2. **Timeline-Analyse:** Für jede UID wird eine chronologische Kette aller Ereignisse (Auftragsbestätigungen, Stornos) gebildet.
3. **Deduplizierung:** Mehrere ABs für dasselbe Fahrzeug (z.B. Zubehör-Ergänzungen) werden als *ein* Verkaufserfolg gezählt.
4. **Storno-Handling:** Ein Storno hebt eine vorherige AB für dieses Fahrzeug auf.
5. **Tausch-Erkennung:** Wenn auf einen Storno eine neue AB für denselben Kunden folgt, wird dies als *ein* Netto-Handelsvorgang gewertet.
6. **Umsatz-Extraktion:** Der finale Umsatz wird aus dem Feld `prices.offer` (bzw. `basic`) der *letzten* gültigen AB für dieses Fahrzeug bezogen.

## Inputs / Outputs (Beispiele)
- **Input:** Liste von Aufträgen mit Status `ORDER` (positiv) oder `CANCELLATION` (negativ).
- **Output:**
  ```json
  {
    "netto_verkauft": 412,
    "netto_umsatz": 12500000.50,
    "brutto_ab_count": 420,
    "storni_count": 8,
    "fahrzeuge": [ ... ]
  }
  ```

## Validierungscheck
Ein Verkaufserfolg von 2026 lässt sich validieren, indem man prüft:
- Haben wir genau 412 verschiedene Fahrzeug-IDs in den Netto-Verkäufen?
- Sind Zubehör-ABs (Status ORDER) ohne VIN-Änderung dedupliziert?
- Sind Stornos abgezogen?

## Troubleshooting
1. **Falsche Zahlen:** Prüfe `shared/sales_engine.py` -> `POSITIVE_STATUSES` (muss AB/ORDER enthalten).
2. **Umsatz fehlt:** Prüfe `_get_order_price` auf die Feldnamen (Syscara API nutzt oft `offer`).
3. **Mitarbeiterzuordnung:** Falls ein Mitarbeiter fehlt, prüfe das Feld `user.order` im Auftrag.
