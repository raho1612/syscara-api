# HANDOVER – Syscara Python API (Backend)

## Status
Zentrales Backend (Flask) für das Syscara Dashboard. Läuft auf Port 5000.

## Komponenten
- **main.py**: Enthält alle API-Routen (`/api/ads`, `/api/stats`, etc.) und die Syscara-Anbindung.
- **Caching**: Nutzt als Ausfallschutz die `api_cache` Tabelle in Supabase. Die Daten werden bei jedem Request parallel in Supabase gesichert und bei einem Ausfall von Syscara automatisch von dort geladen.

## Wichtige Fixes & Logik
- **Listen/Dict-Robustheit**: Syscara API-Antworten (wie `prices`, `features`, `model`, `engine`, `identifier` oder `dimensions`) kommen oft als ungefülltes Array `[]` statt als sauberes Dictionary `{}` an. **Pflichregel:** Immer erst mit `isinstance(obj, list)` abfangen und zu einem Dictionary normalisieren, bevor `.get()` aufgerufen wird, um HTTP 500 Fatal Errors zu vermeiden.
- **Syscara Timeout Limit**: Große Datenpakete wie `sale/orders` und `sale/vehicles` brauchen direkt aus der Syscara API extrem lange (>80 Sekunden). Standard-Requests müssen daher **zwingend** auf `timeout=180` statt `60` gesetzt werden, da sonst Caches bei einem frischen Deployment leer bleiben.
- **Supabase Fallback & Fail-Open**: Fällt die Syscara API aus (Timeout, etc.), übernimmt der gespeicherte Snapshot aus Supabase. 
  **Dringende Backend-Regel:** Wenn Filterlogiken (wie Datums-Mapping über `orders`) auf Supabase-Caches basieren, müssen diese Filter im Falle von **leeren Caches** (Fallback wirft `{}`) zwingend deaktiviert werden ("Fail-Open"), damit Frontends / Dashboards weiterhin Basisdaten anzeigen statt im Nichts ("0 Fahrzeuge") zu verenden!

## Starten
```powershell
python main.py
```

## Offene Punkte
- Klärung des `Not allowed` Fehlers bei `sale/lists/pictures`.
- Verfeinerung der Heizungs-/Getriebe-Erkennung (viele Werte aktuell "Unbekannt").
