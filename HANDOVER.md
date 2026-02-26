# HANDOVER – Syscara Python API (Backend)

## Status
Zentrales Backend (Flask) für das Syscara Dashboard. Läuft auf Port 5000.

## Komponenten
- **main.py**: Enthält alle API-Routen (`/api/ads`, `/api/stats`, etc.) und die Syscara-Anbindung.
- **Caching**: Nutzt als Ausfallschutz die `api_cache` Tabelle in Supabase. Die Daten werden bei jedem Request parallel in Supabase gesichert und bei einem Ausfall von Syscara automatisch von dort geladen.

## Wichtige Fixes & Logik
- **Listen/Dict-Robustheit**: Sicherstellung, dass API-Antworten (wie `features` oder `engine`) korrekt verarbeitet werden, auch wenn sie als Liste statt Dictionary kommen.
- **Stats-Erweiterung**: Berechnung von Längen-Kategorien, Heizungsarten, Getriebetypen und Ausstattungsmerkmalen (Hubbett, Dinette, Dusche).
- **Fehler-Handling**: `request.get_json(silent=True)` und detaillierte Fehler-Logs.
- **Supabase Fallback**: Fällt die Syscara API aus (Timeout, Verweigerung), übernimmt automatisch der zuletzt gespeicherte Snapshot aus Supabase.

## Starten
```powershell
python main.py
```

## Offene Punkte
- Klärung des `Not allowed` Fehlers bei `sale/lists/pictures`.
- Verfeinerung der Heizungs-/Getriebe-Erkennung (viele Werte aktuell "Unbekannt").
