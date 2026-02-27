# Syscara API Python Implementation

Diese Implementierung ersetzt den n8n-Workflow für die Syscara-Fahrzeugsuche durch ein robustes Python-Backend.

## Features
- **Direktzugriff**: Nutzt die Syscara API direkt ohne den Umweg über n8n.
- **Caching**: Speichert API-Antworten lokal in `syscara_cache.json` (für 1 Stunde), um Rate-Limits zu vermeiden und die Geschwindigkeit zu erhöhen.
- **Flask Backend**: Stellt einen Endpunkt `/fahrzeugsuche` bereit, der identisch zum n8n-Webhook funktioniert.
- **Local Frontend**: Inklusive `fahrzeugsuche_local.html` zum Testen der Ergebnisse.

## Installation & Start

1. **Abhängigkeiten installieren**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Server starten**:
   ```bash
   python main.py
   ```
   Der Server läuft standardmäßig auf `http://localhost:5000`.

3. **Frontend öffnen**:
   Einfach die Datei `fahrzeugsuche_local.html` im Browser öffnen.

## Dateien
- `main.py`: Das Flask-Backend mit Filterlogik und Caching.
- `requirements.txt`: Benötigte Python-Bibliotheken.
- `.env`: Enthält die Syscara API Credentials.
- `fahrzeugsuche_local.html`: Eine minimalistische Oberfläche zur Suche.
- `syscara_cache.json`: Wird automatisch erstellt und speichert die Rohdaten.
