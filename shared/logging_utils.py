import json
import os
import datetime as _dt

# Datei für die persistente "Blackbox"-Echtzeit-Log-Kopie
LOG_FILE = "docs/REALTIME_DEBUG.log"

def log_blackbox(category: str, data: any):
    """
    Speichert eine Kopie der aktuellen System-Vorgänge in einer persistenten Log-Datei.
    So hat die KI und der User immer Zugriff auf den "Maschinenraum".
    """
    timestamp = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Sicherstellen, dass der Ordner existiert
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    
    log_entry = {
        "ts": timestamp,
        "cat": category,
        "payload": data
    }
    
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
            
        # Optional: Nur die letzten 1000 Zeilen behalten für Performance
        pass 
    except Exception as e:
        print(f"[LOG ERROR] Konnte Blackbox-Log nicht schreiben: {e}")
