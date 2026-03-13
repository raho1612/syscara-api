import os
from flask import Flask, jsonify, request
from flask_cors import CORS
from core.config import ROOT_DIR, CURRENT_DIR, WORKSPACE_ROOT
from core.database import supabase
from api.ai_analyst import register_ai_analyst_routes
from api.performance import register_performance_routes
from api.vehicles import register_vehicle_routes
from services.sync_service import start_sync_thread, register_sync_routes

# Initialisierung
app = Flask(__name__)
CORS(app, origins=["*"]) # Vereinfacht für maximale Erreichbarkeit

# Routen registrieren
register_ai_analyst_routes(app)
register_performance_routes(app)
register_vehicle_routes(app)
register_sync_routes(app)

@app.route('/api/health')
def health():
    return jsonify({"status": "ok", "version": "2.0.0-modular"})

@app.route('/')
def index():
    return jsonify({
        "name": "Syscara Python API",
        "modular": True,
        "supabase_connected": supabase is not None
    })

if __name__ == '__main__':
    port = int(os.getenv("PORT", 5000))
    print(f"[BOOT] Syscara Modular API auf Port {port}...", flush=True)
    
    # Hintergrund-Sync nur im Hauptthread starten
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
        start_sync_thread(supabase)
    
    app.run(host='0.0.0.0', port=port, debug=False)
