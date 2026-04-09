import os
from pathlib import Path

from api.ai_analyst import register_ai_analyst_routes
from api.evaluation import register_evaluation_routes
from api.kosten import register_kosten_routes
from api.performance import register_performance_routes
from api.vehicles import register_vehicle_routes
from core.config import CURRENT_DIR, ROOT_DIR
from core.database import supabase
from flask import Flask, jsonify
from flask_cors import CORS
from services.sync_service import register_sync_routes, start_sync_thread


def _read_api_version() -> str:
    # 1. Docker/Prodn-Pfad
    try:
        v = Path("/app/api_version.txt").read_text().strip()
        if v:
            return v
    except Exception:
        pass
    
    # 2. Lokaler Pfad (Entwicklung)
    try:
        v = Path(CURRENT_DIR / "VERSION").read_text().strip()
        if v:
            return v
    except Exception:
        pass
        
    return "Modular-v2 vom 09.04.2026 (10:30 Uhr)"  # Sicherer Fallback


# Initialisierung
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "syscara-analyst-secret-key-2026")
CORS(app, origins=["*"])  # Vereinfacht für maximale Erreichbarkeit

# Routen registrieren
register_ai_analyst_routes(app)
register_performance_routes(app)
register_vehicle_routes(app)
register_evaluation_routes(app)
register_kosten_routes(app)
register_sync_routes(app)


@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "version": "2.0.0-modular"})


@app.route("/api/diag")
def api_diag():
    from api.performance import _load_employee_names

    emp_names = _load_employee_names()

    try:
        import openai

        has_openai = True
    except ImportError:
        has_openai = False

    try:
        import anthropic

        has_claude = True
    except ImportError:
        has_claude = False

    try:
        import google.generativeai

        has_gemini = True
    except ImportError:
        has_gemini = False

    return jsonify(
        {
            "success": True,
            "modular": True,
            "api_version": _read_api_version(),
            "employee_names_exists": len(emp_names) > 0,
            "employee_names_count": len(emp_names),
            "has_openai_lib": has_openai,
            "has_claude_lib": has_claude,
            "has_gemini_lib": has_gemini,
            "has_openai_key": bool(os.getenv("OPENAI_API_KEY")),
            "has_anthropic_key": bool(os.getenv("ANTHROPIC_API_KEY")),
            "supabase_connected": supabase is not None,
            "root_dir": str(ROOT_DIR),
            "current_dir": str(CURRENT_DIR),
            "routes": sorted([str(r.rule) for r in app.url_map.iter_rules()]),
        }
    )


@app.route("/")
def index():
    return jsonify(
        {
            "name": "Syscara Python API",
            "modular": True,
            "supabase_connected": supabase is not None,
        }
    )


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print(f"[BOOT] Syscara Modular API auf Port {port}...", flush=True)

    # Hintergrund-Sync nur im Hauptthread starten
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
        start_sync_thread(supabase)

    app.run(host="0.0.0.0", port=port, debug=False)
