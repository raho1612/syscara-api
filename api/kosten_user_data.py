"""
Persistenz-Endpunkte für nutzerspezifische Kosten-Kalkulations-Daten.

Speichert Kundendaten, Kosten-Overrides und Werkstattposten per Fahrzeug-ID
in Supabase. Schreibt NICHT in das externe Syscara-System.
"""

from datetime import datetime, timezone

from core.database import supabase
from flask import jsonify, request


def register_kosten_user_data_routes(app):

    @app.route("/api/kosten_user_data/<vehicle_id>", methods=["GET"])
    def get_kosten_user_data(vehicle_id):
        """Gespeicherte Kalkulationsdaten für ein Fahrzeug laden."""
        if not supabase:
            return jsonify({"data": None}), 200
        try:
            res = (
                supabase.table("kosten_user_data")
                .select("data")
                .eq("vehicle_id", vehicle_id)
                .maybe_single()
                .execute()
            )
            if res.data:
                return jsonify({"data": res.data["data"]}), 200
            return jsonify({"data": None}), 200
        except Exception as e:
            print(f"[WARN] kosten_user_data GET Fehler: {e}", flush=True)
            return jsonify({"data": None}), 200

    @app.route("/api/kosten_user_data/<vehicle_id>", methods=["POST"])
    def save_kosten_user_data(vehicle_id):
        """Kalkulationsdaten für ein Fahrzeug speichern (upsert)."""
        if not supabase:
            return jsonify({"ok": False, "error": "Supabase nicht konfiguriert"}), 503  # ✅ FIX: HTTP 503 statt 200
        try:
            body = request.get_json(force=True) or {}
            supabase.table("kosten_user_data").upsert(
                {
                    "vehicle_id": vehicle_id,
                    "data": body,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
                on_conflict="vehicle_id",
            ).execute()
            return jsonify({"ok": True}), 200
        except Exception as e:
            print(f"[ERROR] kosten_user_data POST Fehler: {e}", flush=True)
            return jsonify({"ok": False, "error": str(e)}), 500
